package synapse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

type terminatorCall struct {
	opts *storage.TerminateServiceOptions
}

// stubStorageServiceTerminator answers submission i from results[i] and
// errors[i]: a wait-stage TerminateServiceError and every result come from the
// wait, and any other error fails the submission. waits, when set, answers
// successive wait calls instead, repeating its last entry.
type stubStorageServiceTerminator struct {
	calls         []terminatorCall
	results       []*storage.TerminateServiceResult
	errors        []error
	submittedHash common.Hash
	waits         []func(context.Context, storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error)
	waitCalls     []storage.TerminateServiceWaitOptions
}

func (s *stubStorageServiceTerminator) SubmitTerminateService(
	_ context.Context,
	_ sdktypes.BigInt,
	opts *storage.TerminateServiceOptions,
) (*storage.TerminateServiceSubmission, error) {
	s.calls = append(s.calls, terminatorCall{opts: opts})
	sub := &storage.TerminateServiceSubmission{Direct: opts.SkipProvider, Accepted: true}
	if s.submittedHash != (common.Hash{}) {
		sub.TransactionID = s.submittedHash.Hex()
		if opts.OnSubmitted != nil {
			opts.OnSubmitted(*sub)
		}
	}
	if err := s.callError(); err != nil && !waitStageFailure(err) {
		return nil, err
	}
	return sub, nil
}

func (s *stubStorageServiceTerminator) WaitForTerminateService(
	ctx context.Context,
	sub storage.TerminateServiceSubmission,
	opts *storage.TerminateServiceWaitOptions,
) (*storage.TerminateServiceResult, error) {
	s.waitCalls = append(s.waitCalls, *opts)
	if len(s.waits) > 0 {
		return s.waits[min(len(s.waitCalls), len(s.waits))-1](ctx, sub)
	}
	var result *storage.TerminateServiceResult
	if index := len(s.calls) - 1; index < len(s.results) {
		result = s.results[index]
	}
	return result, s.callError()
}

func (s *stubStorageServiceTerminator) callError() error {
	if index := len(s.calls) - 1; index < len(s.errors) {
		return s.errors[index]
	}
	return nil
}

func waitStageFailure(err error) bool {
	failure, ok := errors.AsType[*storage.TerminateServiceError](err)
	return ok && failure.Stage == storage.TerminateServiceStageWait
}

// terminationFailure builds the error shape the SDK returns from TerminateService.
func terminationFailure(stage storage.TerminateServiceStage, hash common.Hash, cause error) error {
	failure := &storage.TerminateServiceError{Stage: stage, Cause: cause}
	if hash != (common.Hash{}) {
		failure.Submission = &storage.TerminateServiceSubmission{Accepted: true, TransactionID: hash.Hex()}
	}
	return fmt.Errorf("storage.Service.TerminateService: %w", failure)
}

// stubDataSetStateReader reports successive PDP end epochs, repeating the last.
// Zero means the service is still running.
type stubDataSetStateReader struct {
	endEpochs []int64
	payer     common.Address
	err       error
	// errFrom is the first read, counting from zero, that returns err.
	errFrom int
	reads   int
}

func (s *stubDataSetStateReader) GetDataSet(context.Context, sdktypes.BigInt) (*warmstorage.DataSetInfo, error) {
	s.reads++
	if s.err != nil && s.reads > s.errFrom {
		return nil, s.err
	}
	var epoch int64
	if len(s.endEpochs) > 0 {
		epoch = s.endEpochs[min(s.reads, len(s.endEpochs))-1]
	}
	return &warmstorage.DataSetInfo{PDPEndEpoch: sdktypes.Epoch(epoch), Payer: s.payer}, nil
}

func runningService() *stubDataSetStateReader { return &stubDataSetStateReader{} }

// relaySubmitFailure builds the SDK's failed relay submission, whose handle
// can resume the request.
func relaySubmitFailure(cause error) error {
	return fmt.Errorf("storage.Service.SubmitTerminateService: %w", &storage.TerminateServiceError{
		Stage: storage.TerminateServiceStageSubmit, Submission: &storage.TerminateServiceSubmission{ProviderID: sdktypes.NewBigInt(7)}, Cause: cause,
	})
}

// The payer wallet follows a failed relay only when the relay cannot also land:
// its request never reached the provider, or the provider holds none. Any other
// relay outcome is left to observation.
func TestStorageServiceAdapterTerminationFallsBackOnlyWhenTheRelayHoldsNoRequest(t *testing.T) {
	type wait = func(context.Context, storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error)
	absent := terminationFailure(storage.TerminateServiceStageWait, common.Hash{}, &pdp.WaitForTerminateServiceNotFoundError{})
	unsettled := relaySubmitFailure(errors.New("provider returned 502"))
	ended := func(context.Context, storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error) {
		hash := common.HexToHash("0x456")
		return &storage.TerminateServiceResult{TxHash: &hash, ConfirmedTxHash: &hash, EndEpoch: 84}, nil
	}
	for _, tc := range []struct {
		name       string
		errs       []error
		waits      []wait
		wantDirect bool
	}{
		{name: "relay never connected", errs: []error{relaySubmitFailure(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")})}, waits: []wait{ended}, wantDirect: true},
		{name: "provider holds no request", errs: []error{absent}, waits: []wait{func(context.Context, storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error) {
			return nil, absent
		}, ended}, wantDirect: true},
		{name: "resumed request is absent", errs: []error{unsettled}, waits: []wait{func(_ context.Context, sub storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error) {
			if sub.Direct || !sub.ProviderID.Equal(sdktypes.NewBigInt(7)) {
				return nil, fmt.Errorf("resumed %#v instead of the relayed handle", sub)
			}
			return nil, absent
		}, ended}, wantDirect: true},
		{name: "resumed request is unresolved", errs: []error{unsettled}, waits: []wait{func(context.Context, storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error) {
			return nil, terminationFailure(storage.TerminateServiceStageWait, common.Hash{}, errors.New("provider status unavailable"))
		}}},
		{name: "accepted without a hash in time", errs: []error{terminationFailure(storage.TerminateServiceStageWait, common.Hash{}, context.DeadlineExceeded)}},
		{name: "provider sub-timeout", errs: []error{context.DeadlineExceeded}},
		{name: "unknown relay failure", errs: []error{errors.New("provider relay failed")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubStorageServiceTerminator{errors: tc.errs, waits: tc.waits}
			state := runningService()
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: state}
			got, err := adapter.TerminateService(t.Context(), sdktypes.NewBigInt(42), TerminationHooks{})
			if stub.calls[0].opts.SkipProvider || stub.calls[0].opts.ProviderWaitTimeout != providerTerminationWaitTimeout {
				t.Fatalf("first request = %#v, want a relay bounded by %s", stub.calls[0].opts, providerTerminationWaitTimeout)
			}
			if !tc.wantDirect {
				if len(stub.calls) != 1 || !errors.Is(err, ErrTerminationTransactionNotSent) || errors.Is(err, ErrTerminationNotSent) {
					t.Fatalf("requests=%d err=%v, want the unresolved relay left to observation", len(stub.calls), err)
				}
				return
			}
			if err != nil || len(stub.calls) != 2 || !stub.calls[1].opts.SkipProvider || got == nil || got.EndEpoch == nil || *got.EndEpoch != 84 {
				t.Fatalf("requests=%d result=%#v err=%v, want a direct termination after the relay", len(stub.calls), got, err)
			}
			if state.reads != 2 {
				t.Fatalf("service state reads = %d, want only the initial and fallback prechecks", state.reads)
			}
		})
	}
}

func TestStorageServiceAdapterTerminationDoesNotDuplicatePendingRelay(t *testing.T) {
	stub := &stubStorageServiceTerminator{errors: []error{&pdp.TerminateServicePendingError{Message: "queued"}}}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), TerminationHooks{})
	if !IsProviderUnavailable(err) {
		t.Fatalf("error = %T %v, want dependency wait", err, err)
	}
	if len(stub.calls) != 1 {
		t.Fatalf("calls = %d, want no direct fallback for pending relay", len(stub.calls))
	}
}

func TestStorageServiceAdapterTerminationHonorsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stub := &stubStorageServiceTerminator{errors: []error{context.Canceled}}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(ctx, sdktypes.NewBigInt(42), TerminationHooks{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if len(stub.calls) != 1 {
		t.Fatalf("calls = %d, want no fallback after caller cancellation", len(stub.calls))
	}
}

func TestStorageServiceAdapterTerminationPreservesDirectFailure(t *testing.T) {
	directErr := errors.New("rpc unavailable")
	stub := &stubStorageServiceTerminator{
		errors: []error{relaySubmitFailure(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("provider unavailable")}), directErr},
	}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), TerminationHooks{})
	if !errors.Is(err, directErr) || !strings.Contains(err.Error(), "provider unavailable") {
		t.Fatalf("error = %v, want both provider and direct evidence", err)
	}
}

// Payment debt needs the operator. Falling back to the direct path would hide
// it behind whatever that path reports.
func TestStorageServiceAdapterTerminationReportsPaymentDebtWithoutFallback(t *testing.T) {
	stub := &stubStorageServiceTerminator{errors: []error{
		terminationFailure(storage.TerminateServiceStagePrepare, common.Hash{}, &storage.TerminateServiceDebtError{Shortfall: big.NewInt(1)}),
	}}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), TerminationHooks{})
	if !IsTerminationBlocked(err) {
		t.Fatalf("error = %T %v, want a blocked termination", err, err)
	}
	if len(stub.calls) != 1 {
		t.Fatalf("termination requests = %d, want no direct fallback", len(stub.calls))
	}
}

// A failed termination states what it proves was not sent: nothing at all, or
// no payer transaction. The payer wallet is announced before it may broadcast.
func TestStorageServiceAdapterTerminationReportsWhatWasNotSent(t *testing.T) {
	ours := common.HexToAddress("0x00000000000000000000000000000000000000a1")
	prepare := func(cause error) error {
		return terminationFailure(storage.TerminateServiceStagePrepare, common.Hash{}, cause)
	}
	submit := func(cause error) error {
		return terminationFailure(storage.TerminateServiceStageSubmit, common.Hash{}, cause)
	}
	absent := terminationFailure(storage.TerminateServiceStageWait, common.Hash{}, &pdp.WaitForTerminateServiceNotFoundError{})
	unreachable := relaySubmitFailure(&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")})
	rpcErr, hookErr := errors.New("rpc unavailable"), errors.New("checkpoint unavailable")
	for _, tc := range []struct {
		name          string
		chain         *stubDataSetStateReader
		errs          []error
		direct        bool
		cancel        bool
		hookErr       error
		wantCalls     int
		wantHook      bool
		wantNotSent   bool
		wantTxNotSent bool
		wantIs        error
	}{
		{name: "chain unreadable before sending", chain: &stubDataSetStateReader{err: rpcErr}, wantNotSent: true, wantIs: rpcErr},
		{name: "paid by another wallet", chain: &stubDataSetStateReader{payer: common.HexToAddress("0xc3")}, wantNotSent: true, wantIs: ErrServicePaidByAnother},
		{name: "payment debt", errs: []error{prepare(&storage.TerminateServiceDebtError{Shortfall: big.NewInt(1)})}, wantCalls: 1, wantNotSent: true},
		{name: "cancelled before relay", cancel: true, errs: []error{prepare(context.Canceled)}, wantCalls: 1, wantNotSent: true, wantIs: context.Canceled},
		{name: "relay unknown and chain unreadable", chain: &stubDataSetStateReader{payer: ours, err: rpcErr, errFrom: 1}, errs: []error{submit(errors.New("relay reset"))}, wantCalls: 1, wantTxNotSent: true, wantIs: rpcErr},
		{name: "relay and direct prepared", errs: []error{prepare(errors.New("no provider")), prepare(errors.New("gas estimate failed"))}, wantCalls: 2, wantHook: true, wantNotSent: true},
		{name: "relay unknown", errs: []error{submit(errors.New("relay reset"))}, wantCalls: 1, wantTxNotSent: true},
		{name: "relay unreachable and direct prepared", errs: []error{unreachable, prepare(errors.New("gas estimate failed"))}, wantCalls: 2, wantHook: true, wantNotSent: true},
		{name: "relay absent and direct prepared", errs: []error{absent, prepare(errors.New("gas estimate failed"))}, wantCalls: 2, wantHook: true, wantTxNotSent: true},
		{name: "direct broadcast unknown", errs: []error{absent, submit(errors.New("broadcast reset"))}, wantCalls: 2, wantHook: true},
		{name: "direct hook refuses", errs: []error{absent}, hookErr: hookErr, wantCalls: 1, wantHook: true, wantTxNotSent: true, wantIs: hookErr},
		{name: "direct only prepared", direct: true, errs: []error{prepare(errors.New("gas estimate failed"))}, wantCalls: 1, wantHook: true, wantNotSent: true},
		{name: "direct only unknown", direct: true, errs: []error{submit(errors.New("broadcast reset"))}, wantCalls: 1, wantHook: true},
		{name: "direct only hook refuses", direct: true, hookErr: hookErr, wantHook: true, wantNotSent: true, wantIs: hookErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chain := tc.chain
			if chain == nil {
				chain = &stubDataSetStateReader{payer: ours}
			}
			stub := &stubStorageServiceTerminator{errors: tc.errs}
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: chain, identity: storage.ContextIdentity{Payer: ours}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			hooks, sendsBeforeHook := 0, -1
			terminate := adapter.TerminateService
			if tc.direct {
				terminate = adapter.TerminateServiceDirect
			}
			_, err := terminate(ctx, sdktypes.NewBigInt(42), TerminationHooks{BeforeDirect: func() error {
				hooks++
				sendsBeforeHook = len(stub.calls)
				return tc.hookErr
			}})
			if err == nil || len(stub.calls) != tc.wantCalls || errors.Is(err, ErrTerminationNotSent) != tc.wantNotSent ||
				errors.Is(err, ErrTerminationTransactionNotSent) != tc.wantTxNotSent || (tc.wantIs != nil && !errors.Is(err, tc.wantIs)) {
				t.Fatalf("err=%v calls=%d; want calls=%d notSent=%t txNotSent=%t", err, len(stub.calls), tc.wantCalls, tc.wantNotSent, tc.wantTxNotSent)
			}
			if tc.wantNotSent && !strings.Contains(err.Error(), "this termination request was not sent") {
				t.Fatalf("diagnostic does not identify the unsent request: %v", err)
			}
			if tc.wantTxNotSent && (!strings.Contains(err.Error(), "the payer wallet sent no termination transaction") ||
				!strings.Contains(err.Error(), "the provider may still terminate the data set")) {
				t.Fatalf("diagnostic does not distinguish the wallet from the unresolved provider request: %v", err)
			}
			wantSendsBeforeHook := 1
			if tc.direct {
				wantSendsBeforeHook = 0
			}
			if (hooks == 1) != tc.wantHook || hooks > 1 || (tc.wantHook && sendsBeforeHook != wantSendsBeforeHook) {
				t.Fatalf("BeforeDirect calls=%d after %d requests, want called=%t after %d", hooks, sendsBeforeHook, tc.wantHook, wantSendsBeforeHook)
			}
			for i, call := range stub.calls {
				if call.opts.SkipProvider != (tc.direct || i > 0) {
					t.Fatalf("request %d SkipProvider = %t", i, call.opts.SkipProvider)
				}
			}
		})
	}
}

// A service the chain already shows terminated is never sent another request,
// which is what makes repeating termination after an unobserved outcome safe.
func TestStorageServiceAdapterTerminationReadsTheChainBeforeSending(t *testing.T) {
	tests := []struct {
		name      string
		endEpochs []int64
		errs      []error
		wantEpoch int64
		wantCalls int
	}{
		{name: "already terminated", endEpochs: []int64{77}, wantEpoch: 77},
		{name: "relay landed despite its error", endEpochs: []int64{0, 77}, errs: []error{errors.New("provider relay failed")}, wantEpoch: 77, wantCalls: 1},
		{
			name: "direct path failed after the service ended", endEpochs: []int64{0, 0, 88},
			errs: []error{terminationFailure(storage.TerminateServiceStageWait, common.Hash{}, &pdp.WaitForTerminateServiceNotFoundError{}), errors.New("already terminated")}, wantEpoch: 88, wantCalls: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &stubStorageServiceTerminator{errors: tt.errs}
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: &stubDataSetStateReader{endEpochs: tt.endEpochs}}
			got, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), TerminationHooks{})
			if err != nil || got == nil || (got.EndEpoch == nil || *got.EndEpoch != tt.wantEpoch) {
				t.Fatalf("result = %#v err=%v, want recorded end epoch %d", got, err, tt.wantEpoch)
			}
			if len(stub.calls) != tt.wantCalls {
				t.Fatalf("termination requests = %d, want %d", len(stub.calls), tt.wantCalls)
			}
		})
	}
}

func TestStorageServiceAdapterTerminationRefusesToSendWhileStateIsUnknown(t *testing.T) {
	stub := &stubStorageServiceTerminator{}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: &stubDataSetStateReader{err: errors.New("rpc unavailable")}}
	if _, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), TerminationHooks{}); err == nil {
		t.Fatal("termination succeeded without knowing the service state")
	}
	if len(stub.calls) != 0 {
		t.Fatalf("termination requests = %d, want none while the service state is unknown", len(stub.calls))
	}
}

// A data set ID is only a number, and the record it names may be paid for by
// another wallet. The direct FWSS path carries no payer check of its own, so
// the adapter refuses a record it does not pay for rather than ending it.
func TestStorageServiceAdapterTerminationRefusesADataSetItDoesNotPayFor(t *testing.T) {
	t.Parallel()

	ours := common.HexToAddress("0x00000000000000000000000000000000000000a1")
	theirs := common.HexToAddress("0x00000000000000000000000000000000000000c3")
	tests := []struct {
		name      string
		payer     common.Address
		wantCalls int
	}{
		{name: "someone else's data set", payer: theirs},
		{name: "our own data set", payer: ours, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stub := &stubStorageServiceTerminator{results: []*storage.TerminateServiceResult{{EndEpoch: 84}}}
			adapter := &StorageServiceAdapter{
				terminator: stub, dataSets: &stubDataSetStateReader{payer: tt.payer},
				identity: storage.ContextIdentity{Payer: ours},
			}
			refused := tt.wantCalls == 0
			err := adapter.VerifyServicePayer(context.Background(), sdktypes.NewBigInt(42))
			if refused != errors.Is(err, ErrServicePaidByAnother) || (!refused && err != nil) {
				t.Fatalf("VerifyServicePayer err = %v, want refused=%v", err, refused)
			}
			_, err = adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), TerminationHooks{})
			if refused && (!errors.Is(err, ErrServicePaidByAnother) || !strings.Contains(err.Error(), theirs.Hex())) {
				t.Fatalf("err = %v, want a refusal naming the paying wallet", err)
			}
			if !refused && err != nil {
				t.Fatalf("terminating our own data set: %v", err)
			}
			if len(stub.calls) != tt.wantCalls {
				t.Fatalf("termination requests = %d, want %d", len(stub.calls), tt.wantCalls)
			}
		})
	}
}

type terminationContextResolver struct {
	target *storage.DataSetContext
}

func (r terminationContextResolver) ResolveProviderContext(context.Context, sdktypes.BigInt, storage.NewProviderContextOptions) (*storage.ProviderContext, error) {
	return nil, errors.New("provider context was not requested")
}

func (r terminationContextResolver) ResolveDataSetContext(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (*storage.DataSetContext, error) {
	return r.target, nil
}

type terminationStatusTransport func(*http.Request) (*http.Response, error)

func (f terminationStatusTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func terminationObserver(t *testing.T, transport terminationStatusTransport, chain *stubDataSetStateReader) *StorageServiceAdapter {
	t.Helper()
	payer := common.HexToAddress("0x1001")
	recordKeeper := common.HexToAddress("0x2002")
	chainID := sdktypes.ChainID(314159)
	ref, err := storage.NewDataSetRef(sdktypes.NewBigInt(101), sdktypes.NewBigInt(42), sdktypes.NewBigInt(303))
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.NewDataSetContext(storage.Provider{ID: ref.ProviderID(), ServiceURL: "https://provider.example"}, &inertPDPProviderClient{}, nil, ref,
		storage.WithPayer(payer), storage.WithChainID(chainID), storage.WithRecordKeeper(recordKeeper))
	if err != nil {
		t.Fatal(err)
	}
	service, err := storage.New(storage.Options{ContextResolver: terminationContextResolver{target: target}, PayerAddress: payer, ChainID: chainID, RecordKeeper: recordKeeper})
	if err != nil {
		t.Fatal(err)
	}
	return &StorageServiceAdapter{service: service, dataSets: chain, identity: storage.ContextIdentity{Payer: payer}, providerHTTP: &http.Client{Transport: transport}}
}

func TestTerminationObservationSeparatesProviderOutagesFromBackpressure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       int
		body         string
		transportErr error
		cancel       bool
		wantFallback bool
		wantError    bool
		wantPending  bool
		wantAbsent   bool
	}{
		{name: "provider server error", status: 500, wantFallback: true, wantError: true},
		{name: "provider unavailable", status: 503, wantFallback: true, wantError: true},
		{name: "provider timeout", transportErr: context.DeadlineExceeded, wantFallback: true, wantError: true},
		{name: "provider network failure", transportErr: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, wantFallback: true, wantError: true},
		{name: "rate limited", status: 429, wantError: true},
		{name: "unauthorized", status: 401, wantError: true},
		{name: "invalid response", status: 200, body: `{"terminationTxHash":"invalid"}`, wantError: true},
		{name: "caller cancelled", cancel: true, transportErr: context.Canceled, wantError: true},
		{name: "no request", status: 404, wantError: true, wantAbsent: true},
		{name: "pending relay", status: 200, body: `{}`, wantPending: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := terminationObserver(t, func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodGet || r.URL.Path != "/pdp/data-sets/42/terminate" {
					t.Fatalf("unexpected termination observation %s %s", r.Method, r.URL.Path)
				}
				if tt.transportErr != nil {
					return nil, tt.transportErr
				}
				return &http.Response{StatusCode: tt.status, Body: io.NopCloser(strings.NewReader(tt.body)), Header: http.Header{}, Request: r}, nil
			}, &stubDataSetStateReader{payer: common.HexToAddress("0x1001")})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.cancel {
				cancel()
			}
			ended, pending, err := adapter.ObserveTermination(ctx, sdktypes.NewBigInt(42), TerminationResult{})
			if ended != nil || pending != tt.wantPending || (err != nil) != tt.wantError || errors.Is(err, ErrTerminationObservationUnavailable) != tt.wantFallback ||
				errors.Is(err, ErrTerminationRequestAbsent) != tt.wantAbsent {
				t.Fatalf("observation = %#v, pending=%t, err=%v; want pending=%t, error=%t, fallback=%t, absent=%t", ended, pending, err, tt.wantPending, tt.wantError, tt.wantFallback, tt.wantAbsent)
			}
		})
	}
}

func TestTerminationObservationKeepsChainAndPayerFailuresOutOfFallback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		chain     *stubDataSetStateReader
		wantError bool
		wantEnded bool
	}{
		{name: "chain unavailable", chain: &stubDataSetStateReader{err: &ProviderUnavailableError{Cause: errors.New("RPC unavailable")}}, wantError: true},
		{name: "payer mismatch", chain: &stubDataSetStateReader{payer: common.HexToAddress("0x2002")}, wantError: true},
		{name: "already terminated", chain: &stubDataSetStateReader{payer: common.HexToAddress("0x1001"), endEpochs: []int64{84}}, wantEnded: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := terminationObserver(t, func(*http.Request) (*http.Response, error) {
				t.Fatal("provider read must follow a live payer-checked chain record")
				return nil, errors.New("unexpected provider read")
			}, tt.chain)
			ended, pending, err := adapter.ObserveTermination(t.Context(), sdktypes.NewBigInt(42), TerminationResult{})
			if pending || (ended != nil) != tt.wantEnded || (err != nil) != tt.wantError || errors.Is(err, ErrTerminationObservationUnavailable) {
				t.Fatalf("observation = %#v, pending=%t, err=%v", ended, pending, err)
			}
		})
	}
}

// Outstanding debt is a decision for the operator, not something to retry, so
// it must never look like a transient provider problem.
func TestNormalizeTerminationErrorReportsPaymentDebtAsBlocked(t *testing.T) {
	t.Parallel()

	shortfall := big.NewInt(4200)
	got := normalizeTerminationError(context.Background(), fmt.Errorf(
		"storage.Service.TerminateService: %w",
		&storage.TerminateServiceDebtError{Shortfall: shortfall},
	))
	if !IsTerminationBlocked(got) {
		t.Fatalf("error = %T %v, want a blocked termination", got, got)
	}
	var blocked *TerminationBlockedError
	if !errors.As(got, &blocked) {
		t.Fatalf("error = %T, want *TerminationBlockedError", got)
	}
	if blocked.Reason != "payment_debt" || blocked.Shortfall.Cmp(shortfall) != 0 {
		t.Fatalf("blocked = %+v, want the payment shortfall preserved", blocked)
	}
	if IsProviderUnavailable(got) {
		t.Fatal("payment debt reported as provider unavailability, which would retry forever")
	}
}

// A termination the provider has accepted but not yet published resolves on its
// own, so it must wait rather than raise operator attention.
func TestNormalizeTerminationErrorTreatsPendingPublicationAsRetryable(t *testing.T) {
	t.Parallel()

	got := normalizeTerminationError(context.Background(), fmt.Errorf(
		"storage.Service.TerminateService: %w",
		&pdp.TerminateServicePendingError{Message: "queued"},
	))
	if !IsProviderUnavailable(got) {
		t.Fatalf("error = %T %v, want provider unavailability", got, got)
	}
	if IsTerminationBlocked(got) {
		t.Fatal("pending publication reported as blocked, which would need operator action")
	}
}

func TestNormalizeTerminationErrorPassesThroughUnknownFailures(t *testing.T) {
	t.Parallel()

	if got := normalizeTerminationError(context.Background(), nil); got != nil {
		t.Fatalf("nil error = %v, want nil", got)
	}
	unknown := errors.New("provider exploded")
	got := normalizeTerminationError(context.Background(), unknown)
	if IsTerminationBlocked(got) {
		t.Fatalf("unknown error = %T, want it left retryable", got)
	}
	if !errors.Is(got, unknown) {
		t.Fatalf("unknown error = %v, want the cause preserved", got)
	}
}

type stubBlockNumberSource struct {
	height uint64
	err    error
}

func (s stubBlockNumberSource) BlockNumber(context.Context) (uint64, error) {
	return s.height, s.err
}

func TestChainEpochReaderReportsChainHead(t *testing.T) {
	t.Parallel()

	epoch, err := NewChainEpochReader(stubBlockNumberSource{height: 4096}).CurrentEpoch(context.Background())
	if err != nil {
		t.Fatalf("CurrentEpoch: %v", err)
	}
	if epoch != 4096 {
		t.Fatalf("epoch = %d, want the observed block number", epoch)
	}
}

// Retirement must never treat an unreadable chain as "the epoch was reached".
func TestChainEpochReaderRefusesToGuess(t *testing.T) {
	t.Parallel()

	cases := map[string]ChainEpochReader{
		"rpc failure":  NewChainEpochReader(stubBlockNumberSource{err: errors.New("rpc down")}),
		"out of range": NewChainEpochReader(stubBlockNumberSource{height: math.MaxUint64}),
		"unconfigured": NewChainEpochReader(nil),
	}
	for name, reader := range cases {
		t.Run(name, func(t *testing.T) {
			epoch, err := reader.CurrentEpoch(context.Background())
			if err == nil {
				t.Fatalf("CurrentEpoch = %d, want an error", epoch)
			}
			if epoch != 0 {
				t.Fatalf("epoch = %d on failure, want 0", epoch)
			}
		})
	}
}

func TestStorageServiceAdapterTerminationRetainsSubmittedEvidence(t *testing.T) {
	original, confirmed := common.HexToHash("0x123"), common.HexToHash("0x456")
	writeErr := errors.New("could not record submitted transaction")
	for _, tc := range []struct {
		name                   string
		callbackErr, remoteErr error
		confirmed              bool
		direct                 bool
	}{
		{name: "relay wait timeout", remoteErr: terminationFailure(storage.TerminateServiceStageWait, original, context.DeadlineExceeded)},
		{name: "replacement confirmed", confirmed: true},
		{name: "direct receipt timeout", direct: true, remoteErr: terminationFailure(storage.TerminateServiceStageWait, original, context.DeadlineExceeded)},
		{name: "direct persistence failure", direct: true, callbackErr: writeErr, remoteErr: context.Canceled},
		{name: "direct confirmed", direct: true, confirmed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubStorageServiceTerminator{errors: []error{tc.remoteErr}}
			// A relay reports its transaction only in the wait's result or error,
			// and that hash is recorded as soon as the wait returns.
			if tc.direct {
				stub.submittedHash = original
			}
			if tc.confirmed {
				stub.results = []*storage.TerminateServiceResult{{TxHash: &original, ConfirmedTxHash: &confirmed, EndEpoch: 84}}
			}
			state := &stubDataSetStateReader{endEpochs: []int64{0, 84}}
			if tc.direct {
				state = &stubDataSetStateReader{err: errors.New("unexpected service state read"), errFrom: 1}
			}
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: state}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			terminate := adapter.TerminateService
			if tc.direct {
				terminate = adapter.TerminateServiceDirect
			}
			got, err := terminate(ctx, sdktypes.NewBigInt(42), TerminationHooks{OnSubmitted: func(hash string) error {
				calls++
				if hash != original.Hex() {
					t.Fatalf("submitted hash = %s", hash)
				}
				return tc.callbackErr
			}})
			if calls != 1 || len(stub.calls) != 1 || ctx.Err() != nil || got == nil || got.TxHash != original.Hex() {
				t.Fatalf("result=%#v error=%v callbacks=%d requests=%d", got, err, calls, len(stub.calls))
			}
			if stub.calls[0].opts.SkipProvider != tc.direct {
				t.Fatalf("SkipProvider = %t, want %t", stub.calls[0].opts.SkipProvider, tc.direct)
			}
			if tc.confirmed {
				if err != nil || got.ConfirmedTxHash != confirmed.Hex() || got.EndEpoch == nil || *got.EndEpoch != 84 {
					t.Fatalf("confirmed evidence=%#v error=%v", got, err)
				}
				if tc.direct {
					if got.ReceiptStatus == nil || *got.ReceiptStatus != ethtypes.ReceiptStatusSuccessful || state.reads != 1 {
						t.Fatalf("direct evidence=%#v reads=%d", got, state.reads)
					}
				} else if got.ReceiptStatus != nil {
					t.Fatalf("provider status inferred a receipt: %#v", got)
				}
			} else if err == nil || got.EndEpoch != nil || (tc.callbackErr != nil && !errors.Is(err, writeErr)) {
				t.Fatalf("pending evidence=%#v error=%v", got, err)
			}
		})
	}
}

func TestStorageServiceAdapterTerminationConfirmsRelayOnChain(t *testing.T) {
	original, confirmed := common.HexToHash("0x123"), common.HexToHash("0x456")
	rpcErr := errors.New("rpc unavailable after relay success")
	for _, tc := range []struct {
		name      string
		chain     *stubDataSetStateReader
		wantEpoch *int64
		wantErr   error
	}{
		{name: "provider reports termination while chain is running", chain: runningService()},
		{name: "chain records a different termination epoch", chain: &stubDataSetStateReader{endEpochs: []int64{0, 91}}, wantEpoch: new(int64(91))},
		{name: "chain query fails after relay success", chain: &stubDataSetStateReader{err: rpcErr, errFrom: 1}, wantErr: rpcErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubStorageServiceTerminator{results: []*storage.TerminateServiceResult{{TxHash: &original, ConfirmedTxHash: &confirmed, EndEpoch: 84}}}
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: tc.chain}
			got, err := adapter.TerminateService(t.Context(), sdktypes.NewBigInt(42), TerminationHooks{})
			if !errors.Is(err, tc.wantErr) || errors.Is(err, ErrTerminationNotSent) || errors.Is(err, ErrTerminationTransactionNotSent) {
				t.Fatalf("error=%v, want %v without an unsent marker", err, tc.wantErr)
			}
			if got == nil || got.TxHash != original.Hex() || got.ConfirmedTxHash != confirmed.Hex() || got.ReceiptStatus != nil {
				t.Fatalf("relay evidence=%#v, error=%v", got, err)
			}
			if tc.wantEpoch == nil {
				if got.EndEpoch != nil {
					t.Fatalf("unconfirmed relay epoch=%d", *got.EndEpoch)
				}
			} else if got.EndEpoch == nil || *got.EndEpoch != *tc.wantEpoch {
				t.Fatalf("termination evidence=%#v, want epoch=%d", got, *tc.wantEpoch)
			}
			if len(stub.calls) != 1 || stub.calls[0].opts.SkipProvider || tc.chain.reads != 2 {
				t.Fatalf("requests=%#v, chain reads=%d", stub.calls, tc.chain.reads)
			}
		})
	}
}

func TestStorageServiceAdapterTerminationRequiresDirectConfirmationEvidence(t *testing.T) {
	original := common.HexToHash("0x123")
	for _, tc := range []struct {
		name      string
		confirmed *common.Hash
		epoch     sdktypes.Epoch
	}{
		{name: "missing confirmation hash", epoch: 84},
		{name: "zero confirmation hash", confirmed: new(common.Hash), epoch: 84},
		{name: "zero termination epoch", confirmed: &original},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubStorageServiceTerminator{
				submittedHash: original,
				results:       []*storage.TerminateServiceResult{{TxHash: &original, ConfirmedTxHash: tc.confirmed, EndEpoch: tc.epoch}},
			}
			state := &stubDataSetStateReader{err: errors.New("unexpected service state read"), errFrom: 1}
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: state}
			got, err := adapter.TerminateServiceDirect(t.Context(), sdktypes.NewBigInt(42), TerminationHooks{})
			confirmed := ""
			if tc.confirmed != nil && *tc.confirmed != (common.Hash{}) {
				confirmed = tc.confirmed.Hex()
			}
			if err != nil || got == nil || got.TxHash != original.Hex() || got.ConfirmedTxHash != confirmed || got.EndEpoch != nil || got.ReceiptStatus != nil {
				t.Fatalf("incomplete direct confirmation=%#v, error=%v", got, err)
			}
			if len(stub.calls) != 1 || !stub.calls[0].opts.SkipProvider || state.reads != 1 {
				t.Fatalf("requests=%#v, chain reads=%d", stub.calls, state.reads)
			}
		})
	}
}

// A relayed hash the SDK has polled is recorded when its wait slice ends,
// before waiting continues, so a shutdown during a later slice keeps it.
func TestStorageServiceAdapterTerminationRecordsRelayHashBetweenWaitSlices(t *testing.T) {
	relayed, other := common.HexToHash("0x789"), common.HexToHash("0xabc")
	type wait = func(context.Context, storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error)
	polled := func(ctx context.Context, _ storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error) {
		<-ctx.Done()
		return nil, terminationFailure(storage.TerminateServiceStageWait, relayed, ctx.Err())
	}
	for _, tc := range []struct {
		name    string
		next    wait
		stop    bool
		wantErr error
	}{
		{name: "confirmed in a later slice", next: func(_ context.Context, sub storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error) {
			if sub.TransactionID != relayed.Hex() {
				return nil, fmt.Errorf("later slice waits on %q", sub.TransactionID)
			}
			return &storage.TerminateServiceResult{TxHash: &relayed, EndEpoch: 84}, nil
		}},
		{name: "shutdown during a later slice", next: polled, stop: true, wantErr: context.Canceled},
		{name: "later slice reports another hash", next: func(context.Context, storage.TerminateServiceSubmission) (*storage.TerminateServiceResult, error) {
			return nil, terminationFailure(storage.TerminateServiceStageWait, other, errors.New("provider status changed"))
		}, wantErr: ErrTerminationEvidenceConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubStorageServiceTerminator{waits: []wait{polled, tc.next}}
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: &stubDataSetStateReader{endEpochs: []int64{0, 84}}, relayWaitSlice: 20 * time.Millisecond}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var recorded []string
			waitsBeforeRecord := -1
			got, err := adapter.TerminateService(ctx, sdktypes.NewBigInt(42), TerminationHooks{OnSubmitted: func(hash string) error {
				recorded = append(recorded, hash)
				waitsBeforeRecord = len(stub.waitCalls)
				if tc.stop {
					cancel()
				}
				return nil
			}})
			if len(recorded) != 1 || recorded[0] != relayed.Hex() || waitsBeforeRecord != 1 || len(stub.calls) != 1 || got == nil || got.TxHash != relayed.Hex() {
				t.Fatalf("recorded=%v after %d waits, requests=%d result=%#v err=%v", recorded, waitsBeforeRecord, len(stub.calls), got, err)
			}
			if stub.waitCalls[0].PollInterval != terminationPollInterval {
				t.Fatalf("relay poll interval = %s, want %s", stub.waitCalls[0].PollInterval, terminationPollInterval)
			}
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("error = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil || got.EndEpoch == nil || *got.EndEpoch != 84 {
				t.Fatalf("confirmed relay = %#v, err=%v", got, err)
			}
		})
	}
}

func TestTerminationObservationPreservesProviderHashesWithoutConfirmingEpoch(t *testing.T) {
	original, confirmed := common.HexToHash("0x123"), common.HexToHash("0x456")
	adapter := terminationObserver(t, func(r *http.Request) (*http.Response, error) {
		body := fmt.Sprintf(`{"terminationTxHash":%q,"confirmedTxHash":%q,"fwssTerminated":true,"serviceTerminationEpoch":84}`, original.Hex(), confirmed.Hex())
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}, Request: r}, nil
	}, &stubDataSetStateReader{payer: common.HexToAddress("0x1001")})
	got, pending, err := adapter.ObserveTermination(t.Context(), sdktypes.NewBigInt(42), TerminationResult{})
	if err != nil || !pending || got == nil || got.TxHash != original.Hex() || got.ConfirmedTxHash != confirmed.Hex() || got.EndEpoch != nil {
		t.Fatalf("observation=%#v pending=%t error=%v", got, pending, err)
	}
}

type terminationChainStub struct {
	receipt    *ethtypes.Receipt
	receiptErr error
	tx         *ethtypes.Transaction
	txErr      error
	pending    bool
	queried    []common.Hash
}

func (c *terminationChainStub) TransactionReceipt(_ context.Context, hash common.Hash) (*ethtypes.Receipt, error) {
	c.queried = append(c.queried, hash)
	return c.receipt, c.receiptErr
}

func (c *terminationChainStub) TransactionByHash(_ context.Context, hash common.Hash) (*ethtypes.Transaction, bool, error) {
	c.queried = append(c.queried, hash)
	return c.tx, c.pending, c.txErr
}

func TestTerminationObservationChecksSavedTransactionWithoutInferringAnEpoch(t *testing.T) {
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{Nonce: 7})
	original, replacement := tx.Hash(), common.HexToHash("0x456")
	rpcErr := errors.New("receipt RPC unavailable")
	for _, tc := range []struct {
		name           string
		providerStatus int
		providerBody   string
		chain          terminationChainStub
		epochs         []int64
		wantHash       common.Hash
		wantConfirmed  common.Hash
		wantErr        error
		wantEpoch      bool
		direct         bool
		wantReceipt    *uint64
	}{
		{name: "provider missing and RPC failed", providerStatus: 404, chain: terminationChainStub{receiptErr: rpcErr}, wantHash: original, wantErr: rpcErr},
		{name: "transaction not found", providerStatus: 404, chain: terminationChainStub{receiptErr: ethereum.NotFound, txErr: ethereum.NotFound}, wantHash: original, wantErr: ethereum.NotFound},
		{name: "original pending", providerStatus: 503, chain: terminationChainStub{receiptErr: ethereum.NotFound, tx: tx, pending: true}, wantHash: original},
		{name: "failed receipt", providerStatus: 404, chain: terminationChainStub{receipt: &ethtypes.Receipt{TxHash: original, Status: ethtypes.ReceiptStatusFailed}}, wantHash: original, wantConfirmed: original, wantErr: ErrTerminationTransactionFailed, wantReceipt: new(uint64(0))},
		{name: "replacement receipt without epoch", providerStatus: 200, providerBody: fmt.Sprintf(`{"terminationTxHash":%q,"confirmedTxHash":%q}`, original.Hex(), replacement.Hex()), chain: terminationChainStub{receipt: &ethtypes.Receipt{TxHash: replacement, Status: ethtypes.ReceiptStatusSuccessful}}, wantHash: replacement, wantConfirmed: replacement, wantReceipt: new(uint64(1))},
		{name: "replacement receipt and verified epoch", providerStatus: 200, providerBody: fmt.Sprintf(`{"terminationTxHash":%q,"confirmedTxHash":%q}`, original.Hex(), replacement.Hex()), chain: terminationChainStub{receipt: &ethtypes.Receipt{TxHash: replacement, Status: ethtypes.ReceiptStatusSuccessful}}, epochs: []int64{0, 84}, wantHash: replacement, wantConfirmed: replacement, wantEpoch: true, wantReceipt: new(uint64(1))},
		{name: "direct pending ignores preceding provider request", direct: true, providerStatus: 200, providerBody: fmt.Sprintf(`{"terminationTxHash":%q}`, replacement.Hex()), chain: terminationChainStub{receiptErr: ethereum.NotFound, tx: tx, pending: true}, wantHash: original},
		{name: "direct confirmed ignores preceding provider request", direct: true, providerStatus: 200, providerBody: fmt.Sprintf(`{"terminationTxHash":%q}`, replacement.Hex()), chain: terminationChainStub{receipt: &ethtypes.Receipt{TxHash: original, Status: ethtypes.ReceiptStatusSuccessful}}, epochs: []int64{0, 84}, wantHash: original, wantConfirmed: original, wantEpoch: true, wantReceipt: new(uint64(1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adapter := terminationObserver(t, func(req *http.Request) (*http.Response, error) {
				if tc.direct {
					t.Fatal("direct attempt consulted the provider's preceding request")
				}
				return &http.Response{StatusCode: tc.providerStatus, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(tc.providerBody)), Request: req}, nil
			}, &stubDataSetStateReader{endEpochs: tc.epochs, payer: common.HexToAddress("0x1001")})
			adapter.ConfigureTerminationChain(&tc.chain)
			observe := adapter.ObserveTermination
			if tc.direct {
				observe = adapter.ObserveTerminationDirect
			}
			got, pending, err := observe(t.Context(), sdktypes.NewBigInt(42), TerminationResult{TxHash: original.Hex()})
			if got == nil || got.TxHash != original.Hex() || (got.EndEpoch != nil) != tc.wantEpoch || pending == tc.wantEpoch || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
				t.Fatalf("observation=%#v pending=%t error=%v", got, pending, err)
			}
			if tc.wantConfirmed == (common.Hash{}) && got.ConfirmedTxHash != "" || tc.wantConfirmed != (common.Hash{}) && got.ConfirmedTxHash != tc.wantConfirmed.Hex() {
				t.Fatalf("confirmed hash=%q, want %s", got.ConfirmedTxHash, tc.wantConfirmed)
			}
			if tc.wantReceipt == nil {
				if got.ReceiptStatus != nil {
					t.Fatalf("receipt status = %d, want unknown", *got.ReceiptStatus)
				}
			} else if got.ReceiptStatus == nil || *got.ReceiptStatus != *tc.wantReceipt {
				t.Fatalf("receipt status = %v, want %d", got.ReceiptStatus, *tc.wantReceipt)
			}
			if len(tc.chain.queried) == 0 || tc.chain.queried[0] != tc.wantHash {
				t.Fatalf("queried hashes=%v, want %s", tc.chain.queried, tc.wantHash)
			}
			if tc.wantEpoch && (*got.EndEpoch != 84 || err != nil) {
				t.Fatalf("confirmed result=%#v error=%v", got, err)
			}
			if !tc.wantEpoch && !tc.direct {
				if httpErr, ok := errors.AsType[*pdp.HTTPError](err); (tc.providerStatus >= 400) && (!ok || httpErr.StatusCode != tc.providerStatus) {
					t.Fatalf("provider diagnostic missing from error: %v", err)
				}
			}
		})
	}
}
