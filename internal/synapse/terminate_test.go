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

type stubStorageServiceTerminator struct {
	calls         []terminatorCall
	results       []*storage.TerminateServiceResult
	errors        []error
	submittedHash common.Hash
}

func (s *stubStorageServiceTerminator) TerminateService(
	_ context.Context,
	_ sdktypes.BigInt,
	opts *storage.TerminateServiceOptions,
) (*storage.TerminateServiceResult, error) {
	s.calls = append(s.calls, terminatorCall{opts: opts})
	index := len(s.calls) - 1
	if s.submittedHash != (common.Hash{}) && opts.OnSubmitted != nil {
		opts.OnSubmitted(s.submittedHash)
	}
	var result *storage.TerminateServiceResult
	if index < len(s.results) {
		result = s.results[index]
	}
	if index < len(s.errors) {
		return result, s.errors[index]
	}
	return result, nil
}

// stubDataSetStateReader reports successive PDP end epochs, repeating the last.
// Zero means the service is still running.
type stubDataSetStateReader struct {
	endEpochs []int64
	payer     common.Address
	err       error
	reads     int
}

func (s *stubDataSetStateReader) GetDataSet(context.Context, sdktypes.BigInt) (*warmstorage.DataSetInfo, error) {
	s.reads++
	if s.err != nil {
		return nil, s.err
	}
	var epoch int64
	if len(s.endEpochs) > 0 {
		epoch = s.endEpochs[min(s.reads, len(s.endEpochs))-1]
	}
	return &warmstorage.DataSetInfo{PDPEndEpoch: sdktypes.Epoch(epoch), Payer: s.payer}, nil
}

func runningService() *stubDataSetStateReader { return &stubDataSetStateReader{} }

func TestStorageServiceAdapterTerminationFallsBackToDirect(t *testing.T) {
	stub := &stubStorageServiceTerminator{
		results: []*storage.TerminateServiceResult{nil, {EndEpoch: 84}},
		errors:  []error{errors.New("provider relay failed"), nil},
	}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	got, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), nil)
	if err != nil {
		t.Fatalf("TerminateService: %v", err)
	}
	if got == nil || (got.EndEpoch == nil || *got.EndEpoch != 84) {
		t.Fatalf("result = %#v, want end epoch 84", got)
	}
	if len(stub.calls) != 2 || stub.calls[0].opts.SkipProvider || !stub.calls[1].opts.SkipProvider {
		t.Fatalf("calls = %#v, want provider relay followed by direct termination", stub.calls)
	}
	if stub.calls[0].opts.ProviderWaitTimeout != providerTerminationWaitTimeout {
		t.Fatalf("provider wait timeout = %s, want %s", stub.calls[0].opts.ProviderWaitTimeout, providerTerminationWaitTimeout)
	}
}

func TestStorageServiceAdapterTerminationFallsBackAfterProviderSubTimeout(t *testing.T) {
	stub := &stubStorageServiceTerminator{
		results: []*storage.TerminateServiceResult{nil, {EndEpoch: 91}},
		errors:  []error{context.DeadlineExceeded, nil},
	}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	got, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), nil)
	if err != nil {
		t.Fatalf("TerminateService: %v", err)
	}
	if got == nil || (got.EndEpoch == nil || *got.EndEpoch != 91) {
		t.Fatalf("result = %#v, want end epoch 91", got)
	}
	if len(stub.calls) != 2 || !stub.calls[1].opts.SkipProvider {
		t.Fatalf("calls = %#v, want provider timeout followed by direct termination", stub.calls)
	}
}

func TestStorageServiceAdapterTerminationDoesNotDuplicatePendingRelay(t *testing.T) {
	stub := &stubStorageServiceTerminator{errors: []error{&pdp.TerminateServicePendingError{Message: "queued"}}}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), nil)
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
	_, err := adapter.TerminateService(ctx, sdktypes.NewBigInt(42), nil)
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
		errors: []error{errors.New("provider unavailable"), directErr},
	}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), nil)
	if !errors.Is(err, directErr) || !strings.Contains(err.Error(), "provider unavailable") {
		t.Fatalf("error = %v, want both provider and direct evidence", err)
	}
}

// Payment debt needs the operator. Falling back to the direct path would hide
// it behind whatever that path reports.
func TestStorageServiceAdapterTerminationReportsPaymentDebtWithoutFallback(t *testing.T) {
	stub := &stubStorageServiceTerminator{errors: []error{&storage.TerminateServiceDebtError{Shortfall: big.NewInt(1)}}}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), nil)
	if !IsTerminationBlocked(err) {
		t.Fatalf("error = %T %v, want a blocked termination", err, err)
	}
	if len(stub.calls) != 1 {
		t.Fatalf("termination requests = %d, want no direct fallback", len(stub.calls))
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
			errs: []error{errors.New("provider relay failed"), errors.New("already terminated")}, wantEpoch: 88, wantCalls: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &stubStorageServiceTerminator{errors: tt.errs}
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: &stubDataSetStateReader{endEpochs: tt.endEpochs}}
			got, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), nil)
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
	if _, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), nil); err == nil {
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
			_, err = adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42), nil)
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
	}{
		{name: "provider server error", status: 500, wantFallback: true, wantError: true},
		{name: "provider unavailable", status: 503, wantFallback: true, wantError: true},
		{name: "provider timeout", transportErr: context.DeadlineExceeded, wantFallback: true, wantError: true},
		{name: "provider network failure", transportErr: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, wantFallback: true, wantError: true},
		{name: "rate limited", status: 429, wantError: true},
		{name: "unauthorized", status: 401, wantError: true},
		{name: "invalid response", status: 200, body: `{"terminationTxHash":"invalid"}`, wantError: true},
		{name: "caller cancelled", cancel: true, transportErr: context.Canceled, wantError: true},
		{name: "no request", status: 404},
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
			if ended != nil || pending != tt.wantPending || (err != nil) != tt.wantError || errors.Is(err, ErrTerminationObservationUnavailable) != tt.wantFallback {
				t.Fatalf("observation = %#v, pending=%t, err=%v; want pending=%t, error=%t, fallback=%t", ended, pending, err, tt.wantPending, tt.wantError, tt.wantFallback)
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
		{name: "receipt timeout", remoteErr: context.DeadlineExceeded},
		{name: "submission persistence failure", callbackErr: writeErr, remoteErr: context.Canceled},
		{name: "replacement confirmed", confirmed: true},
		{name: "direct receipt timeout", direct: true, remoteErr: context.DeadlineExceeded},
		{name: "direct persistence failure", direct: true, callbackErr: writeErr, remoteErr: context.Canceled},
		{name: "direct confirmed", direct: true, confirmed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubStorageServiceTerminator{submittedHash: original, errors: []error{tc.remoteErr}}
			if tc.confirmed {
				stub.results = []*storage.TerminateServiceResult{{TxHash: &original, ConfirmedTxHash: &confirmed, EndEpoch: 84}}
			}
			adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			terminate := adapter.TerminateService
			if tc.direct {
				terminate = adapter.TerminateServiceDirect
			}
			got, err := terminate(ctx, sdktypes.NewBigInt(42), func(hash string) error {
				calls++
				if hash != original.Hex() {
					t.Fatalf("submitted hash = %s", hash)
				}
				return tc.callbackErr
			})
			if calls != 1 || len(stub.calls) != 1 || ctx.Err() != nil || got == nil || got.TxHash != original.Hex() {
				t.Fatalf("result=%#v error=%v callbacks=%d requests=%d", got, err, calls, len(stub.calls))
			}
			if stub.calls[0].opts.SkipProvider != tc.direct {
				t.Fatalf("SkipProvider = %t, want %t", stub.calls[0].opts.SkipProvider, tc.direct)
			}
			if tc.confirmed {
				if err != nil || got.ConfirmedTxHash != confirmed.Hex() || got.EndEpoch == nil || *got.EndEpoch != 84 || got.ReceiptStatus == nil || *got.ReceiptStatus != ethtypes.ReceiptStatusSuccessful {
					t.Fatalf("confirmed evidence=%#v error=%v", got, err)
				}
			} else if err == nil || got.EndEpoch != nil || (tc.callbackErr != nil && !errors.Is(err, writeErr)) {
				t.Fatalf("pending evidence=%#v error=%v", got, err)
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
