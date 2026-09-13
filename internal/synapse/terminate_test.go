package synapse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

type terminatorCall struct {
	opts *storage.TerminateServiceOptions
}

type stubStorageServiceTerminator struct {
	calls   []terminatorCall
	results []*storage.TerminateServiceResult
	errors  []error
}

func (s *stubStorageServiceTerminator) TerminateService(
	_ context.Context,
	_ sdktypes.BigInt,
	opts *storage.TerminateServiceOptions,
) (*storage.TerminateServiceResult, error) {
	s.calls = append(s.calls, terminatorCall{opts: opts})
	index := len(s.calls) - 1
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
	got, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42))
	if err != nil {
		t.Fatalf("TerminateService: %v", err)
	}
	if got == nil || got.EndEpoch != 84 {
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
	got, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42))
	if err != nil {
		t.Fatalf("TerminateService: %v", err)
	}
	if got == nil || got.EndEpoch != 91 {
		t.Fatalf("result = %#v, want end epoch 91", got)
	}
	if len(stub.calls) != 2 || !stub.calls[1].opts.SkipProvider {
		t.Fatalf("calls = %#v, want provider timeout followed by direct termination", stub.calls)
	}
}

func TestStorageServiceAdapterTerminationDoesNotDuplicatePendingRelay(t *testing.T) {
	stub := &stubStorageServiceTerminator{errors: []error{&pdp.TerminateServicePendingError{Message: "queued"}}}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42))
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
	_, err := adapter.TerminateService(ctx, sdktypes.NewBigInt(42))
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
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42))
	if !errors.Is(err, directErr) || !strings.Contains(err.Error(), "provider unavailable") {
		t.Fatalf("error = %v, want both provider and direct evidence", err)
	}
}

// Payment debt needs the operator. Falling back to the direct path would hide
// it behind whatever that path reports.
func TestStorageServiceAdapterTerminationReportsPaymentDebtWithoutFallback(t *testing.T) {
	stub := &stubStorageServiceTerminator{errors: []error{&storage.TerminateServiceDebtError{Shortfall: big.NewInt(1)}}}
	adapter := &StorageServiceAdapter{terminator: stub, dataSets: runningService()}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42))
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
			got, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42))
			if err != nil || got == nil || got.EndEpoch != tt.wantEpoch {
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
	if _, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42)); err == nil {
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
			_, err = adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42))
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
