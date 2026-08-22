package synapse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
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

func TestStorageServiceAdapterTerminationFallsBackToDirect(t *testing.T) {
	stub := &stubStorageServiceTerminator{
		results: []*storage.TerminateServiceResult{nil, {EndEpoch: 84}},
		errors: []error{
			&storage.TerminateServiceDebtError{Shortfall: big.NewInt(1)},
			nil,
		},
	}
	adapter := &StorageServiceAdapter{terminator: stub}
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
	adapter := &StorageServiceAdapter{terminator: stub}
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
	adapter := &StorageServiceAdapter{terminator: stub}
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
	adapter := &StorageServiceAdapter{terminator: stub}
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
	adapter := &StorageServiceAdapter{terminator: stub}
	_, err := adapter.TerminateService(context.Background(), sdktypes.NewBigInt(42))
	if !errors.Is(err, directErr) || !strings.Contains(err.Error(), "provider unavailable") {
		t.Fatalf("error = %v, want both provider and direct evidence", err)
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
