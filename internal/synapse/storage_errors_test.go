package synapse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	"github.com/strahe/synapse-go/types"
)

func TestNormalizeCreateContextsError(t *testing.T) {
	t.Parallel()

	known := errors.New("storage.Service.CreateContexts: storage.ServiceResolver.ResolveUploadContexts: no remaining providers")
	if got := normalizeCreateContextsError(known); !IsNoProviderCandidates(got) {
		t.Fatalf("known error = %T %v, want NoProviderCandidatesError", got, got)
	}

	nearMiss := errors.New("storage.Service.CreateContexts: no remaining providers")
	if got := normalizeCreateContextsError(nearMiss); IsNoProviderCandidates(got) {
		t.Fatalf("near-miss error = %T %v, must remain unclassified", got, got)
	}

	typed := fmt.Errorf("provider selection: %w", storage.ErrNoHealthyProviders)
	if got := normalizeCreateContextsError(typed); !IsNoProviderCandidates(got) {
		t.Fatalf("typed health error = %T %v, want NoProviderCandidatesError", got, got)
	}
}

func TestNormalizeCreateContextErrorMatchesRequestedDataSet(t *testing.T) {
	t.Parallel()

	dataSetID := types.NewBigInt(23054)
	opts := &storage.CreateContextOptions{DataSetID: &dataSetID}
	known := errors.New("storage.Service.CreateContext: storage.ServiceResolver.ResolveUploadContexts: data set 23054 does not exist")
	if got := normalizeCreateContextError(known, opts); !IsDataSetServiceEnded(got) {
		t.Fatalf("known error = %T %v, want DataSetServiceEndedError", got, got)
	}

	other := types.NewBigInt(23055)
	if got := normalizeCreateContextError(known, &storage.CreateContextOptions{DataSetID: &other}); IsDataSetServiceEnded(got) {
		t.Fatalf("mismatched data set error = %T %v, must remain unclassified", got, got)
	}
}

func TestNormalizeCreateContextErrorMatchesRequestedProvider(t *testing.T) {
	t.Parallel()

	providerID := types.NewBigInt(303)
	dataSetID := types.NewBigInt(23054)
	opts := &storage.CreateContextOptions{ProviderID: &providerID, DataSetID: &dataSetID}
	known := errors.New("storage.Service.CreateContext: storage.ServiceResolver.ResolveUploadContexts: provider 303 for data set 23054 not found")
	if got := normalizeCreateContextError(known, opts); !IsProviderUnavailable(got) {
		t.Fatalf("known error = %T %v, want ProviderUnavailableError", got, got)
	}

	otherProvider := types.NewBigInt(304)
	if got := normalizeCreateContextError(known, &storage.CreateContextOptions{ProviderID: &otherProvider, DataSetID: &dataSetID}); IsProviderUnavailable(got) {
		t.Fatalf("mismatched provider error = %T %v, must remain unclassified", got, got)
	}
}

func TestNormalizeCreateContextErrorTreatsAssignedDataSetAsUnavailable(t *testing.T) {
	t.Parallel()

	known := errors.New("storage.Service.CreateContext: storage.ServiceResolver.ResolveUploadContexts: no remaining providers")
	dataSetID := types.NewBigInt(23054)
	assigned := normalizeCreateContextError(known, &storage.CreateContextOptions{DataSetID: &dataSetID})
	if !IsProviderUnavailable(assigned) || IsNoProviderCandidates(assigned) {
		t.Fatalf("assigned data set error = %T %v, want only ProviderUnavailableError", assigned, assigned)
	}
	automatic := normalizeCreateContextError(known, &storage.CreateContextOptions{})
	if !IsNoProviderCandidates(automatic) {
		t.Fatalf("automatic selection error = %T %v, want NoProviderCandidatesError", automatic, automatic)
	}
	typed := normalizeCreateContextError(storage.ErrNoHealthyProviders, &storage.CreateContextOptions{DataSetID: &dataSetID})
	if !IsProviderUnavailable(typed) || IsNoProviderCandidates(typed) {
		t.Fatalf("assigned typed health error = %T %v, want only ProviderUnavailableError", typed, typed)
	}
}

func TestProviderAvailabilityClassificationRequiresOperationContext(t *testing.T) {
	t.Parallel()

	rpcFailure := &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", IsNotFound: true}}
	dataSetID := types.NewBigInt(23054)
	if got := normalizeCreateContextError(rpcFailure, &storage.CreateContextOptions{DataSetID: &dataSetID}); IsProviderUnavailable(got) {
		t.Fatalf("CreateContext RPC error = %T %v, must remain an ordinary retryable error", got, got)
	}
	if got := NormalizeProviderOperationError(context.Background(), rpcFailure); !IsProviderUnavailable(got) {
		t.Fatalf("provider endpoint error = %T %v, want ProviderUnavailableError", got, got)
	}
	if got := NormalizeProviderOperationError(context.Background(), &pdp.HTTPError{StatusCode: 503}); !IsProviderUnavailable(got) {
		t.Fatalf("provider HTTP 503 = %T %v, want ProviderUnavailableError", got, got)
	}
	if got := NormalizeProviderOperationError(context.Background(), &pdp.HTTPError{StatusCode: 400}); IsProviderUnavailable(got) {
		t.Fatalf("provider HTTP 400 = %T %v, must remain unclassified", got, got)
	}
	if got := IsProviderUnavailable(rpcFailure); got {
		t.Fatal("raw network error must not bypass adapter classification")
	}
}

func TestNormalizeProviderOperationErrorDistinguishesCallerAndClientTimeouts(t *testing.T) {
	t.Parallel()

	if got := NormalizeProviderOperationError(context.Background(), context.DeadlineExceeded); !IsProviderUnavailable(got) {
		t.Fatalf("client timeout = %T %v, want ProviderUnavailableError", got, got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := NormalizeProviderOperationError(ctx, context.Canceled)
	if !errors.Is(got, context.Canceled) || IsProviderUnavailable(got) {
		t.Fatalf("caller cancellation = %T %v, want unclassified context cancellation", got, got)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 0)
	defer cancel()
	<-ctx.Done()
	got = NormalizeProviderOperationError(ctx, context.DeadlineExceeded)
	if !errors.Is(got, context.DeadlineExceeded) || IsProviderUnavailable(got) {
		t.Fatalf("caller deadline = %T %v, want unclassified context deadline", got, got)
	}
}
