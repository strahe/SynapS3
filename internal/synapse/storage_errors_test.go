package synapse

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/spregistry"
	"github.com/strahe/synapse-go/storage"
)

func TestNormalizeSelectUploadTargetsError(t *testing.T) {
	t.Parallel()

	for _, selectionErr := range []error{
		storage.ErrInsufficientUploadContexts,
		storage.ErrNoHealthyProviders,
		storage.ErrNoEndorsedProvider,
		storage.ErrEndorsementsNotConfigured,
	} {
		t.Run(selectionErr.Error(), func(t *testing.T) {
			t.Parallel()
			got := normalizeSelectUploadTargetsError(fmt.Errorf("select targets: %w", selectionErr))
			if !IsNoProviderCandidates(got) {
				t.Fatalf("error = %T %v, want NoProviderCandidatesError", got, got)
			}
		})
	}

	legacyText := errors.New("storage.Service.CreateContexts: storage.ServiceResolver.ResolveUploadContexts: no remaining providers")
	if got := normalizeSelectUploadTargetsError(legacyText); IsNoProviderCandidates(got) {
		t.Fatalf("legacy string = %T %v, must remain unclassified", got, got)
	}
}

func TestNormalizeOpenDataSetTargetErrorUsesTypedLifecycleError(t *testing.T) {
	t.Parallel()

	got := normalizeOpenDataSetTargetError(context.Background(), fmt.Errorf("open data set: %w", storage.ErrDataSetUnavailable))
	if !IsDataSetServiceEnded(got) {
		t.Fatalf("error = %T %v, want DataSetServiceEndedError", got, got)
	}
	if !errors.Is(got, storage.ErrDataSetUnavailable) {
		t.Fatalf("error = %T %v, want wrapped ErrDataSetUnavailable", got, got)
	}
}

func TestNormalizeOpenTargetErrorsUseTypedProviderNotFound(t *testing.T) {
	t.Parallel()

	for name, normalize := range map[string]func(context.Context, error) error{
		"provider": normalizeOpenProviderTargetError,
		"data set": normalizeOpenDataSetTargetError,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := normalize(context.Background(), fmt.Errorf("open target: %w", spregistry.ErrNotFound))
			if !IsProviderUnavailable(got) {
				t.Fatalf("error = %T %v, want ProviderUnavailableError", got, got)
			}
		})
	}
}

func TestNormalizeOpenDataSetTargetErrorPreservesInvalidArgument(t *testing.T) {
	t.Parallel()

	invalid := fmt.Errorf("provider mismatch: %w", storage.ErrInvalidArgument)
	got := normalizeOpenDataSetTargetError(context.Background(), invalid)
	if !errors.Is(got, storage.ErrInvalidArgument) || IsProviderUnavailable(got) || IsDataSetServiceEnded(got) {
		t.Fatalf("error = %T %v, want ordinary ErrInvalidArgument", got, got)
	}
}

func TestKnownTargetNetworkErrorsAreProviderUnavailable(t *testing.T) {
	t.Parallel()

	rpcFailure := &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", IsNotFound: true}}
	if got := normalizeOpenDataSetTargetError(context.Background(), rpcFailure); !IsProviderUnavailable(got) {
		t.Fatalf("open data set RPC error = %T %v, want ProviderUnavailableError", got, got)
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
