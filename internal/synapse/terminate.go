package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

const providerTerminationWaitTimeout = 30 * time.Second

type storageServiceTerminator interface {
	TerminateService(context.Context, sdktypes.BigInt, *storage.TerminateServiceOptions) (*storage.TerminateServiceResult, error)
}

// TerminationBlockedError means the service cannot be terminated until an
// operator resolves something the gateway must not decide on its own, such as
// settling outstanding payment debt. It is never retried automatically.
type TerminationBlockedError struct {
	Reason    string
	Shortfall *big.Int
	Err       error
}

func (e *TerminationBlockedError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Shortfall != nil {
		return fmt.Sprintf("service termination blocked (%s): outstanding amount %s", e.Reason, e.Shortfall)
	}
	return fmt.Sprintf("service termination blocked (%s)", e.Reason)
}

func (e *TerminationBlockedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsTerminationBlocked reports whether termination needs operator action rather
// than another attempt.
func IsTerminationBlocked(err error) bool {
	var blocked *TerminationBlockedError
	return errors.As(err, &blocked)
}

// TerminateService ends the storage service for one data set. It is the
// destructive boundary of provider replacement and must only be called after
// the retirement safety gate passes.
//
// Termination is relayed through the provider first. A provider-side failure
// falls back to the direct FWSS transaction path; a pending relay does not,
// because the provider has already accepted it. An already-terminated service
// reports its recorded end epoch rather than failing.
func (s *StorageServiceAdapter) TerminateService(ctx context.Context, dataSetID sdktypes.BigInt) (*TerminationResult, error) {
	if s == nil || s.terminator == nil {
		return nil, errors.New("storage service terminator is not configured")
	}
	res, err := s.terminator.TerminateService(ctx, dataSetID, &storage.TerminateServiceOptions{
		ProviderWaitTimeout: providerTerminationWaitTimeout,
	})
	if err != nil {
		providerErr := normalizeTerminationError(ctx, err)
		if terminationPending(err) || ctx.Err() != nil {
			return nil, providerErr
		}
		res, err = s.terminator.TerminateService(ctx, dataSetID, &storage.TerminateServiceOptions{SkipProvider: true})
		if err != nil {
			directErr := NormalizeProviderOperationError(ctx, err)
			return nil, fmt.Errorf("direct termination failed after provider relay error (%v): %w", providerErr, directErr)
		}
	}
	if res == nil {
		return nil, errors.New("storage service returned no termination result")
	}
	out := &TerminationResult{EndEpoch: int64(res.EndEpoch)}
	if res.TxHash != nil {
		out.TxHash = res.TxHash.Hex()
	}
	return out, nil
}

func terminationPending(err error) bool {
	var pending *pdp.TerminateServicePendingError
	return errors.As(err, &pending)
}

func normalizeTerminationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var debt *storage.TerminateServiceDebtError
	if errors.As(err, &debt) {
		return &TerminationBlockedError{Reason: "payment_debt", Shortfall: debt.Shortfall, Err: err}
	}
	// The provider accepted the request but has not published it yet. That
	// resolves on its own, so it is a dependency wait rather than a failure.
	var pending *pdp.TerminateServicePendingError
	if errors.As(err, &pending) {
		return &ProviderUnavailableError{Cause: err}
	}
	return NormalizeProviderOperationError(ctx, err)
}
