package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

const providerTerminationWaitTimeout = 30 * time.Second

type storageServiceTerminator interface {
	TerminateService(context.Context, sdktypes.BigInt, *storage.TerminateServiceOptions) (*storage.TerminateServiceResult, error)
}

// dataSetStateReader reads a data set's FWSS service state from the chain.
type dataSetStateReader interface {
	GetDataSet(context.Context, sdktypes.BigInt) (*warmstorage.DataSetInfo, error)
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

// ErrServicePaidByAnother means the chain records the data set as paid for by a
// wallet other than the one signing. Under this configuration its number names
// someone else's service, so it is never terminated; an operator has to put the
// original wallet or network back.
var ErrServicePaidByAnother = errors.New("storage service is paid for by another wallet")

// TerminateService ends the storage service for one data set. It is the
// destructive boundary of provider replacement and must only be called after
// the retirement safety gate passes.
//
// The chain is read before every request: a service that already ended reports
// its recorded end epoch and nothing is sent, so calling this again after an
// unobserved outcome cannot end a service twice. Termination is relayed through
// the provider first. Payment debt and a pending relay are returned as they are;
// any other relay failure falls back to the direct FWSS transaction only while
// the chain still shows the service running.
func (s *StorageServiceAdapter) TerminateService(ctx context.Context, dataSetID sdktypes.BigInt) (*TerminationResult, error) {
	if s == nil || s.terminator == nil || s.dataSets == nil {
		return nil, errors.New("storage service terminator is not configured")
	}
	if ended, err := s.recordedTermination(ctx, dataSetID); err != nil || ended != nil {
		return ended, err
	}
	res, err := s.terminator.TerminateService(ctx, dataSetID, &storage.TerminateServiceOptions{
		ProviderWaitTimeout: providerTerminationWaitTimeout,
	})
	if err != nil {
		providerErr := normalizeTerminationError(ctx, err)
		if IsTerminationBlocked(providerErr) || terminationPending(err) || ctx.Err() != nil {
			return nil, providerErr
		}
		ended, readErr := s.recordedTermination(ctx, dataSetID)
		if readErr != nil {
			return nil, fmt.Errorf("provider termination failed (%v) and the service state is unknown: %w", providerErr, readErr)
		}
		if ended != nil {
			return ended, nil
		}
		res, err = s.terminator.TerminateService(ctx, dataSetID, &storage.TerminateServiceOptions{SkipProvider: true})
		if err != nil {
			directErr := NormalizeProviderOperationError(ctx, err)
			if ended, readErr := s.recordedTermination(ctx, dataSetID); readErr == nil && ended != nil {
				return ended, nil
			}
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

// VerifyServicePayer refuses a data set this wallet does not pay for. It only
// reads the chain, so retirement runs it before committing to a request.
func (s *StorageServiceAdapter) VerifyServicePayer(ctx context.Context, dataSetID sdktypes.BigInt) error {
	if s == nil || s.dataSets == nil {
		return errors.New("storage service terminator is not configured")
	}
	_, err := s.recordedTermination(ctx, dataSetID)
	return err
}

// recordedTermination returns the recorded end of a service the chain already
// shows terminated, or nil while the service is still running.
func (s *StorageServiceAdapter) recordedTermination(ctx context.Context, dataSetID sdktypes.BigInt) (*TerminationResult, error) {
	info, err := s.dataSets.GetDataSet(ctx, dataSetID)
	if err != nil {
		return nil, fmt.Errorf("reading storage service state: %w", err)
	}
	if info == nil {
		return nil, errors.New("reading storage service state: no data set record")
	}
	// The direct path carries no payer check of its own, so a record that
	// belongs to someone else is refused here rather than terminated.
	if s.identity.Payer != (common.Address{}) && info.Payer != s.identity.Payer {
		return nil, fmt.Errorf("reading storage service state: data set %s is paid for by %s, not %s: %w",
			dataSetID.String(), info.Payer.Hex(), s.identity.Payer.Hex(), ErrServicePaidByAnother)
	}
	if info.PDPEndEpoch == 0 {
		return nil, nil
	}
	return &TerminationResult{EndEpoch: int64(info.PDPEndEpoch)}, nil
}

func terminationPending(err error) bool {
	var pending *pdp.TerminateServicePendingError
	return errors.As(err, &pending)
}

func normalizeTerminationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if debt, ok := errors.AsType[*storage.TerminateServiceDebtError](err); ok {
		return &TerminationBlockedError{Reason: "payment_debt", Shortfall: debt.Shortfall, Err: err}
	}
	// The provider accepted the request but has not published it yet. That
	// resolves on its own, so it is a dependency wait rather than a failure.
	if _, ok := errors.AsType[*pdp.TerminateServicePendingError](err); ok {
		return &ProviderUnavailableError{Cause: err}
	}
	return NormalizeProviderOperationError(ctx, err)
}
