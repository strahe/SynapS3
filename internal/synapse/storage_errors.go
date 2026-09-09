package synapse

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/spregistry"
	"github.com/strahe/synapse-go/storage"
	"github.com/strahe/synapse-go/warmstorage"
)

// NoProviderCandidatesError reports that automatic selection cannot currently
// satisfy a newly authorized replica slot.
type NoProviderCandidatesError struct {
	Cause error
}

func (e *NoProviderCandidatesError) Error() string {
	if e == nil || e.Cause == nil {
		return "no storage provider candidates"
	}
	return e.Cause.Error()
}

func (e *NoProviderCandidatesError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// ProviderUnavailableError reports a recoverable provider or transport
// dependency failure.
type ProviderUnavailableError struct {
	Cause error
}

func (e *ProviderUnavailableError) Error() string {
	if e == nil || e.Cause == nil {
		return "storage provider unavailable"
	}
	return e.Cause.Error()
}

func (e *ProviderUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// DataSetServiceEndedError reports typed evidence that an established data
// set no longer represents a writable service.
type DataSetServiceEndedError struct {
	Cause error
}

type PullErrorDisposition uint8

const (
	PullErrorUnknown PullErrorDisposition = iota
	PullErrorRetryable
	PullErrorTerminal
)

// ErrProviderTransactionRejected is the adapter-level identity for a provider
// transaction that reached a terminal rejected state.
var ErrProviderTransactionRejected = pdp.ErrTxRejected

func (e *DataSetServiceEndedError) Error() string {
	if e == nil || e.Cause == nil {
		return "storage data set service ended"
	}
	return e.Cause.Error()
}

func (e *DataSetServiceEndedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func IsNoProviderCandidates(err error) bool {
	var target *NoProviderCandidatesError
	return errors.As(err, &target)
}

func IsProviderUnavailable(err error) bool {
	var unavailable *ProviderUnavailableError
	return errors.As(err, &unavailable)
}

// ClassifyPullError keeps SDK-specific pull failures at the adapter boundary.
// Provider and caller interruptions remain retryable; deterministic request or
// provider rejection errors are terminal. Unknown errors use the task's bounded
// retry budget so a newly introduced SDK error cannot create a permanent loop.
func ClassifyPullError(err error) PullErrorDisposition {
	if err == nil {
		return PullErrorUnknown
	}
	if IsProviderUnavailable(err) || errors.Is(err, context.Canceled) || providerOperationUnavailable(err) {
		return PullErrorRetryable
	}
	if errors.Is(err, pdp.ErrPullFailed) || errors.Is(err, storage.ErrInvalidArgument) || IsDataSetServiceEnded(err) {
		return PullErrorTerminal
	}
	if httpErr, ok := errors.AsType[*pdp.HTTPError](err); ok {
		if providerHTTPStatusUnavailable(httpErr.StatusCode) {
			return PullErrorRetryable
		}
		if httpErr.StatusCode >= http.StatusBadRequest && httpErr.StatusCode < http.StatusInternalServerError {
			return PullErrorTerminal
		}
	}
	return PullErrorUnknown
}

func providerOperationUnavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if httpErr, ok := errors.AsType[*pdp.HTTPError](err); ok {
		return providerHTTPStatusUnavailable(httpErr.StatusCode)
	}
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return true
	}
	if _, ok := errors.AsType[*net.OpError](err); ok {
		return true
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) && networkErr.Timeout()
}

func providerHTTPStatusUnavailable(statusCode int) bool {
	return statusCode == http.StatusRequestTimeout ||
		statusCode == http.StatusTooEarly ||
		statusCode == http.StatusTooManyRequests ||
		statusCode >= http.StatusInternalServerError
}

func IsDataSetServiceEnded(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := errors.AsType[*DataSetServiceEndedError](err); ok {
		return true
	}
	var notLive *warmstorage.DataSetNotLiveError
	return errors.As(err, &notLive) || errors.Is(err, warmstorage.ErrNotFound) || errors.Is(err, storage.ErrDataSetUnavailable)
}

// IsDataSetWriteBlocked reports whether err means the data set can no longer
// accept writes because its PDP payment ended. The SDK raises it while
// validating the data set, before the provider is contacted, so a commit that
// fails this way never reached the provider.
func IsDataSetWriteBlocked(err error) bool {
	if err == nil {
		return false
	}
	_, ok := errors.AsType[*storage.DataSetPDPPaymentTerminatedError](err)
	return ok
}

func normalizeDataSetLifecycleError(err error) error {
	if err == nil || IsNoProviderCandidates(err) {
		return err
	}
	if _, ok := errors.AsType[*DataSetServiceEndedError](err); ok {
		return err
	}
	if IsDataSetServiceEnded(err) {
		return &DataSetServiceEndedError{Cause: err}
	}
	return err
}

// NormalizeProviderOperationError classifies failures from a known provider
// endpoint. Callers must not use it for chain RPC or mixed resolution work.
func NormalizeProviderOperationError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return err
	}
	err = normalizeDataSetLifecycleError(err)
	if IsNoProviderCandidates(err) || IsDataSetServiceEnded(err) {
		return err
	}
	if _, ok := errors.AsType[*ProviderUnavailableError](err); ok {
		return err
	}
	if providerOperationUnavailable(err) {
		return &ProviderUnavailableError{Cause: err}
	}
	return err
}

func normalizeResolutionOperationError(err error) error {
	err = normalizeDataSetLifecycleError(err)
	if err == nil || IsNoProviderCandidates(err) || IsDataSetServiceEnded(err) || IsProviderUnavailable(err) {
		return err
	}
	var httpErr *pdp.HTTPError
	if errors.As(err, &httpErr) && providerHTTPStatusUnavailable(httpErr.StatusCode) {
		return &ProviderUnavailableError{Cause: err}
	}
	return err
}

func normalizeSelectUploadTargetsError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrInsufficientUploadContexts) ||
		errors.Is(err, storage.ErrNoHealthyProviders) ||
		errors.Is(err, storage.ErrNoEndorsedProvider) ||
		errors.Is(err, storage.ErrEndorsementsNotConfigured) {
		return &NoProviderCandidatesError{Cause: err}
	}
	return normalizeResolutionOperationError(err)
}

func normalizeOpenProviderTargetError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, spregistry.ErrNotFound) {
		return &ProviderUnavailableError{Cause: err}
	}
	return NormalizeProviderOperationError(ctx, err)
}

func normalizeOpenDataSetTargetError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	err = normalizeDataSetLifecycleError(err)
	if IsDataSetServiceEnded(err) {
		return err
	}
	if errors.Is(err, spregistry.ErrNotFound) {
		return &ProviderUnavailableError{Cause: err}
	}
	return NormalizeProviderOperationError(ctx, err)
}
