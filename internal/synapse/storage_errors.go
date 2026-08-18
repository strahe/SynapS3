package synapse

import (
	"context"
	"errors"
	"fmt"
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

func providerOperationUnavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var httpErr *pdp.HTTPError
	if errors.As(err, &httpErr) {
		return providerHTTPStatusUnavailable(httpErr.StatusCode)
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var operationErr *net.OpError
	if errors.As(err, &operationErr) {
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
	var ended *DataSetServiceEndedError
	if errors.As(err, &ended) {
		return true
	}
	var notLive *warmstorage.DataSetNotLiveError
	return errors.As(err, &notLive) || errors.Is(err, warmstorage.ErrNotFound)
}

func normalizeDataSetLifecycleError(err error) error {
	if err == nil || IsNoProviderCandidates(err) {
		return err
	}
	var ended *DataSetServiceEndedError
	if errors.As(err, &ended) {
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
	var unavailable *ProviderUnavailableError
	if errors.As(err, &unavailable) {
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

func normalizeCreateContextsError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrNoHealthyProviders) {
		return &NoProviderCandidatesError{Cause: err}
	}
	switch err.Error() {
	case "storage.Service.CreateContexts: storage.ServiceResolver.ResolveUploadContexts: no approved providers",
		"storage.Service.CreateContexts: storage.ServiceResolver.ResolveUploadContexts: no remaining providers":
		return &NoProviderCandidatesError{Cause: err}
	default:
		return normalizeResolutionOperationError(err)
	}
}

func normalizeCreateContextError(err error, opts *storage.CreateContextOptions) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrNoHealthyProviders) {
		if opts != nil && opts.DataSetID != nil {
			return &ProviderUnavailableError{Cause: err}
		}
		return &NoProviderCandidatesError{Cause: err}
	}
	switch err.Error() {
	case "storage.Service.CreateContext: storage.ServiceResolver.ResolveUploadContexts: no approved providers",
		"storage.Service.CreateContext: storage.ServiceResolver.ResolveUploadContexts: no remaining providers":
		if opts != nil && opts.DataSetID != nil {
			return &ProviderUnavailableError{Cause: err}
		}
		return &NoProviderCandidatesError{Cause: err}
	}
	if opts != nil && opts.DataSetID != nil {
		missing := fmt.Sprintf(
			"storage.Service.CreateContext: storage.ServiceResolver.ResolveUploadContexts: data set %s does not exist",
			opts.DataSetID.String(),
		)
		if err.Error() == missing {
			return &DataSetServiceEndedError{Cause: err}
		}
	}
	if opts != nil && opts.ProviderID != nil && opts.DataSetID != nil {
		missing := fmt.Sprintf(
			"storage.Service.CreateContext: storage.ServiceResolver.ResolveUploadContexts: provider %s for data set %s not found",
			opts.ProviderID.String(),
			opts.DataSetID.String(),
		)
		if err.Error() == missing {
			return &ProviderUnavailableError{Cause: err}
		}
	}
	if opts != nil && opts.ProviderID != nil && errors.Is(err, spregistry.ErrNotFound) {
		return &ProviderUnavailableError{Cause: err}
	}
	return normalizeResolutionOperationError(err)
}
