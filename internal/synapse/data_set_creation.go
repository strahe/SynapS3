package synapse

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/strahe/synapse-go/pdp"
)

// DataSetProviderRejectionError is a refusal of the initial creation POST,
// before the provider reported a submitted transaction.
type DataSetProviderRejectionError struct {
	StatusCode int
	Cause      error
}

func (e *DataSetProviderRejectionError) Error() string { return e.Cause.Error() }
func (e *DataSetProviderRejectionError) Unwrap() error { return e.Cause }

func normalizeCreateDataSetError(ctx context.Context, err error) error {
	if response, ok := errors.AsType[*pdp.HTTPError](err); ok && response.Method == http.MethodPost {
		endpoint, parseErr := url.Parse(response.URL)
		if parseErr == nil && strings.HasSuffix(endpoint.Path, "/pdp/data-sets") &&
			(response.StatusCode == 400 || response.StatusCode == 401 || response.StatusCode == 403) {
			return &DataSetProviderRejectionError{StatusCode: response.StatusCode, Cause: err}
		}
	}
	return NormalizeProviderOperationError(ctx, err)
}
