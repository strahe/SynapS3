package synapse

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/strahe/synapse-go/pdp"
)

const providerErrorResponseLimit = pdp.MaxControlResponseBytes

func readProviderHTTPError(req *http.Request, resp *http.Response) error {
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, providerErrorResponseLimit+1))
	truncated := len(body) > providerErrorResponseLimit
	if truncated {
		body = body[:providerErrorResponseLimit]
	}
	httpErr := &pdp.HTTPError{Method: req.Method, URL: req.URL.String(), StatusCode: resp.StatusCode, Body: string(body)}
	if readErr != nil {
		return errors.Join(httpErr, fmt.Errorf("could not read complete provider response: %w", readErr))
	}
	if truncated {
		return errors.Join(httpErr, fmt.Errorf("provider response exceeded %d bytes; remaining body not read", providerErrorResponseLimit))
	}
	return httpErr
}
