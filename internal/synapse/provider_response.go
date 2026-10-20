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
	if len(body) > providerErrorResponseLimit {
		body = body[:providerErrorResponseLimit]
		body = append(body, []byte(fmt.Sprintf("\n[provider response exceeded %d bytes; remaining body not read]", providerErrorResponseLimit))...)
	}
	httpErr := &pdp.HTTPError{Method: req.Method, URL: req.URL.String(), StatusCode: resp.StatusCode, Body: string(body)}
	if readErr != nil {
		return errors.Join(httpErr, fmt.Errorf("could not read complete provider response: %w", readErr))
	}
	return httpErr
}
