package provider

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"syscall"
	"time"

	"github.com/strahe/synapse-go/pdp"
)

const defaultHealthConcurrency = 10

// HealthResult is the outcome of one provider health probe. Status is
// "reachable", "unreachable", or "n/a" (no service URL). Detail explains a
// failed probe to operators and never includes the URL or response body.
type HealthResult struct {
	Status string
	Detail string
}

// HealthChecker performs provider health probes with a reusable HTTP client.
type HealthChecker struct {
	client *http.Client
	logger *slog.Logger
}

// NewHealthChecker creates a provider health checker.
func NewHealthChecker(client *http.Client) *HealthChecker {
	if client == nil {
		client = &http.Client{
			// A PDP ping redirect indicates a misconfigured provider endpoint.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return &HealthChecker{client: client, logger: slog.Default()}
}

// CheckHealth performs a PDP ping against the given service URL and returns
// "reachable", "unreachable", or "n/a" (for empty URLs).
func CheckHealth(ctx context.Context, serviceURL string, timeout time.Duration) string {
	return NewHealthChecker(nil).Check(ctx, serviceURL, timeout)
}

// Check performs a single provider health probe and returns its status.
func (h *HealthChecker) Check(ctx context.Context, serviceURL string, timeout time.Duration) string {
	return h.Probe(ctx, serviceURL, timeout).Status
}

// Probe performs a single provider health probe and explains a failure.
func (h *HealthChecker) Probe(ctx context.Context, serviceURL string, timeout time.Duration) HealthResult {
	if serviceURL == "" {
		return HealthResult{Status: "n/a", Detail: "Provider has no service URL"}
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	client, err := pdp.New(serviceURL, pdp.WithHTTPClient(h.client), pdp.WithMaxRetries(0))
	if err != nil {
		if h.logger != nil {
			h.logger.Warn("failed to create PDP health client", "error", err)
		}
		return HealthResult{Status: "unreachable", Detail: "Service URL is invalid"}
	}
	if err := client.Ping(ctx); err != nil {
		return HealthResult{Status: "unreachable", Detail: pingFailureDetail(err)}
	}
	return HealthResult{Status: "reachable"}
}

// pingFailureDetail classifies a failed PDP ping without exposing the URL,
// response body, or raw network error text.
func pingFailureDetail(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "Health check was interrupted"
	case errors.Is(err, context.DeadlineExceeded):
		return "Health check timed out"
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return "Health check timed out"
	}
	if httpErr, ok := errors.AsType[*pdp.HTTPError](err); ok {
		if httpErr.StatusCode >= http.StatusMultipleChoices && httpErr.StatusCode < http.StatusBadRequest {
			return fmt.Sprintf("Health check was redirected (HTTP %d)", httpErr.StatusCode)
		}
		return fmt.Sprintf("Health check returned HTTP %d", httpErr.StatusCode)
	}
	if errors.Is(err, pdp.ErrPingResponseMismatch) {
		return "Service URL did not answer as a PDP provider"
	}
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return "Service URL host could not be resolved"
	}
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return "Provider TLS certificate could not be verified"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "Provider refused the connection"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) {
		return "Provider closed the connection before replying"
	}
	return "Could not connect to the provider"
}

// CheckHealthBatch performs concurrent health checks on all providers in the slice,
// updating each provider's HealthStatus in place. The concurrency is bounded to
// avoid overwhelming the network.
func CheckHealthBatch(ctx context.Context, providers []ProviderDetail, timeout time.Duration) {
	if len(providers) == 0 {
		return
	}

	checker := NewHealthChecker(nil)
	sem := make(chan struct{}, defaultHealthConcurrency)
	var wg sync.WaitGroup

	for i := range providers {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			providers[idx].HealthStatus = checker.Check(ctx, providers[idx].ServiceURL, timeout)
		}(i)
	}

	wg.Wait()
}
