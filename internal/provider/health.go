package provider

import (
	"context"
	"net/http"
	"sync"
	"time"
)

const defaultHealthConcurrency = 10

// HealthChecker performs provider health probes with a reusable HTTP client.
type HealthChecker struct {
	client *http.Client
}

// NewHealthChecker creates a provider health checker.
func NewHealthChecker(client *http.Client) *HealthChecker {
	if client == nil {
		client = &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return &HealthChecker{client: client}
}

// CheckHealth performs an HTTP HEAD request to the given service URL and returns
// "reachable", "unreachable", or "n/a" (for empty URLs).
func CheckHealth(ctx context.Context, serviceURL string, timeout time.Duration) string {
	return NewHealthChecker(nil).Check(ctx, serviceURL, timeout)
}

// Check performs a single provider health probe.
func (h *HealthChecker) Check(ctx context.Context, serviceURL string, timeout time.Duration) string {
	if serviceURL == "" {
		return "n/a"
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, serviceURL, nil)
	if err != nil {
		return "unreachable"
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return "unreachable"
	}
	_ = resp.Body.Close()
	return "reachable"
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
