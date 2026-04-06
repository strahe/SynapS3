package provider

import (
	"context"
	"net/http"
	"sync"
	"time"
)

const defaultHealthConcurrency = 10

// CheckHealth performs an HTTP HEAD request to the given service URL and returns
// "reachable", "unreachable", or "n/a" (for empty URLs).
func CheckHealth(ctx context.Context, serviceURL string, timeout time.Duration) string {
	if serviceURL == "" {
		return "n/a"
	}

	client := &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, serviceURL, nil)
	if err != nil {
		return "unreachable"
	}

	resp, err := client.Do(req)
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
			providers[idx].HealthStatus = CheckHealth(ctx, providers[idx].ServiceURL, timeout)
		}(i)
	}

	wg.Wait()
}
