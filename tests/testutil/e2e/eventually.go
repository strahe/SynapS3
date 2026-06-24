package e2e

import (
	"context"
	"testing"
	"time"
)

type EventuallyOption func(*eventuallyOptions)

type eventuallyOptions struct {
	interval time.Duration
}

func WithPollInterval(interval time.Duration) EventuallyOption {
	return func(opts *eventuallyOptions) {
		if interval > 0 {
			opts.interval = interval
		}
	}
}

func Eventually[T any](
	t testing.TB,
	ctx context.Context,
	timeout time.Duration,
	description string,
	poll func(context.Context) (T, bool, error),
	options ...EventuallyOption,
) T {
	t.Helper()
	opts := eventuallyOptions{interval: 100 * time.Millisecond}
	for _, option := range options {
		option(&opts)
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(opts.interval)
	defer ticker.Stop()
	started := time.Now()
	var last T
	var lastErr error
	for {
		value, ready, err := poll(waitCtx)
		last, lastErr = value, err
		if err == nil && ready {
			return value
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("timed out waiting for %s:\n%s", description, DiagnosticValue(NewWaitSnapshot(description, started, last, lastErr)))
		case <-ticker.C:
		}
	}
}
