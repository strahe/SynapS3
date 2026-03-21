package worker

import (
	"testing"
	"time"
)

func TestRetryDelay_Exponential(t *testing.T) {
	// Expected approximate centres: 10s, 20s, 40s, 80s, 160s, 300s (capped).
	expected := []time.Duration{
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		80 * time.Second,
		160 * time.Second,
		300 * time.Second,
	}

	for i, want := range expected {
		got := retryDelay(i)
		lo := time.Duration(float64(want) * (1 - jitterFraction))
		hi := time.Duration(float64(want) * (1 + jitterFraction))
		if got < lo || got > hi {
			t.Errorf("retryDelay(%d) = %v; want in [%v, %v]", i, got, lo, hi)
		}
	}
}

func TestRetryDelay_MaxCap(t *testing.T) {
	upper := time.Duration(float64(maxBackoff) * (1 + jitterFraction))
	for _, rc := range []int{10, 20, 50, 100} {
		got := retryDelay(rc)
		if got > upper {
			t.Errorf("retryDelay(%d) = %v; exceeds max %v (with jitter %v)", rc, got, maxBackoff, upper)
		}
	}
}

func TestRetryDelay_Jitter(t *testing.T) {
	seen := make(map[time.Duration]bool)
	for range 100 {
		seen[retryDelay(3)] = true
	}
	if len(seen) < 2 {
		t.Errorf("expected jitter spread across runs, got %d unique values", len(seen))
	}
}

func TestRetryDelay_ZeroRetry(t *testing.T) {
	got := retryDelay(0)
	lo := time.Duration(float64(baseDelay) * (1 - jitterFraction))
	hi := time.Duration(float64(baseDelay) * (1 + jitterFraction))
	if got < lo || got > hi {
		t.Errorf("retryDelay(0) = %v; want in [%v, %v]", got, lo, hi)
	}
}

func TestRetryDelay_Floor(t *testing.T) {
	floor := time.Duration(float64(baseDelay) * (1 - jitterFraction))
	for rc := range 20 {
		got := retryDelay(rc)
		if got < floor {
			t.Errorf("retryDelay(%d) = %v; below floor %v", rc, got, floor)
		}
	}
}
