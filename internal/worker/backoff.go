package worker

import (
	"math"
	"math/rand/v2"
	"time"
)

const (
	baseDelay      = 10 * time.Second
	maxBackoff     = 5 * time.Minute
	jitterFraction = 0.20
)

// retryDelay computes an exponential backoff delay with jitter.
// Formula: min(base * 2^retryCount + jitter, maxBackoff)
// where jitter is ±20% of the computed delay.
func retryDelay(retryCount int) time.Duration {
	delay := float64(baseDelay) * math.Pow(2, float64(retryCount))
	if delay > float64(maxBackoff) {
		delay = float64(maxBackoff)
	}

	// Apply jitter: ±jitterFraction of the computed delay.
	jitter := delay * jitterFraction * (2*rand.Float64() - 1)
	delay += jitter

	if delay < float64(baseDelay)*(1-jitterFraction) {
		delay = float64(baseDelay) * (1 - jitterFraction)
	}

	return time.Duration(delay)
}
