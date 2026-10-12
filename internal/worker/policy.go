package worker

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

var ErrInvalidPolicy = errors.New("invalid task execution policy")

type BackoffPolicy struct {
	InitialDelay time.Duration `json:"initial_delay"`
	Multiplier   float64       `json:"multiplier"`
	MaximumDelay time.Duration `json:"maximum_delay"`
	Jitter       float64       `json:"jitter"`
}

type ExecutionPolicy struct {
	MaxAttempts       int           `json:"max_attempts"`
	Backoff           BackoffPolicy `json:"backoff"`
	InvocationTimeout time.Duration `json:"invocation_timeout"`
	ObservationWindow time.Duration `json:"observation_window"`
}

func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{InitialDelay: 10 * time.Second, Multiplier: 2, MaximumDelay: 5 * time.Minute, Jitter: 0.2}
}

func (p ExecutionPolicy) validate() error {
	b := p.Backoff
	if p.MaxAttempts < 1 || p.InvocationTimeout < 0 || p.ObservationWindow < 0 || b.InitialDelay <= 0 || b.MaximumDelay < b.InitialDelay || math.IsNaN(b.Multiplier) || math.IsInf(b.Multiplier, 0) || b.Multiplier < 1 || math.IsNaN(b.Jitter) || math.IsInf(b.Jitter, 0) || b.Jitter < 0 || b.Jitter > 1 {
		return ErrInvalidPolicy
	}
	return nil
}

func (p BackoffPolicy) delay(retryCount int) time.Duration {
	base := math.Min(float64(p.MaximumDelay), float64(p.InitialDelay)*math.Pow(p.Multiplier, float64(retryCount)))
	return min(time.Duration(base*(1-p.Jitter+2*p.Jitter*rand.Float64())), p.MaximumDelay)
}
