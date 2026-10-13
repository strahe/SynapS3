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
	MaxAttempts         int            `json:"max_attempts"`
	Backoff             BackoffPolicy  `json:"backoff"`
	ResendBackoff       *BackoffPolicy `json:"resend_backoff,omitempty"`
	InvocationTimeout   time.Duration  `json:"invocation_timeout"`
	ObservationWindow   time.Duration  `json:"observation_window"`
	ObservationInterval time.Duration  `json:"observation_interval,omitempty"`
}

func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{InitialDelay: 10 * time.Second, Multiplier: 2, MaximumDelay: 5 * time.Minute, Jitter: 0.2}
}

func (p ExecutionPolicy) validate() error {
	if p.MaxAttempts < 1 || p.InvocationTimeout < 0 || p.ObservationWindow < 0 || p.ObservationInterval < 0 || !p.Backoff.valid() || (p.ResendBackoff != nil && (!p.ResendBackoff.valid() || p.ResendBackoff.Jitter != 0)) {
		return ErrInvalidPolicy
	}
	return nil
}

func (p ExecutionPolicy) clone() ExecutionPolicy {
	if p.ResendBackoff != nil {
		resend := *p.ResendBackoff
		p.ResendBackoff = &resend
	}
	return p
}

func (p BackoffPolicy) valid() bool {
	return p.InitialDelay > 0 && p.MaximumDelay >= p.InitialDelay && !math.IsNaN(p.Multiplier) && !math.IsInf(p.Multiplier, 0) && p.Multiplier >= 1 && !math.IsNaN(p.Jitter) && !math.IsInf(p.Jitter, 0) && p.Jitter >= 0 && p.Jitter <= 1
}

func (p BackoffPolicy) baseDelay(retryCount int) float64 {
	return math.Min(float64(p.MaximumDelay), float64(p.InitialDelay)*math.Pow(p.Multiplier, float64(retryCount)))
}

func (p BackoffPolicy) delay(retryCount int) time.Duration {
	base := p.baseDelay(retryCount)
	return min(time.Duration(base*(1-p.Jitter+2*p.Jitter*rand.Float64())), p.MaximumDelay)
}
