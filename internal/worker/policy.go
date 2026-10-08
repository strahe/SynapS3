package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"time"

	"github.com/strahe/synaps3/internal/model"
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

// The budget has one persisted source: tasks.retry_limit.
type policySnapshot struct {
	Version           int           `json:"version"`
	Legacy            bool          `json:"legacy,omitempty"`
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

func encodePolicy(p ExecutionPolicy) (json.RawMessage, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(policySnapshot{Version: 1, Backoff: p.Backoff, InvocationTimeout: p.InvocationTimeout, ObservationWindow: p.ObservationWindow})
}

func readPolicySnapshot(raw json.RawMessage) (policySnapshot, error) {
	var snapshot policySnapshot
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return snapshot, ErrInvalidPolicy
	}
	for _, key := range []string{"version", "backoff", "invocation_timeout", "observation_window"} {
		if len(fields[key]) == 0 || bytes.Equal(fields[key], []byte("null")) {
			return snapshot, ErrInvalidPolicy
		}
	}
	var backoff map[string]json.RawMessage
	if err := json.Unmarshal(fields["backoff"], &backoff); err != nil {
		return snapshot, ErrInvalidPolicy
	}
	for _, key := range []string{"initial_delay", "multiplier", "maximum_delay", "jitter"} {
		if len(backoff[key]) == 0 || bytes.Equal(backoff[key], []byte("null")) {
			return snapshot, ErrInvalidPolicy
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&snapshot); err != nil {
		return snapshot, fmt.Errorf("%w: %v", ErrInvalidPolicy, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return snapshot, ErrInvalidPolicy
	}
	return snapshot, nil
}

func DecodePolicy(task *model.Task) (ExecutionPolicy, error) {
	if task == nil || task.RetryLimit == nil || *task.RetryLimit < 0 {
		return ExecutionPolicy{}, ErrInvalidPolicy
	}
	snapshot, err := readPolicySnapshot(task.Policy)
	if err != nil {
		return ExecutionPolicy{}, err
	}
	if snapshot.Version != 1 || snapshot.Legacy {
		return ExecutionPolicy{}, fmt.Errorf("%w: unsupported snapshot", ErrInvalidPolicy)
	}
	p := ExecutionPolicy{MaxAttempts: *task.RetryLimit + 1, Backoff: snapshot.Backoff, InvocationTimeout: snapshot.InvocationTimeout, ObservationWindow: snapshot.ObservationWindow}
	if err := p.validate(); err != nil {
		return ExecutionPolicy{}, err
	}
	return p, nil
}

func legacyPolicy(task *model.Task) bool {
	snapshot, err := readPolicySnapshot(task.Policy)
	return err == nil && snapshot.Version == 0 && snapshot.Legacy
}

func (p BackoffPolicy) delay(retryCount int) time.Duration {
	base := math.Min(float64(p.MaximumDelay), float64(p.InitialDelay)*math.Pow(p.Multiplier, float64(retryCount)))
	return min(time.Duration(base*(1-p.Jitter+2*p.Jitter*rand.Float64())), p.MaximumDelay)
}
