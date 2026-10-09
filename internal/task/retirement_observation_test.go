package task_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/pdp"
	sdktypes "github.com/strahe/synapse-go/types"
)

type observingRetirementTerminator struct {
	testServiceTerminator
	observations atomic.Int64
	pending      bool
	observeErr   error
}

func (t *observingRetirementTerminator) ObserveTermination(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, bool, error) {
	t.observations.Add(1)
	return nil, t.pending, t.observeErr
}

func TestRetirementObservationPreservesSafeTerminationAndPendingWait(t *testing.T) {
	tests := []struct {
		name         string
		pending      bool
		observeErr   error
		wantRequests int64
		wantRetries  int
		wantFailed   bool
		minimumDelay time.Duration
	}{
		{name: "provider observation unavailable", observeErr: errors.Join(synapse.ErrTerminationObservationUnavailable, &synapse.ProviderUnavailableError{Cause: errors.New("provider unavailable")}), wantRequests: 1},
		{name: "no provider request", wantRequests: 1},
		{name: "pending provider relay", pending: true},
		{name: "backpressure", observeErr: &synapse.ProviderUnavailableError{Cause: &pdp.HTTPError{StatusCode: 429, RetryAfter: time.Hour}}, wantRetries: 1, minimumDelay: time.Hour},
		{name: "chain state unknown", observeErr: errors.New("reading storage service state: RPC unavailable"), wantRetries: 2, wantFailed: true},
		{name: "different payer", observeErr: synapse.ErrServicePaidByAnother, wantRetries: 2, wantFailed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			terminator := &observingRetirementTerminator{
				testServiceTerminator: testServiceTerminator{result: &synapse.TerminationResult{EndEpoch: 84}},
				pending:               tt.pending,
				observeErr:            tt.observeErr,
			}
			maxAttempts := 3
			runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true, maxAttempts: &maxAttempts})
			_, taskRow := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
			started := time.Now()
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)

			waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
				wantStatus := model.TaskStatusPending
				if tt.wantFailed {
					wantStatus = model.TaskStatusFailed
				}
				return terminator.observations.Load() > 0 && task.Status == wantStatus && task.RetryCount == tt.wantRetries &&
					(tt.wantRequests == 0 || task.Checkpoint != nil) &&
					(tt.minimumDelay == 0 || !task.AvailableAt.Before(started.Add(tt.minimumDelay)))
			})
			if got := terminator.calls.Load(); got != tt.wantRequests {
				t.Fatalf("termination requests = %d, want %d", got, tt.wantRequests)
			}
			if tt.wantRequests > 0 {
				wakeTask(t, runtime, taskRow.ID)
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
				if got := terminator.calls.Load(); got != tt.wantRequests {
					t.Fatalf("termination requests after completion = %d, want %d", got, tt.wantRequests)
				}
			}
		})
	}
}
