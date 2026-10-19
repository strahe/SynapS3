package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
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

func TestRetirementObservationRateLimitCanResumeBeforeAnySend(t *testing.T) {
	terminator := &observingRetirementTerminator{
		testServiceTerminator: testServiceTerminator{result: &synapse.TerminationResult{EndEpoch: 84}},
		observeErr:            &pdp.HTTPError{StatusCode: 429, RetryAfter: time.Hour},
	}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{terminator: terminator, epochs: testEpochReader{epoch: 90}, retireOnly: true})
	_, row := seedAbandonedTargetRetirement(t, runtime, storedObjectSequence.Add(1))
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	var checkpoint struct {
		storagepipeline.ExecutionIdentity
		Sends                     int       `json:"sends,omitempty"`
		AttemptedAt               time.Time `json:"attempted_at"`
		ProtocolNextObservationAt time.Time `json:"protocol_next_observation_at,omitzero"`
	}
	if err := json.Unmarshal(row.Checkpoint, &checkpoint); err != nil || !checkpoint.Complete() || checkpoint.Sends != 0 || !checkpoint.AttemptedAt.IsZero() || checkpoint.ProtocolNextObservationAt.IsZero() || terminator.calls.Load() != 0 {
		t.Fatalf("observation throttle changed send evidence: checkpoint=%s requests=%d err=%v", row.Checkpoint, terminator.calls.Load(), err)
	}
	checkpoint.ProtocolNextObservationAt = time.Now().UTC().Add(-time.Second)
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw("UPDATE tasks SET checkpoint_json = ?, available_at = ? WHERE id = ?", json.RawMessage(raw), time.Now().Add(-time.Second), row.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	terminator.observeErr = nil
	terminator.pending = true
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if terminator.observations.Load() != 2 || terminator.calls.Load() != 0 || row.WaitReason == nil || *row.WaitReason != "provider_confirmation" {
		t.Fatalf("rate-limited observation could not resume: task=%#v observations=%d requests=%d", row, terminator.observations.Load(), terminator.calls.Load())
	}
	terminator.pending = false
	wakeTask(t, runtime, row.ID)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if row.ResumeMode != model.TaskResumeModeExecute || terminator.calls.Load() != 0 {
		t.Fatalf("pure observation was treated as a sent request: %#v requests=%d", row, terminator.calls.Load())
	}
	wakeTask(t, runtime, row.ID)
	row = runOneStorageTask(t, runtime, row, model.TaskStatusPending)
	if terminator.calls.Load() != 1 {
		t.Fatalf("first retirement request was not admitted once: task=%#v requests=%d", row, terminator.calls.Load())
	}
}

func (t *observingRetirementTerminator) ObserveTermination(context.Context, sdktypes.BigInt) (*synapse.TerminationResult, bool, error) {
	t.observations.Add(1)
	return nil, t.pending, t.observeErr
}

func TestRetirementObservationPreservesSafeTerminationAndProtocolWait(t *testing.T) {
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
		{name: "backpressure", observeErr: &synapse.ProviderUnavailableError{Cause: &pdp.HTTPError{StatusCode: 429, RetryAfter: time.Hour}}, minimumDelay: time.Hour},
		{name: "unavailable with Retry-After", observeErr: errors.Join(synapse.ErrTerminationObservationUnavailable, &pdp.HTTPError{StatusCode: 503, RetryAfter: time.Hour}), minimumDelay: time.Hour},
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

			settled := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
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
			if tt.minimumDelay > 0 {
				deadline, observations := settled.AvailableAt, terminator.observations.Load()
				wakeTask(t, runtime, taskRow.ID)
				waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
					return task.Status == model.TaskStatusPending && task.ClaimGeneration > settled.ClaimGeneration && task.AvailableAt.Equal(deadline)
				})
				if terminator.observations.Load() != observations || terminator.calls.Load() != 0 {
					t.Fatal("early wake bypassed the provider observation deadline")
				}
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
