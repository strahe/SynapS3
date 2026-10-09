package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/model"
)

func TestRegistryRequiresWorkStartPolicy(t *testing.T) {
	for _, policy := range []WorkStartPolicy{0, 255} {
		definition := testDefinition(nil, false)
		definition.WorkStart = policy
		if err := NewRegistry().Register(scriptedHandler{definition: definition}); err == nil {
			t.Fatalf("registered invalid work start policy %d", policy)
		}
	}
}

func TestWorkStartsAtDeclaredBoundaryAndSurvivesRecovery(t *testing.T) {
	for _, policy := range []WorkStartPolicy{WorkStartOnHandler, WorkStartOnEffect} {
		t.Run(map[WorkStartPolicy]string{WorkStartOnHandler: "handler", WorkStartOnEffect: "effect"}[policy], func(t *testing.T) {
			definition := testDefinition(nil, true)
			definition.WorkStart = policy
			var harness taskHarness
			var startedAt time.Time
			handler := scriptedHandler{
				definition: definition,
				execute: func(ctx context.Context, execution Execution) Result {
					if err := execution.WriteCheckpoint(ctx, map[string]bool{"prepared": true}); err != nil {
						t.Fatal(err)
					}
					prepared, err := harness.repos.Tasks.GetByID(ctx, execution.ID())
					if err != nil || (prepared.WorkStartedAt != nil) != (policy == WorkStartOnHandler) {
						t.Fatalf("start before actual effect = %#v, err=%v", prepared, err)
					}
					ready := time.Now()
					attempted, err := execution.WithCheckpointedEffect(ctx, ResourceProviderMutation, "test-effect", map[string]bool{"attempted": true}, nil, func(ctx context.Context) error {
						stored, err := harness.repos.Tasks.GetByID(ctx, execution.ID())
						if err != nil || stored.WorkStartedAt == nil {
							t.Fatalf("start during effect = %#v, err=%v", stored, err)
						}
						startedAt = *stored.WorkStartedAt
						if policy == WorkStartOnEffect && startedAt.Before(ready) {
							t.Fatal("effect timing included preparation")
						}
						return errors.New("effect needs recovery")
					})
					if !attempted || err == nil {
						t.Fatalf("effect = %v, %v", attempted, err)
					}
					return Retry(err, "effect_uncertain", 0, nil)
				},
				recover: func(context.Context, Execution) Result { return Complete("recovered", nil) },
			}
			harness = newTaskHarness(t, handler, nil)
			row := enqueueTestTask(t, harness, "work-boundary", "work-boundary")
			harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
			harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
			stored, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
			if err != nil || stored.Status != model.TaskStatusCompleted || stored.WorkStartedAt == nil || !stored.WorkStartedAt.Equal(startedAt) {
				t.Fatalf("recovery reset work start: %#v, err=%v", stored, err)
			}
		})
	}
}

func TestRecurringWorkStartBelongsToIndependentCycles(t *testing.T) {
	definition := testDefinition(nil, true)
	definition.Type = model.TaskTypeObservabilityRefresh
	definition.WorkStart = WorkStartOnHandler
	calls := 0
	h := newTaskHarness(t, scriptedHandler{definition: definition, execute: func(context.Context, Execution) Result {
		calls++
		if calls == 1 {
			return ResourceWait("Waiting for capacity")
		}
		return CompleteCycle(time.Hour, "Done", nil)
	}}, nil)
	if err := h.service.bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	row, _ := h.repos.Tasks.GetByIdentity(t.Context(), definition.Type, "test-cycle:1")
	h.engine.resourceWaitDelay = func(int) time.Duration { return 0 }
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	waiting, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
	if waiting.WorkStartedAt == nil {
		t.Fatal("Wait lost work start")
	}
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	finished, _ := h.repos.Tasks.GetByID(t.Context(), row.ID)
	if finished.Status != model.TaskStatusCompleted || !finished.WorkStartedAt.Equal(*waiting.WorkStartedAt) {
		t.Fatal("cycle overwrote work timing")
	}
	schedule, _ := h.repos.TaskSchedules.GetForUpdate(t.Context(), "test-cycle")
	if err := h.repos.TaskSchedules.ScheduleNext(t.Context(), schedule.Key, row.ID, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := h.service.dispatchSchedules(t.Context()); err != nil {
		t.Fatal(err)
	}
	next, _ := h.repos.Tasks.GetByIdentity(t.Context(), definition.Type, "test-cycle:2")
	if next == nil || next.WorkStartedAt != nil || next.RetryCount != 0 || next.ID <= row.ID {
		t.Fatalf("new cycle: %#v", next)
	}
}
