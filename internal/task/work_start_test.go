package task

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
					attempted, err := execution.WithCheckpointedEffect(ctx, ResourceProviderMutation, map[string]bool{"attempted": true}, nil, func(ctx context.Context) error {
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

func TestRecurringWorkStartResetsOnlyAfterSuccessfulCycle(t *testing.T) {
	definition := testDefinition(nil, false)
	definition.Type = model.TaskTypeGC
	definition.WorkStart = WorkStartOnHandler
	calls := 0
	harness := newTaskHarness(t, scriptedHandler{
		definition: definition,
		execute: func(context.Context, Execution) Result {
			calls++
			if calls == 1 {
				return ResourceWait("Waiting for capacity")
			}
			return Suspend(model.TaskResumeModeExecute, time.Hour, "scheduled", "", nil)
		},
	}, nil)
	row, _, err := harness.service.Enqueue(t.Context(), EnqueueRequest{Type: model.TaskTypeGC, IdempotencyKey: "cycle", Input: testInput{Value: "cycle"}})
	if err != nil {
		t.Fatal(err)
	}
	harness.engine.resourceWaitDelay = func(int) time.Duration { return 0 }
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
	waiting, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || waiting.WorkStartedAt == nil {
		t.Fatalf("same-cycle wait lost start: %#v, err=%v", waiting, err)
	}
	harness.engine.executeClaim(t.Context(), claimTestTask(t, harness))
	scheduled, err := harness.repos.Tasks.GetByID(t.Context(), row.ID)
	if err != nil || scheduled.WorkStartedAt != nil || scheduled.WaitReason == nil || *scheduled.WaitReason != "scheduled" {
		t.Fatalf("next cycle retained start: %#v, err=%v", scheduled, err)
	}
}
