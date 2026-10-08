package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func makeLegacyPending(t *testing.T, h taskHarness, task *model.Task, future time.Time) *model.Task {
	t.Helper()
	var snapshot map[string]any
	if err := json.Unmarshal(task.Policy, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot["version"] = 0
	snapshot["legacy"] = true
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.NewUpdate().Model((*model.TaskPayload)(nil)).Set("policy_json = ?", json.RawMessage(raw)).Set("checkpoint_json = ?", json.RawMessage(`{"submitted":true}`)).Where("task_id = ?", task.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.NewUpdate().Model((*model.Task)(nil)).Set("claim_generation = 3").Set("retry_count = 2").Set("available_at = ?", future).Set("cancellation_requested_at = ?", time.Now()).Set("cancellation_reason = ?", "requested before upgrade").Where("id = ?", task.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	stored, err := h.repos.Tasks.GetByID(t.Context(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}

func TestLegacyPendingHandoffPreservesFutureAndCancellation(t *testing.T) {
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true)}, nil)
	row := enqueueTestTask(t, h, "legacy", "legacy")
	future := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	source := makeLegacyPending(t, h, row, future)
	if err := h.service.bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	next, err := h.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if next == nil || next.RetryCount != 0 || !next.AvailableAt.Equal(future) || !next.CancellationRequested() || string(next.Checkpoint) != string(source.Checkpoint) || next.WorkStartedAt != nil {
		t.Fatalf("handoff successor: %#v", next)
	}
	if _, err := DecodePolicy(next); err != nil {
		t.Fatal(err)
	}
	original, _ := h.repos.Tasks.GetByID(t.Context(), source.ID)
	if original.Status != model.TaskStatusCancelled || original.SupersededAt == nil || original.RetryCount != 2 || original.ClaimGeneration != 3 || string(original.Policy) != string(source.Policy) {
		t.Fatalf("legacy history: %#v", original)
	}
	if err := h.service.bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	replayed, _ := h.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
	if replayed.ID != next.ID {
		t.Fatal("bootstrap created another successor")
	}
}

func TestLegacyOwnerHandoffRollsBackWholeRound(t *testing.T) {
	definition := testDefinition(nil, true)
	definition.LegacyHandoff = func(context.Context, *repository.Repositories, *model.Task, *model.Task) error {
		return repository.ErrConflict
	}
	h := newTaskHarness(t, scriptedHandler{definition: definition}, nil)
	row := enqueueTestTask(t, h, "rollback-handoff", "rollback-handoff")
	source := makeLegacyPending(t, h, row, time.Now().Add(time.Hour))
	if err := h.service.handoff(t.Context(), source); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("handoff=%v", err)
	}
	original, _ := h.repos.Tasks.GetByID(t.Context(), source.ID)
	next, _ := h.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
	if original.Status != model.TaskStatusPending || original.SupersededAt != nil || next != nil {
		t.Fatal("handoff partially committed")
	}
}

func TestOldGCRetiresBeforeHandlerLookup(t *testing.T) {
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true)}, nil)
	task, err := h.service.prepare(EnqueueRequest{Type: testTaskType, IdempotencyKey: "old-gc", Input: testInput{Value: "gc"}})
	if err != nil {
		t.Fatal(err)
	}
	task.Type = model.TaskTypeGC
	stored, _, err := h.repos.Tasks.Enqueue(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	retired, _ := h.repos.Tasks.GetByID(t.Context(), stored.ID)
	next, _ := h.repos.Tasks.GetDirectSuccessor(t.Context(), stored.ID)
	if retired.Status != model.TaskStatusCancelled || retired.FailureReason != nil || next != nil {
		t.Fatalf("GC retirement=%#v", retired)
	}
}

func TestUnverifiableLegacyClaimStopsWithoutSettlingDomainEvidence(t *testing.T) {
	definition := testDefinition(nil, true)
	definition.OnEngineFailure = func(*model.Task, string) Settlement {
		t.Fatal("unverifiable legacy task reached domain failure settlement")
		return nil
	}
	h := newTaskHarness(t, scriptedHandler{definition: definition, execute: func(context.Context, Execution) Result {
		t.Fatal("unverifiable legacy task reached its handler")
		return Complete("", nil)
	}}, nil)
	source := makeLegacyPending(t, h, enqueueTestTask(t, h, "invalid-legacy", "invalid-legacy"), time.Now().Add(-time.Second))
	if _, err := h.db.NewUpdate().Model((*model.Task)(nil)).Set("input_hash = ?", "invalid").Where("id = ?", source.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	h.engine.executeClaim(t.Context(), claimTestTask(t, h))
	stored, err := h.repos.Tasks.GetByID(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	next, _ := h.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
	if stored.Status != model.TaskStatusFailed || dereference(stored.FailureReason) != "invalid_legacy_task" || next != nil || string(stored.Checkpoint) != string(source.Checkpoint) || string(stored.Policy) != string(source.Policy) || stored.RetryCount != source.RetryCount {
		t.Fatalf("unverifiable legacy evidence: %#v", stored)
	}
}
