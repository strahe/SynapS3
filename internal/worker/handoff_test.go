package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
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
	if _, err := h.db.NewUpdate().Model((*model.Task)(nil)).Set("policy_json = ?", json.RawMessage(raw)).Set("checkpoint_json = ?", json.RawMessage(`{"submitted":true}`)).Where("id = ?", task.ID).Exec(t.Context()); err != nil {
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

func TestLegacyHandoffPreservesCancellationAcceptedAfterRead(t *testing.T) {
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true)}, nil)
	source := makeLegacyPending(t, h, enqueueTestTask(t, h, "cancel-during-handoff", "cancel-during-handoff"), time.Now().Add(time.Hour))
	if _, err := h.db.NewUpdate().Model((*model.Task)(nil)).
		Set("cancellation_requested_at = NULL").Set("cancellation_reason = NULL").
		Set("resume_mode = ?", model.TaskResumeModeExecute).Where("id = ?", source.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	source, err := h.repos.Tasks.GetByID(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.repos.Tasks.RequestCancellation(t.Context(), source.ID, "cancel during handoff"); err != nil {
		t.Fatal(err)
	}
	current, err := h.repos.Tasks.GetByID(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.service.handoff(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	next, err := h.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if next == nil || !next.CancellationRequested() || next.CancellationReason == nil ||
		*next.CancellationReason != *current.CancellationReason || !next.CancellationRequestedAt.Equal(*current.CancellationRequestedAt) ||
		next.ResumeMode != model.TaskResumeModeRecover || !next.AvailableAt.Equal(current.AvailableAt) {
		t.Fatalf("accepted cancellation was lost during handoff: %#v", next)
	}
}

func TestLegacyRunningHandoffUsesCurrentCheckpointAndRecoveryMode(t *testing.T) {
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true)}, nil)
	makeLegacyPending(t, h, enqueueTestTask(t, h, "running-handoff", "running-handoff"), time.Now().Add(-time.Second))
	source := claimTestTask(t, h)
	checkpoint := json.RawMessage(`{"submitted":true,"confirmed":true}`)
	if err := h.repos.Tasks.WriteCheckpoint(t.Context(), source.ID, source.ClaimGeneration, checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := h.service.handoff(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	next, err := h.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if next == nil || next.ResumeMode != model.TaskResumeModeRecover || string(next.Checkpoint) != string(checkpoint) {
		t.Fatalf("running handoff lost current recovery evidence: %#v", next)
	}
}

func TestBootstrapIsolatesInvalidLegacyPendingWithoutStoppingEngine(t *testing.T) {
	assertBootstrapIsolatesInvalidLegacyPending(t, testutil.NewTestFileDB)
}

func assertBootstrapIsolatesInvalidLegacyPending(t *testing.T, newDB func(*testing.T) *bun.DB) {
	t.Helper()
	for _, invalid := range []string{"hash", "version", "subject"} {
		t.Run(invalid, func(t *testing.T) {
			domainFailure := make(chan struct{}, 1)
			definition := testDefinition(nil, true)
			definition.Type = model.TaskTypeCacheReconcileDurability
			if invalid == "subject" {
				definition.Subject = func(canonical json.RawMessage) (Subject, error) {
					var input testInput
					if err := json.Unmarshal(canonical, &input); err != nil {
						return Subject{}, err
					}
					return Subject{Type: "bucket", Key: input.Value}, nil
				}
			}
			definition.OnEngineFailure = func(*model.Task, string) Settlement {
				select {
				case domainFailure <- struct{}{}:
				default:
				}
				return nil
			}
			h := newTaskHarnessWithDB(t, newDB(t), scriptedHandler{definition: definition, execute: func(context.Context, Execution) Result {
				return Complete("finished", nil)
			}}, nil)
			source, _, err := h.service.Enqueue(t.Context(), EnqueueRequest{Type: definition.Type, IdempotencyKey: "invalid-legacy", Input: testInput{Value: "invalid-legacy"}})
			if err != nil {
				t.Fatal(err)
			}
			future := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
			source = makeLegacyPending(t, h, source, future)
			update := h.db.NewUpdate().Model((*model.Task)(nil)).Where("id = ?", source.ID)
			switch invalid {
			case "hash":
				update.Set("input_hash = ?", "invalid")
			case "version":
				update.Set("input_version = ?", definition.InputVersion+1)
			case "subject":
				update.Set("subject_key = ?", "another-bucket")
			}
			if _, err := update.Exec(t.Context()); err != nil {
				t.Fatal(err)
			}
			source, err = h.repos.Tasks.GetByID(t.Context(), source.ID)
			if err != nil {
				t.Fatal(err)
			}
			bucket := &model.Bucket{Name: "legacy-owner", DefaultCopies: 1, MinimumDurableCopies: 1, DurabilityGeneration: 1, DurabilityTaskID: &source.ID}
			if err := h.repos.Buckets.Create(t.Context(), bucket); err != nil {
				t.Fatal(err)
			}
			healthy, _, err := h.service.Enqueue(t.Context(), EnqueueRequest{Type: definition.Type, IdempotencyKey: "healthy", Input: testInput{Value: "healthy"}})
			if err != nil {
				t.Fatal(err)
			}
			completed := make(chan struct{}, 1)
			h.engine.config.OnTaskSettled = func(task *model.Task, transition repository.TaskTransition) {
				if task.ID == healthy.ID && transition.Status == model.TaskStatusCompleted {
					completed <- struct{}{}
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			stopped := make(chan struct{})
			var runErr error
			go func() {
				runErr = h.engine.Run(ctx)
				close(stopped)
			}()
			defer func() { cancel(); <-stopped }()
			select {
			case <-completed:
			case <-stopped:
				t.Fatalf("invalid legacy task stopped the engine: %v", runErr)
			case <-time.After(5 * time.Second):
				t.Fatal("healthy task did not complete")
			}
			stored, err := h.repos.Tasks.GetByID(t.Context(), source.ID)
			if err != nil {
				t.Fatal(err)
			}
			next, err := h.repos.Tasks.GetDirectSuccessor(t.Context(), source.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status != model.TaskStatusFailed || dereference(stored.FailureReason) != "invalid_legacy_task" || stored.FinishedAt == nil || next != nil ||
				string(stored.Input) != string(source.Input) || string(stored.Checkpoint) != string(source.Checkpoint) || string(stored.Policy) != string(source.Policy) ||
				stored.InputHash != source.InputHash || stored.InputVersion != source.InputVersion || stored.RetryCount != source.RetryCount ||
				dereference(stored.SubjectType) != dereference(source.SubjectType) || dereference(stored.SubjectKey) != dereference(source.SubjectKey) ||
				stored.ClaimGeneration != source.ClaimGeneration || !stored.AvailableAt.Equal(future) || stored.SupersededAt != nil {
				t.Fatalf("invalid legacy evidence changed: %#v", stored)
			}
			if err := h.db.NewSelect().Model(bucket).Where("id = ?", bucket.ID).Scan(t.Context()); err != nil {
				t.Fatal(err)
			}
			if bucket.DurabilityTaskID == nil || *bucket.DurabilityTaskID != source.ID {
				t.Fatal("invalid legacy input released the domain owner")
			}
			events, err := h.repos.Tasks.ListEvents(t.Context(), source.ID, 0, 128)
			if err != nil || len(events) != 2 || events[0].Type != string(model.TaskStatusFailed) {
				t.Fatalf("invalid legacy failure event: %#v, %v", events, err)
			}
			select {
			case <-domainFailure:
				t.Fatal("invalid legacy input invoked domain failure settlement")
			default:
			}
		})
	}
}

func TestBootstrapLegacyIsolationRollsBackWhenFailureEventCannotPersist(t *testing.T) {
	h := newTaskHarness(t, scriptedHandler{definition: testDefinition(nil, true)}, nil)
	source := makeLegacyPending(t, h, enqueueTestTask(t, h, "invalid-legacy", "invalid-legacy"), time.Now().Add(time.Hour))
	if _, err := h.db.NewUpdate().Model((*model.Task)(nil)).Set("input_hash = ?", "invalid").Where("id = ?", source.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.ExecContext(t.Context(), `CREATE TRIGGER reject_failure_event BEFORE UPDATE OF events_json ON tasks BEGIN SELECT RAISE(ABORT, 'event storage unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := h.service.bootstrap(t.Context()); err == nil {
		t.Fatal("bootstrap ignored failure-event storage error")
	}
	stored, err := h.repos.Tasks.GetByID(t.Context(), source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != model.TaskStatusPending || stored.FailureReason != nil || stored.FinishedAt != nil ||
		stored.RetryCount != source.RetryCount || string(stored.Checkpoint) != string(source.Checkpoint) {
		t.Fatalf("failure-event error partially committed isolation: %#v", stored)
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
