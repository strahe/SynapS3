package repository_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestTaskOperationAdmissionAndRollback(t *testing.T) {
	assertTaskOperationAdmissionAndRollback(t, testDB(t))
}

func assertTaskOperationAdmissionAndRollback(t *testing.T, db *bun.DB) {
	t.Helper()
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	row := enqueueAndClaimTask(t, repos, "effect-admission", time.Minute)
	observed, err := repos.Tasks.ObserveOperation(ctx, row.ID, row.ClaimGeneration, "upload")
	if err != nil {
		t.Fatal(err)
	}
	again, err := repos.Tasks.ObserveOperation(ctx, row.ID, row.ClaimGeneration, "upload")
	if err != nil || !again.Equal(observed) {
		t.Fatalf("observation reset: %v, %v", again, err)
	}
	if err := repos.Tasks.AdmitEffect(ctx, row.ID, row.ClaimGeneration, "upload", json.RawMessage(`{"submitted":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.AdmitEffect(ctx, row.ID, row.ClaimGeneration, "upload", json.RawMessage(`{"duplicate":true}`)); !errors.Is(err, repository.ErrTaskEffectAlreadyAdmitted) {
		t.Fatalf("duplicate admission=%v", err)
	}
	if _, err := repos.Tasks.ObserveOperation(ctx, row.ID, row.ClaimGeneration, "other"); !errors.Is(err, repository.ErrTaskOperationUnresolved) {
		t.Fatalf("unresolved operation switch=%v", err)
	}
	if err := repos.Tasks.Settle(ctx, row.ID, row.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: time.Now(), IncrementRetry: true}); err != nil {
		t.Fatal(err)
	}
	recovered, err := repos.Tasks.ClaimNext(ctx, time.Minute)
	if err != nil || recovered == nil {
		t.Fatalf("recover=%#v,%v", recovered, err)
	}
	var checkpoint struct {
		Submitted bool `json:"submitted"`
	}
	decodeErr := json.Unmarshal(recovered.Checkpoint, &checkpoint)
	if recovered.RetryCount != 1 || decodeErr != nil || !checkpoint.Submitted {
		t.Fatalf("count or checkpoint=%#v", recovered)
	}
	if err := repos.Tasks.AdmitEffect(ctx, row.ID, row.ClaimGeneration, "upload", json.RawMessage(`{}`)); !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("stale admission=%v", err)
	}
	if err := repos.Tasks.AdmitEffect(ctx, row.ID, recovered.ClaimGeneration, "upload", json.RawMessage(`{"resubmitted":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.ResolveOperation(ctx, row.ID, recovered.ClaimGeneration, "upload"); err != nil {
		t.Fatal(err)
	}
	aborted := errors.New("abort")
	err = repos.WithTx(ctx, func(tx *repository.Repositories) error {
		if err := tx.Tasks.AdmitEffect(ctx, row.ID, recovered.ClaimGeneration, "other", json.RawMessage(`{"rolled_back":true}`)); err != nil {
			return err
		}
		return aborted
	})
	if !errors.Is(err, aborted) {
		t.Fatal(err)
	}
	if err := repos.Tasks.AdmitEffect(ctx, row.ID, recovered.ClaimGeneration, "other", json.RawMessage(`{"next":true}`)); err != nil {
		t.Fatalf("rolled back permission consumed=%v", err)
	}
	stored, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || stored.RetryCount != 1 {
		t.Fatalf("effects changed retry budget: %#v,%v", stored, err)
	}
}

func TestTaskEventsAreBoundedAndTransactional(t *testing.T) {
	assertTaskEventsAreBoundedAndTransactional(t, testDB(t))
}

func assertTaskEventsAreBoundedAndTransactional(t *testing.T, db *bun.DB) {
	t.Helper()
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	row := enqueueAndClaimTask(t, repos, "events", time.Minute)
	for i := range 140 {
		if err := repos.Tasks.AppendEvent(ctx, row.ID, "observation", json.RawMessage(fmt.Sprintf(`{"index":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	before, err := repos.Tasks.ListEvents(ctx, row.ID, 0, 128)
	if err != nil || len(before) != 128 || before[0].Sequence != 141 || before[127].Sequence != 14 {
		t.Fatalf("events count/order=%v,%v", before, err)
	}
	aborted := errors.New("abort")
	err = repos.WithTx(ctx, func(tx *repository.Repositories) error {
		if err := tx.Tasks.AppendEvent(ctx, row.ID, "rolled_back", nil); err != nil {
			return err
		}
		return aborted
	})
	if !errors.Is(err, aborted) {
		t.Fatal(err)
	}
	after, err := repos.Tasks.ListEvents(ctx, row.ID, 0, 1)
	if err != nil || after[0].Sequence != before[0].Sequence {
		t.Fatalf("rollback appended=%#v,%v", after, err)
	}
	next, err := repos.Tasks.ListEvents(ctx, row.ID, before[0].Sequence, 2)
	if err != nil || len(next) != 2 || next[0].Sequence != 140 {
		t.Fatalf("event paging=%#v,%v", next, err)
	}
}

func TestTaskCancellationPreservesRecoveryBackoff(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	ctx := t.Context()
	row := enqueueAndClaimTask(t, repos, "cancel-backoff", time.Minute)
	if err := repos.Tasks.RequestCancellation(ctx, row.ID, "cancel"); err != nil {
		t.Fatal(err)
	}
	due := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	if err := repos.Tasks.Settle(ctx, row.ID, row.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeRecover, AvailableAt: due, CancellationObserved: true, IncrementRetry: true}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.RequestCancellation(ctx, row.ID, "cancel again"); err != nil {
		t.Fatal(err)
	}
	stored, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || !stored.AvailableAt.Equal(due) || stored.RetryCount != 1 {
		t.Fatalf("cancel reset backoff=%#v,%v", stored, err)
	}
	if claimed, err := repos.Tasks.ClaimNext(ctx, time.Minute); err != nil || claimed != nil {
		t.Fatalf("claimed delayed cancellation=%#v,%v", claimed, err)
	}
}

func TestTaskRoundIdentityAndHistory(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	source := enqueueAndClaimTask(t, repos, "round-history", time.Minute)
	if err := repos.Tasks.Settle(ctx, source.ID, source.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover, FailureReason: new("attempts_exhausted"), LastError: new("unavailable")}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.AcknowledgeFailed(ctx, source.ID); err != nil {
		t.Fatal(err)
	}
	child := repositorySuccessor(t, repos, source, false)
	duplicate, created, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{Type: child.Type, IdempotencyKey: child.IdempotencyKey, InputVersion: 1, Input: json.RawMessage(`{}`), InputHash: "test"}))
	if err != nil || created || duplicate.ID != child.ID {
		t.Fatalf("current identity=%#v,%v,%v", duplicate, created, err)
	}
	original, err := repos.Tasks.GetByID(ctx, source.ID)
	if err != nil || original.Status != model.TaskStatusFailed || original.AcknowledgedAt == nil || original.SupersededAt == nil {
		t.Fatalf("old history=%#v,%v", original, err)
	}
	page, err := repos.Tasks.ListHistory(ctx, source.ID, 0, 1)
	if err != nil || len(page.Tasks) != 1 || page.Tasks[0].ID != child.ID || page.NextBeforeID != child.ID {
		t.Fatalf("first history=%#v,%v", page, err)
	}
	page, err = repos.Tasks.ListHistory(ctx, source.ID, page.NextBeforeID, 1)
	if err != nil || len(page.Tasks) != 1 || page.Tasks[0].ID != source.ID || page.NextBeforeID != 0 {
		t.Fatalf("second history=%#v,%v", page, err)
	}
	if _, err := repos.Tasks.ListHistory(ctx, source.ID, child.ID+1, 1); !errors.Is(err, repository.ErrInvalidInput) {
		t.Fatalf("foreign history cursor=%v", err)
	}
}
