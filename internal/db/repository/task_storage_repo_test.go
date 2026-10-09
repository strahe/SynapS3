package repository_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func TestTaskArchiveRollsBackAndPreservesLatestEvidence(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	row := enqueueAndClaimTask(t, repos, "archive-evidence", time.Minute)
	if err := repos.Tasks.RequestCancellation(ctx, row.ID, "stop"); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.AdmitEffect(ctx, row.ID, row.ClaimGeneration, "effect", json.RawMessage(`{"proof":"accepted"}`)); err != nil {
		t.Fatal(err)
	}
	before, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TRIGGER refuse_task_archive BEFORE DELETE ON tasks BEGIN SELECT RAISE(ABORT, 'archive refused'); END`); err != nil {
		t.Fatal(err)
	}
	transition := repository.TaskTransition{Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover}
	if err := repos.Tasks.Settle(ctx, row.ID, row.ClaimGeneration, transition); err == nil {
		t.Fatal("archive unexpectedly committed")
	}
	if count, err := db.NewSelect().TableExpr("task_history").Where("task_id = ?", row.ID).Count(ctx); err != nil || count != 0 {
		t.Fatalf("partial history = %d, %v", count, err)
	}
	failed, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || failed == nil || failed.Status != model.TaskStatusRunning || !bytes.Equal(failed.Events, before.Events) || !bytes.Equal(failed.Checkpoint, before.Checkpoint) {
		t.Fatalf("rollback changed source: %#v, %v", failed, err)
	}
	if _, err := db.ExecContext(ctx, "DROP TRIGGER refuse_task_archive"); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.Settle(ctx, row.ID, row.ClaimGeneration, transition); err != nil {
		t.Fatal(err)
	}
	archived, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || archived == nil || archived.Status != model.TaskStatusCompleted || archived.CancellationRequestedAt == nil || archived.WorkStartedAt == nil || archived.FinishedAt == nil || !bytes.Equal(archived.Input, before.Input) || !bytes.Equal(archived.Policy, before.Policy) || !bytes.Equal(archived.Runtime, before.Runtime) || !bytes.Equal(archived.Checkpoint, before.Checkpoint) {
		t.Fatalf("incomplete archive: %#v, %v", archived, err)
	}
	events, err := repos.Tasks.ListEvents(ctx, row.ID, 0, 128)
	if err != nil || len(events) != 3 || events[0].Type != "completed" {
		t.Fatalf("terminal events = %#v, %v", events, err)
	}
	if err := repos.Tasks.Settle(ctx, row.ID, row.ClaimGeneration, transition); !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("repeated settlement = %v", err)
	}
}

func TestTaskDuplicateAcrossTablesIsReported(t *testing.T) {
	assertTaskDuplicateAcrossTablesIsReported(t, testDB(t))
}

func assertTaskDuplicateAcrossTablesIsReported(t *testing.T, db *bun.DB) {
	t.Helper()
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	row := enqueueAndClaimTask(t, repos, "duplicate-round", time.Minute)
	snapshot := model.TaskHistoryFromTask(row)
	now := time.Now()
	snapshot.Status, snapshot.FinishedAt = model.TaskStatusCompleted, &now
	snapshot.ClaimedAt, snapshot.LeaseUntil = nil, nil
	if _, err := db.NewInsert().Model(snapshot).Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Tasks.GetByID(ctx, row.ID); !errors.Is(err, repository.ErrTaskDataCorrupted) {
		t.Fatalf("duplicate ID error = %v", err)
	}
	if _, err := repos.Tasks.GetByIdentity(ctx, row.Type, row.IdempotencyKey); !errors.Is(err, repository.ErrTaskDataCorrupted) {
		t.Fatalf("duplicate identity error = %v", err)
	}
	if _, err := repos.Tasks.List(ctx, repository.TaskListFilter{}); !errors.Is(err, repository.ErrTaskDataCorrupted) {
		t.Fatalf("duplicate list error = %v", err)
	}
}
