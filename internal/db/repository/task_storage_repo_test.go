package repository_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestTaskArchiveRollsBackAndPreservesLatestEvidence(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	row := enqueueAndClaimTask(t, repos, "archive-evidence", time.Minute)
	if err := repos.Tasks.AdmitEffect(ctx, row.ID, row.ClaimGeneration, "effect", json.RawMessage(`{"proof":"accepted"}`)); err != nil {
		t.Fatal(err)
	}
	before, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	restoreRefuseTaskArchive := testutil.FailWrites(t, db, "tasks", "UPDATE", "NEW.status = 'completed'", "archive refused")
	transition := repository.TaskTransition{Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover}
	if err := repos.Tasks.Settle(ctx, row.ID, row.ClaimGeneration, transition); err == nil {
		t.Fatal("archive unexpectedly committed")
	}
	failed, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || failed == nil || failed.Status != model.TaskStatusRunning || !bytes.Equal(failed.Checkpoint, before.Checkpoint) {
		t.Fatalf("rollback changed source: %#v, %v", failed, err)
	}
	restoreRefuseTaskArchive()
	if err := repos.Tasks.Settle(ctx, row.ID, row.ClaimGeneration, transition); err != nil {
		t.Fatal(err)
	}
	archived, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || archived == nil || archived.Status != model.TaskStatusCompleted || archived.WorkStartedAt == nil || archived.FinishedAt == nil || !bytes.Equal(archived.Input, before.Input) || !bytes.Equal(archived.Runtime, before.Runtime) || !bytes.Equal(archived.Checkpoint, before.Checkpoint) {
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
