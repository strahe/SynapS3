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

func TestLatestTasksForSubjectsFiltersAndKeepsCurrentRounds(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	enqueue := func(subjectType, subjectKey string, taskType model.TaskType) *model.Task {
		t.Helper()
		task, _, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
			Type: taskType, SubjectType: subjectType, SubjectKey: subjectKey, InputVersion: 1, Input: json.RawMessage(`{}`),
		}))
		if err != nil {
			t.Fatal(err)
		}
		return task
	}
	enqueue("bucket", "1", model.TaskTypeBucketProvision)
	wantFirst := enqueue("bucket", "1", model.TaskTypeBucketProvision)
	wantSecond := enqueue("bucket", "2", model.TaskTypeBucketProvision)
	superseded := enqueue("bucket", "1", model.TaskTypeBucketProvision)
	if _, err := db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).Set("finished_at = ?", time.Now()).Set("superseded_at = ?", time.Now()).
		Where("id = ?", superseded.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	enqueue("bucket", "1", model.TaskTypeCacheEvict)
	enqueue("other", "1", model.TaskTypeBucketProvision)
	enqueue("bucket", "4", model.TaskTypeBucketProvision)
	latest, err := repos.Tasks.LatestForSubjects(t.Context(), "bucket", []string{"1", "2", "3"}, model.TaskTypeBucketProvision)
	if err != nil || len(latest) != 2 || latest["1"].ID != wantFirst.ID || latest["2"].ID != wantSecond.ID {
		t.Fatalf("latest tasks=%#v, err=%v", latest, err)
	}
	if empty, err := repos.Tasks.LatestForSubjects(t.Context(), "bucket", nil, model.TaskTypeBucketProvision); err != nil || len(empty) != 0 {
		t.Fatalf("empty subject selection=%#v, err=%v", empty, err)
	}
}

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
