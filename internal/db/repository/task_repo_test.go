package repository_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

func seedTask(t *testing.T, repos *repository.Repositories, taskType model.TaskType) *model.Task {
	t.Helper()
	task := &model.Task{
		Type:           taskType,
		RefType:        "object",
		RefID:          1,
		RefVersionID:   "01J0000000000000000000TASK",
		IdempotencyKey: "idem-" + string(taskType) + "-" + time.Now().Format(time.RFC3339Nano),
		Status:         model.TaskStatusPending,
	}
	if err := repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("seeding task: %v", err)
	}
	return task
}

func TestTaskRepo_ClaimPending(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// No tasks yet — should return nil.
	task, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending empty: %v", err)
	}
	if task != nil {
		t.Fatal("expected nil when no tasks")
	}

	// Seed a task and claim it.
	seeded := seedTask(t, repos, model.TaskTypeUpload)
	claimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected claimed task, got nil")
	}
	if claimed.ID != seeded.ID {
		t.Errorf("expected task ID %d, got %d", seeded.ID, claimed.ID)
	}
	if claimed.Status != model.TaskStatusRunning {
		t.Errorf("expected status running, got %s", claimed.Status)
	}
	if claimed.ClaimedAt == nil || claimed.LeaseUntil == nil {
		t.Error("expected claimed_at and lease_until to be set")
	}

	// Claiming again should return nil (already running).
	again, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending again: %v", err)
	}
	if again != nil {
		t.Fatal("expected nil when no pending tasks left")
	}
}

func TestTaskRepo_RenewLease(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil || claimed.LeaseUntil == nil {
		t.Fatal("expected claimed task with lease")
	}
	oldLeaseUntil := *claimed.LeaseUntil

	if err := repos.Tasks.RenewLease(ctx, claimed.ID, 10*time.Minute); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}

	task, err := repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if task.Status != model.TaskStatusRunning {
		t.Fatalf("task status = %s, want running", task.Status)
	}
	if task.LeaseUntil == nil || !task.LeaseUntil.After(oldLeaseUntil) {
		t.Fatalf("lease_until = %v, want after %s", task.LeaseUntil, oldLeaseUntil)
	}
	if task.ClaimedAt == nil || task.StartedAt == nil {
		t.Fatalf("claimed_at/start_at = %v/%v, want preserved", task.ClaimedAt, task.StartedAt)
	}
}

func TestTaskRepo_Complete(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	if err := repos.Tasks.Complete(ctx, claimed.ID); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusCompleted {
		t.Errorf("expected completed, got %s", task.Status)
	}
	if task.CompletedAt == nil {
		t.Error("expected completed_at to be set")
	}
}

func TestTaskRepo_Complete_NotRunning(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUpload)
	// Try to complete a pending task — should fail.
	err := repos.Tasks.Complete(ctx, seeded.ID)
	if err == nil {
		t.Fatal("expected error completing pending task")
	}
}

func TestTaskRepo_Fail(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)

	if err := repos.Tasks.Fail(ctx, claimed.ID, "SP unreachable"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusFailed {
		t.Errorf("expected failed, got %s", task.Status)
	}
	if task.LastError == nil || *task.LastError != "SP unreachable" {
		t.Error("expected last_error to be set")
	}
	if task.RetryCount != 1 {
		t.Errorf("expected retry_count 1, got %d", task.RetryCount)
	}
}

func TestTaskRepo_ReleaseExpiredLeases(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	// Claim with a very short lease that will be expired
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, -1*time.Second)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	released, err := repos.Tasks.ReleaseExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReleaseExpiredLeases: %v", err)
	}
	if released != 1 {
		t.Errorf("expected 1 released, got %d", released)
	}

	// Task should be pending again
	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusPending {
		t.Errorf("expected pending after release, got %s", task.Status)
	}

	// Can claim it again
	reclaimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if reclaimed == nil {
		t.Fatal("expected to reclaim released task")
	}
}

func TestTaskRepo_ReleaseExpiredLeasesPreservesActiveLease(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	released, err := repos.Tasks.ReleaseExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReleaseExpiredLeases: %v", err)
	}
	if released != 0 {
		t.Fatalf("released = %d, want 0", released)
	}

	task, err := repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if task.Status != model.TaskStatusRunning {
		t.Fatalf("task status = %s, want running", task.Status)
	}
}

func TestTaskRepo_Requeue(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUpload)

	// Claim and fail the task first
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}
	if err := repos.Tasks.Fail(ctx, claimed.ID, "test error"); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// Requeue with backoff
	if err := repos.Tasks.Requeue(ctx, seeded.ID, 30*time.Second); err != nil {
		t.Fatalf("Requeue: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, seeded.ID)
	if task.Status != model.TaskStatusPending {
		t.Errorf("expected pending after requeue, got %s", task.Status)
	}
	if task.ClaimedAt != nil {
		t.Error("expected claimed_at to be nil after requeue")
	}

	// Requeue of non-failed task should error
	err := repos.Tasks.Requeue(ctx, seeded.ID, time.Second)
	if err == nil {
		t.Fatal("expected error requeuing non-failed task")
	}
}

func TestTaskRepo_DeferRunning(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeEvictCache)
	claimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected claimed task")
	}

	before := time.Now()
	if err := repos.Tasks.DeferRunning(ctx, claimed.ID, 2*time.Minute, "waiting for all copies"); err != nil {
		t.Fatalf("DeferRunning: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusPending {
		t.Fatalf("task status = %s, want pending", task.Status)
	}
	if task.RetryCount != 0 {
		t.Fatalf("retry_count = %d, want 0", task.RetryCount)
	}
	if task.LastError == nil || *task.LastError != "waiting for all copies" {
		t.Fatalf("last_error = %v, want waiting reason", task.LastError)
	}
	if task.ClaimedAt != nil || task.LeaseUntil != nil || task.StartedAt != nil {
		t.Fatalf("task lease fields = claimed:%v lease:%v started:%v, want cleared", task.ClaimedAt, task.LeaseUntil, task.StartedAt)
	}
	if !task.ScheduledAt.After(before) {
		t.Fatalf("scheduled_at = %s, want after %s", task.ScheduledAt, before)
	}

	next, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending deferred: %v", err)
	}
	if next != nil {
		t.Fatalf("deferred task was claimable before scheduled_at: %#v", next)
	}
}

func TestTaskRepo_FailTerminal(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	if err := repos.Tasks.FailTerminal(ctx, claimed.ID, "max retries reached"); err != nil {
		t.Fatalf("FailTerminal: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusDeadLetter {
		t.Errorf("expected dead_letter, got %s", task.Status)
	}
	if task.LastError == nil || *task.LastError != "max retries reached" {
		t.Error("expected last_error to be set")
	}
	if task.RetryCount != 1 {
		t.Errorf("expected retry_count 1, got %d", task.RetryCount)
	}
	if task.CompletedAt == nil {
		t.Error("expected completed_at to be set")
	}
}

func TestTaskRepo_FailTerminal_NotRunning(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUpload)
	err := repos.Tasks.FailTerminal(ctx, seeded.ID, "should fail")
	if err == nil {
		t.Fatal("expected error marking pending task as dead-letter")
	}
}

func TestTaskRepo_ListDeadLetters(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Initially empty
	tasks, err := repos.Tasks.ListDeadLetters(ctx, 100)
	if err != nil {
		t.Fatalf("ListDeadLetters empty: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected 0 dead-letter tasks, got %d", len(tasks))
	}

	// Create a dead-letter task
	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	_ = repos.Tasks.FailTerminal(ctx, claimed.ID, "permanent failure")

	tasks, err = repos.Tasks.ListDeadLetters(ctx, 100)
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 dead-letter task, got %d", len(tasks))
	}
	if tasks[0].ID != claimed.ID {
		t.Errorf("expected task ID %d, got %d", claimed.ID, tasks[0].ID)
	}

	// Test limit
	seedTask(t, repos, model.TaskTypeEvictCache)
	claimed2, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, 5*time.Minute)
	_ = repos.Tasks.FailTerminal(ctx, claimed2.ID, "permanent failure 2")

	tasks, err = repos.Tasks.ListDeadLetters(ctx, 1)
	if err != nil {
		t.Fatalf("ListDeadLetters limit: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task with limit=1, got %d", len(tasks))
	}
}

func TestTaskRepo_RetryDeadLetter(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Create and move to dead-letter
	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	_ = repos.Tasks.FailTerminal(ctx, claimed.ID, "permanent failure")

	// Retry
	if err := repos.Tasks.RetryDeadLetter(ctx, claimed.ID); err != nil {
		t.Fatalf("RetryDeadLetter: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusPending {
		t.Errorf("expected pending after retry, got %s", task.Status)
	}
	if task.RetryCount != 0 {
		t.Errorf("expected retry_count 0, got %d", task.RetryCount)
	}
	if task.ClaimedAt != nil {
		t.Error("expected claimed_at to be nil")
	}
	if task.LastError != nil {
		t.Error("expected last_error to be nil")
	}
	if task.CompletedAt != nil {
		t.Error("expected completed_at to be nil")
	}

	// Can be claimed again
	reclaimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending after retry: %v", err)
	}
	if reclaimed == nil {
		t.Fatal("expected to reclaim retried task")
	}
}

func TestTaskRepo_RetryDeadLetterClearsFailedUploadObject(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "retry-object-bucket")
	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000RETRY", 10)
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading state: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, version.VersionID, model.ObjectStateUploading, "create dataset failed"); err != nil {
		t.Fatalf("failed state: %v", err)
	}

	stage := "ensure_dataset"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: "retry-object-failed",
		Payload:        map[string]interface{}{"copy_index": 0},
		Status:         model.TaskStatusDeadLetter,
		RetryCount:     5,
		MaxRetries:     5,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	if err := repos.Tasks.RetryDeadLetter(ctx, task.ID); err != nil {
		t.Fatalf("RetryDeadLetter: %v", err)
	}

	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID: %v", err)
	}
	if got.State != model.ObjectStateUploading {
		t.Fatalf("state = %s, want uploading", got.State)
	}
	if got.FailedAtState != nil || got.LastError != nil {
		t.Fatalf("failure details = failed_at_state:%#v last_error:%#v, want nil", got.FailedAtState, got.LastError)
	}
}

func TestTaskRepo_RetryPrimaryCommitDeadLetterRestoresCommittingState(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "retry-commit-bucket")
	version := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000COMMIT", 10)
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading state: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing state: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, version.VersionID, model.ObjectStateCommitting, "commit failed"); err != nil {
		t.Fatalf("failed state: %v", err)
	}

	stage := "primary_commit"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: "retry-primary-commit-failed",
		Payload:        map[string]interface{}{"upload_id": 12},
		Status:         model.TaskStatusDeadLetter,
		RetryCount:     5,
		MaxRetries:     5,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	if err := repos.Tasks.RetryDeadLetter(ctx, task.ID); err != nil {
		t.Fatalf("RetryDeadLetter: %v", err)
	}

	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID: %v", err)
	}
	if got.State != model.ObjectStateCommitting {
		t.Fatalf("state = %s, want committing", got.State)
	}
	if got.FailedAtState != nil || got.LastError != nil {
		t.Fatalf("failure details = failed_at_state:%#v last_error:%#v, want nil", got.FailedAtState, got.LastError)
	}
}

func TestTaskRepo_RetryDeadLetter_NotDeadLetter(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUpload)
	err := repos.Tasks.RetryDeadLetter(ctx, seeded.ID)
	if err == nil {
		t.Fatal("expected error retrying non-dead-letter task")
	}
}

func TestTaskRepo_List(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Empty table.
	tasks, total, err := repos.Tasks.List(ctx, "", "", "", 10, 0)
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if total != 0 || len(tasks) != 0 {
		t.Fatalf("expected 0/0, got %d/%d", len(tasks), total)
	}

	// Seed tasks: 2 upload (pending), 1 evict_cache (pending).
	seedTask(t, repos, model.TaskTypeUpload)
	seedTask(t, repos, model.TaskTypeUpload)
	seedTask(t, repos, model.TaskTypeEvictCache)

	// Claim one upload task to make it running.
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	// List all — should return 3.
	tasks, total, err = repos.Tasks.List(ctx, "", "", "", 10, 0)
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if total != 3 {
		t.Errorf("expected total 3, got %d", total)
	}
	if len(tasks) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(tasks))
	}

	// Filter by type.
	_, total, err = repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", "", 10, 0)
	if err != nil {
		t.Fatalf("List by type: %v", err)
	}
	if total != 2 {
		t.Errorf("expected 2 upload, got %d", total)
	}

	// Filter by status.
	_, total, err = repos.Tasks.List(ctx, "", "", string(model.TaskStatusPending), 10, 0)
	if err != nil {
		t.Fatalf("List by status: %v", err)
	}
	if total != 2 {
		t.Errorf("expected 2 pending, got %d", total)
	}

	// Filter by type + status.
	tasks, total, err = repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", string(model.TaskStatusRunning), 10, 0)
	if err != nil {
		t.Fatalf("List by type+status: %v", err)
	}
	if total != 1 {
		t.Errorf("expected 1 running upload, got %d", total)
	}
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}

	// Pagination: limit 2, offset 0.
	tasks, total, err = repos.Tasks.List(ctx, "", "", "", 2, 0)
	if err != nil {
		t.Fatalf("List paginated: %v", err)
	}
	if total != 3 {
		t.Errorf("expected total 3, got %d", total)
	}
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks with limit=2, got %d", len(tasks))
	}

	// Pagination: limit 2, offset 2 — should return 1.
	tasks, total, err = repos.Tasks.List(ctx, "", "", "", 2, 2)
	if err != nil {
		t.Fatalf("List paginated offset: %v", err)
	}
	if total != 3 {
		t.Errorf("expected total 3, got %d", total)
	}
	if len(tasks) != 1 {
		t.Errorf("expected 1 task at offset 2, got %d", len(tasks))
	}
}

func TestTaskRepo_ListFiltersByStage(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	primaryCommit := "primary_commit"
	prepare := "prepare_upload"
	for _, task := range []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			Stage:          &primaryCommit,
			RefType:        "object",
			RefID:          1,
			RefVersionID:   "01J000000000000000STAGE01",
			IdempotencyKey: "stage-primary-commit",
			Status:         model.TaskStatusPending,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeUpload,
			Stage:          &prepare,
			RefType:        "object",
			RefID:          2,
			RefVersionID:   "01J000000000000000STAGE02",
			IdempotencyKey: "stage-prepare",
			Status:         model.TaskStatusPending,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          3,
			RefVersionID:   "01J000000000000000STAGE03",
			IdempotencyKey: "stage-evict",
			Status:         model.TaskStatusPending,
			ScheduledAt:    time.Now(),
		},
	} {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create task %q: %v", task.IdempotencyKey, err)
		}
	}

	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), primaryCommit, string(model.TaskStatusPending), 10, 0)
	if err != nil {
		t.Fatalf("List by stage: %v", err)
	}
	if total != 1 || len(tasks) != 1 || tasks[0].IdempotencyKey != "stage-primary-commit" {
		t.Fatalf("stage filtered tasks = total:%d tasks:%#v, want primary_commit only", total, tasks)
	}

	claimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimPending returned nil")
	}
	if claimed.Type != model.TaskTypeUpload {
		t.Fatalf("claimed type = %s, want upload", claimed.Type)
	}
}

func TestTaskRepo_CountByStatus(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Empty table — should return empty slice.
	counts, err := repos.Tasks.CountByStatus(ctx)
	if err != nil {
		t.Fatalf("CountByStatus empty: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("expected 0 counts, got %d", len(counts))
	}

	// Seed tasks of different types and statuses.
	seedTask(t, repos, model.TaskTypeUpload)
	seedTask(t, repos, model.TaskTypeUpload)
	seedTask(t, repos, model.TaskTypeEvictCache)

	// Claim one upload task to get a running status.
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, 5*time.Minute)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	counts, err = repos.Tasks.CountByStatus(ctx)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}

	// Build lookup map for verification.
	lookup := make(map[string]int64)
	for _, c := range counts {
		lookup[c.Type+"/"+c.Status] = c.Count
	}

	if lookup[string(model.TaskTypeUpload)+"/"+string(model.TaskStatusPending)] != 1 {
		t.Errorf("expected 1 pending upload, got %d", lookup[string(model.TaskTypeUpload)+"/"+string(model.TaskStatusPending)])
	}
	if lookup[string(model.TaskTypeUpload)+"/"+string(model.TaskStatusRunning)] != 1 {
		t.Errorf("expected 1 running upload, got %d", lookup[string(model.TaskTypeUpload)+"/"+string(model.TaskStatusRunning)])
	}
	if lookup[string(model.TaskTypeEvictCache)+"/"+string(model.TaskStatusPending)] != 1 {
		t.Errorf("expected 1 pending evict_cache, got %d", lookup[string(model.TaskTypeEvictCache)+"/"+string(model.TaskStatusPending)])
	}
}

func TestTaskRepo_CountActiveObjectTasksByBucket(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucketA := seedBucket(t, db, "task-bucket-a")
	bucketB := seedBucket(t, db, "task-bucket-b")

	versionA := newObjectVersion(bucketA.ID, "a.txt", "01J00000000000000000000TA", 1)
	objectA, err := repos.Objects.CreateVersionAndSetCurrent(ctx, versionA)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent objectA: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionA.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("objectA uploading: %v", err)
	}
	acceptTestStorageUploadForVersion(t, repos, bucketA.ID, versionA, "piece-a")

	versionB := newObjectVersion(bucketB.ID, "b.txt", "01J00000000000000000000TB", 1)
	objectB, err := repos.Objects.CreateVersionAndSetCurrent(ctx, versionB)
	if err != nil {
		t.Fatalf("CreateVersionAndSetCurrent objectB: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionB.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("objectB uploading: %v", err)
	}
	acceptTestStorageUploadForVersion(t, repos, bucketB.ID, versionB, "piece-b")

	for _, task := range []*model.Task{
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          objectA,
			RefVersionID:   versionA.VersionID,
			IdempotencyKey: "count-active-pending",
			Status:         model.TaskStatusPending,
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          objectA,
			RefVersionID:   versionA.VersionID,
			IdempotencyKey: "count-active-running",
			Status:         model.TaskStatusRunning,
		},
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          objectA,
			RefVersionID:   versionA.VersionID,
			IdempotencyKey: "count-active-completed",
			Status:         model.TaskStatusCompleted,
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "bucket",
			RefID:          bucketA.ID,
			RefVersionID:   "",
			IdempotencyKey: "count-active-bucket-task",
			Status:         model.TaskStatusPending,
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          objectB,
			RefVersionID:   versionB.VersionID,
			IdempotencyKey: "count-active-other-bucket",
			Status:         model.TaskStatusPending,
		},
	} {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Tasks.Create(%s): %v", task.IdempotencyKey, err)
		}
	}

	count, err := repos.Tasks.CountActiveObjectTasksByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("CountActiveObjectTasksByBucket: %v", err)
	}
	if count != 2 {
		t.Fatalf("active object task count = %d, want 2", count)
	}
}

func TestTaskRepo_CountActiveBucketTasksByBucketID(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucketA := seedBucket(t, db, "bucket-task-a")
	bucketB := seedBucket(t, db, "bucket-task-b")

	for _, task := range []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			RefType:        "bucket",
			RefID:          bucketA.ID,
			RefVersionID:   "",
			IdempotencyKey: "create-ps-a",
			Status:         model.TaskStatusPending,
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "bucket",
			RefID:          bucketA.ID,
			RefVersionID:   "",
			IdempotencyKey: "delete-ps-a",
			Status:         model.TaskStatusRunning,
		},
		{
			Type:           model.TaskTypeUpload,
			RefType:        "bucket",
			RefID:          bucketA.ID,
			RefVersionID:   "",
			IdempotencyKey: "create-ps-a-completed",
			Status:         model.TaskStatusCompleted,
		},
		{
			Type:           model.TaskTypeUpload,
			RefType:        "bucket",
			RefID:          bucketB.ID,
			RefVersionID:   "",
			IdempotencyKey: "create-ps-b",
			Status:         model.TaskStatusPending,
		},
	} {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Tasks.Create(%s): %v", task.IdempotencyKey, err)
		}
	}

	count, err := repos.Tasks.CountActiveBucketTasksByBucketID(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("CountActiveBucketTasksByBucketID: %v", err)
	}
	if count != 2 {
		t.Fatalf("active bucket task count = %d, want 2 (pending upload + running evict_cache)", count)
	}

	countB, err := repos.Tasks.CountActiveBucketTasksByBucketID(ctx, bucketB.ID)
	if err != nil {
		t.Fatalf("CountActiveBucketTasksByBucketID bucketB: %v", err)
	}
	if countB != 1 {
		t.Fatalf("active bucket task count for B = %d, want 1", countB)
	}
}

func TestTaskRepo_CompleteByRef(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "complete-ref-bucket")

	// Seed a pending bucket-scoped task.
	pendingTask := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "bucket",
		RefID:          bucket.ID,
		RefVersionID:   "",
		IdempotencyKey: "cbr-pending-" + time.Now().Format(time.RFC3339Nano),
		Status:         model.TaskStatusPending,
	}
	if err := repos.Tasks.Create(ctx, pendingTask); err != nil {
		t.Fatalf("seed pending task: %v", err)
	}

	// Happy path: complete the pending task by ref.
	if err := repos.Tasks.CompleteByRef(ctx, "bucket", bucket.ID, model.TaskTypeUpload); err != nil {
		t.Fatalf("CompleteByRef (happy): %v", err)
	}

	// Verify the task is now completed.
	got, err := repos.Tasks.GetByID(ctx, pendingTask.ID)
	if err != nil {
		t.Fatalf("GetByID after complete: %v", err)
	}
	if got.Status != model.TaskStatusCompleted {
		t.Fatalf("status = %s, want completed", got.Status)
	}
	if got.CompletedAt.IsZero() {
		t.Fatal("CompletedAt should be set after CompleteByRef")
	}

	// Idempotency: calling again on already-completed task returns ErrNotFound
	// because no pending/running rows match.
	err = repos.Tasks.CompleteByRef(ctx, "bucket", bucket.ID, model.TaskTypeUpload)
	if err == nil {
		t.Fatal("CompleteByRef on completed task should return error")
	}
	if !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("CompleteByRef on completed = %v, want ErrNotFound", err)
	}

	// Zero-match: non-existent ref returns ErrNotFound.
	err = repos.Tasks.CompleteByRef(ctx, "bucket", 999999, model.TaskTypeUpload)
	if err == nil {
		t.Fatal("CompleteByRef on non-existent ref should return error")
	}
	if !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("CompleteByRef non-existent = %v, want ErrNotFound", err)
	}
}
