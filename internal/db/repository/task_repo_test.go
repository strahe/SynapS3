package repository_test

import (
	"context"
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
		RefGeneration:  1,
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
	task, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending empty: %v", err)
	}
	if task != nil {
		t.Fatal("expected nil when no tasks")
	}

	// Seed a task and claim it.
	seeded := seedTask(t, repos, model.TaskTypeUploadToSP)
	claimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
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
	again, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending again: %v", err)
	}
	if again != nil {
		t.Fatal("expected nil when no pending tasks left")
	}
}

func TestTaskRepo_Complete(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUploadToSP)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
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

	seeded := seedTask(t, repos, model.TaskTypeUploadToSP)
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

	seedTask(t, repos, model.TaskTypeUploadToSP)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)

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

	seedTask(t, repos, model.TaskTypeUploadToSP)
	// Claim with a very short lease that will be expired
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, -1*time.Second)
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
	reclaimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
	if reclaimed == nil {
		t.Fatal("expected to reclaim released task")
	}
}

func TestTaskRepo_Requeue(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUploadToSP)

	// Claim and fail the task first
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
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

func TestTaskRepo_FailTerminal(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUploadToSP)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
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

	seeded := seedTask(t, repos, model.TaskTypeUploadToSP)
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
	seedTask(t, repos, model.TaskTypeUploadToSP)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
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
	seedTask(t, repos, model.TaskTypeAddRoots)
	claimed2, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeAddRoots, 5*time.Minute)
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
	seedTask(t, repos, model.TaskTypeUploadToSP)
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
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
	reclaimed, err := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending after retry: %v", err)
	}
	if reclaimed == nil {
		t.Fatal("expected to reclaim retried task")
	}
}

func TestTaskRepo_RetryDeadLetter_NotDeadLetter(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUploadToSP)
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
	tasks, total, err := repos.Tasks.List(ctx, "", "", 10, 0)
	if err != nil {
		t.Fatalf("List empty: %v", err)
	}
	if total != 0 || len(tasks) != 0 {
		t.Fatalf("expected 0/0, got %d/%d", len(tasks), total)
	}

	// Seed tasks: 2 upload_to_sp (pending), 1 add_roots (pending).
	seedTask(t, repos, model.TaskTypeUploadToSP)
	seedTask(t, repos, model.TaskTypeUploadToSP)
	seedTask(t, repos, model.TaskTypeAddRoots)

	// Claim one upload task to make it running.
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	// List all — should return 3.
	tasks, total, err = repos.Tasks.List(ctx, "", "", 10, 0)
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
	_, total, err = repos.Tasks.List(ctx, string(model.TaskTypeUploadToSP), "", 10, 0)
	if err != nil {
		t.Fatalf("List by type: %v", err)
	}
	if total != 2 {
		t.Errorf("expected 2 upload_to_sp, got %d", total)
	}

	// Filter by status.
	_, total, err = repos.Tasks.List(ctx, "", string(model.TaskStatusPending), 10, 0)
	if err != nil {
		t.Fatalf("List by status: %v", err)
	}
	if total != 2 {
		t.Errorf("expected 2 pending, got %d", total)
	}

	// Filter by type + status.
	tasks, total, err = repos.Tasks.List(ctx, string(model.TaskTypeUploadToSP), string(model.TaskStatusRunning), 10, 0)
	if err != nil {
		t.Fatalf("List by type+status: %v", err)
	}
	if total != 1 {
		t.Errorf("expected 1 running upload_to_sp, got %d", total)
	}
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}

	// Pagination: limit 2, offset 0.
	tasks, total, err = repos.Tasks.List(ctx, "", "", 2, 0)
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
	tasks, total, err = repos.Tasks.List(ctx, "", "", 2, 2)
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
	seedTask(t, repos, model.TaskTypeUploadToSP)
	seedTask(t, repos, model.TaskTypeUploadToSP)
	seedTask(t, repos, model.TaskTypeAddRoots)

	// Claim one upload task to get a running status.
	claimed, _ := repos.Tasks.ClaimPending(ctx, model.TaskTypeUploadToSP, 5*time.Minute)
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

	if lookup[string(model.TaskTypeUploadToSP)+"/"+string(model.TaskStatusPending)] != 1 {
		t.Errorf("expected 1 pending upload_to_sp, got %d", lookup[string(model.TaskTypeUploadToSP)+"/"+string(model.TaskStatusPending)])
	}
	if lookup[string(model.TaskTypeUploadToSP)+"/"+string(model.TaskStatusRunning)] != 1 {
		t.Errorf("expected 1 running upload_to_sp, got %d", lookup[string(model.TaskTypeUploadToSP)+"/"+string(model.TaskStatusRunning)])
	}
	if lookup[string(model.TaskTypeAddRoots)+"/"+string(model.TaskStatusPending)] != 1 {
		t.Errorf("expected 1 pending add_roots, got %d", lookup[string(model.TaskTypeAddRoots)+"/"+string(model.TaskStatusPending)])
	}
}
