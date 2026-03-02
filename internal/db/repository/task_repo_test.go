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
