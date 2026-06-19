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
		Status:         model.TaskStatusQueued,
	}
	if err := repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("seeding task: %v", err)
	}
	return task
}

func TestTaskRepo_ClaimReady(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// No tasks yet — should return nil.
	task, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady empty: %v", err)
	}
	if task != nil {
		t.Fatal("expected nil when no tasks")
	}

	// Seed a task and claim it.
	seeded := seedTask(t, repos, model.TaskTypeUpload)
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
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
	again, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady again: %v", err)
	}
	if again != nil {
		t.Fatal("expected nil when no ready tasks left")
	}
}

func TestTaskRepo_ClaimReadyHandlesQueuedScheduledAndWaiting(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	now := time.Now()

	queued := seedTask(t, repos, model.TaskTypeUpload)
	scheduled := seedTask(t, repos, model.TaskTypeUpload)
	waiting := seedTask(t, repos, model.TaskTypeUpload)
	futureScheduled := seedTask(t, repos, model.TaskTypeUpload)

	mustExec(t, db, `UPDATE tasks SET scheduled_at = ? WHERE id = ?`, now.Add(-3*time.Minute), queued.ID)
	mustExec(t, db, `UPDATE tasks SET status = ?, scheduled_at = ? WHERE id = ?`, model.TaskStatusScheduled, now.Add(-2*time.Minute), scheduled.ID)
	mustExec(t, db, `UPDATE tasks SET status = ?, wait_reason = ?, status_message = ?, scheduled_at = ? WHERE id = ?`, model.TaskStatusWaiting, model.TaskWaitReasonDependency, "waiting for all copies", now.Add(-time.Minute), waiting.ID)
	mustExec(t, db, `UPDATE tasks SET status = ?, scheduled_at = ? WHERE id = ?`, model.TaskStatusScheduled, now.Add(time.Hour), futureScheduled.ID)

	for _, wantID := range []int64{queued.ID, scheduled.ID, waiting.ID} {
		claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
		if err != nil {
			t.Fatalf("ClaimReady: %v", err)
		}
		if claimed == nil {
			t.Fatalf("ClaimReady returned nil, want task %d", wantID)
		}
		if claimed.ID != wantID {
			t.Fatalf("ClaimReady ID = %d, want %d", claimed.ID, wantID)
		}
		if claimed.Status != model.TaskStatusRunning {
			t.Fatalf("ClaimReady status = %s, want running", claimed.Status)
		}
		if claimed.LastError != nil || claimed.WaitReason != nil || claimed.StatusMessage != nil {
			t.Fatalf("claimed task diagnostics = last:%v wait:%v message:%v, want cleared", claimed.LastError, claimed.WaitReason, claimed.StatusMessage)
		}
	}

	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady future scheduled: %v", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimReady claimed future scheduled task: %#v", claimed)
	}
}

func TestTaskRepo_ClaimReadyBreaksScheduledTiesByID(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	scheduledAt := time.Now().Add(-time.Minute)

	first := seedTask(t, repos, model.TaskTypeUpload)
	second := seedTask(t, repos, model.TaskTypeUpload)
	mustExec(t, db, `UPDATE tasks SET scheduled_at = ? WHERE id IN (?, ?)`, scheduledAt, first.ID, second.ID)

	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimReady returned nil, want task")
	}
	if claimed.ID != first.ID {
		t.Fatalf("ClaimReady ID = %d, want lowest ID %d", claimed.ID, first.ID)
	}
}

func TestTaskRepo_RunningFailureTransitions(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	retryable := seedTask(t, repos, model.TaskTypeUpload)
	retryable.MaxRetries = 3
	mustExec(t, db, `UPDATE tasks SET max_retries = ? WHERE id = ?`, retryable.MaxRetries, retryable.ID)
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)

	status, err := repos.Tasks.ScheduleRetryRunning(ctx, claimed, "temporary rpc error", time.Minute)
	if err != nil {
		t.Fatalf("ScheduleRetryRunning: %v", err)
	}
	if status != model.TaskStatusScheduled {
		t.Fatalf("ScheduleRetryRunning status = %s, want scheduled", status)
	}
	got, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if got.Status != model.TaskStatusScheduled {
		t.Fatalf("retryable status = %s, want scheduled", got.Status)
	}
	if got.RetryCount != 1 {
		t.Fatalf("retry_count = %d, want 1", got.RetryCount)
	}
	if got.LastError == nil || *got.LastError != "temporary rpc error" {
		t.Fatalf("last_error = %v, want temporary rpc error", got.LastError)
	}
	if got.WaitReason != nil || got.StatusMessage != nil {
		t.Fatalf("wait diagnostics = %v/%v, want nil", got.WaitReason, got.StatusMessage)
	}

	exhausted := seedTask(t, repos, model.TaskTypeUpload)
	mustExec(t, db, `UPDATE tasks SET retry_count = 2, max_retries = 3 WHERE id = ?`, exhausted.ID)
	claimed, _ = repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	status, err = repos.Tasks.ScheduleRetryRunning(ctx, claimed, "permanent timeout", time.Minute)
	if err != nil {
		t.Fatalf("ScheduleRetryRunning exhausted: %v", err)
	}
	if status != model.TaskStatusExhausted {
		t.Fatalf("ScheduleRetryRunning exhausted status = %s, want exhausted", status)
	}
	got, _ = repos.Tasks.GetByID(ctx, claimed.ID)
	if got.Status != model.TaskStatusExhausted {
		t.Fatalf("exhausted status = %s, want exhausted", got.Status)
	}
	if got.CompletedAt == nil {
		t.Fatal("completed_at is nil, want set for exhausted task")
	}

	zeroRetry := seedTask(t, repos, model.TaskTypeUpload)
	mustExec(t, db, `UPDATE tasks SET max_retries = 0 WHERE id = ?`, zeroRetry.ID)
	claimed, _ = repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	status, err = repos.Tasks.ScheduleRetryRunning(ctx, claimed, "no retries configured", time.Minute)
	if err != nil {
		t.Fatalf("ScheduleRetryRunning zero retry: %v", err)
	}
	if status != model.TaskStatusExhausted {
		t.Fatalf("ScheduleRetryRunning zero retry status = %s, want exhausted", status)
	}
	got, _ = repos.Tasks.GetByID(ctx, claimed.ID)
	if got.Status != model.TaskStatusExhausted {
		t.Fatalf("zero retry status = %s, want exhausted", got.Status)
	}
	if got.RetryCount != 1 {
		t.Fatalf("zero retry count = %d, want 1", got.RetryCount)
	}

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ = repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err := repos.Tasks.FailRunning(ctx, claimed, "invalid object state"); err != nil {
		t.Fatalf("FailRunning: %v", err)
	}
	got, _ = repos.Tasks.GetByID(ctx, claimed.ID)
	if got.Status != model.TaskStatusFailed {
		t.Fatalf("failed status = %s, want failed", got.Status)
	}
	if got.LastError == nil || *got.LastError != "invalid object state" {
		t.Fatalf("last_error = %v, want invalid object state", got.LastError)
	}
}

func TestTaskRepo_ScheduleRetryRunningRejectsStaleClaim(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	staleClaim, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, -time.Second)
	if err != nil {
		t.Fatalf("ClaimReady stale: %v", err)
	}
	if staleClaim == nil || staleClaim.ClaimedAt == nil {
		t.Fatal("ClaimReady stale returned no claimed task")
	}
	if _, err := repos.Tasks.ScheduleRetryRunning(ctx, staleClaim, "expired worker failure", time.Minute); err == nil {
		t.Fatal("expected expired claim retry to fail")
	}
	got, err := repos.Tasks.GetByID(ctx, staleClaim.ID)
	if err != nil {
		t.Fatalf("GetByID expired: %v", err)
	}
	if got.Status != model.TaskStatusRunning {
		t.Fatalf("status after expired retry = %s, want running", got.Status)
	}
	if got.RetryCount != 0 {
		t.Fatalf("retry_count after expired retry = %d, want 0", got.RetryCount)
	}
	if _, err := repos.Tasks.ReleaseExpiredLeases(ctx); err != nil {
		t.Fatalf("ReleaseExpiredLeases: %v", err)
	}
	freshClaim, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady fresh: %v", err)
	}
	if freshClaim == nil || freshClaim.ClaimedAt == nil {
		t.Fatal("ClaimReady fresh returned no claimed task")
	}
	if staleClaim.ID != freshClaim.ID {
		t.Fatalf("claim ids = stale:%d fresh:%d, want same task", staleClaim.ID, freshClaim.ID)
	}

	if _, err := repos.Tasks.ScheduleRetryRunning(ctx, staleClaim, "stale worker failure", time.Minute); err == nil {
		t.Fatal("expected stale claim retry to fail")
	}
	got, err = repos.Tasks.GetByID(ctx, freshClaim.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.TaskStatusRunning {
		t.Fatalf("status after stale retry = %s, want running", got.Status)
	}
	if got.RetryCount != 0 {
		t.Fatalf("retry_count after stale retry = %d, want 0", got.RetryCount)
	}
	if got.LastError != nil {
		t.Fatalf("last_error after stale retry = %v, want nil", got.LastError)
	}
	if got.ClaimedAt == nil || !got.ClaimedAt.Equal(*freshClaim.ClaimedAt) {
		t.Fatalf("claimed_at after stale retry = %v, want fresh claim %v", got.ClaimedAt, freshClaim.ClaimedAt)
	}

	status, err := repos.Tasks.ScheduleRetryRunning(ctx, freshClaim, "fresh worker failure", time.Minute)
	if err != nil {
		t.Fatalf("ScheduleRetryRunning fresh: %v", err)
	}
	if status != model.TaskStatusScheduled {
		t.Fatalf("fresh retry status = %s, want scheduled", status)
	}
}

func TestTaskRepo_WaitRunningStoresNonErrorReason(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeEvictCache)
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimReady returned nil")
	}

	if err := repos.Tasks.WaitRunning(ctx, claimed, model.TaskWaitReasonDependency, "waiting for all copies to commit", time.Minute); err != nil {
		t.Fatalf("WaitRunning: %v", err)
	}
	got, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if got.Status != model.TaskStatusWaiting {
		t.Fatalf("status = %s, want waiting", got.Status)
	}
	if got.RetryCount != 0 {
		t.Fatalf("retry_count = %d, want 0", got.RetryCount)
	}
	if got.LastError != nil {
		t.Fatalf("last_error = %v, want nil", got.LastError)
	}
	if got.WaitReason == nil || *got.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("wait_reason = %v, want dependency", got.WaitReason)
	}
	if got.StatusMessage == nil || *got.StatusMessage != "waiting for all copies to commit" {
		t.Fatalf("status_message = %v, want waiting message", got.StatusMessage)
	}
}

func TestTaskRepo_RenewLease(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil || claimed.LeaseUntil == nil {
		t.Fatal("expected claimed task with lease")
	}
	oldLeaseUntil := *claimed.LeaseUntil

	if err := repos.Tasks.RenewLease(ctx, claimed, 10*time.Minute); err != nil {
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

func TestTaskRepo_RenewLeaseRejectsStaleClaim(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	staleClaim, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, -time.Second)
	if err != nil {
		t.Fatalf("ClaimReady stale: %v", err)
	}
	if staleClaim == nil || staleClaim.ClaimedAt == nil {
		t.Fatal("ClaimReady stale returned no claimed task")
	}
	if err := repos.Tasks.RenewLease(ctx, staleClaim, time.Minute); err == nil {
		t.Fatal("expected expired lease renewal to fail")
	}
	if _, err := repos.Tasks.ReleaseExpiredLeases(ctx); err != nil {
		t.Fatalf("ReleaseExpiredLeases: %v", err)
	}
	freshClaim, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady fresh: %v", err)
	}
	if freshClaim == nil || freshClaim.ClaimedAt == nil || freshClaim.LeaseUntil == nil {
		t.Fatal("ClaimReady fresh returned no active claim")
	}
	freshLeaseUntil := *freshClaim.LeaseUntil

	if err := repos.Tasks.RenewLease(ctx, staleClaim, 10*time.Minute); err == nil {
		t.Fatal("expected stale claim renewal to fail")
	}
	got, err := repos.Tasks.GetByID(ctx, freshClaim.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.LeaseUntil == nil || !got.LeaseUntil.Equal(freshLeaseUntil) {
		t.Fatalf("lease_until after stale renewal = %v, want %s", got.LeaseUntil, freshLeaseUntil)
	}
}

func TestTaskRepo_Complete(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	if err := repos.Tasks.Complete(ctx, claimed); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusCompleted {
		t.Errorf("expected completed, got %s", task.Status)
	}
	if task.CompletedAt == nil {
		t.Error("expected completed_at to be set")
	}
	if task.ClaimedAt != nil || task.LeaseUntil != nil || task.StartedAt != nil {
		t.Fatalf("completed task lease fields = claimed:%v lease:%v started:%v, want cleared", task.ClaimedAt, task.LeaseUntil, task.StartedAt)
	}
}

func TestTaskRepo_CompleteWithMessage(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	message := "completed successfully with special condition"
	if err := repos.Tasks.CompleteWithMessage(ctx, claimed, message); err != nil {
		t.Fatalf("CompleteWithMessage: %v", err)
	}

	task, err := repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if task == nil {
		t.Fatal("expected task to be found, got nil")
	}
	if task.Status != model.TaskStatusCompleted {
		t.Errorf("expected status %s, got %s", model.TaskStatusCompleted, task.Status)
	}
	if task.StatusMessage == nil {
		t.Errorf("expected status_message %q, got nil", message)
	} else if *task.StatusMessage != message {
		t.Errorf("expected status_message %q, got %q", message, *task.StatusMessage)
	}
}

func TestTaskRepo_Complete_NotRunning(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUpload)
	// Try to complete a queued task — should fail.
	err := repos.Tasks.Complete(ctx, seeded)
	if err == nil {
		t.Fatal("expected error completing queued task")
	}
}

func TestTaskRepo_Fail(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)

	if err := repos.Tasks.FailRunning(ctx, claimed, "SP unreachable"); err != nil {
		t.Fatalf("FailRunning: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusFailed {
		t.Errorf("expected failed, got %s", task.Status)
	}
	if task.LastError == nil || *task.LastError != "SP unreachable" {
		t.Error("expected last_error to be set")
	}
	if task.RetryCount != 0 {
		t.Errorf("expected retry_count 0, got %d", task.RetryCount)
	}
	if task.CompletedAt == nil {
		t.Error("expected completed_at to be set")
	}
}

func TestTaskRepo_ReleaseExpiredLeases(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	// Claim with a very short lease that will be expired
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, -1*time.Second)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}
	mustExec(t, db, `UPDATE tasks SET last_error = ? WHERE id = ?`, "stale lease diagnostic", claimed.ID)

	released, err := repos.Tasks.ReleaseExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("ReleaseExpiredLeases: %v", err)
	}
	if released != 1 {
		t.Errorf("expected 1 released, got %d", released)
	}

	// Task should be queued again.
	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusQueued {
		t.Errorf("expected queued after release, got %s", task.Status)
	}
	if task.LastError != nil {
		t.Fatalf("last_error = %v, want cleared", task.LastError)
	}

	// Can claim it again
	reclaimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if reclaimed == nil {
		t.Fatal("expected to reclaim released task")
	}
}

func TestTaskRepo_ReleaseRunningClearsError(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}
	mustExec(t, db, `UPDATE tasks SET last_error = ? WHERE id = ?`, "stale running diagnostic", claimed.ID)

	if err := repos.Tasks.ReleaseRunning(ctx, claimed); err != nil {
		t.Fatalf("ReleaseRunning: %v", err)
	}

	got, err := repos.Tasks.GetByID(ctx, claimed.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.TaskStatusQueued {
		t.Fatalf("status = %s, want queued", got.Status)
	}
	if got.LastError != nil || got.ClaimedAt != nil || got.LeaseUntil != nil || got.StartedAt != nil {
		t.Fatalf("released task fields = error:%v claimed:%v lease:%v started:%v, want cleared", got.LastError, got.ClaimedAt, got.LeaseUntil, got.StartedAt)
	}
}

func TestTaskRepo_ReleaseExpiredLeasesPreservesActiveLease(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
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

func TestTaskRepo_MarkRunningExhausted(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if claimed == nil {
		t.Fatal("setup: could not claim task")
	}

	if err := repos.Tasks.MarkRunningExhausted(ctx, claimed, "max retries reached"); err != nil {
		t.Fatalf("MarkRunningExhausted: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusExhausted {
		t.Errorf("expected exhausted, got %s", task.Status)
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

func TestTaskRepo_MarkRunningExhausted_NotRunning(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUpload)
	err := repos.Tasks.MarkRunningExhausted(ctx, seeded, "should fail")
	if err == nil {
		t.Fatal("expected error marking queued task as exhausted")
	}
}

func TestTaskRepo_ListExhausted(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Initially empty
	tasks, err := repos.Tasks.ListExhausted(ctx, 100)
	if err != nil {
		t.Fatalf("ListExhausted empty: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected 0 exhausted tasks, got %d", len(tasks))
	}

	// Create a exhausted task
	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	_ = repos.Tasks.MarkRunningExhausted(ctx, claimed, "permanent failure")

	tasks, err = repos.Tasks.ListExhausted(ctx, 100)
	if err != nil {
		t.Fatalf("ListExhausted: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 exhausted task, got %d", len(tasks))
	}
	if tasks[0].ID != claimed.ID {
		t.Errorf("expected task ID %d, got %d", claimed.ID, tasks[0].ID)
	}

	// Test limit
	seedTask(t, repos, model.TaskTypeEvictCache)
	claimed2, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, 5*time.Minute)
	_ = repos.Tasks.MarkRunningExhausted(ctx, claimed2, "permanent failure 2")

	tasks, err = repos.Tasks.ListExhausted(ctx, 1)
	if err != nil {
		t.Fatalf("ListExhausted limit: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task with limit=1, got %d", len(tasks))
	}
}

func TestTaskRepo_RetryExhausted(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	// Create and move to exhausted
	seedTask(t, repos, model.TaskTypeUpload)
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	_ = repos.Tasks.MarkRunningExhausted(ctx, claimed, "permanent failure")

	// Retry
	if err := repos.Tasks.RetryExhausted(ctx, claimed.ID); err != nil {
		t.Fatalf("RetryExhausted: %v", err)
	}

	task, _ := repos.Tasks.GetByID(ctx, claimed.ID)
	if task.Status != model.TaskStatusQueued {
		t.Errorf("expected queued after retry, got %s", task.Status)
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
	reclaimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady after retry: %v", err)
	}
	if reclaimed == nil {
		t.Fatal("expected to reclaim retried task")
	}
}

func TestTaskRepo_RetryExhaustedClearsFailedUploadObject(t *testing.T) {
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
		Status:         model.TaskStatusExhausted,
		RetryCount:     5,
		MaxRetries:     5,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	if err := repos.Tasks.RetryExhausted(ctx, task.ID); err != nil {
		t.Fatalf("RetryExhausted: %v", err)
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

func TestTaskRepo_RetryPrimaryCommitExhaustedRestoresCommittingState(t *testing.T) {
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

	stage := "ingress_commit"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: "retry-primary-commit-failed",
		Payload:        map[string]interface{}{"upload_id": 12},
		Status:         model.TaskStatusExhausted,
		RetryCount:     5,
		MaxRetries:     5,
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create task: %v", err)
	}

	if err := repos.Tasks.RetryExhausted(ctx, task.ID); err != nil {
		t.Fatalf("RetryExhausted: %v", err)
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

func TestTaskRepo_RetryExhausted_NotExhausted(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	seeded := seedTask(t, repos, model.TaskTypeUpload)
	err := repos.Tasks.RetryExhausted(ctx, seeded.ID)
	if err == nil {
		t.Fatal("expected error retrying non-exhausted task")
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

	// Seed tasks: 2 upload (queued), 1 evict_cache (queued).
	seedTask(t, repos, model.TaskTypeUpload)
	seedTask(t, repos, model.TaskTypeUpload)
	seedTask(t, repos, model.TaskTypeEvictCache)

	// Claim one upload task to make it running.
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
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
	_, total, err = repos.Tasks.List(ctx, "", "", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List by status: %v", err)
	}
	if total != 2 {
		t.Errorf("expected 2 queued, got %d", total)
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

	ingressCommit := "ingress_commit"
	prepare := "prepare_upload"
	for _, task := range []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			Stage:          &ingressCommit,
			RefType:        "object",
			RefID:          1,
			RefVersionID:   "01J000000000000000STAGE01",
			IdempotencyKey: "stage-primary-commit",
			Status:         model.TaskStatusQueued,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeUpload,
			Stage:          &prepare,
			RefType:        "object",
			RefID:          2,
			RefVersionID:   "01J000000000000000STAGE02",
			IdempotencyKey: "stage-prepare",
			Status:         model.TaskStatusQueued,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          3,
			RefVersionID:   "01J000000000000000STAGE03",
			IdempotencyKey: "stage-evict",
			Status:         model.TaskStatusQueued,
			ScheduledAt:    time.Now(),
		},
	} {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("Create task %q: %v", task.IdempotencyKey, err)
		}
	}

	tasks, total, err := repos.Tasks.List(ctx, string(model.TaskTypeUpload), ingressCommit, string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List by stage: %v", err)
	}
	if total != 1 || len(tasks) != 1 || tasks[0].IdempotencyKey != "stage-primary-commit" {
		t.Fatalf("stage filtered tasks = total:%d tasks:%#v, want ingress_commit only", total, tasks)
	}

	claimed, err := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimReady returned nil")
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
	claimed, _ := repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, 5*time.Minute)
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

	if lookup[string(model.TaskTypeUpload)+"/"+string(model.TaskStatusQueued)] != 1 {
		t.Errorf("expected 1 queued upload, got %d", lookup[string(model.TaskTypeUpload)+"/"+string(model.TaskStatusQueued)])
	}
	if lookup[string(model.TaskTypeUpload)+"/"+string(model.TaskStatusRunning)] != 1 {
		t.Errorf("expected 1 running upload, got %d", lookup[string(model.TaskTypeUpload)+"/"+string(model.TaskStatusRunning)])
	}
	if lookup[string(model.TaskTypeEvictCache)+"/"+string(model.TaskStatusQueued)] != 1 {
		t.Errorf("expected 1 queued evict_cache, got %d", lookup[string(model.TaskTypeEvictCache)+"/"+string(model.TaskStatusQueued)])
	}
}

func TestTaskRepo_OverviewActivePipeline(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	prepare := seedTask(t, repos, model.TaskTypeUpload)
	ensure := seedTask(t, repos, model.TaskTypeUpload)
	upload := seedTask(t, repos, model.TaskTypeUpload)
	commit := seedTask(t, repos, model.TaskTypeUpload)
	syncPull := seedTask(t, repos, model.TaskTypeUpload)
	syncCommit := seedTask(t, repos, model.TaskTypeUpload)
	evict := seedTask(t, repos, model.TaskTypeEvictCache)
	cleanup := seedTask(t, repos, model.TaskTypeStorageCleanup)
	completed := seedTask(t, repos, model.TaskTypeUpload)
	failed := seedTask(t, repos, model.TaskTypeUpload)
	exhausted := seedTask(t, repos, model.TaskTypeEvictCache)

	mustExec(t, db, `UPDATE tasks SET stage = NULL, status = ? WHERE id = ?`, model.TaskStatusQueued, prepare.ID)
	mustExec(t, db, `UPDATE tasks SET stage = ?, status = ? WHERE id = ?`, "ensure_dataset", model.TaskStatusScheduled, ensure.ID)
	mustExec(t, db, `UPDATE tasks SET stage = ?, status = ? WHERE id = ?`, "ingress_store", model.TaskStatusRunning, upload.ID)
	mustExec(t, db, `UPDATE tasks SET stage = ?, status = ? WHERE id = ?`, "ingress_commit", model.TaskStatusWaiting, commit.ID)
	mustExec(t, db, `UPDATE tasks SET stage = ?, status = ? WHERE id = ?`, "peer_pull", model.TaskStatusQueued, syncPull.ID)
	mustExec(t, db, `UPDATE tasks SET stage = ?, status = ? WHERE id = ?`, "peer_commit", model.TaskStatusScheduled, syncCommit.ID)
	mustExec(t, db, `UPDATE tasks SET status = ? WHERE id = ?`, model.TaskStatusRunning, evict.ID)
	mustExec(t, db, `UPDATE tasks SET status = ? WHERE id = ?`, model.TaskStatusWaiting, cleanup.ID)
	mustExec(t, db, `UPDATE tasks SET stage = ?, status = ? WHERE id = ?`, "ingress_store", model.TaskStatusCompleted, completed.ID)
	mustExec(t, db, `UPDATE tasks SET stage = ?, status = ? WHERE id = ?`, "ingress_store", model.TaskStatusFailed, failed.ID)
	mustExec(t, db, `UPDATE tasks SET status = ? WHERE id = ?`, model.TaskStatusExhausted, exhausted.ID)

	counts, err := repos.Tasks.CountOverviewActivePipeline(ctx)
	if err != nil {
		t.Fatalf("CountOverviewActivePipeline: %v", err)
	}
	lookup := make(map[string]int64)
	for _, count := range counts {
		lookup[count.Pipeline+"/"+count.Status] = count.Count
	}
	assertPipelineCount := func(pipeline string, status model.TaskStatus, want int64) {
		t.Helper()
		if got := lookup[pipeline+"/"+string(status)]; got != want {
			t.Fatalf("%s/%s = %d, want %d", pipeline, status, got, want)
		}
	}
	assertPipelineCount("prepare", model.TaskStatusQueued, 1)
	assertPipelineCount("prepare", model.TaskStatusScheduled, 1)
	assertPipelineCount("upload", model.TaskStatusRunning, 1)
	assertPipelineCount("commit", model.TaskStatusWaiting, 1)
	assertPipelineCount("sync", model.TaskStatusQueued, 1)
	assertPipelineCount("sync", model.TaskStatusScheduled, 1)
	assertPipelineCount("evict", model.TaskStatusRunning, 1)
	assertPipelineCount("cleanup", model.TaskStatusWaiting, 1)
	if _, ok := lookup["upload/"+string(model.TaskStatusCompleted)]; ok {
		t.Fatal("completed upload should not appear in active pipeline")
	}
	if _, ok := lookup["upload/"+string(model.TaskStatusFailed)]; ok {
		t.Fatal("failed upload should not appear in active pipeline")
	}
	if _, ok := lookup["evict/"+string(model.TaskStatusExhausted)]; ok {
		t.Fatal("exhausted evict task should not appear in active pipeline")
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
			IdempotencyKey: "count-active-queued",
			Status:         model.TaskStatusQueued,
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
			Status:         model.TaskStatusQueued,
		},
		{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          objectB,
			RefVersionID:   versionB.VersionID,
			IdempotencyKey: "count-active-other-bucket",
			Status:         model.TaskStatusQueued,
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
			Status:         model.TaskStatusQueued,
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
			Status:         model.TaskStatusQueued,
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
		t.Fatalf("active bucket task count = %d, want 2 (queued upload + running evict_cache)", count)
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

	// Seed a queued bucket-scoped task.
	queuedTask := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "bucket",
		RefID:          bucket.ID,
		RefVersionID:   "",
		IdempotencyKey: "cbr-queued-" + time.Now().Format(time.RFC3339Nano),
		Status:         model.TaskStatusQueued,
	}
	if err := repos.Tasks.Create(ctx, queuedTask); err != nil {
		t.Fatalf("seed queued task: %v", err)
	}

	// Happy path: complete the queued task by ref.
	if err := repos.Tasks.CompleteByRef(ctx, "bucket", bucket.ID, model.TaskTypeUpload); err != nil {
		t.Fatalf("CompleteByRef (happy): %v", err)
	}

	// Verify the task is now completed.
	got, err := repos.Tasks.GetByID(ctx, queuedTask.ID)
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
	// because no active rows match.
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

func TestTaskRepo_CompleteByRefClearsWaitingFields(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	bucket := seedBucket(t, db, "complete-ref-waiting-bucket")
	waitReason := model.TaskWaitReasonDependency
	statusMessage := "waiting for dependency"
	lastError := "previous error"
	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "bucket",
		RefID:          bucket.ID,
		RefVersionID:   "",
		IdempotencyKey: "cbr-waiting-" + time.Now().Format(time.RFC3339Nano),
		Status:         model.TaskStatusWaiting,
		WaitReason:     &waitReason,
		StatusMessage:  &statusMessage,
		LastError:      &lastError,
		ScheduledAt:    time.Now().Add(time.Minute),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("seed waiting task: %v", err)
	}

	if err := repos.Tasks.CompleteByRef(ctx, "bucket", bucket.ID, model.TaskTypeUpload); err != nil {
		t.Fatalf("CompleteByRef: %v", err)
	}

	got, err := repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Status != model.TaskStatusCompleted {
		t.Fatalf("status = %s, want completed", got.Status)
	}
	if got.WaitReason != nil || got.StatusMessage != nil || got.LastError != nil {
		t.Fatalf("completed task diagnostics = wait:%v message:%v error:%v, want cleared", got.WaitReason, got.StatusMessage, got.LastError)
	}
}
