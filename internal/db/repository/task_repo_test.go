package repository_test

import (
	"errors"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func enqueueAndClaimTask(t *testing.T, repos *repository.Repositories, key string, lease time.Duration) *model.Task {
	t.Helper()
	if _, created, err := repos.Tasks.Enqueue(t.Context(), repositoryTestTask(&model.Task{
		Type: "repository_test", IdempotencyKey: key, InputVersion: 1,
		Input: []byte(`{}`), InputHash: "test", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})); err != nil || !created {
		t.Fatalf("enqueue task %s: created=%v err=%v", key, created, err)
	}
	claimed, err := repos.Tasks.ClaimNext(t.Context(), lease)
	if err != nil || claimed == nil {
		t.Fatalf("claim task %s = %#v err=%v", key, claimed, err)
	}
	return claimed
}

func TestTaskClaimsClearWaitDetailsAndPreserveWorkStart(t *testing.T) {
	assertTaskClaimsClearWaitDetailsAndPreserveWorkStart(t, testDB(t))
}

func assertTaskClaimsClearWaitDetailsAndPreserveWorkStart(t *testing.T, db *bun.DB) {
	t.Helper()
	ctx := t.Context()
	repos := repository.NewRepositories(db)
	row, _, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{
		Type: model.TaskTypeUploadPlan, IdempotencyKey: "claim-timing", InputVersion: 1,
		Input: []byte(`{}`), InputHash: "test", AvailableAt: time.Now(),
		WaitReason: new("resource"), StatusMessage: new("Old wait"), LastError: new("Old error"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	claim := func() *model.Task {
		t.Helper()
		claimed, err := repos.Tasks.ClaimNext(ctx, time.Minute)
		if err != nil || claimed == nil || claimed.WaitReason != nil || claimed.StatusMessage != nil {
			t.Fatalf("claim retained wait details: %#v, err=%v", claimed, err)
		}
		return claimed
	}
	first := claim()
	if first.WorkStartedAt != nil || first.StartedAt == nil || first.LastError == nil || *first.LastError != "Old error" {
		t.Fatalf("claim invented work start or cleared history: %#v", first)
	}
	actual := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if err := repos.Tasks.MarkWorkStarted(ctx, row.ID, first.ClaimGeneration, actual); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.MarkWorkStarted(ctx, row.ID, first.ClaimGeneration, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := repos.Tasks.Settle(ctx, row.ID, first.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusPending, ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
		WaitReason: new("resource"), StatusMessage: new("New wait"), LastError: new("Old error"),
	}); err != nil {
		t.Fatal(err)
	}
	second := claim()
	if second.WorkStartedAt == nil || !second.WorkStartedAt.Equal(actual) {
		t.Fatalf("reclaim reset actual work start: %#v", second)
	}
	if _, err := db.ExecContext(ctx, `UPDATE tasks SET lease_until = ?, wait_reason = 'resource', status_message = 'Expired wait' WHERE id = ?`, time.Now().Add(-time.Second), row.ID); err != nil {
		t.Fatal(err)
	}
	recovered := claim()
	if recovered.ResumeMode != model.TaskResumeModeRecover || recovered.WorkStartedAt == nil || !recovered.WorkStartedAt.Equal(actual) {
		t.Fatalf("expired reclaim changed operation timing: %#v", recovered)
	}
	if err := repos.Tasks.MarkWorkStarted(ctx, row.ID, second.ClaimGeneration, time.Now()); !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("stale start writer = %v", err)
	}
	if err := repos.Tasks.Settle(ctx, row.ID, recovered.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover}); err != nil {
		t.Fatal(err)
	}

	stored, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil {
		t.Fatal(err)
	}
	child := repositorySuccessor(t, repos, stored, false)
	retried := claim()
	if retried.ID != child.ID || retried.WorkStartedAt != nil || retried.RetryCount != 0 {
		t.Fatalf("successor retained previous work start or budget: %#v", retried)
	}
	original, err := repos.Tasks.GetByID(ctx, row.ID)
	if err != nil || original.Status != model.TaskStatusFailed || original.WorkStartedAt == nil || !original.WorkStartedAt.Equal(actual) {
		t.Fatalf("retry changed original timing: %#v, %v", original, err)
	}
}

func TestWriteCheckpointStoresJSONText(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	claimed := enqueueAndClaimTask(t, repos, "checkpoint-text", time.Minute)
	if err := repos.Tasks.WriteCheckpoint(t.Context(), claimed.ID, claimed.ClaimGeneration, []byte(`{"attempted":true}`)); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	// A BLOB would still pass json_valid(); the schema promises TEXT JSON.
	var storageClass string
	if err := db.NewRaw(`SELECT typeof(checkpoint_json) FROM tasks WHERE id = ?`, claimed.ID).Scan(t.Context(), &storageClass); err != nil || storageClass != "text" {
		t.Fatalf("checkpoint storage class = %q, err=%v", storageClass, err)
	}
	stored, err := repos.Tasks.GetByID(t.Context(), claimed.ID)
	if err != nil {
		t.Fatalf("load task: %v", err)
	}
	if string(stored.Checkpoint) != `{"attempted":true}` || stored.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("stored checkpoint = %s, resume mode = %s", stored.Checkpoint, stored.ResumeMode)
	}
}

// TestListKeepsCheckpointForRetryChecks checks that a task listing carries the
// checkpoint that manual-retry decisions read, and leaves the input out.
func TestListKeepsCheckpointForRetryChecks(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	claimed := enqueueAndClaimTask(t, repos, "list-checkpoint", time.Minute)
	if err := repos.Tasks.WriteCheckpoint(t.Context(), claimed.ID, claimed.ClaimGeneration, []byte(`{"attempted":true}`)); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	page, err := repos.Tasks.List(t.Context(), repository.TaskListFilter{Limit: 10})
	if err != nil || len(page.Tasks) != 1 {
		t.Fatalf("List = %#v, err=%v", page, err)
	}
	if listed := page.Tasks[0]; string(listed.Checkpoint) != `{"attempted":true}` || string(listed.Input) != `{}` {
		t.Fatalf("listed checkpoint = %s, input = %s", listed.Checkpoint, listed.Input)
	}
}

// TestAcknowledgeFailedMatchingDismissesTheSelectedBacklog checks that one bulk
// dismissal covers unacknowledged failures of the selected operation that failed
// before the cutoff, and leaves everything else visible.
func TestAcknowledgeFailedMatchingDismissesTheSelectedBacklog(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	seedFailed := func(key string, taskType model.TaskType, failedAt time.Time) int64 {
		t.Helper()
		row, created, err := repos.Tasks.Enqueue(ctx, repositoryTestTask(&model.Task{
			Type: taskType, IdempotencyKey: key, InputVersion: 1,
			Input: []byte(`{}`), InputHash: key, Status: model.TaskStatusPending,
			ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
		}))
		if err != nil || !created {
			t.Fatalf("enqueue %s: created=%v err=%v", key, created, err)
		}
		claimed, err := repos.Tasks.ClaimNext(ctx, time.Minute)
		if err != nil || claimed == nil || claimed.ID != row.ID {
			t.Fatalf("claim %s = %#v err=%v", key, claimed, err)
		}
		if err := repos.Tasks.Settle(ctx, claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
			Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
			FailureReason: new("store_not_started"), LastError: new("provider unavailable"),
		}); err != nil {
			t.Fatalf("settle %s: %v", key, err)
		}
		if _, err := db.NewRaw(`UPDATE tasks SET finished_at = ? WHERE id = ?`, failedAt, row.ID).Exec(ctx); err != nil {
			t.Fatalf("stamp %s failure time: %v", key, err)
		}
		return row.ID
	}
	cutoff := time.Now().Add(-time.Minute)
	matching := seedFailed("bulk-matching", model.TaskTypeStorageStore, cutoff.Add(-time.Minute))
	otherType := seedFailed("bulk-other-type", model.TaskTypeCacheEvict, cutoff.Add(-time.Minute))
	afterCutoff := seedFailed("bulk-after-cutoff", model.TaskTypeStorageStore, cutoff.Add(time.Minute))
	alreadyDismissed := seedFailed("bulk-already-dismissed", model.TaskTypeStorageStore, cutoff.Add(-time.Minute))
	if err := repos.Tasks.AcknowledgeFailed(ctx, alreadyDismissed); err != nil {
		t.Fatalf("AcknowledgeFailed: %v", err)
	}

	count, err := repos.Tasks.AcknowledgeFailedMatching(ctx, repository.TaskAcknowledgeFilter{
		Type: model.TaskTypeStorageStore, FailedBefore: cutoff,
	})
	if err != nil || count != 1 {
		t.Fatalf("AcknowledgeFailedMatching = %d, err=%v, want 1", count, err)
	}
	for _, check := range []struct {
		id        int64
		dismissed bool
		what      string
	}{
		{matching, true, "matching failure"},
		{afterCutoff, false, "failure after the cutoff"},
		{otherType, false, "failure of another operation"},
	} {
		stored, err := repos.Tasks.GetByID(ctx, check.id)
		if err != nil || stored == nil {
			t.Fatalf("load %s: %#v err=%v", check.what, stored, err)
		}
		if (stored.AcknowledgedAt != nil) != check.dismissed {
			t.Fatalf("%s dismissed = %v, want %v", check.what, stored.AcknowledgedAt != nil, check.dismissed)
		}

	}
}

func TestWriteCheckpointRequiresLiveClaim(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	claimed := enqueueAndClaimTask(t, repos, "checkpoint-archived", time.Minute)
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeExecute}); err != nil {
		t.Fatal(err)
	}
	err := repos.Tasks.WriteCheckpoint(t.Context(), claimed.ID, claimed.ClaimGeneration, []byte(`{"attempted":true}`))
	if !errors.Is(err, repository.ErrTaskLeaseLost) {
		t.Fatalf("archived checkpoint error = %v", err)
	}
	stored, err := repos.Tasks.GetByID(t.Context(), claimed.ID)
	if err != nil || stored == nil || len(stored.Checkpoint) != 0 {
		t.Fatalf("archived task changed: %#v, %v", stored, err)
	}
}

func TestShortenLeaseNeverExtendsLease(t *testing.T) {
	repos := repository.NewRepositories(testDB(t))
	claimed := enqueueAndClaimTask(t, repos, "shorten-lease", 2*time.Second)
	if err := repos.Tasks.ShortenLease(t.Context(), claimed.ID, claimed.ClaimGeneration, time.Hour); err != nil {
		t.Fatalf("shorten lease: %v", err)
	}
	kept, err := repos.Tasks.GetByID(t.Context(), claimed.ID)
	if err != nil {
		t.Fatalf("load task: %v", err)
	}
	if kept.LeaseUntil == nil || kept.LeaseUntil.After(*claimed.LeaseUntil) {
		t.Fatalf("lease after longer request = %v, want at most %v", kept.LeaseUntil, claimed.LeaseUntil)
	}
	if err := repos.Tasks.ShortenLease(t.Context(), claimed.ID, claimed.ClaimGeneration, 100*time.Millisecond); err != nil {
		t.Fatalf("shorten lease: %v", err)
	}
	shortened, err := repos.Tasks.GetByID(t.Context(), claimed.ID)
	if err != nil {
		t.Fatalf("load task: %v", err)
	}
	if shortened.LeaseUntil == nil || !shortened.LeaseUntil.Before(*kept.LeaseUntil) {
		t.Fatalf("lease after shorter request = %v, want before %v", shortened.LeaseUntil, kept.LeaseUntil)
	}
}
