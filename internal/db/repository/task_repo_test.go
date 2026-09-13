package repository_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

type taskGCSelectionBarrier struct {
	selected chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (b *taskGCSelectionBarrier) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (b *taskGCSelectionBarrier) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	query := strings.ToLower(event.Query)
	if event.Err != nil || event.Operation() != "SELECT" ||
		!strings.Contains(query, "retention_until is not null") ||
		!strings.Contains(query, "not exists") {
		return
	}
	b.once.Do(func() {
		close(b.selected)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	})
}

func TestTaskGCDoesNotDeleteTaskRecoveredAfterSelection(t *testing.T) {
	db := testutil.NewTestFileDB(t)
	db.SetMaxOpenConns(2)
	repos := repository.NewRepositories(db)
	taskRow, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
		Type: "gc_race", IdempotencyKey: "recover-after-selection", InputVersion: 1,
		Input: []byte(`{}`), InputHash: "test", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})
	if err != nil || !created {
		t.Fatalf("enqueue task = %#v created=%v err=%v", taskRow, created, err)
	}
	claimed, err := repos.Tasks.ClaimNext(t.Context(), time.Minute)
	if err != nil || claimed == nil {
		t.Fatalf("claim task = %#v err=%v", claimed, err)
	}
	if err := repos.Tasks.Settle(t.Context(), claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusFailed, ResumeMode: model.TaskResumeModeRecover,
	}); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	if err := repos.Tasks.AcknowledgeFailed(t.Context(), taskRow.ID, time.Hour); err != nil {
		t.Fatalf("acknowledge task: %v", err)
	}
	if _, err := db.NewRaw(`UPDATE tasks SET retention_until = ? WHERE id = ?`, time.Now().Add(-time.Minute), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatalf("expire task retention: %v", err)
	}

	barrier := &taskGCSelectionBarrier{selected: make(chan struct{}), release: make(chan struct{})}
	db.AddQueryHook(barrier)
	type gcResult struct {
		deleted int
		err     error
	}
	result := make(chan gcResult, 1)
	go func() {
		deleted, deleteErr := repos.Tasks.DeleteRetained(context.Background(), time.Now(), 10)
		result <- gcResult{deleted: deleted, err: deleteErr}
	}()
	select {
	case <-barrier.selected:
	case <-time.After(time.Second):
		close(barrier.release)
		t.Fatal("task GC did not reach the selection barrier")
	}
	if err := repos.Tasks.RetryFailed(t.Context(), taskRow.ID); err != nil {
		close(barrier.release)
		t.Fatalf("recover task during GC: %v", err)
	}
	close(barrier.release)
	out := <-result
	if out.err != nil || out.deleted != 0 {
		t.Fatalf("task GC result = deleted:%d err:%v", out.deleted, out.err)
	}
	stored, err := repos.Tasks.GetByID(t.Context(), taskRow.ID)
	if err != nil || stored == nil || stored.Status != model.TaskStatusPending || stored.ResumeMode != model.TaskResumeModeRecover {
		t.Fatalf("recovered task = %#v err=%v", stored, err)
	}
}

func enqueueAndClaimTask(t *testing.T, repos *repository.Repositories, key string, lease time.Duration) *model.Task {
	t.Helper()
	if _, created, err := repos.Tasks.Enqueue(t.Context(), &model.Task{
		Type: "repository_test", IdempotencyKey: key, InputVersion: 1,
		Input: []byte(`{}`), InputHash: "test", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	}); err != nil || !created {
		t.Fatalf("enqueue task %s: created=%v err=%v", key, created, err)
	}
	claimed, err := repos.Tasks.ClaimNext(t.Context(), lease)
	if err != nil || claimed == nil {
		t.Fatalf("claim task %s = %#v err=%v", key, claimed, err)
	}
	return claimed
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
	if err := db.NewRaw(`SELECT typeof(checkpoint_json) FROM task_payloads WHERE task_id = ?`, claimed.ID).Scan(t.Context(), &storageClass); err != nil || storageClass != "text" {
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
	if listed := page.Tasks[0]; string(listed.Checkpoint) != `{"attempted":true}` || len(listed.Input) != 0 {
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
		row, created, err := repos.Tasks.Enqueue(ctx, &model.Task{
			Type: taskType, IdempotencyKey: key, InputVersion: 1,
			Input: []byte(`{}`), InputHash: key, Status: model.TaskStatusPending,
			ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
		})
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
	if err := repos.Tasks.AcknowledgeFailed(ctx, alreadyDismissed, time.Hour); err != nil {
		t.Fatalf("AcknowledgeFailed: %v", err)
	}

	count, err := repos.Tasks.AcknowledgeFailedMatching(ctx, repository.TaskAcknowledgeFilter{
		Type: model.TaskTypeStorageStore, FailedBefore: cutoff,
	}, time.Hour)
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
		if check.dismissed && stored.RetentionUntil == nil {
			t.Fatalf("%s was dismissed without a retention deadline", check.what)
		}
	}
}

func TestWriteCheckpointRequiresPayloadRow(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	claimed := enqueueAndClaimTask(t, repos, "checkpoint-missing-payload", time.Minute)
	if _, err := db.NewRaw(`DELETE FROM task_payloads WHERE task_id = ?`, claimed.ID).Exec(t.Context()); err != nil {
		t.Fatalf("delete payload: %v", err)
	}
	err := repos.Tasks.WriteCheckpoint(t.Context(), claimed.ID, claimed.ClaimGeneration, []byte(`{"attempted":true}`))
	if !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("checkpoint without payload err = %v, want ErrNotFound", err)
	}
	var resumeMode string
	if err := db.NewRaw(`SELECT resume_mode FROM tasks WHERE id = ?`, claimed.ID).Scan(t.Context(), &resumeMode); err != nil || resumeMode != string(model.TaskResumeModeExecute) {
		t.Fatalf("resume mode after rejected checkpoint = %q, err=%v", resumeMode, err)
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
