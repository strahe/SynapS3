package repository_test

import (
	"context"
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
