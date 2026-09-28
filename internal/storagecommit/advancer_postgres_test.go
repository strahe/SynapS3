//go:build postgres

package storagecommit_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

type commitReleaseTaskLockSignal struct {
	once    sync.Once
	started chan struct{}
}

type commitReleaseContextKey struct{}

func (h *commitReleaseTaskLockSignal) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	query := strings.ToLower(event.Query)
	if ctx.Value(commitReleaseContextKey{}) != nil && strings.Contains(query, "update") &&
		strings.Contains(query, "tasks") && strings.Contains(query, "updated_at = updated_at") {
		h.once.Do(func() { close(h.started) })
	}
	return ctx
}

func (*commitReleaseTaskLockSignal) AfterQuery(context.Context, *bun.QueryEvent) {}

func TestPostgresReleaseCommitAttentionLocksTaskBeforeStorage(t *testing.T) {
	db := testutil.NewTestPostgresDB(t)
	repos, copyRow, claimed, attemptID := seedCommitAttentionTask(t, db)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin settlement: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := repository.NewRepositories(tx).Tasks.ValidateClaim(ctx, claimed.ID, claimed.ClaimGeneration); err != nil {
		t.Fatalf("lock settling task: %v", err)
	}
	signal := &commitReleaseTaskLockSignal{started: make(chan struct{})}
	db.AddQueryHook(signal)
	released := make(chan error, 1)
	go func() {
		releaseCtx := context.WithValue(ctx, commitReleaseContextKey{}, true)
		released <- repos.Contents.ReleaseCommitAttention(releaseCtx, storagecommit.ManualReleaseInput{
			CopyID: copyRow.ID, ExpectedAttemptID: attemptID, AcknowledgePossibleDuplicate: true,
		})
	}()
	select {
	case <-signal.started:
	case <-ctx.Done():
		t.Fatalf("release did not try to lock the task first: %v", ctx.Err())
	}
	if _, err := tx.NewRaw("UPDATE storage_contents SET updated_at = updated_at WHERE id = ?", copyRow.ContentID).Exec(ctx); err != nil {
		t.Fatalf("settlement storage lock: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit settlement: %v", err)
	}
	select {
	case err := <-released:
		if err != nil {
			t.Fatalf("release after settlement lock: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("release deadlocked with settlement: %v", ctx.Err())
	}
}
