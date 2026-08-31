package repository_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

type commitAttentionRaceContextKey struct{}

type commitAttentionReleaseClaimBarrier struct {
	releaseItemUpdated chan struct{}
	claimUpdateStarted chan struct{}
	allowRelease       chan struct{}
	releaseOnce        sync.Once
	claimOnce          sync.Once
}

func (h *commitAttentionReleaseClaimBarrier) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	operation, _ := ctx.Value(commitAttentionRaceContextKey{}).(string)
	query := strings.ToLower(event.Query)
	if operation == "claim" &&
		strings.Contains(query, "update storage_replacement_items") &&
		strings.Contains(query, "attempts = attempts + 1") &&
		strings.Contains(query, "returning *") {
		h.claimOnce.Do(func() { close(h.claimUpdateStarted) })
	}
	return ctx
}

func (h *commitAttentionReleaseClaimBarrier) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	operation, _ := ctx.Value(commitAttentionRaceContextKey{}).(string)
	query := strings.ToLower(event.Query)
	if operation == "release" &&
		strings.Contains(query, "storage_replacement_items") &&
		strings.Contains(query, "set \"status\" = 'failed'") &&
		strings.Contains(query, "claimed_at is null") {
		h.releaseOnce.Do(func() {
			close(h.releaseItemUpdated)
			<-h.allowRelease
		})
	}
}

func waitPostgresCommitClaimBlocked(t *testing.T, ctx context.Context, db *bun.DB) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		err := db.NewRaw(`SELECT EXISTS (
			SELECT 1
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND state = 'active'
			  AND wait_event_type = 'Lock'
			  AND cardinality(pg_blocking_pids(pid)) > 0
			  AND query ILIKE '%UPDATE storage_replacement_items%'
			  AND query ILIKE '%attempts = attempts + 1%'
		)`).Scan(waitCtx, &blocked)
		if err != nil {
			t.Fatalf("observe blocked terminal replacement claim: %v", err)
		}
		if blocked {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("timed out waiting for terminal replacement claim to block on the release transaction")
		case <-ticker.C:
		}
	}
}

func TestPostgresConcurrentStorageCommitReservationsRespectCapacity(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}
	assertConcurrentStorageCommitReservationsRespectCapacity(
		t,
		newPostgresReplacementDB(t, context.Background(), dsn),
	)
}

func TestPostgresCommitAttentionReleaseDoesNotReviveTerminalReplacementClaim(t *testing.T) {
	dsn := os.Getenv("SYNAPS3_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("SYNAPS3_POSTGRES_TEST_DSN is not set")
	}
	ctx := context.Background()
	db := newPostgresReplacementDB(t, ctx, dsn)
	fixture := seedCommitAttentionReplacement(
		t,
		db,
		"postgres-commit-release-claim-race",
		storagereplacement.StatusFailed,
		false,
	)
	barrier := &commitAttentionReleaseClaimBarrier{
		releaseItemUpdated: make(chan struct{}),
		claimUpdateStarted: make(chan struct{}),
		allowRelease:       make(chan struct{}),
	}
	db.AddQueryHook(barrier)
	defer func() {
		select {
		case <-barrier.allowRelease:
		default:
			close(barrier.allowRelease)
		}
	}()

	releaseResult := make(chan error, 1)
	go func() {
		releaseCtx := context.WithValue(ctx, commitAttentionRaceContextKey{}, "release")
		releaseResult <- fixture.repos.Uploads.ReleaseCommitAttention(releaseCtx, storagecommit.ManualReleaseInput{
			CopyID:                       fixture.copyRow.ID,
			ExpectedAttemptID:            fixture.attemptID,
			AcknowledgePossibleDuplicate: true,
		})
	}()
	waitReplacementSignal(t, barrier.releaseItemUpdated, "terminal replacement release update")

	type claimResult struct {
		item *storagereplacement.Item
		err  error
	}
	claimed := make(chan claimResult, 1)
	go func() {
		claimCtx := context.WithValue(ctx, commitAttentionRaceContextKey{}, "claim")
		item, err := fixture.repos.Replacements.ClaimReadyReplacementItem(claimCtx, time.Minute)
		claimed <- claimResult{item: item, err: err}
	}()
	waitReplacementSignal(t, barrier.claimUpdateStarted, "competing terminal replacement claim update")
	waitPostgresCommitClaimBlocked(t, ctx, db)
	close(barrier.allowRelease)

	if err := waitReplacementResult(t, releaseResult, "storage confirmation release"); err != nil {
		t.Fatalf("ReleaseCommitAttention: %v", err)
	}
	var claim claimResult
	select {
	case claim = <-claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for terminal replacement claim")
	}
	if claim.err != nil || claim.item != nil {
		t.Fatalf("terminal claim after release = %#v err=%v, want no work", claim.item, claim.err)
	}

	copyRow, err := fixture.repos.Uploads.GetUploadCopyByID(ctx, fixture.copyRow.ID)
	if err != nil || copyRow.CommitAttemptID != nil || copyRow.CommitAttentionAt != nil {
		t.Fatalf("copy after release = %#v err=%v, want cleared attention fence", copyRow, err)
	}
	item := new(storagereplacement.Item)
	if err := db.NewSelect().Model(item).Where("id = ?", fixture.item.ID).Scan(ctx); err != nil {
		t.Fatalf("load replacement item: %v", err)
	}
	if item.Status != storagereplacement.ItemStatusFailed || item.ClaimedAt != nil || item.LeaseUntil != nil {
		t.Fatalf("replacement item after race = %#v, want failed without a claim", item)
	}
}
