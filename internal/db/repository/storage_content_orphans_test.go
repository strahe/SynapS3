package repository_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
)

func TestDiscardOrphanedContentRemovesOnlyContentNoVersionEverNamed(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "orphan-bucket")

	orphanID := seedContent(t, repos, bucket.ID, "orphan", 10)
	named := newObjectVersion(bucket.ID, "named.txt", "01J00000000000000000000B01", 10)
	if _, err := createVersion(t, repos, named); err != nil {
		t.Fatalf("seed named version: %v", err)
	}
	// Content whose last version went away is cleanup's to remove, not the
	// orphan scan's.
	cleanupID := seedContent(t, repos, bucket.ID, "under-cleanup", 10)
	if _, err := db.NewUpdate().Model((*model.StorageContent)(nil)).
		Set("cleanup_generation = 1").Where("id = ?", cleanupID).Exec(ctx); err != nil {
		t.Fatalf("mark content under cleanup: %v", err)
	}

	listed, err := repos.Contents.ListOrphanedContents(ctx, time.Now().Add(time.Minute), 0, 10)
	if err != nil {
		t.Fatalf("ListOrphanedContents: %v", err)
	}
	if len(listed) != 1 || listed[0].ContentID != orphanID || listed[0].BucketName != bucket.Name {
		t.Fatalf("orphans = %#v, want only content %d in %s", listed, orphanID, bucket.Name)
	}
	if early, err := repos.Contents.ListOrphanedContents(ctx, time.Now().Add(-time.Hour), 0, 10); err != nil || len(early) != 0 {
		t.Fatalf("orphans created before an earlier time = %#v, err=%v, want none", early, err)
	}

	for _, contentID := range []int64{*named.ContentID, cleanupID} {
		discarded, err := repos.Contents.DiscardOrphanedContent(ctx, contentID, func() error {
			t.Fatalf("released the cache file of content %d", contentID)
			return nil
		})
		if err != nil || discarded {
			t.Fatalf("discard content %d = %t, err=%v, want kept", contentID, discarded, err)
		}
	}

	// A file removed before the row delete fails leaves the row for a later
	// discard to finish.
	releases := 0
	if _, err := repos.Contents.DiscardOrphanedContent(ctx, orphanID, func() error {
		releases++
		return errors.New("file removed, then the discard failed")
	}); err == nil {
		t.Fatal("discard with a failing release succeeded")
	}
	if content, err := repos.Contents.GetByID(ctx, orphanID); err != nil || content == nil {
		t.Fatalf("orphan after failed discard = %#v, err=%v, want kept", content, err)
	}
	discarded, err := repos.Contents.DiscardOrphanedContent(ctx, orphanID, func() error {
		releases++
		return nil
	})
	if err != nil || !discarded || releases != 2 {
		t.Fatalf("retried discard = %t, err=%v, releases=%d", discarded, err, releases)
	}
	if content, err := repos.Contents.GetByID(ctx, orphanID); err != nil || content != nil {
		t.Fatalf("orphan after discard = %#v, err=%v, want gone", content, err)
	}
	if again, err := repos.Contents.DiscardOrphanedContent(ctx, orphanID, func() error { return nil }); err != nil || again {
		t.Fatalf("discard of removed content = %t, err=%v, want no-op", again, err)
	}
}

func TestEnsureContentAdoptsCurrentCopiesOnlyForUnnamedContent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "adopt-bucket")
	ensure := func(checksum string, copies int) *model.StorageContent {
		t.Helper()
		content, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
			BucketID: bucket.ID, ContentSize: 10, Checksum: testutil.StorageChecksum(checksum), RequestedCopies: copies,
		})
		if err != nil {
			t.Fatalf("EnsureContent(%s): %v", checksum, err)
		}
		return content
	}

	// A failed write left this row; the next write of the same bytes brings
	// the bucket's current policy.
	left := ensure("left-behind", 1)
	if adopted := ensure("left-behind", 3); adopted.ID != left.ID || adopted.RequestedCopies != 3 {
		t.Fatalf("content after later write = %#v, want row %d with 3 copies", adopted, left.ID)
	}
	if stored, err := repos.Contents.GetByID(ctx, left.ID); err != nil || stored.RequestedCopies != 3 {
		t.Fatalf("stored content = %#v, err=%v, want 3 copies", stored, err)
	}

	// Once a version names the content, its target stays frozen.
	named := newObjectVersion(bucket.ID, "named.txt", "01J00000000000000000000B02", 10)
	named.ContentID = &ensure("named", 1).ID
	if _, err := createVersion(t, repos, named); err != nil {
		t.Fatalf("seed named version: %v", err)
	}
	if kept := ensure("named", 3); kept.RequestedCopies != 1 {
		t.Fatalf("named content = %#v, want its original 1 copy", kept)
	}
}

// deleteConflictingContentHook deletes a content row right after an insert of
// the same bytes conflicts with it, the moment a finishing cleanup or the
// discard of a failed write can remove it.
type deleteConflictingContentHook struct {
	db        *bun.DB
	contentID int64
	fired     atomic.Bool
	err       error
}

func (*deleteConflictingContentHook) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h *deleteConflictingContentHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	if event.Err == nil || !strings.Contains(strings.ToLower(event.Query), `insert into "storage_contents"`) ||
		!h.fired.CompareAndSwap(false, true) {
		return
	}
	_, h.err = h.db.NewDelete().Model((*model.StorageContent)(nil)).Where("id = ?", h.contentID).Exec(ctx)
}

func TestEnsureContentInsertsAgainWhenTheConflictingRowIsDeleted(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "vanishing-bucket")
	leftID := seedContent(t, repos, bucket.ID, "vanishing", 10)
	hook := &deleteConflictingContentHook{db: db, contentID: leftID}
	db.AddQueryHook(hook)

	content, err := repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 10, Checksum: testutil.StorageChecksum("vanishing"), RequestedCopies: 1,
	})
	if !hook.fired.Load() || hook.err != nil {
		t.Fatalf("conflicting row deleted = %t, err=%v", hook.fired.Load(), hook.err)
	}
	if err != nil {
		t.Fatalf("EnsureContent after the conflicting row went away: %v", err)
	}
	if content.ID == leftID {
		t.Fatalf("content = deleted row %d, want a new row", leftID)
	}
	if stored, err := repos.Contents.GetByID(ctx, content.ID); err != nil || stored == nil {
		t.Fatalf("new content %d = %#v, err=%v", content.ID, stored, err)
	}
}
