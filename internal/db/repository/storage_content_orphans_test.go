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

// A write registers its bytes long before its version commits, from a policy
// read when it started. The target is settled only when a version first names
// the content, from the bucket's policy at that moment, and then stays frozen.
func TestFirstReferenceFreezesTheBucketsCurrentCopyPolicy(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := &model.Bucket{Name: "first-reference-policy", Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Create bucket: %v", err)
	}
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
	raisePolicy := func(copies int) {
		t.Helper()
		if _, err := repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
			Name: bucket.Name, SetDefaultCopies: true, DefaultCopies: &copies,
		}); err != nil {
			t.Fatalf("UpdateCopyPolicy(%d): %v", copies, err)
		}
	}
	name := func(key string, contentID int64) {
		t.Helper()
		version := newObjectVersion(bucket.ID, key, model.NewVersionID(), 10)
		version.ContentID = &contentID
		if _, err := createVersion(t, repos, version); err != nil {
			t.Fatalf("name content with %s: %v", key, err)
		}
	}
	requestedCopies := func(contentID int64) int {
		t.Helper()
		stored, err := repos.Contents.GetByID(ctx, contentID)
		if err != nil || stored == nil {
			t.Fatalf("GetByID(%d) = %#v, %v", contentID, stored, err)
		}
		return stored.RequestedCopies
	}

	content := ensure("stale-snapshot", 1)
	raisePolicy(3)
	// A write that read the old policy registers the same bytes again.
	if again := ensure("stale-snapshot", 1); again.ID != content.ID {
		t.Fatalf("content after second registration = %d, want %d", again.ID, content.ID)
	}
	name("first.txt", content.ID)
	if got := requestedCopies(content.ID); got != 3 {
		t.Fatalf("requested copies after the first reference = %d, want the bucket's current 3", got)
	}

	raisePolicy(4)
	name("second.txt", content.ID)
	if got := requestedCopies(content.ID); got != 3 {
		t.Fatalf("requested copies after a later reference = %d, want the frozen 3", got)
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
