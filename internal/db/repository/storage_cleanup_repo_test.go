package repository_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
)

// TestStorageCleanupBlocksReuseUntilContentIsFinalized walks content through
// its last delete: cleanup starts only once no live version remains, new
// versions cannot name the content until it is finalized, and finalizing waits
// for cached bytes and blocking replacement items before it deletes the
// current-state rows and leaves the ledgers.
func TestStorageCleanupBlocksReuseUntilContentIsFinalized(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := t.Context()
	bucket := seedBucket(t, db, "cleanup-finalize")
	contentID := seedContent(t, repos, bucket.ID, "cleanup-finalize", 10)
	source, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: onChainID(t, "101"), CopyIndex: 0, CreatedByContentID: contentID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	versions := make([]*model.ObjectVersion, 2)
	for i := range versions {
		versions[i] = newObjectVersion(bucket.ID, fmt.Sprintf("file-%d.txt", i), model.NewVersionID(), 10)
		versions[i].ContentID = &contentID
		if _, err := createVersion(t, repos, versions[i]); err != nil {
			t.Fatalf("create version %d: %v", i, err)
		}
	}
	deleteVersion := func(version *model.ObjectVersion) *repository.StorageCleanupReservation {
		t.Helper()
		result, err := repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID: bucket.ID, Key: version.Key, VersionID: version.VersionID,
		})
		if err != nil {
			t.Fatalf("DeleteObjectVersionPermanently(%s): %v", version.Key, err)
		}
		return result.StorageCleanup
	}
	if cleanup := deleteVersion(versions[0]); cleanup != nil {
		t.Fatalf("cleanup reserved while another version is live: %#v", cleanup)
	}
	// Nothing was ever stored remotely, and the last delete still starts cleanup.
	cleanup := deleteVersion(versions[1])
	if cleanup == nil || cleanup.ContentID != contentID || cleanup.TaskID != nil {
		t.Fatalf("last delete cleanup = %#v", cleanup)
	}
	taskRow, created, err := repos.Tasks.Enqueue(ctx, &model.Task{
		Type: model.TaskTypeStorageCleanup, IdempotencyKey: "cleanup-finalize", InputVersion: 1,
		Input: []byte(`{}`), InputHash: "cleanup-finalize", Status: model.TaskStatusPending,
		ResumeMode: model.TaskResumeModeExecute, AvailableAt: time.Now(),
	})
	if err != nil || !created {
		t.Fatalf("Enqueue = %#v, created=%v, err=%v", taskRow, created, err)
	}
	if err := repos.StorageCleanup.BindTask(ctx, contentID, cleanup.Generation, taskRow.ID); err != nil {
		t.Fatalf("BindTask: %v", err)
	}
	reuse := func() error {
		version := newObjectVersion(bucket.ID, "reuse.txt", model.NewVersionID(), 10)
		version.ContentID = &contentID
		_, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version)
		return err
	}
	if err := reuse(); !errors.Is(err, repository.ErrContentCleanupInProgress) {
		t.Fatalf("reuse during cleanup = %v, want ErrContentCleanupInProgress", err)
	}

	replacement, _, err := repos.Replacements.Authorize(ctx, repository.AuthorizeReplacementInput{
		BucketID: bucket.ID, SourceDataSetID: source.ID, SelectionMode: storagereplacement.SelectionModeManual,
		TargetProviderID: onChainID(t, "202"), ClientRequestID: "cleanup-finalize",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	item := &storagereplacement.Item{
		ReplacementID: replacement.ID, ContentID: contentID, TargetDataSetID: replacement.TargetDataSetID,
		Status: storagereplacement.ItemStatusAttention,
	}
	if _, err := db.NewInsert().Model(item).Exec(ctx); err != nil {
		t.Fatalf("insert replacement item: %v", err)
	}
	now := time.Now()
	if _, err := db.NewInsert().Model(&model.ObjectCache{ContentID: contentID, InCache: true, CreatedAt: now, UpdatedAt: now}).
		On("CONFLICT (content_id) DO UPDATE").Set("in_cache = EXCLUDED.in_cache").Exec(ctx); err != nil {
		t.Fatalf("mark content cached: %v", err)
	}
	finalize := func(taskID int64) error {
		return repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
			return txRepos.StorageCleanup.FinalizeContent(ctx, contentID, cleanup.Generation, taskID)
		})
	}
	if err := finalize(taskRow.ID); !errors.Is(err, repository.ErrContentCleanupNotReady) {
		t.Fatalf("finalize with cached bytes = %v, want ErrContentCleanupNotReady", err)
	}
	if released, err := repos.Objects.ReleaseContentCacheIfUnreferenced(ctx, contentID, func() error { return nil }); err != nil || !released {
		t.Fatalf("release cache = %t, %v", released, err)
	}
	if err := finalize(taskRow.ID); !errors.Is(err, repository.ErrContentCleanupNotReady) {
		t.Fatalf("finalize with a replacement item in attention = %v, want ErrContentCleanupNotReady", err)
	}
	if _, err := db.NewUpdate().Model(item).Set("status = ?", storagereplacement.ItemStatusCancelled).WherePK().Exec(ctx); err != nil {
		t.Fatalf("cancel replacement item: %v", err)
	}
	if err := finalize(taskRow.ID + 1); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("finalize by another task = %v, want ErrConflict", err)
	}
	if err := finalize(taskRow.ID); err != nil {
		t.Fatalf("FinalizeContent: %v", err)
	}

	for _, rows := range []struct {
		table, column string
		want          int
	}{
		{"storage_contents", "id", 0},
		{"storage_copies", "content_id", 0},
		{"object_cache", "content_id", 0},
		{"storage_data_sets", "created_by_content_id", 0},
		{"object_deletions", "content_id", 2},
		{"storage_replacement_items", "content_id", 1},
	} {
		var count int
		if err := db.NewRaw("SELECT count(*) FROM "+rows.table+" WHERE "+rows.column+" = ?", contentID).Scan(ctx, &count); err != nil {
			t.Fatalf("count %s: %v", rows.table, err)
		}
		if count != rows.want {
			t.Fatalf("%s rows naming the content = %d, want %d", rows.table, count, rows.want)
		}
	}
	// A write that resolved the content before it was finalized is refused the
	// same way, and a cache file left under its key is an orphan.
	if err := reuse(); !errors.Is(err, repository.ErrContentCleanupInProgress) {
		t.Fatalf("reuse after finalizing = %v, want ErrContentCleanupInProgress", err)
	}
	releases := 0
	released, err := repos.Objects.ReleaseContentCacheIfUnreferenced(ctx, contentID, func() error {
		releases++
		return nil
	})
	if err != nil || !released || releases != 1 {
		t.Fatalf("orphan cache release = %t, %v, calls=%d", released, err, releases)
	}
}

func TestStorageCleanupCopyTransitionsAreGuardedAndIdempotent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "cleanup-transitions")
	contentID := seedContent(t, repos, bucket.ID, "cleanup-transitions", 10)
	providerID := onChainID(t, "701")
	binding, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
		BucketID: bucket.ID, ProviderID: providerID, CopyIndex: 0, CreatedByContentID: contentID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	insertCopy := func(piece int64) *model.StorageCleanupCopy {
		t.Helper()
		row := &model.StorageCleanupCopy{
			ContentID: contentID, BucketID: bucket.ID, CopyIndex: 0, ProviderID: providerID,
			StorageDataSetID: binding.ID, PieceID: onChainID(t, strconv.FormatInt(piece, 10)), PieceCID: "piece-cid",
			Checksum: "cleanup-transitions-checksum", Status: model.StorageCleanupCopyStatusPending,
		}
		if _, err := db.NewInsert().Model(row).Exec(t.Context()); err != nil {
			t.Fatalf("insert cleanup copy: %v", err)
		}
		return row
	}

	scheduled := insertCopy(801)
	if err := repos.StorageCleanup.MarkCopyDeleteScheduled(t.Context(), scheduled.ID, "0xtx"); err != nil {
		t.Fatalf("MarkCopyDeleteScheduled: %v", err)
	}
	first := new(model.StorageCleanupCopy)
	if err := db.NewSelect().Model(first).Where("id = ?", scheduled.ID).Scan(t.Context()); err != nil {
		t.Fatalf("load scheduled copy: %v", err)
	}
	if err := repos.StorageCleanup.MarkCopyDeleteScheduled(t.Context(), scheduled.ID, "0xtx"); err != nil {
		t.Fatalf("idempotent MarkCopyDeleteScheduled: %v", err)
	}
	replayed := new(model.StorageCleanupCopy)
	if err := db.NewSelect().Model(replayed).Where("id = ?", scheduled.ID).Scan(t.Context()); err != nil {
		t.Fatalf("reload scheduled copy: %v", err)
	}
	if first.ScheduledAt == nil || replayed.ScheduledAt == nil || !first.ScheduledAt.Equal(*replayed.ScheduledAt) {
		t.Fatalf("scheduled_at changed across replay: %v -> %v", first.ScheduledAt, replayed.ScheduledAt)
	}
	if err := repos.StorageCleanup.MarkCopyDeleteScheduled(t.Context(), scheduled.ID, "0xother"); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("changed scheduling evidence = %v, want ErrConflict", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), scheduled.ID); err != nil {
		t.Fatalf("MarkCopyRemoved(scheduled): %v", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), scheduled.ID); err != nil {
		t.Fatalf("idempotent MarkCopyRemoved: %v", err)
	}

	failed := insertCopy(802)
	if err := repos.StorageCleanup.MarkCopyFailed(t.Context(), failed.ID, "unknown outcome"); err != nil {
		t.Fatalf("MarkCopyFailed: %v", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), failed.ID); err != nil {
		t.Fatalf("MarkCopyRemoved(failed): %v", err)
	}

	unsupported := insertCopy(803)
	if err := repos.StorageCleanup.MarkCopyUnsupported(t.Context(), unsupported.ID, "unsupported"); err != nil {
		t.Fatalf("MarkCopyUnsupported: %v", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), unsupported.ID); !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("MarkCopyRemoved(unsupported) = %v, want ErrConflict", err)
	}
	if err := repos.StorageCleanup.MarkCopyRemoved(t.Context(), 999999); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("MarkCopyRemoved(missing) = %v, want ErrNotFound", err)
	}
}
