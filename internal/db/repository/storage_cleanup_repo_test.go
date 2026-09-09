package repository_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
)

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
			Status: model.StorageCleanupCopyStatusPending,
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
