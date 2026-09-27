package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// OrphanedContent is content no object version has ever named.
type OrphanedContent struct {
	ContentID  int64  `bun:"content_id"`
	BucketName string `bun:"bucket_name"`
}

// orphanedContentSQL matches content no object version has ever named, given
// the table reference the content row is read under. A version's removal
// always raises cleanup_generation, and cache residency and copies only follow
// a version, so these conditions describe a row whose write never committed.
func orphanedContentSQL(content string) string {
	return content + `.cleanup_generation = 0
		AND ` + content + `.cleanup_task_id IS NULL
		AND NOT EXISTS (SELECT 1 FROM object_versions AS orphan_version WHERE orphan_version.content_id = ` + content + `.id)
		AND NOT EXISTS (SELECT 1 FROM storage_copies AS orphan_copy WHERE orphan_copy.content_id = ` + content + `.id)
		AND NOT EXISTS (SELECT 1 FROM object_cache AS orphan_cache WHERE orphan_cache.content_id = ` + content + `.id)
		AND NOT EXISTS (SELECT 1 FROM storage_data_sets AS orphan_created WHERE orphan_created.created_by_content_id = ` + content + `.id)
		AND NOT EXISTS (SELECT 1 FROM storage_data_sets AS orphan_used WHERE orphan_used.last_used_content_id = ` + content + `.id)`
}

// ListOrphanedContents pages through content created before createdBefore that
// no object version has ever named.
func (r *BunStorageContentRepo) ListOrphanedContents(ctx context.Context, createdBefore time.Time, afterID int64, limit int) ([]OrphanedContent, error) {
	if limit <= 0 || createdBefore.IsZero() {
		return nil, fmt.Errorf("listing orphaned content: %w", ErrInvalidInput)
	}
	var rows []OrphanedContent
	err := r.db.NewSelect().
		TableExpr("storage_contents AS storage_content").
		ColumnExpr("storage_content.id AS content_id").
		ColumnExpr("bucket.name AS bucket_name").
		Join("JOIN buckets AS bucket ON bucket.id = storage_content.bucket_id").
		Where("storage_content.id > ?", afterID).
		Where("storage_content.created_at < ?", createdBefore).
		Where(orphanedContentSQL("storage_content")).
		OrderExpr("storage_content.id ASC").
		Limit(limit).
		Scan(ctx, &rows)
	if err != nil {
		return nil, fmt.Errorf("listing orphaned content: %w", err)
	}
	return rows, nil
}

// DiscardOrphanedContent deletes content no object version has ever named,
// after release removes its cached file. The file goes first: if the row
// delete then fails, a row without a file is left for the next discard, and a
// later write of the same bytes commits its own file before naming the row.
// The reverse order could leave a file that nothing names. It reports whether
// the content was deleted.
func (r *BunStorageContentRepo) DiscardOrphanedContent(ctx context.Context, contentID int64, release func() error) (bool, error) {
	if contentID <= 0 || release == nil {
		return false, fmt.Errorf("discarding orphaned content: %w", ErrInvalidInput)
	}
	discarded := false
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		if _, err := lockStorageContentsByID(ctx, db, []int64{contentID}); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
		orphaned, err := db.NewSelect().
			TableExpr("storage_contents AS storage_content").
			Where("storage_content.id = ?", contentID).
			Where(orphanedContentSQL("storage_content")).
			Exists(ctx)
		if err != nil {
			return fmt.Errorf("checking orphaned content %d: %w", contentID, err)
		}
		if !orphaned {
			return nil
		}
		if err := release(); err != nil {
			return fmt.Errorf("deleting orphaned content cache file: %w", err)
		}
		if _, err := db.NewDelete().Model((*model.StorageContent)(nil)).Where("id = ?", contentID).Exec(ctx); err != nil {
			return fmt.Errorf("deleting orphaned content %d: %w", contentID, err)
		}
		discarded = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return discarded, nil
}

// adoptRequestedCopies lets content no version has ever named take the copy
// target of the write about to name it. The target otherwise freezes when the
// content is created, and a row left by a failed write would keep the policy
// of that write.
func (r *BunStorageContentRepo) adoptRequestedCopies(ctx context.Context, content *model.StorageContent, requestedCopies int) error {
	if content.RequestedCopies == requestedCopies || content.CleanupGeneration != 0 || content.CleanupTaskID != nil {
		return nil
	}
	result, err := r.db.NewRaw(`UPDATE storage_contents
		SET requested_copies = ?, updated_at = ?
		WHERE storage_contents.id = ? AND `+orphanedContentSQL("storage_contents"),
		requestedCopies, time.Now(), content.ID).Exec(ctx)
	if err != nil {
		return fmt.Errorf("adopting requested copies for content %d: %w", content.ID, err)
	}
	if rows, _ := result.RowsAffected(); rows == 1 {
		content.RequestedCopies = requestedCopies
	}
	return nil
}
