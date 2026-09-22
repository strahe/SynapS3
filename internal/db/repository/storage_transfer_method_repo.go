package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func (r *BunStorageContentRepo) IsPendingReplacementCopy(ctx context.Context, copyID int64) (bool, error) {
	var count int
	err := r.db.NewRaw(`SELECT COUNT(*) FROM storage_copies AS copy
		JOIN storage_replacement_items AS item
		  ON item.content_id = copy.content_id AND item.target_data_set_id = copy.storage_data_set_id
		JOIN storage_replacements AS replacement ON replacement.id = item.replacement_id
		WHERE copy.id = ? AND item.status = ?
		  AND replacement.status NOT IN (?, ?)`, copyID, storagereplacement.ItemStatusPending,
		storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).Scan(ctx, &count)
	return count > 0, err
}

// SetCopyCacheRestore is fenced by the copy task and the pending replacement.
// Locking the cache entry before the content uses the same order as eviction.
func (r *BunStorageContentRepo) SetCopyCacheRestore(ctx context.Context, copyID, generation, taskID int64, pullAttemptID string) error {
	if copyID < 1 || generation < 1 || taskID < 1 {
		return ErrInvalidInput
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		copyRow := new(model.StorageCopy)
		if err := db.NewSelect().Model(copyRow).Where("id = ?", copyID).Scan(ctx); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		entry, err := lockCacheEntry(ctx, db, copyRow.ContentID)
		if err != nil {
			return err
		}
		if !entry.InCache {
			return ErrConflict
		}
		if err := lockStorageContentForCopyMutation(ctx, db, copyRow.ContentID); err != nil {
			return err
		}
		pending, err := (&BunStorageContentRepo{db: db}).IsPendingReplacementCopy(ctx, copyID)
		if err != nil {
			return err
		}
		if !pending {
			return ErrConflict
		}
		now := time.Now()
		if pullAttemptID != "" {
			result, err := db.NewUpdate().Model((*storagepull.Attempt)(nil)).
				Set("status = ?", storagepull.AttemptStatusAbandoned).
				Set("resolved_at = ?", now).Set("last_error = ?", "pull failed; recovering from cache").
				Set("updated_at = ?", now).
				Where("attempt_id = ? AND content_id = ? AND storage_data_set_id = ?", pullAttemptID, copyRow.ContentID, copyRow.StorageDataSetID).
				Where("status = ? AND resolved_at IS NULL", storagepull.AttemptStatusAttempted).Exec(ctx)
			if err != nil {
				return err
			}
			if rows, _ := result.RowsAffected(); rows != 1 {
				return ErrConflict
			}
		}
		result, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
			Set("transfer_method = ?", model.StorageCopyTransferMethodCacheRestore).
			Set("commit_extra_data_hex = NULL").Set("ingress_bytes_transferred = 0").
			Set("ingress_store_attempt = 0").Set("progress_updated_at = NULL").
			Set("updated_at = ?", now).
			Where("id = ? AND work_generation = ? AND active_task_id = ?", copyID, generation, taskID).
			Where("status = ? AND transfer_method = ?", model.StorageCopyStatusPending, model.StorageCopyTransferMethodPeerPull).
			Where(`NOT EXISTS (SELECT 1 FROM storage_commit_attempts AS attempt
				WHERE attempt.content_id = storage_copy.content_id
				  AND attempt.storage_data_set_id = storage_copy.storage_data_set_id
				  AND attempt.resolved_at IS NULL)`).Exec(ctx)
		if err != nil {
			return fmt.Errorf("setting cache restore transfer: %w", err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunStorageContentRepo) AbandonMigrationPull(ctx context.Context, copyID, generation, taskID int64, pullAttemptID string) error {
	if copyID < 1 || generation < 1 || taskID < 1 || pullAttemptID == "" {
		return ErrInvalidInput
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		copyRow := new(model.StorageCopy)
		if err := db.NewSelect().Model(copyRow).
			Where("id = ? AND work_generation = ? AND active_task_id = ?", copyID, generation, taskID).
			Where("status = ? AND transfer_method = ?", model.StorageCopyStatusPending, model.StorageCopyTransferMethodPeerPull).
			Scan(ctx); err != nil {
			return err
		}
		pending, err := (&BunStorageContentRepo{db: db}).IsPendingReplacementCopy(ctx, copyID)
		if err != nil || !pending {
			return errors.Join(err, ErrConflict)
		}
		now := time.Now()
		result, err := db.NewUpdate().Model((*storagepull.Attempt)(nil)).
			Set("status = ?", storagepull.AttemptStatusAbandoned).
			Set("last_error = ?", "pull failed; local cache unavailable").
			Set("resolved_at = ?", now).Set("updated_at = ?", now).
			Where("attempt_id = ? AND content_id = ? AND storage_data_set_id = ?", pullAttemptID, copyRow.ContentID, copyRow.StorageDataSetID).
			Where("status = ? AND resolved_at IS NULL", storagepull.AttemptStatusAttempted).Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunStorageContentRepo) PromotePendingIngress(ctx context.Context, contentID int64) (*model.StorageCopy, error) {
	var promoted *model.StorageCopy
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockStorageContentForCopyMutation(ctx, db, contentID); err != nil {
			return err
		}
		var candidates []model.StorageCopy
		err := db.NewSelect().Model(&candidates).
			Where("content_id = ? AND status = ? AND transfer_method = ?", contentID, model.StorageCopyStatusPending, model.StorageCopyTransferMethodPeerPull).
			Where(`NOT EXISTS (SELECT 1 FROM storage_pull_attempts AS attempt
				WHERE attempt.content_id = storage_copy.content_id AND attempt.storage_data_set_id = storage_copy.storage_data_set_id)`).
			Where(`NOT EXISTS (SELECT 1 FROM storage_replacement_items AS item
				WHERE item.content_id = storage_copy.content_id AND item.target_data_set_id = storage_copy.storage_data_set_id)`).
			OrderExpr("copy_index ASC").Limit(1).Scan(ctx)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			return nil
		}
		result, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
			Set("transfer_method = ?", model.StorageCopyTransferMethodIngress).
			Set("updated_at = ?", time.Now()).
			Where("id = ? AND status = ? AND transfer_method = ?", candidates[0].ID, model.StorageCopyStatusPending, model.StorageCopyTransferMethodPeerPull).
			Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		_, err = db.NewUpdate().Model((*model.StorageContent)(nil)).Set("error_message = NULL").
			Where("id = ? AND accepted_at IS NULL", contentID).Exec(ctx)
		if err != nil {
			return err
		}
		promoted = &candidates[0]
		promoted.TransferMethod = model.StorageCopyTransferMethodIngress
		return nil
	})
	return promoted, err
}

func (r *BunStorageContentRepo) ReopenFailedIngressForPull(ctx context.Context, contentID int64) ([]model.StorageCopy, error) {
	var reopened []model.StorageCopy
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockStorageContentForCopyMutation(ctx, db, contentID); err != nil {
			return err
		}
		var failed []model.StorageCopy
		if err := db.NewSelect().Model(&failed).
			Where("content_id = ? AND status = ? AND transfer_method = ? AND active_task_id IS NULL", contentID, model.StorageCopyStatusFailed, model.StorageCopyTransferMethodIngress).
			OrderExpr("copy_index ASC").Scan(ctx); err != nil {
			return err
		}
		for _, copyRow := range failed {
			result, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
				Set("transfer_method = ?", model.StorageCopyTransferMethodPeerPull).
				Set("ingress_bytes_transferred = 0").Set("ingress_store_attempt = 0").Set("progress_updated_at = NULL").
				Set("updated_at = ?", time.Now()).Where("id = ? AND status = ?", copyRow.ID, model.StorageCopyStatusFailed).Exec(ctx)
			if err != nil {
				return err
			}
			if rows, _ := result.RowsAffected(); rows != 1 {
				return ErrConflict
			}
			if err := reopenFailedUploadCopy(ctx, db, copyRow.ID); err != nil {
				return err
			}
			copyRow.TransferMethod = model.StorageCopyTransferMethodPeerPull
			copyRow.Status = model.StorageCopyStatusPending
			reopened = append(reopened, copyRow)
		}
		return nil
	})
	return reopened, err
}
