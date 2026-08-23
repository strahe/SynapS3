package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

type BunStorageCleanupRepo struct {
	db bun.IDB
}

var _ StorageCleanupRepository = (*BunStorageCleanupRepo)(nil)

func (r *BunStorageCleanupRepo) ListCopiesForTask(ctx context.Context, taskID int64) ([]model.StorageCleanupCopy, error) {
	var copies []model.StorageCleanupCopy
	if err := r.db.NewSelect().
		Model(&copies).
		Where("task_id = ?", taskID).
		OrderExpr("copy_index ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing storage cleanup copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageCleanupRepo) MarkCopyRemoved(ctx context.Context, id int64) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.StorageCleanupCopy)(nil)).
		Set("status = ?", model.StorageCleanupCopyStatusRemoved).
		Set("removed_at = COALESCE(removed_at, ?)", now).
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Exec(ctx)
	return storageCleanupCopyUpdateResult(res, err, "marking storage cleanup copy removed")
}

func (r *BunStorageCleanupRepo) MarkCopyDeleteScheduled(ctx context.Context, id int64, txHash string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.StorageCleanupCopy)(nil)).
		Set("status = ?", model.StorageCleanupCopyStatusDeleteScheduled).
		Set("delete_tx_hash = ?", txHash).
		Set("scheduled_at = COALESCE(scheduled_at, ?)", now).
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Exec(ctx)
	return storageCleanupCopyUpdateResult(res, err, "marking storage cleanup copy scheduled")
}

func (r *BunStorageCleanupRepo) MarkCopyUnsupported(ctx context.Context, id int64, message string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.StorageCleanupCopy)(nil)).
		Set("status = ?", model.StorageCleanupCopyStatusUnsupported).
		Set("last_error = ?", message).
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Exec(ctx)
	return storageCleanupCopyUpdateResult(res, err, "marking storage cleanup copy unsupported")
}

func (r *BunStorageCleanupRepo) UploadHasObjectReferences(ctx context.Context, uploadID int64) (bool, error) {
	return uploadHasObjectReferences(ctx, r.db, uploadID)
}

func uploadHasObjectReferences(ctx context.Context, db bun.IDB, uploadID int64) (bool, error) {
	var row struct {
		Count int `bun:"count"`
	}
	err := db.NewRaw(`SELECT COUNT(DISTINCT object_version.version_id) AS count
		FROM storage_uploads AS storage_upload
		JOIN object_versions AS object_version
		  ON `+objectVersionReferencesStorageUploadSQL("object_version", "storage_upload")+`
		WHERE storage_upload.id = ?
		  AND object_version.is_delete_marker = ?`, uploadID, false).
		Scan(ctx, &row)
	if err != nil {
		return false, fmt.Errorf("checking storage cleanup object references: %w", err)
	}
	return row.Count > 0, nil
}

func (r *BunStorageCleanupRepo) TaskHasObjectReferences(ctx context.Context, taskID int64, uploadID int64) (bool, error) {
	var row struct {
		Count int `bun:"count"`
	}
	err := r.db.NewRaw(`SELECT COUNT(DISTINCT object_version.version_id) AS count
		FROM storage_cleanup_copies AS cleanup_copy
		JOIN storage_upload_copies AS storage_copy
		  ON storage_copy.status = ?
		 AND (
			storage_copy.upload_id = ?
			OR (
				cleanup_copy.provider_id IS NOT NULL AND cleanup_copy.provider_id <> ''
				AND cleanup_copy.data_set_id IS NOT NULL AND cleanup_copy.data_set_id <> ''
				AND cleanup_copy.piece_id IS NOT NULL AND cleanup_copy.piece_id <> ''
				AND storage_copy.provider_id = cleanup_copy.provider_id
				AND storage_copy.piece_id = cleanup_copy.piece_id
			)
		 )
		LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		JOIN storage_uploads AS referenced_upload ON referenced_upload.id = storage_copy.upload_id
		JOIN object_versions AS object_version
		  ON `+objectVersionReferencesStorageUploadSQL("object_version", "referenced_upload")+`
		WHERE cleanup_copy.task_id = ?
		  AND object_version.is_delete_marker = FALSE
		  AND (
			storage_copy.upload_id = ?
			OR storage_data_set.data_set_id = cleanup_copy.data_set_id
		  )`,
		model.StorageUploadCopyStatusCommitted, uploadID, taskID, uploadID,
	).Scan(ctx, &row)
	if err != nil {
		return false, fmt.Errorf("checking storage cleanup task references: %w", err)
	}
	if row.Count > 0 {
		return true, nil
	}
	row.Count = 0
	err = r.db.NewRaw(`SELECT COUNT(DISTINCT active_upload.id) AS count
		FROM storage_cleanup_copies AS cleanup_copy
		JOIN storage_upload_copies AS storage_copy
		  ON storage_copy.status = ?
		 AND (
			storage_copy.upload_id = ?
			OR (
				cleanup_copy.provider_id IS NOT NULL AND cleanup_copy.provider_id <> ''
				AND cleanup_copy.data_set_id IS NOT NULL AND cleanup_copy.data_set_id <> ''
				AND cleanup_copy.piece_id IS NOT NULL AND cleanup_copy.piece_id <> ''
				AND storage_copy.provider_id = cleanup_copy.provider_id
				AND storage_copy.piece_id = cleanup_copy.piece_id
			)
		 )
		LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		JOIN storage_uploads AS active_upload ON active_upload.id = storage_copy.upload_id
		WHERE cleanup_copy.task_id = ?
		  AND active_upload.status IN (?)
		  AND (
			storage_copy.upload_id = ?
			OR storage_data_set.data_set_id = cleanup_copy.data_set_id
		  )`,
		model.StorageUploadCopyStatusCommitted, uploadID, taskID, bun.List(activeUploadStatuses()), uploadID,
	).Scan(ctx, &row)
	if err != nil {
		return false, fmt.Errorf("checking storage cleanup active upload references: %w", err)
	}
	return row.Count > 0, nil
}

func (r *BunStorageCleanupRepo) DeleteUploadProvenanceIfUnreferenced(ctx context.Context, uploadID int64) error {
	if uploadID <= 0 {
		return fmt.Errorf("deleting unreferenced storage upload provenance: %w", ErrInvalidInput)
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		if _, err := lockStorageUploadsByID(ctx, db, []int64{uploadID}); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return fmt.Errorf("locking storage upload before deleting provenance: %w", err)
		}
		hasRefs, err := uploadHasObjectReferences(ctx, db, uploadID)
		if err != nil {
			return err
		}
		if hasRefs {
			return nil
		}
		// Replacement items retain upload provenance while they are executable.
		// Settle and remove them explicitly so the RESTRICT foreign key remains a
		// guard against bypassing the replacement progress transaction.
		var items []storagereplacement.Item
		if err := db.NewRaw(
			lockReplacementItemsByUploadSQL(),
			uploadID,
		).Scan(ctx, &items); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("locking replacement items before deleting provenance: %w", err)
		}
		lockedReplacements := make(map[int64]struct{}, len(items))
		replacementIDs := make([]int64, 0, len(items))
		for i := range items {
			if _, ok := lockedReplacements[items[i].ReplacementID]; ok {
				continue
			}
			lockedReplacements[items[i].ReplacementID] = struct{}{}
			replacementIDs = append(replacementIDs, items[i].ReplacementID)
		}
		slices.Sort(replacementIDs)
		for _, replacementID := range replacementIDs {
			if _, err := lockReplacementByID(ctx, db, replacementID); err != nil {
				return err
			}
		}
		for i := range items {
			item := &items[i]
			if err := settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled); err != nil {
				return err
			}
		}
		if len(items) > 0 {
			if _, err := db.NewDelete().
				Model((*storagereplacement.Item)(nil)).
				Where("upload_id = ?", uploadID).
				Exec(ctx); err != nil {
				return fmt.Errorf("deleting settled replacement items: %w", err)
			}
		}
		if _, err := db.NewDelete().
			Model((*model.StorageUpload)(nil)).
			Where("id = ?", uploadID).
			Exec(ctx); err != nil {
			return fmt.Errorf("deleting unreferenced storage upload provenance: %w", err)
		}
		return nil
	})
}

func lockReplacementItemsByUploadSQL() string {
	return `UPDATE storage_replacement_items
		SET updated_at = updated_at
		WHERE upload_id = ?
		RETURNING *`
}

func (r *BunStorageCleanupRepo) runMaybeTx(ctx context.Context, fn func(bun.IDB) error) error {
	return runMaybeTx(ctx, r.db, fn)
}

func storageCleanupCopyUpdateResult(res sql.Result, err error, op string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	return nil
}
