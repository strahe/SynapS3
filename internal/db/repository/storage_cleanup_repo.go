package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

type BunStorageCleanupRepo struct {
	db bun.IDB
}

var _ StorageCleanupRepository = (*BunStorageCleanupRepo)(nil)

func (r *BunStorageCleanupRepo) BindTask(ctx context.Context, contentID, generation, taskID int64) error {
	if contentID < 1 || generation < 1 || taskID < 1 {
		return ErrInvalidInput
	}
	result, err := r.db.NewUpdate().
		Model((*model.StorageContent)(nil)).
		Set("cleanup_task_id = ?", taskID).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", contentID).
		Where("cleanup_generation = ?", generation).
		Where("cleanup_task_id IS NULL OR cleanup_task_id = ?", taskID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("binding storage cleanup task: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunStorageCleanupRepo) AuthorizeTask(ctx context.Context, contentID, generation, taskID int64) ([]model.StorageCleanupCopy, error) {
	var owner struct {
		TaskID *int64 `bun:"cleanup_task_id"`
	}
	if err := r.db.NewSelect().
		Model((*model.StorageContent)(nil)).
		Column("cleanup_task_id").
		Where("id = ? AND cleanup_generation = ?", contentID, generation).
		Scan(ctx, &owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("authorizing storage cleanup task: %w", err)
	}
	if owner.TaskID == nil || *owner.TaskID != taskID {
		return nil, ErrConflict
	}
	var copies []model.StorageCleanupCopy
	if err := r.db.NewSelect().
		Model(&copies).
		Where("content_id = ?", contentID).
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
		Where("status IN (?)", bun.List([]model.StorageCleanupCopyStatus{
			model.StorageCleanupCopyStatusPending,
			model.StorageCleanupCopyStatusDeleteScheduled,
			model.StorageCleanupCopyStatusFailed,
		})).
		Exec(ctx)
	if err := storageCleanupCopyTransitionResult(ctx, r.db, res, err, id, model.StorageCleanupCopyStatusRemoved, "", "marking storage cleanup copy removed"); err != nil {
		return err
	}
	return nil
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
		Where("status = ?", model.StorageCleanupCopyStatusPending).
		Exec(ctx)
	return storageCleanupCopyTransitionResult(
		ctx, r.db, res, err, id, model.StorageCleanupCopyStatusDeleteScheduled, txHash,
		"marking storage cleanup copy scheduled",
	)
}

func (r *BunStorageCleanupRepo) MarkCopyFailed(ctx context.Context, id int64, message string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.StorageCleanupCopy)(nil)).
		Set("status = ?", model.StorageCleanupCopyStatusFailed).
		Set("last_error = ?", message).
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Exec(ctx)
	return storageCleanupCopyUpdateResult(res, err, "marking storage cleanup copy failed")
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

func (r *BunStorageCleanupRepo) UploadHasObjectReferences(ctx context.Context, contentID int64) (bool, error) {
	return uploadHasObjectReferences(ctx, r.db, contentID)
}

func uploadHasObjectReferences(ctx context.Context, db bun.IDB, contentID int64) (bool, error) {
	var row struct {
		Count int `bun:"count"`
	}
	err := db.NewRaw(`SELECT COUNT(DISTINCT object_version.version_id) AS count
		FROM storage_contents AS storage_content
		JOIN object_versions AS object_version
		  ON `+objectVersionReferencesStorageContentSQL("object_version", "storage_content")+`
		WHERE storage_content.id = ?
		  AND object_version.is_delete_marker = ?`, contentID, false).
		Scan(ctx, &row)
	if err != nil {
		return false, fmt.Errorf("checking storage cleanup object references: %w", err)
	}
	return row.Count > 0, nil
}

func (r *BunStorageCleanupRepo) CleanupHasObjectReferences(ctx context.Context, contentID int64) (bool, error) {
	var row struct {
		Count int `bun:"count"`
	}
	err := r.db.NewRaw(`SELECT COUNT(DISTINCT object_version.version_id) AS count
		FROM storage_cleanup_copies AS cleanup_copy
		JOIN storage_copies AS storage_copy
		  ON storage_copy.status = ?
		 AND (
			storage_copy.content_id = ?
			OR (
				cleanup_copy.provider_id IS NOT NULL AND cleanup_copy.provider_id <> ''
				AND cleanup_copy.data_set_id IS NOT NULL AND cleanup_copy.data_set_id <> ''
				AND cleanup_copy.piece_id IS NOT NULL AND cleanup_copy.piece_id <> ''
				AND storage_copy.provider_id = cleanup_copy.provider_id
				AND storage_copy.piece_id = cleanup_copy.piece_id
			)
		 )
		LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		JOIN storage_contents AS referenced_upload ON referenced_upload.id = storage_copy.content_id
		JOIN object_versions AS object_version
		  ON `+objectVersionReferencesStorageContentSQL("object_version", "referenced_upload")+`
		WHERE cleanup_copy.content_id = ?
		  AND object_version.is_delete_marker = FALSE
		  AND (
			storage_copy.content_id = ?
			OR storage_data_set.data_set_id = cleanup_copy.data_set_id
		  )`,
		model.StorageCopyStatusCommitted, contentID, contentID, contentID,
	).Scan(ctx, &row)
	if err != nil {
		return false, fmt.Errorf("checking storage cleanup task references: %w", err)
	}
	if row.Count > 0 {
		return true, nil
	}
	row.Count = 0
	err = r.db.NewRaw(`SELECT COUNT(DISTINCT active_content.id) AS count
		FROM storage_cleanup_copies AS cleanup_copy
		JOIN storage_copies AS storage_copy
		  ON storage_copy.status = ?
		 AND (
			storage_copy.content_id = ?
			OR (
				cleanup_copy.provider_id IS NOT NULL AND cleanup_copy.provider_id <> ''
				AND cleanup_copy.data_set_id IS NOT NULL AND cleanup_copy.data_set_id <> ''
				AND cleanup_copy.piece_id IS NOT NULL AND cleanup_copy.piece_id <> ''
				AND storage_copy.provider_id = cleanup_copy.provider_id
				AND storage_copy.piece_id = cleanup_copy.piece_id
			)
		 )
		LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		JOIN storage_contents AS active_content ON active_content.id = storage_copy.content_id
		WHERE cleanup_copy.content_id = ?
		  AND active_content.accepted_at IS NULL
		  AND (
			storage_copy.content_id = ?
			OR storage_data_set.data_set_id = cleanup_copy.data_set_id
		  )`,
		model.StorageCopyStatusCommitted, contentID, contentID, contentID,
	).Scan(ctx, &row)
	if err != nil {
		return false, fmt.Errorf("checking storage cleanup active upload references: %w", err)
	}
	return row.Count > 0, nil
}

func (r *BunStorageCleanupRepo) CompleteTask(ctx context.Context, contentID, generation, taskID int64) error {
	result, err := r.db.NewUpdate().
		Model((*model.StorageContent)(nil)).
		Set("cleanup_task_id = NULL").
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND cleanup_generation = ? AND cleanup_task_id = ?", contentID, generation, taskID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing storage cleanup task: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrConflict
	}
	return nil
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

func storageCleanupCopyTransitionResult(
	ctx context.Context,
	db bun.IDB,
	res sql.Result,
	err error,
	id int64,
	idempotentStatus model.StorageCleanupCopyStatus,
	idempotentTxHash string,
	op string,
) error {
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 1 {
		return nil
	}
	var current struct {
		Status       model.StorageCleanupCopyStatus `bun:"status"`
		DeleteTxHash *string                        `bun:"delete_tx_hash"`
	}
	err = db.NewSelect().
		Model((*model.StorageCleanupCopy)(nil)).
		Column("status", "delete_tx_hash").
		Where("id = ?", id).
		Scan(ctx, &current)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("%s: checking current state: %w", op, err)
	}
	if current.Status == idempotentStatus &&
		(idempotentStatus != model.StorageCleanupCopyStatusDeleteScheduled ||
			(current.DeleteTxHash != nil && *current.DeleteTxHash == idempotentTxHash)) {
		return nil
	}
	return fmt.Errorf("%s: %w", op, ErrConflict)
}
