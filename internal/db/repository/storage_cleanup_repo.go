package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagereplacement"
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

// BeginFailedCopyRetry resets the failed ledger row in the same transaction
// that checkpoints the new external request.
func (r *BunStorageCleanupRepo) BeginFailedCopyRetry(ctx context.Context, id int64, oldHash string) error {
	now := time.Now()
	query := r.db.NewUpdate().
		Model((*model.StorageCleanupCopy)(nil)).
		Set("status = ?", model.StorageCleanupCopyStatusPending).
		Set("delete_tx_hash = NULL").
		Set("scheduled_at = NULL").
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Where("status = ?", model.StorageCleanupCopyStatusFailed)
	if oldHash == "" {
		query = query.Where("delete_tx_hash IS NULL")
	} else {
		query = query.Where("delete_tx_hash = ?", oldHash)
	}
	result, err := query.Exec(ctx)
	if err != nil {
		return fmt.Errorf("beginning failed storage cleanup retry: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return fmt.Errorf("beginning failed storage cleanup retry: %w", ErrConflict)
	}
	return nil
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
		  AND active_content.id <> cleanup_copy.content_id
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

// FinalizeContent deletes the current-state rows of content whose remote
// cleanup finished: its cache record, its copies, and the content row. Commit,
// pull, replacement, cleanup, and deletion ledgers keep their rows and name the
// content by value. It returns ErrContentCleanupNotReady while anything could
// still need those rows, and must run in the caller's transaction.
func (r *BunStorageCleanupRepo) FinalizeContent(ctx context.Context, contentID, generation, taskID int64) error {
	if contentID < 1 || generation < 1 || taskID < 1 {
		return ErrInvalidInput
	}
	contents, err := lockStorageContentsByID(ctx, r.db, []int64{contentID})
	if err != nil {
		return fmt.Errorf("finalizing storage cleanup: %w", err)
	}
	content := contents[contentID]
	if content.CleanupGeneration != generation || content.CleanupTaskID == nil || *content.CleanupTaskID != taskID {
		return fmt.Errorf("finalizing storage cleanup: %w", ErrConflict)
	}
	ready, err := contentCleanupReady(ctx, r.db, contentID)
	if err != nil {
		return err
	}
	if !ready {
		return ErrContentCleanupNotReady
	}
	now := time.Now()
	for _, column := range []string{"created_by_content_id", "last_used_content_id"} {
		if _, err := r.db.NewUpdate().
			Model((*model.StorageDataSet)(nil)).
			Set(column+" = NULL").
			Set("updated_at = ?", now).
			Where(column+" = ?", contentID).
			Exec(ctx); err != nil {
			return fmt.Errorf("finalizing storage cleanup: clearing data set %s: %w", column, err)
		}
	}
	for _, rows := range []struct {
		model  any
		column string
	}{
		{(*model.ObjectCache)(nil), "content_id"},
		{(*model.StorageCopy)(nil), "content_id"},
		{(*model.StorageContent)(nil), "id"},
	} {
		if _, err := r.db.NewDelete().Model(rows.model).Where(rows.column+" = ?", contentID).Exec(ctx); err != nil {
			return fmt.Errorf("finalizing storage cleanup: %w", err)
		}
	}
	return nil
}

// contentCleanupReady reports whether nothing can still need a cleaned-up
// content's rows: no live version names it, its bytes are out of the cache with
// no cache task running, no commit attempt or copy task is still open, and no
// replacement item that blocks retirement names it.
func contentCleanupReady(ctx context.Context, db bun.IDB, contentID int64) (bool, error) {
	unreferenced, err := contentIsUnreferenced(ctx, db, contentID)
	if err != nil || !unreferenced {
		return false, err
	}
	for _, check := range []struct {
		what  string
		query *bun.SelectQuery
	}{
		{"cache residency", db.NewSelect().Model((*model.ObjectCache)(nil)).
			Where("content_id = ?", contentID).
			Where(`in_cache = ? OR EXISTS (
				SELECT 1 FROM tasks AS cache_task
				WHERE cache_task.id = object_cache.cache_active_task_id
				  AND cache_task.status IN (?, ?)
			)`, true, model.TaskStatusPending, model.TaskStatusRunning)},
		{"open commit attempts", db.NewSelect().Model((*storagecommit.Attempt)(nil)).
			Where("content_id = ? AND resolved_at IS NULL", contentID)},
		{"copy tasks", db.NewSelect().Model((*model.StorageCopy)(nil)).
			Where("content_id = ?", contentID).
			Where(`EXISTS (
				SELECT 1 FROM tasks AS copy_task
				WHERE copy_task.id = storage_copy.active_task_id
				  AND copy_task.status IN (?, ?)
			)`, model.TaskStatusPending, model.TaskStatusRunning)},
		// Pending and attention items are the ones ItemStatus.Blocking counts.
		{"replacement items", db.NewSelect().Model((*storagereplacement.Item)(nil)).
			Where("content_id = ?", contentID).
			Where("status IN (?)", bun.List([]storagereplacement.ItemStatus{
				storagereplacement.ItemStatusPending, storagereplacement.ItemStatusAttention,
			}))},
	} {
		count, err := check.query.Count(ctx)
		if err != nil {
			return false, fmt.Errorf("checking %s before finalizing storage cleanup: %w", check.what, err)
		}
		if count > 0 {
			return false, nil
		}
	}
	return true, nil
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
