package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/storagecleanup"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepull"
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
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		task, err := taskForBinding(ctx, db, taskID, "storage_content", strconv.FormatInt(contentID, 10), model.TaskTypeStorageCleanup)
		if err != nil {
			return err
		}
		var input storagecleanup.Input
		if json.Unmarshal(task.Input, &input) != nil || input.ContentID != contentID || input.Generation != generation {
			return ErrConflict
		}

		result, err := db.NewUpdate().
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
	})
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
		OrderExpr("copy_index ASC, id ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing storage cleanup copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageCleanupRepo) MarkCopyUnsupported(ctx context.Context, id int64, message string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.StorageCleanupCopy)(nil)).
		Set("status = ?", model.StorageCleanupCopyStatusUnsupported).
		Set("last_error = ?", errtext.Redact(message)).
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Exec(ctx)
	return storageCleanupCopyUpdateResult(res, err, "marking storage cleanup copy unsupported")
}

func (r *BunStorageCleanupRepo) UploadHasObjectReferences(ctx context.Context, contentID int64) (bool, error) {
	referenced, err := uploadReferencedContentIDs(ctx, r.db, []int64{contentID})
	return len(referenced) > 0, err
}

func uploadReferencedContentIDs(ctx context.Context, db bun.IDB, contentIDs []int64) ([]int64, error) {
	if len(contentIDs) == 0 {
		return nil, nil
	}
	var referenced []int64
	err := db.NewRaw(`SELECT DISTINCT storage_content.id
		FROM storage_contents AS storage_content
		JOIN object_versions AS object_version
		  ON `+objectVersionReferencesStorageContentSQL("object_version", "storage_content")+`
		WHERE storage_content.id IN (?)
		  AND object_version.is_delete_marker = ?`, bun.List(contentIDs), false).
		Scan(ctx, &referenced)
	if err != nil {
		return nil, fmt.Errorf("checking storage cleanup object references: %w", err)
	}
	return referenced, nil
}

func (r *BunStorageCleanupRepo) CleanupHasObjectReferences(ctx context.Context, contentID int64) (bool, error) {
	referenced, err := cleanupReferencedContentIDs(ctx, r.db, []int64{contentID})
	return len(referenced) > 0, err
}

func unreferencedContentIDs(contentIDs, referenced []int64) []int64 {
	known := make(map[int64]bool, len(referenced))
	for _, id := range referenced {
		known[id] = true
	}
	remaining := make([]int64, 0, len(contentIDs))
	for _, id := range contentIDs {
		if !known[id] {
			remaining = append(remaining, id)
		}
	}
	return remaining
}

func cleanupReferencedContentIDs(ctx context.Context, db bun.IDB, contentIDs []int64) ([]int64, error) {
	if len(contentIDs) == 0 {
		return nil, nil
	}
	var referenced []int64
	err := db.NewRaw(`SELECT DISTINCT cleanup_copy.content_id
		FROM storage_cleanup_copies AS cleanup_copy
		JOIN storage_copies AS storage_copy
		  ON storage_copy.status = ?
		 AND (
			storage_copy.content_id = cleanup_copy.content_id
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
		WHERE cleanup_copy.content_id IN (?)
		  AND object_version.is_delete_marker = FALSE
		  AND (
			storage_copy.content_id = cleanup_copy.content_id
			OR storage_data_set.data_set_id = cleanup_copy.data_set_id
		  )`,
		model.StorageCopyStatusCommitted, bun.List(contentIDs),
	).Scan(ctx, &referenced)
	if err != nil {
		return nil, fmt.Errorf("checking storage cleanup task references: %w", err)
	}
	remaining := unreferencedContentIDs(contentIDs, referenced)
	if len(remaining) == 0 {
		return referenced, nil
	}
	var active []int64
	err = db.NewRaw(`SELECT DISTINCT cleanup_copy.content_id
		FROM storage_cleanup_copies AS cleanup_copy
		JOIN storage_copies AS storage_copy
		  ON storage_copy.status = ?
		 AND (
			storage_copy.content_id = cleanup_copy.content_id
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
		WHERE cleanup_copy.content_id IN (?)
		  AND active_content.id <> cleanup_copy.content_id
		  AND active_content.accepted_at IS NULL
		  AND (
			storage_copy.content_id = cleanup_copy.content_id
			OR storage_data_set.data_set_id = cleanup_copy.data_set_id
		  )`,
		model.StorageCopyStatusCommitted, bun.List(remaining),
	).Scan(ctx, &active)
	if err != nil {
		return nil, fmt.Errorf("checking storage cleanup active upload references: %w", err)
	}
	return append(referenced, active...), nil
}

// FinalizeContent deletes the current-state rows of content whose remote
// cleanup finished: its cache record, its copies, and the content row. Commit,
// pull, replacement, cleanup, and deletion ledgers keep their rows and name the
// content by value. Unresolved pulls or other unfinished work return
// ErrContentCleanupNotReady. It must run in the caller's transaction.
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
	if _, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("created_by_content_id = NULL").
		Set("updated_at = ?", time.Now()).
		Where("created_by_content_id = ?", contentID).
		Exec(ctx); err != nil {
		return fmt.Errorf("finalizing storage cleanup: clearing data set created_by_content_id: %w", err)
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
// no cache task running, and no commit attempt or copy task is still open.
// Replacement items do not hold it back; the coordinator cancels an item once
// its content is gone.
func contentCleanupReady(ctx context.Context, db bun.IDB, contentID int64) (bool, error) {
	var ready bool
	err := db.NewRaw(`SELECT NOT EXISTS (?) AND NOT EXISTS (?) AND NOT EXISTS (?) AND NOT EXISTS (?) AND NOT EXISTS (?)`,
		db.NewSelect().Model((*model.ObjectVersion)(nil)).ColumnExpr("1").
			Where("content_id = ?", contentID),
		db.NewSelect().Model((*storagepull.Attempt)(nil)).ColumnExpr("1").
			Where("content_id = ? AND resolved_at IS NULL", contentID),
		db.NewSelect().Model((*model.ObjectCache)(nil)).ColumnExpr("1").
			Where("content_id = ?", contentID).
			Where(`in_cache = ? OR EXISTS (
				SELECT 1 FROM tasks AS cache_task
				WHERE cache_task.id = object_cache.cache_active_task_id
				  AND cache_task.status IN (?, ?)
			)`, true, model.TaskStatusPending, model.TaskStatusRunning),
		db.NewSelect().Model((*model.StorageCopy)(nil)).ColumnExpr("1").
			Join("JOIN storage_commit_requests AS commit_request ON commit_request.request_id = storage_copy.commit_request_id").
			Where("storage_copy.content_id = ?", contentID).
			Where("commit_request.status NOT IN (?)", bun.List([]storagecommit.RequestStatus{
				storagecommit.RequestStatusConfirmed, storagecommit.RequestStatusAbandoned,
			})),
		db.NewSelect().Model((*model.StorageCopy)(nil)).ColumnExpr("1").
			Where("content_id = ?", contentID).
			Where(`EXISTS (
				SELECT 1 FROM tasks AS copy_task
				WHERE copy_task.id = storage_copy.active_task_id
				  AND copy_task.status IN (?, ?)
			)`, model.TaskStatusPending, model.TaskStatusRunning),
	).Scan(ctx, &ready)
	if err != nil {
		return false, fmt.Errorf("checking storage cleanup readiness: %w", err)
	}
	return ready, nil
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
