package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type BunStorageUploadRepo struct {
	db bun.IDB
}

var _ StorageUploadRepository = (*BunStorageUploadRepo)(nil)

func (r *BunStorageUploadRepo) StartObjectUploadAttempt(ctx context.Context, input StartObjectUploadAttemptInput) (*model.StorageUpload, error) {
	upload := &model.StorageUpload{
		BucketID:        input.BucketID,
		SourceVersionID: input.SourceVersionID,
		ContentSize:     input.ContentSize,
		Checksum:        input.Checksum,
		Status:          model.StorageUploadStatusRunning,
	}
	if input.SourceTaskID != 0 {
		upload.SourceTaskID = &input.SourceTaskID
	}
	if _, err := r.db.NewInsert().Model(upload).Exec(ctx); err != nil {
		if isUniqueViolation(err) && input.SourceVersionID != "" {
			existing, selectErr := r.findActiveUploadBySourceVersion(ctx, input.SourceVersionID)
			if selectErr != nil {
				return nil, selectErr
			}
			if existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("starting storage upload attempt: %w", err)
	}
	return upload, nil
}

func (r *BunStorageUploadRepo) GetByID(ctx context.Context, uploadID int64) (*model.StorageUpload, error) {
	upload := new(model.StorageUpload)
	err := r.db.NewSelect().
		Model(upload).
		Where("id = ?", uploadID).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage upload: %w", err)
	}
	return upload, nil
}

func (r *BunStorageUploadRepo) GetByIDs(ctx context.Context, uploadIDs []int64) (map[int64]model.StorageUpload, error) {
	uploadsByID := make(map[int64]model.StorageUpload, len(uploadIDs))
	if len(uploadIDs) == 0 {
		return uploadsByID, nil
	}
	var uploads []model.StorageUpload
	if err := r.db.NewSelect().
		Model(&uploads).
		Where("id IN (?)", bun.List(uploadIDs)).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("selecting storage uploads by id: %w", err)
	}
	for _, upload := range uploads {
		uploadsByID[upload.ID] = upload
	}
	return uploadsByID, nil
}

func (r *BunStorageUploadRepo) ListCopies(ctx context.Context, uploadID int64) ([]model.StorageUploadCopy, error) {
	var copies []model.StorageUploadCopy
	query := `SELECT storage_copy.*, storage_data_set.data_set_id AS data_set_id
		FROM storage_upload_copies AS storage_copy
		LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		WHERE storage_copy.upload_id = ?
		ORDER BY storage_copy.copy_index ASC`
	if err := r.db.NewRaw(query, uploadID).Scan(ctx, &copies); err != nil {
		return nil, fmt.Errorf("listing storage upload copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageUploadRepo) ListReadablePrimaryCopy(ctx context.Context, uploadID int64) ([]ReadableStorageCopy, error) {
	return r.listReadableCopies(ctx, uploadID, true)
}

func (r *BunStorageUploadRepo) ListReadableCopies(ctx context.Context, uploadID int64) ([]ReadableStorageCopy, error) {
	return r.listReadableCopies(ctx, uploadID, false)
}

func (r *BunStorageUploadRepo) listReadableCopies(ctx context.Context, uploadID int64, primaryOnly bool) ([]ReadableStorageCopy, error) {
	var copies []ReadableStorageCopy
	query := `SELECT
			storage_copy.upload_id,
			storage_upload.piece_cid,
			storage_copy.copy_index,
			storage_copy.provider_id,
			storage_data_set.data_set_id,
			storage_copy.piece_id,
			storage_copy.role,
			storage_copy.retrieval_url
		FROM storage_upload_copies AS storage_copy
		JOIN storage_uploads AS storage_upload ON storage_upload.id = storage_copy.upload_id
		JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		WHERE storage_copy.upload_id = ?
		  AND storage_upload.piece_cid IS NOT NULL AND storage_upload.piece_cid <> ''
		  AND storage_copy.status = ?
		  AND storage_copy.storage_data_set_id IS NOT NULL
		  AND storage_copy.provider_id IS NOT NULL AND storage_copy.provider_id <> ''
		  AND storage_data_set.data_set_id IS NOT NULL AND storage_data_set.data_set_id <> ''
		  AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> ''
		  AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> ''`
	args := []interface{}{uploadID, model.StorageUploadCopyStatusCommitted}
	if primaryOnly {
		query += " AND storage_copy.copy_index = 0 AND storage_copy.role = 'primary'"
	} else {
		query += " AND storage_upload.status = ?"
		args = append(args, model.StorageUploadStatusAllCopiesCommitted)
	}
	query += " ORDER BY CASE WHEN storage_copy.role = 'primary' THEN 0 ELSE 1 END, storage_copy.copy_index ASC"
	if err := r.db.NewRaw(query, args...).Scan(ctx, &copies); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("listing readable storage copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageUploadRepo) ListDataSetBindings(ctx context.Context, bucketID int64) ([]model.StorageDataSet, error) {
	var bindings []model.StorageDataSet
	if err := r.db.NewSelect().
		Model(&bindings).
		Where("bucket_id = ?", bucketID).
		OrderExpr("copy_index ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing storage data set bindings: %w", err)
	}
	return bindings, nil
}

func (r *BunStorageUploadRepo) GetDataSetBindingByCopyIndex(ctx context.Context, bucketID int64, copyIndex int) (*model.StorageDataSet, error) {
	binding := new(model.StorageDataSet)
	err := r.db.NewSelect().
		Model(binding).
		Where("bucket_id = ? AND copy_index = ?", bucketID, copyIndex).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage data set binding: %w", err)
	}
	return binding, nil
}

func (r *BunStorageUploadRepo) EnsureDataSetBinding(ctx context.Context, input EnsureDataSetBindingInput) (*model.StorageDataSet, error) {
	var binding *model.StorageDataSet
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		got, err := ensureDataSetBinding(ctx, db, input)
		if err != nil {
			return err
		}
		binding = got
		return nil
	})
	return binding, err
}

func (r *BunStorageUploadRepo) MarkDataSetCreating(ctx context.Context, input MarkDataSetCreatingInput) error {
	now := time.Now()
	_, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusCreating).
		Set("create_transaction_id = ?", nullableString(input.TransactionID)).
		Set("create_status_url = ?", nullableString(input.StatusURL)).
		Set("client_data_set_id = ?", input.ClientDataSetID).
		Set("last_used_upload_id = ?", nullableInt64(input.UploadID)).
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", input.ID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage data set creating: %w", err)
	}
	return nil
}

func (r *BunStorageUploadRepo) MarkDataSetReady(ctx context.Context, input MarkDataSetReadyInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		return markDataSetReady(ctx, db, input.ID, input.UploadID, input.DataSetID, input.ClientDataSetID)
	})
}

func (r *BunStorageUploadRepo) MarkDataSetFailed(ctx context.Context, id int64, lastError string) error {
	_, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusFailed).
		Set("last_error = ?", lastError).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage data set failed: %w", err)
	}
	return nil
}

func (r *BunStorageUploadRepo) CreateUploadCopiesForBindings(ctx context.Context, uploadID int64, copies []UploadCopyBindingInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		for _, input := range copies {
			if input.ProviderID.IsZero() {
				return fmt.Errorf("providerID is required: %w", ErrInvalidInput)
			}
			providerID := input.ProviderID
			copyRow := &model.StorageUploadCopy{
				UploadID:         uploadID,
				CopyIndex:        input.CopyIndex,
				ProviderID:       &providerID,
				Role:             input.Role,
				Status:           model.StorageUploadCopyStatusPending,
				StorageDataSetID: &input.StorageDataSetID,
			}
			if _, err := db.NewInsert().
				Model(copyRow).
				On("CONFLICT (upload_id, copy_index) DO NOTHING").
				Exec(ctx); err != nil {
				return fmt.Errorf("creating storage upload copy row: %w", err)
			}
		}
		_, err := db.NewUpdate().
			Model((*model.StorageUpload)(nil)).
			Set("requested_copies = ?", len(copies)).
			Set("updated_at = ?", time.Now()).
			Where("id = ?", uploadID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("updating storage upload requested copies: %w", err)
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) GetUploadCopy(ctx context.Context, uploadID int64, copyIndex int) (*model.StorageUploadCopy, error) {
	copyRow := new(model.StorageUploadCopy)
	err := r.db.NewSelect().
		Model(copyRow).
		Where("upload_id = ? AND copy_index = ?", uploadID, copyIndex).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage upload copy: %w", err)
	}
	return copyRow, nil
}

func (r *BunStorageUploadRepo) MarkUploadCopyPieceReady(ctx context.Context, input MarkUploadCopyPieceReadyInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("status = ?", model.StorageUploadCopyStatusPieceReady).
			Set("piece_id = COALESCE(?, piece_id)", input.PieceID).
			Set("retrieval_url = COALESCE(?, retrieval_url)", nullableString(input.RetrievalURL)).
			Set("last_error = NULL").
			Set("updated_at = ?", now).
			Where("upload_id = ? AND copy_index = ?", input.UploadID, input.CopyIndex).
			Where("status <> ?", model.StorageUploadCopyStatusCommitted).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload copy piece ready: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return nil
		}
		if input.CopyIndex == 0 {
			if err := updateUploadPrimaryStored(ctx, db, input.UploadID, input.PieceCID, now); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) MarkUploadCopyCommitting(ctx context.Context, input MarkUploadCopyCommittingInput) error {
	_, err := r.db.NewUpdate().
		Model((*model.StorageUploadCopy)(nil)).
		Set("status = CASE WHEN ? IS NOT NULL THEN ? ELSE status END", nullableString(input.CommitTransactionID), model.StorageUploadCopyStatusCommitting).
		Set("commit_extra_data_hex = COALESCE(?, commit_extra_data_hex)", nullableString(input.CommitExtraDataHex)).
		Set("commit_transaction_id = COALESCE(?, commit_transaction_id)", nullableString(input.CommitTransactionID)).
		Set("last_error = NULL").
		Set("updated_at = ?", time.Now()).
		Where("upload_id = ? AND copy_index = ?", input.UploadID, input.CopyIndex).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage upload copy committing: %w", err)
	}
	return nil
}

func (r *BunStorageUploadRepo) MarkUploadCopyCommitted(ctx context.Context, input MarkUploadCopyCommittedInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		_, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("status = ?", model.StorageUploadCopyStatusCommitted).
			Set("piece_id = COALESCE(?, piece_id)", input.PieceID).
			Set("retrieval_url = COALESCE(?, retrieval_url)", nullableString(input.RetrievalURL)).
			Set("commit_extra_data_hex = COALESCE(?, commit_extra_data_hex)", nullableString(input.CommitExtraDataHex)).
			Set("commit_transaction_id = COALESCE(?, commit_transaction_id)", nullableString(input.CommitTransactionID)).
			Set("last_error = NULL").
			Set("updated_at = ?", now).
			Where("upload_id = ? AND copy_index = ?", input.UploadID, input.CopyIndex).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload copy committed: %w", err)
		}
		if input.CopyIndex == 0 {
			if err := updateUploadPrimaryCommitted(ctx, db, input.UploadID, input.PieceCID, now); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) MarkUploadCopyFailed(ctx context.Context, uploadID int64, copyIndex int, lastError string) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("status = ?", model.StorageUploadCopyStatusFailed).
			Set("last_error = ?", lastError).
			Set("updated_at = ?", now).
			Where("upload_id = ? AND copy_index = ?", uploadID, copyIndex).
			Where("status <> ?", model.StorageUploadCopyStatusCommitted).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload copy failed: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return nil
		}
		if copyIndex == 0 {
			_, err = db.NewUpdate().
				Model((*model.StorageUpload)(nil)).
				Set("status = ?", model.StorageUploadStatusFailed).
				Set("error_message = ?", lastError).
				Set("updated_at = ?", now).
				Where("id = ?", uploadID).
				Where("status IN (?)", bun.List(activeUploadStatuses())).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("marking storage upload failed: %w", err)
			}
		} else {
			_, err = db.NewUpdate().
				Model((*model.StorageUpload)(nil)).
				Set("status = ?", model.StorageUploadStatusPartial).
				Set("error_message = ?", lastError).
				Set("updated_at = ?", now).
				Where("id = ?", uploadID).
				Where("status IN (?)", bun.List([]model.StorageUploadStatus{
					model.StorageUploadStatusPrimaryCommitted,
					model.StorageUploadStatusPartial,
				})).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("marking storage upload partial: %w", err)
			}
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) BindPrimaryCommittedUploadForContent(ctx context.Context, input BindPrimaryCommittedUploadInput) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		upload := new(model.StorageUpload)
		if err := db.NewSelect().Model(upload).Where("id = ?", input.UploadID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for primary bind: %w", err)
		}
		if err := requireCommittedPrimaryCopy(ctx, db, input.UploadID); err != nil {
			return err
		}
		now := time.Now()
		if err := updateUploadPrimaryCommitted(ctx, db, input.UploadID, derefString(upload.PieceCID), now); err != nil {
			return err
		}
		// Bind the source committing version and matching waiting followers.
		// Followers with their own active upload/task are left untouched.
		query := `UPDATE object_versions
			SET storage_upload_id = ?, state = ?, failed_at_state = NULL, last_error = NULL, updated_at = ?
			WHERE bucket_id = ? AND size = ? AND checksum = ?
			  AND (
				(version_id = ? AND state IN (?, ?))
				OR (
					state = ?
					AND NOT EXISTS (
						SELECT 1 FROM storage_uploads AS active_upload
						WHERE active_upload.source_version_id = object_versions.version_id
						  AND active_upload.id <> ?
						  AND active_upload.status IN ('running', 'stored_on_primary', 'primary_committed', 'partial')
					)
					AND NOT EXISTS (
						SELECT 1 FROM tasks AS active_task
						WHERE active_task.ref_type = 'object'
						  AND active_task.ref_version_id = object_versions.version_id
						  AND active_task.type = ?
						  AND active_task.status IN (?, ?)
					)
				)
			  )
			RETURNING object_id, version_id`
		err := db.NewRaw(query,
			input.UploadID, model.ObjectStateReplicating, now,
			input.BucketID, input.ContentSize, input.Checksum,
			upload.SourceVersionID, model.ObjectStateCommitting, model.ObjectStateFailed,
			model.ObjectStateUploading,
			input.UploadID,
			model.TaskTypeUpload, model.TaskStatusPending, model.TaskStatusRunning,
		).Scan(ctx, &refs)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("binding primary committed upload for content: %w", err)
		}
		return nil
	})
	return refs, err
}

func (r *BunStorageUploadRepo) BindPrimaryCommittedUploadForVersion(ctx context.Context, input BindPrimaryCommittedUploadForVersionInput) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		upload := new(model.StorageUpload)
		if err := db.NewSelect().Model(upload).Where("id = ?", input.UploadID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for version primary bind: %w", err)
		}
		if err := requireCommittedPrimaryCopy(ctx, db, input.UploadID); err != nil {
			return err
		}
		now := time.Now()
		if err := updateUploadPrimaryCommitted(ctx, db, input.UploadID, derefString(upload.PieceCID), now); err != nil {
			return err
		}
		query := `UPDATE object_versions
			SET storage_upload_id = ?, state = ?, failed_at_state = NULL, last_error = NULL, updated_at = ?
			WHERE version_id = ? AND bucket_id = ? AND size = ? AND checksum = ? AND state = ?
			  AND NOT EXISTS (
				SELECT 1 FROM storage_uploads AS active_upload
				WHERE active_upload.source_version_id = object_versions.version_id
				  AND active_upload.id <> ?
				  AND active_upload.status IN ('running', 'stored_on_primary', 'primary_committed', 'partial')
			  )
			RETURNING object_id, version_id`
		err := db.NewRaw(query,
			input.UploadID, model.ObjectStateReplicating, now,
			input.VersionID, input.BucketID, input.ContentSize, input.Checksum, model.ObjectStateUploading,
			input.UploadID,
		).Scan(ctx, &refs)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("binding primary committed upload for version: %w", err)
		}
		if len(refs) > 0 {
			if err := completeUploadTasksForVersion(ctx, db, input.VersionID, now); err != nil {
				return err
			}
		}
		return nil
	})
	return refs, err
}

func (r *BunStorageUploadRepo) FinalizeUploadIfAllCopiesCommitted(ctx context.Context, input FinalizeUploadInput) (bool, []ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	finalized := false
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		total, committed, err := countUploadCopies(ctx, db, input.UploadID)
		if err != nil {
			return err
		}
		if total == 0 || committed < total {
			return nil
		}
		now := time.Now()
		_, err = db.NewUpdate().
			Model((*model.StorageUpload)(nil)).
			Set("status = ?", model.StorageUploadStatusAllCopiesCommitted).
			Set("accepted_at = COALESCE(accepted_at, ?)", now).
			Set("accept_error = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", input.UploadID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload all copies committed: %w", err)
		}
		err = db.NewRaw(`UPDATE object_versions
			SET state = ?, updated_at = ?
			WHERE storage_upload_id = ? AND state = ?
			RETURNING object_id, version_id`,
			model.ObjectStateStored, now, input.UploadID, model.ObjectStateReplicating,
		).Scan(ctx, &refs)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("finalizing object versions for upload: %w", err)
		}
		finalized = true
		return nil
	})
	return finalized, refs, err
}

func (r *BunStorageUploadRepo) FindAcceptableUploadAttempt(ctx context.Context, taskID int64, versionID string) (*model.StorageUpload, error) {
	if taskID == 0 && versionID == "" {
		return nil, nil
	}
	upload := new(model.StorageUpload)
	q := r.db.NewSelect().
		Model(upload).
		Where("status = ?", model.StorageUploadStatusAllCopiesCommitted).
		Where("accepted_at IS NULL").
		OrderExpr("updated_at DESC").
		OrderExpr("id DESC").
		Limit(1)
	if taskID != 0 {
		q = q.Where("source_task_id = ?", taskID)
	}
	if versionID != "" {
		q = q.Where("source_version_id = ?", versionID)
	}
	err := q.Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("finding acceptable storage upload: %w", err)
	}
	return upload, nil
}

func (r *BunStorageUploadRepo) FindActiveUploadBySourceVersion(ctx context.Context, versionID string) (*model.StorageUpload, error) {
	return r.findActiveUploadBySourceVersion(ctx, versionID)
}

func (r *BunStorageUploadRepo) FindLatestUploadBySourceVersion(ctx context.Context, versionID string) (*model.StorageUpload, error) {
	upload := new(model.StorageUpload)
	err := r.db.NewSelect().
		Model(upload).
		Where("source_version_id = ?", versionID).
		OrderExpr("id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting latest storage upload: %w", err)
	}
	return upload, nil
}

func (r *BunStorageUploadRepo) FindLatestUploadsBySourceVersions(ctx context.Context, versionIDs []string) (map[string]model.StorageUpload, error) {
	uploadsByVersionID := make(map[string]model.StorageUpload, len(versionIDs))
	if len(versionIDs) == 0 {
		return uploadsByVersionID, nil
	}
	var uploads []model.StorageUpload
	err := r.db.NewRaw(`SELECT storage_upload.*
		FROM storage_uploads AS storage_upload
		JOIN (
			SELECT source_version_id, MAX(id) AS id
			FROM storage_uploads
			WHERE source_version_id IN (?)
			GROUP BY source_version_id
		) AS latest_upload ON latest_upload.id = storage_upload.id`,
		bun.List(versionIDs),
	).Scan(ctx, &uploads)
	if err != nil {
		return nil, fmt.Errorf("selecting latest storage uploads by source version: %w", err)
	}
	for _, upload := range uploads {
		uploadsByVersionID[upload.SourceVersionID] = upload
	}
	return uploadsByVersionID, nil
}

func (r *BunStorageUploadRepo) SetAcceptError(ctx context.Context, uploadID int64, message string) error {
	_, err := r.db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("accept_error = ?", message).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", uploadID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("setting storage upload accept error: %w", err)
	}
	return nil
}

func (r *BunStorageUploadRepo) RecordUploadResult(ctx context.Context, input RecordUploadResultInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		upload := new(model.StorageUpload)
		if err := db.NewSelect().Model(upload).Where("id = ?", input.UploadID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for result: %w", err)
		}
		staged, err := isStagedUploadResultPath(ctx, db, upload)
		if err != nil {
			return err
		}
		if staged {
			return fmt.Errorf("recording legacy upload result for staged upload %d: %w", input.UploadID, ErrAlreadyExists)
		}
		if _, err := db.NewDelete().Model((*model.StorageUploadCopy)(nil)).Where("upload_id = ?", input.UploadID).Exec(ctx); err != nil {
			return fmt.Errorf("clearing storage upload copies: %w", err)
		}
		if _, err := db.NewDelete().Model((*model.StorageUploadFailure)(nil)).Where("upload_id = ?", input.UploadID).Exec(ctx); err != nil {
			return fmt.Errorf("clearing storage upload failures: %w", err)
		}
		conflicted, err := hasStorageDataSetConflict(ctx, db, upload, input.Copies)
		if err != nil {
			return err
		}
		usableCopies := 0
		for i, copyInput := range input.Copies {
			var storageDataSetID *int64
			status := model.StorageUploadCopyStatusPending
			if !conflicted && bindableCopy(copyInput) {
				binding, err := ensureDataSetBinding(ctx, db, EnsureDataSetBindingInput{
					BucketID:          upload.BucketID,
					ProviderID:        *copyInput.ProviderID,
					CopyIndex:         i,
					CreatedByUploadID: upload.ID,
				})
				if err != nil {
					return err
				}
				if err := markDataSetReady(ctx, db, binding.ID, upload.ID, *copyInput.DataSetID, nil); err != nil {
					if errors.Is(err, ErrAlreadyExists) {
						conflicted = true
						break
					}
					return err
				}
				storageDataSetID = &binding.ID
				if copyInput.PieceID != nil {
					status = model.StorageUploadCopyStatusCommitted
				}
				if usableCopy(copyInput) {
					usableCopies++
				}
			}
			copyRow := &model.StorageUploadCopy{
				UploadID:         input.UploadID,
				CopyIndex:        i,
				ProviderID:       copyInput.ProviderID,
				PieceID:          copyInput.PieceID,
				Role:             copyInput.Role,
				RetrievalURL:     copyInput.RetrievalURL,
				Status:           status,
				StorageDataSetID: storageDataSetID,
			}
			if _, err := db.NewInsert().Model(copyRow).Exec(ctx); err != nil {
				return fmt.Errorf("inserting storage upload copy: %w", err)
			}
		}
		if conflicted {
			usableCopies = 0
			if _, err := db.NewDelete().Model((*model.StorageUploadCopy)(nil)).Where("upload_id = ?", input.UploadID).Exec(ctx); err != nil {
				return fmt.Errorf("clearing conflicted storage upload copies: %w", err)
			}
			if _, err := db.NewDelete().Model((*model.StorageDataSet)(nil)).Where("created_by_upload_id = ?", input.UploadID).Exec(ctx); err != nil {
				return fmt.Errorf("clearing conflicted storage data sets: %w", err)
			}
			for i, copyInput := range input.Copies {
				copyRow := &model.StorageUploadCopy{
					UploadID:     input.UploadID,
					CopyIndex:    i,
					ProviderID:   copyInput.ProviderID,
					PieceID:      copyInput.PieceID,
					Role:         copyInput.Role,
					RetrievalURL: copyInput.RetrievalURL,
					Status:       model.StorageUploadCopyStatusPending,
				}
				if _, err := db.NewInsert().Model(copyRow).Exec(ctx); err != nil {
					return fmt.Errorf("inserting rejected storage upload copy: %w", err)
				}
			}
		}
		for i, failureInput := range input.Failures {
			failure := &model.StorageUploadFailure{
				UploadID:     input.UploadID,
				AttemptIndex: i,
				ProviderID:   failureInput.ProviderID,
				Role:         failureInput.Role,
				Stage:        failureInput.Stage,
				ErrorMessage: failureInput.ErrorMessage,
				Explicit:     failureInput.Explicit,
			}
			if _, err := db.NewInsert().Model(failure).Exec(ctx); err != nil {
				return fmt.Errorf("inserting storage upload failure: %w", err)
			}
		}
		status := model.StorageUploadStatusFailed
		var acceptError *string
		switch {
		case conflicted:
			status = model.StorageUploadStatusRejected
			msg := "provider data set belongs to another bucket"
			acceptError = &msg
		case input.Complete && input.PieceCID != nil && *input.PieceCID != "" && usableCopies > 0:
			status = model.StorageUploadStatusAllCopiesCommitted
		case input.Complete:
			status = model.StorageUploadStatusRejected
			msg := "upload completed without usable identifiers"
			acceptError = &msg
		case len(input.Copies) > 0:
			status = model.StorageUploadStatusPartial
		}
		raw := json.RawMessage(nil)
		if len(input.RawResultJSON) > 0 {
			raw = append(json.RawMessage(nil), input.RawResultJSON...)
		}
		_, err = db.NewUpdate().
			Model((*model.StorageUpload)(nil)).
			Set("status = ?", status).
			Set("piece_cid = ?", input.PieceCID).
			Set("requested_copies = ?", input.RequestedCopies).
			Set("raw_result_json = ?", raw).
			Set("error_message = ?", input.ErrorMessage).
			Set("accept_error = ?", acceptError).
			Set("updated_at = ?", time.Now()).
			Where("id = ?", input.UploadID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("updating storage upload result: %w", err)
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) AcceptCompleteUploadForContent(ctx context.Context, input AcceptCompleteUploadInput) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		upload := new(model.StorageUpload)
		if err := db.NewSelect().Model(upload).Where("id = ?", input.UploadID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for accept: %w", err)
		}
		if upload.Status != model.StorageUploadStatusAllCopiesCommitted {
			return fmt.Errorf("storage upload %d is %s: %w", input.UploadID, upload.Status, ErrNotFound)
		}
		if upload.PieceCID == nil || *upload.PieceCID == "" {
			return fmt.Errorf("storage upload %d has no piece cid: %w", input.UploadID, ErrNotFound)
		}
		usableCount, err := countReadableCommittedCopies(ctx, db, input.UploadID)
		if err != nil {
			return err
		}
		if usableCount == 0 {
			return fmt.Errorf("storage upload %d has no usable copies: %w", input.UploadID, ErrNotFound)
		}
		now := time.Now()
		err = db.NewRaw(`UPDATE object_versions
			SET storage_upload_id = ?, state = ?, updated_at = ?
			WHERE bucket_id = ? AND size = ? AND checksum = ? AND state IN (?, ?, ?)
			RETURNING object_id, version_id`,
			input.UploadID, model.ObjectStateStored, now,
			input.BucketID, input.ContentSize, input.Checksum,
			model.ObjectStateUploading, model.ObjectStateCommitting, model.ObjectStateReplicating,
		).Scan(ctx, &refs)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("accepting storage upload for content: %w", err)
		}
		if len(refs) == 0 {
			if err := handleNoAcceptedVersions(ctx, db, upload, input, now); err != nil {
				return err
			}
			return nil
		}
		if input.AutoEvict {
			for _, ref := range refs {
				task := &model.Task{
					Type:           model.TaskTypeEvictCache,
					RefType:        "object",
					RefID:          ref.ObjectID,
					RefVersionID:   ref.VersionID,
					IdempotencyKey: "evict_cache:" + ref.VersionID,
					Status:         model.TaskStatusPending,
					MaxRetries:     input.EvictMaxRetries,
					ScheduledAt:    now,
				}
				if _, err := db.NewInsert().Model(task).Exec(ctx); err != nil && !isUniqueViolation(err) {
					return fmt.Errorf("creating evict task: %w", err)
				}
			}
		}
		if err := completeTaskIfRunning(ctx, db, input.TaskID, now); err != nil {
			return err
		}
		_, err = db.NewUpdate().
			Model((*model.StorageUpload)(nil)).
			Set("accepted_at = ?", now).
			Set("accept_error = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", input.UploadID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload accepted: %w", err)
		}
		return nil
	})
	return refs, err
}

func (r *BunStorageUploadRepo) runMaybeTx(ctx context.Context, fn func(bun.IDB) error) error {
	if db, ok := r.db.(*bun.DB); ok {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(tx)
		})
	}
	return fn(r.db)
}

func (r *BunStorageUploadRepo) findActiveUploadBySourceVersion(ctx context.Context, versionID string) (*model.StorageUpload, error) {
	upload := new(model.StorageUpload)
	err := r.db.NewSelect().
		Model(upload).
		Where("source_version_id = ?", versionID).
		Where("status IN (?)", bun.List(activeUploadStatuses())).
		OrderExpr("id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting active storage upload: %w", err)
	}
	return upload, nil
}

func ensureDataSetBinding(ctx context.Context, db bun.IDB, input EnsureDataSetBindingInput) (*model.StorageDataSet, error) {
	if input.BucketID == 0 || input.ProviderID.IsZero() || input.CopyIndex < 0 {
		return nil, fmt.Errorf("invalid storage data set binding input: %w", ErrInvalidInput)
	}
	existingByProvider := new(model.StorageDataSet)
	err := db.NewSelect().
		Model(existingByProvider).
		Where("bucket_id = ? AND provider_id = ?", input.BucketID, input.ProviderID).
		Scan(ctx)
	if err == nil {
		if existingByProvider.CopyIndex != input.CopyIndex {
			return nil, fmt.Errorf("provider %s already bound to copy_index %d: %w", input.ProviderID, existingByProvider.CopyIndex, ErrAlreadyExists)
		}
		return existingByProvider, nil
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("selecting storage data set by provider: %w", err)
	}
	existingByIndex := new(model.StorageDataSet)
	err = db.NewSelect().
		Model(existingByIndex).
		Where("bucket_id = ? AND copy_index = ?", input.BucketID, input.CopyIndex).
		Scan(ctx)
	if err == nil {
		return nil, fmt.Errorf("copy_index %d already bound to provider %s: %w", input.CopyIndex, existingByIndex.ProviderID, ErrAlreadyExists)
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("selecting storage data set by copy index: %w", err)
	}
	now := time.Now()
	binding := &model.StorageDataSet{
		BucketID:          input.BucketID,
		ProviderID:        input.ProviderID,
		CopyIndex:         input.CopyIndex,
		Status:            model.StorageDataSetStatusPending,
		CreatedByUploadID: nullableInt64(input.CreatedByUploadID),
		LastUsedUploadID:  nullableInt64(input.CreatedByUploadID),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	res, err := db.NewInsert().
		Model(binding).
		On("CONFLICT DO NOTHING").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("inserting storage data set binding: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		existing := new(model.StorageDataSet)
		selectErr := db.NewSelect().
			Model(existing).
			Where("bucket_id = ? AND provider_id = ?", input.BucketID, input.ProviderID).
			Scan(ctx)
		if selectErr == nil && existing.CopyIndex == input.CopyIndex {
			return existing, nil
		}
		if selectErr != nil && selectErr != sql.ErrNoRows {
			return nil, fmt.Errorf("selecting storage data set after conflict: %w", selectErr)
		}
		return nil, fmt.Errorf("storage data set binding already exists: %w", ErrAlreadyExists)
	}
	return binding, nil
}

func markDataSetReady(ctx context.Context, db bun.IDB, id int64, uploadID int64, dataSetID types.OnChainID, clientDataSetID *types.OnChainID) error {
	if dataSetID.IsZero() {
		return fmt.Errorf("dataSetID is required: %w", ErrInvalidInput)
	}
	res, err := db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusReady).
		Set("data_set_id = ?", dataSetID).
		Set("client_data_set_id = COALESCE(?, client_data_set_id)", clientDataSetID).
		Set("last_used_upload_id = ?", nullableInt64(uploadID)).
		Set("last_error = NULL").
		Set("updated_at = ?", time.Now()).
		Where("id = ?", id).
		Where(`NOT EXISTS (
			SELECT 1 FROM storage_data_sets AS other
			WHERE other.id <> ?
			  AND other.provider_id = (SELECT provider_id FROM storage_data_sets WHERE id = ?)
			  AND other.data_set_id = ?
		)`, id, id, dataSetID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage data set ready: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows > 0 {
		return nil
	}
	count, err := db.NewSelect().
		Model((*model.StorageDataSet)(nil)).
		Where("id = ?", id).
		Count(ctx)
	if err != nil {
		return fmt.Errorf("checking storage data set ready result: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("storage data set %d not found: %w", id, ErrNotFound)
	}
	return fmt.Errorf("provider data set already bound to another bucket: %w", ErrAlreadyExists)
}

func isStagedUploadResultPath(ctx context.Context, db bun.IDB, upload *model.StorageUpload) (bool, error) {
	count, err := db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		Where("upload_id = ?", upload.ID).
		Where("storage_data_set_id IS NOT NULL").
		Where("status IN (?)", bun.List([]model.StorageUploadCopyStatus{
			model.StorageUploadCopyStatusPending,
			model.StorageUploadCopyStatusPieceReady,
			model.StorageUploadCopyStatusCommitting,
		})).
		Count(ctx)
	if err != nil {
		return false, fmt.Errorf("checking staged upload copies: %w", err)
	}
	if count == 0 {
		return false, nil
	}
	if upload.SourceTaskID != nil {
		task := new(model.Task)
		err := db.NewSelect().Model(task).Where("id = ?", *upload.SourceTaskID).Scan(ctx)
		if err == nil {
			if task.Stage != nil && *task.Stage != "" {
				return *task.Stage != uploadStageLegacyName, nil
			}
			if stage, _ := task.Payload["stage"].(string); stage == uploadStageLegacyName {
				return false, nil
			} else if stage != "" {
				return true, nil
			}
		} else if err != sql.ErrNoRows {
			return false, fmt.Errorf("checking upload source task: %w", err)
		}
	}
	return true, nil
}

const uploadStageLegacyName = "legacy_upload"

func hasStorageDataSetConflict(ctx context.Context, db bun.IDB, upload *model.StorageUpload, copies []StorageUploadCopyInput) (bool, error) {
	for _, copyInput := range copies {
		if !presentRequiredOnChainID(copyInput.ProviderID) || !presentRequiredOnChainID(copyInput.DataSetID) {
			continue
		}
		existing := new(model.StorageDataSet)
		err := db.NewSelect().
			Model(existing).
			Where("provider_id = ? AND data_set_id = ?", *copyInput.ProviderID, *copyInput.DataSetID).
			Scan(ctx)
		if err == nil {
			if existing.BucketID != upload.BucketID {
				return true, nil
			}
			continue
		}
		if err != sql.ErrNoRows {
			return false, fmt.Errorf("checking storage data set conflict: %w", err)
		}
	}
	return false, nil
}

func usableCopy(copyInput StorageUploadCopyInput) bool {
	return bindableCopy(copyInput) &&
		copyInput.PieceID != nil &&
		copyInput.RetrievalURL != nil && *copyInput.RetrievalURL != ""
}

func bindableCopy(copyInput StorageUploadCopyInput) bool {
	return presentRequiredOnChainID(copyInput.ProviderID) &&
		presentRequiredOnChainID(copyInput.DataSetID)
}

func presentRequiredOnChainID(id *types.OnChainID) bool {
	return id != nil && !id.IsZero()
}

func countReadableCommittedCopies(ctx context.Context, db bun.IDB, uploadID int64) (int, error) {
	count, err := db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		Where("upload_id = ?", uploadID).
		Where("status = ?", model.StorageUploadCopyStatusCommitted).
		Where("storage_data_set_id IS NOT NULL").
		Where("provider_id IS NOT NULL AND provider_id <> ''").
		Where("piece_id IS NOT NULL AND piece_id <> ''").
		Where("retrieval_url IS NOT NULL AND retrieval_url <> ''").
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting readable storage upload copies: %w", err)
	}
	return count, nil
}

func requireCommittedPrimaryCopy(ctx context.Context, db bun.IDB, uploadID int64) error {
	count, err := db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		Where("upload_id = ? AND copy_index = ? AND role = ?", uploadID, 0, "primary").
		Where("status = ?", model.StorageUploadCopyStatusCommitted).
		Where("piece_id IS NOT NULL AND piece_id <> ''").
		Where("retrieval_url IS NOT NULL AND retrieval_url <> ''").
		Count(ctx)
	if err != nil {
		return fmt.Errorf("checking committed primary copy: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("storage upload %d has no committed primary copy: %w", uploadID, ErrNotFound)
	}
	return nil
}

func countUploadCopies(ctx context.Context, db bun.IDB, uploadID int64) (int, int, error) {
	var rows []struct {
		Status model.StorageUploadCopyStatus `bun:"status"`
		Count  int                           `bun:"count"`
	}
	if err := db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		ColumnExpr("status, COUNT(*) AS count").
		Where("upload_id = ?", uploadID).
		GroupExpr("status").
		Scan(ctx, &rows); err != nil {
		return 0, 0, fmt.Errorf("counting storage upload copies: %w", err)
	}
	total := 0
	committed := 0
	for _, row := range rows {
		total += row.Count
		if row.Status == model.StorageUploadCopyStatusCommitted {
			committed += row.Count
		}
	}
	return total, committed, nil
}

func updateUploadPrimaryStored(ctx context.Context, db bun.IDB, uploadID int64, pieceCID string, now time.Time) error {
	_, err := db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("status = ?", model.StorageUploadStatusStoredOnPrimary).
		Set("piece_cid = COALESCE(?, piece_cid)", nullableString(pieceCID)).
		Set("updated_at = ?", now).
		Where("id = ?", uploadID).
		Where("status IN (?)", bun.List([]model.StorageUploadStatus{
			model.StorageUploadStatusRunning,
			model.StorageUploadStatusStoredOnPrimary,
			model.StorageUploadStatusFailed,
		})).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage upload stored on primary: %w", err)
	}
	return nil
}

func updateUploadPrimaryCommitted(ctx context.Context, db bun.IDB, uploadID int64, pieceCID string, now time.Time) error {
	_, err := db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("status = ?", model.StorageUploadStatusPrimaryCommitted).
		Set("piece_cid = COALESCE(?, piece_cid)", nullableString(pieceCID)).
		Set("updated_at = ?", now).
		Where("id = ?", uploadID).
		Where("status IN (?)", bun.List([]model.StorageUploadStatus{
			model.StorageUploadStatusRunning,
			model.StorageUploadStatusStoredOnPrimary,
			model.StorageUploadStatusPrimaryCommitted,
			model.StorageUploadStatusFailed,
		})).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage upload primary committed: %w", err)
	}
	return nil
}

func handleNoAcceptedVersions(ctx context.Context, db bun.IDB, upload *model.StorageUpload, input AcceptCompleteUploadInput, now time.Time) error {
	if upload.SourceVersionID == "" {
		return fmt.Errorf("no object versions matched content: %w", ErrNotFound)
	}
	version := new(model.ObjectVersion)
	err := db.NewSelect().
		Model(version).
		Where("version_id = ?", upload.SourceVersionID).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("source object version not found: %w", ErrNotFound)
		}
		return fmt.Errorf("selecting source object version after accept miss: %w", err)
	}
	if version.StorageUploadID != nil && *version.StorageUploadID == upload.ID &&
		(version.State == model.ObjectStateStored || version.State == model.ObjectStateCacheEvicted) {
		if err := completeTaskIfRunning(ctx, db, input.TaskID, now); err != nil {
			return err
		}
		_, err := db.NewUpdate().
			Model((*model.StorageUpload)(nil)).
			Set("accepted_at = ?", now).
			Set("accept_error = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", input.UploadID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload accepted: %w", err)
		}
		return nil
	}
	if version.StorageUploadID != nil && (version.State == model.ObjectStateStored || version.State == model.ObjectStateCacheEvicted) {
		if err := completeTaskIfRunning(ctx, db, input.TaskID, now); err != nil {
			return err
		}
		_, err := db.NewUpdate().
			Model((*model.StorageUpload)(nil)).
			Set("status = ?", model.StorageUploadStatusSuperseded).
			Set("updated_at = ?", now).
			Set("accept_error = ?", "source version already stored by another upload").
			Where("id = ?", input.UploadID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload superseded: %w", err)
		}
		return nil
	}
	return fmt.Errorf("no object versions matched content: %w", ErrNotFound)
}

func completeTaskIfRunning(ctx context.Context, db bun.IDB, taskID int64, now time.Time) error {
	if taskID == 0 {
		return nil
	}
	res, err := db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusCompleted).
		Set("completed_at = ?", now).
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing upload task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows > 0 {
		return nil
	}
	count, err := db.NewSelect().
		Model((*model.Task)(nil)).
		Where("id = ? AND status = ?", taskID, model.TaskStatusCompleted).
		Count(ctx)
	if err != nil {
		return fmt.Errorf("checking completed upload task: %w", err)
	}
	if count == 1 {
		return nil
	}
	return fmt.Errorf("completing upload task %d: not in running state", taskID)
}

func completeUploadTasksForVersion(ctx context.Context, db bun.IDB, versionID string, now time.Time) error {
	if versionID == "" {
		return nil
	}
	_, err := db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusCompleted).
		Set("completed_at = ?", now).
		Where("ref_type = ? AND ref_version_id = ? AND type = ?", "object", versionID, model.TaskTypeUpload).
		Where("status IN (?, ?)", model.TaskStatusPending, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing upload tasks for bound version: %w", err)
	}
	return nil
}

func activeUploadStatuses() []model.StorageUploadStatus {
	return []model.StorageUploadStatus{
		model.StorageUploadStatusRunning,
		model.StorageUploadStatusStoredOnPrimary,
		model.StorageUploadStatusPrimaryCommitted,
		model.StorageUploadStatusPartial,
	}
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func nullableInt64(value int64) *int64 {
	if value == 0 {
		return nil
	}
	return &value
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
