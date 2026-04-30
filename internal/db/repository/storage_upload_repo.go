package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
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

func (r *BunStorageUploadRepo) ListCopies(ctx context.Context, uploadID int64) ([]model.StorageUploadCopy, error) {
	var copies []model.StorageUploadCopy
	if err := r.db.NewSelect().
		Model(&copies).
		Where("upload_id = ?", uploadID).
		OrderExpr("copy_index ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing storage upload copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageUploadRepo) ListReadableCopies(ctx context.Context, uploadID int64) ([]ReadableStorageCopy, error) {
	var copies []ReadableStorageCopy
	query := `SELECT
			storage_copy.upload_id,
			storage_upload.piece_cid,
			storage_copy.copy_index,
			storage_copy.provider_id,
			storage_copy.data_set_id,
			storage_copy.piece_id,
			storage_copy.role,
			storage_copy.retrieval_url
		FROM storage_upload_copies AS storage_copy
		JOIN storage_uploads AS storage_upload ON storage_upload.id = storage_copy.upload_id
		WHERE storage_copy.upload_id = ?
		  AND storage_upload.status = ?
		  AND storage_upload.piece_cid IS NOT NULL AND storage_upload.piece_cid <> ''
		  AND storage_copy.storage_data_set_id IS NOT NULL
		  AND storage_copy.provider_id IS NOT NULL AND storage_copy.provider_id <> ''
		  AND storage_copy.data_set_id IS NOT NULL AND storage_copy.data_set_id <> ''
		  AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> ''
		  AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> ''
		ORDER BY CASE WHEN storage_copy.role = 'primary' THEN 0 ELSE 1 END, storage_copy.copy_index ASC`
	if err := r.db.NewRaw(query, uploadID, model.StorageUploadStatusComplete).Scan(ctx, &copies); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("listing readable storage copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageUploadRepo) FindAcceptableUploadAttempt(ctx context.Context, taskID int64, versionID string) (*model.StorageUpload, error) {
	if taskID == 0 && versionID == "" {
		return nil, nil
	}
	upload := new(model.StorageUpload)
	q := r.db.NewSelect().
		Model(upload).
		Where("status = ?", model.StorageUploadStatusComplete).
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
		if err := db.NewSelect().
			Model(upload).
			Where("id = ?", input.UploadID).
			Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for result: %w", err)
		}

		if _, err := db.NewDelete().
			Model((*model.StorageUploadCopy)(nil)).
			Where("upload_id = ?", input.UploadID).
			Exec(ctx); err != nil {
			return fmt.Errorf("clearing storage upload copies: %w", err)
		}
		if _, err := db.NewDelete().
			Model((*model.StorageUploadFailure)(nil)).
			Where("upload_id = ?", input.UploadID).
			Exec(ctx); err != nil {
			return fmt.Errorf("clearing storage upload failures: %w", err)
		}

		conflicted, err := hasStorageDataSetConflict(ctx, db, upload, input.Copies)
		if err != nil {
			return err
		}
		usableCopies := 0
		copyRows := make([]*model.StorageUploadCopy, 0, len(input.Copies))
		for i, copyInput := range input.Copies {
			var storageDataSetID *int64
			if !conflicted {
				var conflict bool
				storageDataSetID, conflict, err = reserveStorageDataSetForCopy(ctx, db, upload, copyInput)
				if err != nil {
					return err
				}
				if conflict {
					conflicted = true
					break
				}
			}
			copyRows = append(copyRows, &model.StorageUploadCopy{
				UploadID:         input.UploadID,
				CopyIndex:        i,
				ProviderID:       copyInput.ProviderID,
				DataSetID:        copyInput.DataSetID,
				PieceID:          copyInput.PieceID,
				Role:             copyInput.Role,
				RetrievalURL:     copyInput.RetrievalURL,
				IsNewDataSet:     copyInput.IsNewDataSet,
				StorageDataSetID: storageDataSetID,
			})
		}
		if conflicted {
			if err := deleteStorageDataSetsCreatedByUpload(ctx, db, input.UploadID); err != nil {
				return err
			}
			copyRows = copyRows[:0]
			for i, copyInput := range input.Copies {
				copyRows = append(copyRows, &model.StorageUploadCopy{
					UploadID:     input.UploadID,
					CopyIndex:    i,
					ProviderID:   copyInput.ProviderID,
					DataSetID:    copyInput.DataSetID,
					PieceID:      copyInput.PieceID,
					Role:         copyInput.Role,
					RetrievalURL: copyInput.RetrievalURL,
					IsNewDataSet: copyInput.IsNewDataSet,
				})
			}
		} else {
			for _, copyRow := range copyRows {
				if copyRow.StorageDataSetID == nil {
					continue
				}
				if err := updateStorageDataSetLastSeen(ctx, db, *copyRow.StorageDataSetID, upload.ID); err != nil {
					return err
				}
				if usableCopy(input.Copies[copyRow.CopyIndex]) {
					usableCopies++
				}
			}
		}
		for _, copyRow := range copyRows {
			if _, err := db.NewInsert().Model(copyRow).Exec(ctx); err != nil {
				return fmt.Errorf("inserting storage upload copy: %w", err)
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
			status = model.StorageUploadStatusComplete
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
		if err := db.NewSelect().
			Model(upload).
			Where("id = ?", input.UploadID).
			Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for accept: %w", err)
		}
		if upload.Status != model.StorageUploadStatusComplete {
			return fmt.Errorf("storage upload %d is %s: %w", input.UploadID, upload.Status, ErrNotFound)
		}
		if upload.PieceCID == nil || *upload.PieceCID == "" {
			return fmt.Errorf("storage upload %d has no piece cid: %w", input.UploadID, ErrNotFound)
		}
		usableCount, err := countUsableCopies(ctx, db, input.UploadID)
		if err != nil {
			return err
		}
		if usableCount == 0 {
			return fmt.Errorf("storage upload %d has no usable copies: %w", input.UploadID, ErrNotFound)
		}

		now := time.Now()
		query := `UPDATE object_versions
			SET storage_upload_id = ?, state = ?, updated_at = ?
			WHERE bucket_id = ? AND size = ? AND checksum = ? AND state = ?
			RETURNING object_id, version_id`
		if err := db.NewRaw(query,
			input.UploadID, model.ObjectStateStored, now,
			input.BucketID, input.ContentSize, input.Checksum, model.ObjectStateUploading,
		).Scan(ctx, &refs); err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("accepting storage upload for content: %w", err)
		}

		if len(refs) == 0 {
			return handleNoAcceptedVersions(ctx, db, upload, input, now)
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

func hasStorageDataSetConflict(ctx context.Context, db bun.IDB, upload *model.StorageUpload, copies []StorageUploadCopyInput) (bool, error) {
	for _, copyInput := range copies {
		if copyInput.ProviderID == nil || *copyInput.ProviderID == "" || copyInput.DataSetID == nil || *copyInput.DataSetID == "" {
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

func reserveStorageDataSetForCopy(ctx context.Context, db bun.IDB, upload *model.StorageUpload, copyInput StorageUploadCopyInput) (*int64, bool, error) {
	if copyInput.ProviderID == nil || *copyInput.ProviderID == "" || copyInput.DataSetID == nil || *copyInput.DataSetID == "" {
		return nil, false, nil
	}
	existing := new(model.StorageDataSet)
	err := db.NewSelect().
		Model(existing).
		Where("provider_id = ? AND data_set_id = ?", *copyInput.ProviderID, *copyInput.DataSetID).
		Scan(ctx)
	if err == nil {
		if existing.BucketID != upload.BucketID {
			return nil, true, nil
		}
		return &existing.ID, false, nil
	}
	if err != sql.ErrNoRows {
		return nil, false, fmt.Errorf("selecting storage data set: %w", err)
	}
	insertedID, err := insertStorageDataSetIfAbsent(ctx, db, upload, copyInput)
	if err != nil {
		return nil, false, fmt.Errorf("inserting storage data set: %w", err)
	}
	if insertedID != nil {
		return insertedID, false, nil
	}
	return resolveStorageDataSetConflict(ctx, db, upload, copyInput)
}

func insertStorageDataSetIfAbsent(ctx context.Context, db bun.IDB, upload *model.StorageUpload, copyInput StorageUploadCopyInput) (*int64, error) {
	var inserted struct {
		ID int64 `bun:"id"`
	}
	now := time.Now()
	err := db.NewRaw(`INSERT INTO storage_data_sets
		(bucket_id, provider_id, data_set_id, first_seen_upload_id, last_seen_upload_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (provider_id, data_set_id) DO NOTHING
		RETURNING id`,
		upload.BucketID,
		*copyInput.ProviderID,
		*copyInput.DataSetID,
		upload.ID,
		upload.ID,
		now,
		now,
	).Scan(ctx, &inserted)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &inserted.ID, nil
}

func resolveStorageDataSetConflict(ctx context.Context, db bun.IDB, upload *model.StorageUpload, copyInput StorageUploadCopyInput) (*int64, bool, error) {
	existing := new(model.StorageDataSet)
	err := db.NewSelect().
		Model(existing).
		Where("provider_id = ? AND data_set_id = ?", *copyInput.ProviderID, *copyInput.DataSetID).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false, fmt.Errorf("storage data set conflict row not found: %w", ErrNotFound)
		}
		return nil, false, fmt.Errorf("selecting storage data set after conflict: %w", err)
	}
	if existing.BucketID != upload.BucketID {
		return nil, true, nil
	}
	return &existing.ID, false, nil
}

func deleteStorageDataSetsCreatedByUpload(ctx context.Context, db bun.IDB, uploadID int64) error {
	_, err := db.NewDelete().
		Model((*model.StorageDataSet)(nil)).
		Where("first_seen_upload_id = ?", uploadID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deleting storage data sets created by rejected upload: %w", err)
	}
	return nil
}

func updateStorageDataSetLastSeen(ctx context.Context, db bun.IDB, storageDataSetID int64, uploadID int64) error {
	_, err := db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("last_seen_upload_id = ?", uploadID).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", storageDataSetID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating storage data set last seen: %w", err)
	}
	return nil
}

func usableCopy(copyInput StorageUploadCopyInput) bool {
	return copyInput.ProviderID != nil && *copyInput.ProviderID != "" &&
		copyInput.DataSetID != nil && *copyInput.DataSetID != "" &&
		copyInput.PieceID != nil && *copyInput.PieceID != "" &&
		copyInput.RetrievalURL != nil && *copyInput.RetrievalURL != ""
}

func countUsableCopies(ctx context.Context, db bun.IDB, uploadID int64) (int, error) {
	count, err := db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		Where("upload_id = ?", uploadID).
		Where("storage_data_set_id IS NOT NULL").
		Where("provider_id IS NOT NULL AND provider_id <> ''").
		Where("data_set_id IS NOT NULL AND data_set_id <> ''").
		Where("piece_id IS NOT NULL AND piece_id <> ''").
		Where("retrieval_url IS NOT NULL AND retrieval_url <> ''").
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting usable storage upload copies: %w", err)
	}
	return count, nil
}

func handleNoAcceptedVersions(ctx context.Context, db bun.IDB, upload *model.StorageUpload, input AcceptCompleteUploadInput, now time.Time) error {
	if upload.SourceVersionID == "" {
		return fmt.Errorf("no uploading object versions matched content: %w", ErrNotFound)
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
			return fmt.Errorf("marking idempotent storage upload accepted: %w", err)
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
	return fmt.Errorf("no uploading object versions matched content: %w", ErrNotFound)
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
