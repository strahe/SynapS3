package repository

import (
	"context"
	"database/sql"
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
	requestedCopies := input.RequestedCopies
	if !model.ValidStorageCopies(requestedCopies) {
		return nil, fmt.Errorf("requested copies must be between %d and %d, got %d", model.StorageCopiesMin, model.StorageCopiesMax, requestedCopies)
	}
	upload := &model.StorageUpload{
		BucketID:        input.BucketID,
		SourceVersionID: input.SourceVersionID,
		ContentSize:     input.ContentSize,
		Checksum:        input.Checksum,
		Status:          model.StorageUploadStatusRunning,
		RequestedCopies: requestedCopies,
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

func (r *BunStorageUploadRepo) BeginIngressStoreProgress(ctx context.Context, uploadID int64) (*model.StorageUpload, error) {
	if uploadID == 0 {
		return nil, fmt.Errorf("uploadID is required: %w", ErrInvalidInput)
	}
	now := time.Now()
	_, err := r.db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("ingress_store_attempt = ingress_store_attempt + 1").
		Set("ingress_bytes_transferred = 0").
		Set("progress_updated_at = ?", now).
		Where("id = ?", uploadID).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning ingress store progress: %w", err)
	}
	upload, err := r.GetByID(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if upload == nil {
		return nil, fmt.Errorf("beginning ingress store progress: %w", ErrNotFound)
	}
	return upload, nil
}

func (r *BunStorageUploadRepo) RecordIngressStoreProgress(ctx context.Context, input RecordIngressStoreProgressInput) (*model.StorageUpload, error) {
	if input.UploadID == 0 {
		return nil, fmt.Errorf("uploadID is required: %w", ErrInvalidInput)
	}
	if input.Attempt <= 0 {
		return nil, fmt.Errorf("attempt is required: %w", ErrInvalidInput)
	}
	now := time.Now()
	_, err := r.db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("progress_updated_at = CASE WHEN ingress_bytes_transferred < content_size AND ? > ingress_bytes_transferred THEN ? ELSE progress_updated_at END", input.BytesUploaded, now).
		Set("ingress_bytes_transferred = CASE WHEN ? > content_size THEN content_size WHEN ? > ingress_bytes_transferred THEN ? ELSE ingress_bytes_transferred END", input.BytesUploaded, input.BytesUploaded, input.BytesUploaded).
		Where("id = ?", input.UploadID).
		Where("ingress_store_attempt = ?", input.Attempt).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("recording ingress store progress: %w", err)
	}
	upload, err := r.GetByID(ctx, input.UploadID)
	if err != nil {
		return nil, err
	}
	if upload == nil {
		return nil, fmt.Errorf("recording ingress store progress: %w", ErrNotFound)
	}
	return upload, nil
}

func (r *BunStorageUploadRepo) GetUploadProvenance(ctx context.Context, uploadID int64) (*StorageUploadProvenance, error) {
	upload, err := r.GetByID(ctx, uploadID)
	if err != nil || upload == nil {
		return nil, err
	}
	copies, err := r.ListCopies(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	failures, err := r.listFailures(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	return &StorageUploadProvenance{
		Upload:   *upload,
		Copies:   copies,
		Failures: failures,
	}, nil
}

func (r *BunStorageUploadRepo) AppendUploadFailure(ctx context.Context, input AppendUploadFailureInput) error {
	if input.UploadID == 0 {
		return fmt.Errorf("uploadID is required: %w", ErrInvalidInput)
	}
	for {
		err := r.appendUploadFailureOnce(ctx, input)
		if err == nil {
			return nil
		}
		if !shouldRetryUploadFailureAppend(err) {
			return err
		}
		if err := waitUploadFailureAppendRetry(ctx); err != nil {
			return fmt.Errorf("retrying storage upload failure append: %w", err)
		}
	}
}

func (r *BunStorageUploadRepo) appendUploadFailureOnce(ctx context.Context, input AppendUploadFailureInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		providerID := input.ProviderID
		transferMethod := input.TransferMethod
		if providerID == nil || transferMethod == "" {
			copyRow := new(model.StorageUploadCopy)
			err := db.NewSelect().
				Model(copyRow).
				Where("upload_id = ? AND copy_index = ?", input.UploadID, input.CopyIndex).
				Scan(ctx)
			if err != nil && err != sql.ErrNoRows {
				return fmt.Errorf("loading upload copy for failure: %w", err)
			}
			if err == nil {
				if providerID == nil {
					providerID = copyRow.ProviderID
				}
				if transferMethod == "" {
					transferMethod = string(copyRow.TransferMethod)
				}
			}
		}
		var next struct {
			AttemptIndex int `bun:"attempt_index"`
		}
		if err := db.NewRaw(`SELECT COALESCE(MAX(attempt_index), -1) + 1 AS attempt_index FROM storage_upload_failures WHERE upload_id = ?`, input.UploadID).Scan(ctx, &next); err != nil {
			return fmt.Errorf("selecting next upload failure index: %w", err)
		}
		failure := &model.StorageUploadFailure{
			UploadID:       input.UploadID,
			AttemptIndex:   next.AttemptIndex,
			ProviderID:     providerID,
			TransferMethod: transferMethod,
			Stage:          nullableString(input.Stage),
			ErrorMessage:   nullableString(input.ErrorMessage),
			Explicit:       input.Explicit,
		}
		if _, err := db.NewInsert().Model(failure).Exec(ctx); err != nil {
			return fmt.Errorf("appending storage upload failure: %w", err)
		}
		return nil
	})
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

func shouldRetryUploadFailureAppend(err error) bool {
	return isUniqueViolation(err) || isSQLiteBusy(err)
}

func waitUploadFailureAppendRetry(ctx context.Context) error {
	const retryDelay = 5 * time.Millisecond
	timer := time.NewTimer(retryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *BunStorageUploadRepo) listFailures(ctx context.Context, uploadID int64) ([]model.StorageUploadFailure, error) {
	var failures []model.StorageUploadFailure
	if err := r.db.NewSelect().
		Model(&failures).
		Where("upload_id = ?", uploadID).
		OrderExpr("attempt_index ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing storage upload failures: %w", err)
	}
	return failures, nil
}

func (r *BunStorageUploadRepo) ListReadableCommittedCopies(ctx context.Context, uploadID int64) ([]ReadableStorageCopy, error) {
	var copies []ReadableStorageCopy
	query := `SELECT
			storage_copy.upload_id,
			storage_upload.piece_cid,
			storage_copy.copy_index,
			storage_copy.provider_id,
			storage_data_set.data_set_id,
			storage_copy.piece_id,
			storage_copy.transfer_method,
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
		  AND storage_data_set.status IN ('ready', 'draining')
		  AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> ''
		  AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> ''`
	args := []interface{}{uploadID, model.StorageUploadCopyStatusCommitted}
	query += " ORDER BY storage_copy.copy_index ASC"
	if err := r.db.NewRaw(query, args...).Scan(ctx, &copies); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("listing readable storage copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageUploadRepo) ListCurrentObjectCopyHealthSummaries(ctx context.Context, bucketID int64, staleBefore time.Time) ([]CurrentObjectCopyHealthSummary, error) {
	var summaries []CurrentObjectCopyHealthSummary
	currentVersionBucketFilter := ""
	args := make([]interface{}, 0, 5)
	if bucketID > 0 {
		currentVersionBucketFilter = `
			  AND bucket_id = ?`
		args = append(args, bucketID)
	}
	query := fmt.Sprintf(`WITH current_versions AS (
			SELECT bucket_id, version_id, storage_upload_id
			FROM object_versions
			WHERE is_current = TRUE
			  AND is_delete_marker = FALSE
%s
		),
		latest_upload_refs AS (
			SELECT source_upload.source_version_id, MAX(source_upload.id) AS latest_upload_id
			FROM storage_uploads AS source_upload
			JOIN current_versions AS current_version
			  ON current_version.version_id = source_upload.source_version_id
			 AND current_version.storage_upload_id IS NULL
			WHERE source_upload.source_version_id <> ''
			GROUP BY source_upload.source_version_id
		),
		object_uploads AS (
			SELECT
				current_version.bucket_id,
				current_version.version_id,
				storage_upload.id AS upload_id,
				storage_upload.status AS upload_status,
				COALESCE(storage_upload.requested_copies, 0) AS requested_copies
			FROM current_versions AS current_version
			LEFT JOIN latest_upload_refs AS latest_upload_ref ON latest_upload_ref.source_version_id = current_version.version_id
			LEFT JOIN storage_uploads AS storage_upload ON storage_upload.id = COALESCE(current_version.storage_upload_id, latest_upload_ref.latest_upload_id)
		),
		copy_inputs AS (
			SELECT
				object_upload.bucket_id,
				object_upload.version_id,
				object_upload.upload_id,
				object_upload.upload_status,
				object_upload.requested_copies,
				storage_copy.copy_index,
				storage_copy.status AS copy_status,
				storage_copy.provider_id,
				storage_data_set.id AS local_data_set_id,
				storage_data_set.data_set_id AS chain_data_set_id,
				storage_copy.piece_id,
				storage_copy.retrieval_url,
				observation.status AS observation_status,
				observation.last_checked_at AS observation_last_checked_at
			FROM object_uploads AS object_upload
			LEFT JOIN storage_upload_copies AS storage_copy ON storage_copy.upload_id = object_upload.upload_id
			LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
			LEFT JOIN observability_data_set_states AS observation ON observation.local_data_set_id = storage_data_set.id
		),
		copy_evidence AS (
			SELECT
				copy_inputs.*,
				CASE
					WHEN copy_status = 'committed'
					  AND provider_id IS NOT NULL AND provider_id <> ''
					  AND local_data_set_id IS NOT NULL AND local_data_set_id <> 0
					  AND chain_data_set_id IS NOT NULL AND chain_data_set_id <> ''
					  AND piece_id IS NOT NULL AND piece_id <> ''
					  AND retrieval_url IS NOT NULL AND TRIM(retrieval_url) <> ''
					THEN 1 ELSE 0
				END AS has_committed_evidence
			FROM copy_inputs
			WHERE copy_status IS NOT NULL
		),
		copy_candidates AS (
			SELECT
				copy_evidence.*,
				CASE
					WHEN copy_status = 'committed'
					  AND has_committed_evidence = 1
					  AND observation_status = 'available'
					  AND observation_last_checked_at IS NOT NULL
					  AND observation_last_checked_at >= ?
					THEN 1 ELSE 0
				END AS is_readable,
				CASE WHEN copy_status IN ('pending', 'piece_ready') THEN 1 ELSE 0 END AS is_pending,
				CASE WHEN copy_status = 'committing' THEN 1 ELSE 0 END AS is_committing,
				CASE WHEN copy_status = 'failed' THEN 1 ELSE 0 END AS is_failed,
				CASE
					WHEN copy_status = 'committed'
					  AND has_committed_evidence = 1
					  AND NOT (
						  observation_status = 'available'
						  AND observation_last_checked_at IS NOT NULL
						  AND observation_last_checked_at >= ?
					  )
					THEN 1 ELSE 0
				END AS is_unverified,
				CASE
					WHEN copy_status = 'committed' AND has_committed_evidence = 0 THEN 1
					WHEN copy_status NOT IN ('pending', 'piece_ready', 'committing', 'committed', 'failed') THEN 1
					ELSE 0
				END AS is_unknown,
				CASE WHEN copy_status = 'committed' AND (provider_id IS NULL OR provider_id = '') THEN 1 ELSE 0 END AS reason_missing_provider,
				CASE WHEN copy_status = 'committed' AND (local_data_set_id IS NULL OR local_data_set_id = 0 OR chain_data_set_id IS NULL OR chain_data_set_id = '') THEN 1 ELSE 0 END AS reason_missing_data_set,
				CASE WHEN copy_status = 'committed' AND (piece_id IS NULL OR piece_id = '') THEN 1 ELSE 0 END AS reason_missing_piece,
				CASE WHEN copy_status = 'committed' AND (retrieval_url IS NULL OR TRIM(retrieval_url) = '') THEN 1 ELSE 0 END AS reason_missing_retrieval_url,
				CASE
					WHEN copy_status = 'committed'
					  AND has_committed_evidence = 1
					  AND NOT (
						  observation_status = 'available'
						  AND observation_last_checked_at IS NOT NULL
						  AND observation_last_checked_at >= ?
					  )
					THEN 1 ELSE 0
				END AS reason_observation_missing,
				CASE
					WHEN copy_status = 'committed'
					  AND has_committed_evidence = 1
					  AND observation_status = 'available'
					  AND observation_last_checked_at IS NOT NULL
					  AND observation_last_checked_at >= ? THEN 0
					WHEN copy_status = 'committed' AND has_committed_evidence = 1 THEN 1
					WHEN copy_status IN ('pending', 'piece_ready', 'committing') THEN 2
					WHEN copy_status = 'failed' THEN 3
					ELSE 4
				END AS selection_rank
			FROM copy_evidence
		),
		ranked_candidates AS (
			SELECT
				copy_candidates.*,
				ROW_NUMBER() OVER (
					PARTITION BY bucket_id, version_id
					ORDER BY selection_rank ASC, copy_index ASC
				) AS candidate_rank
			FROM copy_candidates
		),
		selected_candidates AS (
			SELECT *
			FROM ranked_candidates
			WHERE requested_copies > 0
			  AND candidate_rank <= requested_copies
		),
		object_rollup AS (
			SELECT
				object_upload.bucket_id,
				object_upload.version_id,
				object_upload.upload_id,
				object_upload.upload_status,
				object_upload.requested_copies,
				COUNT(selected.copy_index) AS selected_copies,
				COALESCE(SUM(CASE WHEN selected.is_readable = 1 THEN 1 ELSE 0 END), 0) AS readable_copies,
				COALESCE(SUM(CASE WHEN selected.is_pending = 1 OR selected.is_committing = 1 THEN 1 ELSE 0 END), 0) AS pending_selected,
				COALESCE(SUM(CASE WHEN selected.is_failed = 1 THEN 1 ELSE 0 END), 0) AS failed_selected,
				COALESCE(SUM(CASE WHEN selected.is_unverified = 1 OR selected.is_unknown = 1 THEN 1 ELSE 0 END), 0) AS unknown_selected,
				COALESCE(MAX(CASE WHEN selected.is_pending = 1 OR selected.is_committing = 1 OR selected.is_failed = 1 OR selected.is_unknown = 1 THEN 1 ELSE 0 END), 0) AS selected_under_replicated,
				COALESCE(MAX(CASE WHEN selected.is_pending = 1 THEN 1 ELSE 0 END), 0) AS selected_pending,
				COALESCE(MAX(CASE WHEN selected.is_committing = 1 THEN 1 ELSE 0 END), 0) AS selected_committing,
				COALESCE(MAX(CASE WHEN selected.reason_missing_provider = 1 THEN 1 ELSE 0 END), 0) AS selected_missing_provider,
				COALESCE(MAX(CASE WHEN selected.reason_missing_data_set = 1 THEN 1 ELSE 0 END), 0) AS selected_missing_data_set,
				COALESCE(MAX(CASE WHEN selected.reason_missing_piece = 1 THEN 1 ELSE 0 END), 0) AS selected_missing_piece,
				COALESCE(MAX(CASE WHEN selected.reason_missing_retrieval_url = 1 THEN 1 ELSE 0 END), 0) AS selected_missing_retrieval_url,
				COALESCE(MAX(CASE WHEN selected.reason_observation_missing = 1 THEN 1 ELSE 0 END), 0) AS selected_observation_missing,
				MIN(selected.observation_last_checked_at) AS last_checked_at
			FROM object_uploads AS object_upload
			LEFT JOIN selected_candidates AS selected
			  ON selected.bucket_id = object_upload.bucket_id
			 AND selected.version_id = object_upload.version_id
			GROUP BY object_upload.bucket_id, object_upload.version_id, object_upload.upload_id, object_upload.upload_status, object_upload.requested_copies
		),
		object_counts AS (
			SELECT
				object_rollup.*,
				CASE
					WHEN requested_copies > selected_copies THEN requested_copies - selected_copies
					ELSE 0
				END AS gap_copies
			FROM object_rollup
		),
		object_final AS (
			SELECT
				object_counts.*,
				failed_selected + CASE WHEN upload_status IN ('failed', 'rejected') THEN gap_copies ELSE 0 END AS failed_copies,
				pending_selected + CASE WHEN upload_status IN ('running', 'ingress_ready', 'readable') THEN gap_copies ELSE 0 END AS pending_copies,
				unknown_selected + CASE
					WHEN upload_id IS NOT NULL AND upload_status NOT IN ('failed', 'rejected', 'running', 'ingress_ready', 'readable') THEN gap_copies
					ELSE 0
				END AS unknown_copies
			FROM object_counts
		),
		object_health AS (
			SELECT
				object_final.*,
				CASE
					WHEN failed_copies > 0 THEN 3
					WHEN upload_id IS NULL OR unknown_copies > 0 THEN 2
					WHEN pending_copies > 0 THEN 1
					ELSE 0
				END AS status_rank,
				CASE WHEN selected_under_replicated > 0 OR gap_copies > 0 THEN 1 ELSE 0 END AS copy_under_replicated,
				CASE WHEN pending_copies > 0 AND (selected_pending > 0 OR gap_copies > 0 AND upload_status IN ('running', 'ingress_ready', 'readable')) THEN 1 ELSE 0 END AS copy_pending,
				CASE WHEN pending_copies > 0 AND selected_committing > 0 THEN 1 ELSE 0 END AS copy_committing,
				CASE WHEN failed_copies > 0 THEN 1 ELSE 0 END AS copy_failed,
				CASE WHEN unknown_copies > 0 AND selected_missing_provider > 0 THEN 1 ELSE 0 END AS copy_missing_provider,
				CASE WHEN unknown_copies > 0 AND selected_missing_data_set > 0 THEN 1 ELSE 0 END AS copy_missing_data_set,
				CASE WHEN unknown_copies > 0 AND selected_missing_piece > 0 THEN 1 ELSE 0 END AS copy_missing_piece,
				CASE WHEN unknown_copies > 0 AND selected_missing_retrieval_url > 0 THEN 1 ELSE 0 END AS copy_missing_retrieval_url,
				CASE
					WHEN upload_id IS NULL THEN 1
					WHEN unknown_copies > 0 AND (selected_observation_missing > 0 OR gap_copies > 0 AND upload_status NOT IN ('failed', 'rejected', 'running', 'ingress_ready', 'readable')) THEN 1
					ELSE 0
				END AS copy_observation_missing
			FROM object_final
		)
		SELECT
			bucket_id,
			CASE MAX(status_rank)
				WHEN 3 THEN 'unavailable'
				WHEN 2 THEN 'unknown'
				WHEN 1 THEN 'degraded'
				ELSE 'available'
			END AS status,
			COUNT(*) AS total_objects,
			COALESCE(SUM(CASE WHEN status_rank > 0 THEN 1 ELSE 0 END), 0) AS unhealthy_objects,
			COALESCE(SUM(requested_copies), 0) AS requested_copies,
			COALESCE(SUM(readable_copies), 0) AS readable_copies,
			COALESCE(SUM(pending_copies), 0) AS pending_copies,
			COALESCE(SUM(failed_copies), 0) AS failed_copies,
			COALESCE(SUM(unknown_copies), 0) AS unknown_copies,
			COALESCE(SUM(copy_under_replicated), 0) > 0 AS copy_under_replicated,
			COALESCE(SUM(copy_pending), 0) > 0 AS copy_pending,
			COALESCE(SUM(copy_committing), 0) > 0 AS copy_committing,
			COALESCE(SUM(copy_failed), 0) > 0 AS copy_failed,
			COALESCE(SUM(copy_missing_provider), 0) > 0 AS copy_missing_provider,
			COALESCE(SUM(copy_missing_data_set), 0) > 0 AS copy_missing_data_set,
			COALESCE(SUM(copy_missing_piece), 0) > 0 AS copy_missing_piece,
			COALESCE(SUM(copy_missing_retrieval_url), 0) > 0 AS copy_missing_retrieval_url,
			COALESCE(SUM(copy_observation_missing), 0) > 0 AS copy_observation_missing,
			MIN(last_checked_at) AS last_checked_at
		FROM object_health
		GROUP BY bucket_id
		ORDER BY bucket_id ASC`, currentVersionBucketFilter)
	args = append(args, staleBefore, staleBefore, staleBefore, staleBefore)
	if err := r.db.NewRaw(query, args...).Scan(ctx, &summaries); err != nil {
		return nil, fmt.Errorf("listing current object copy health summaries: %w", err)
	}
	return summaries, nil
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

func (r *BunStorageUploadRepo) ListDataSetSummaries(ctx context.Context, bucketID int64) ([]StorageDataSetSummary, error) {
	var summaries []StorageDataSetSummary
	query := `SELECT
			storage_data_set.id,
			storage_data_set.bucket_id,
			bucket.name AS bucket_name,
			storage_data_set.copy_index,
			storage_data_set.provider_id,
			storage_data_set.data_set_id,
			storage_data_set.client_data_set_id,
			storage_data_set.status,
			storage_data_set.created_by_upload_id,
			storage_data_set.last_used_upload_id,
			COALESCE(copy_stats.committed_copies, 0) AS committed_copies,
			COALESCE(copy_stats.readable_copies, 0) AS readable_copies,
			COALESCE(copy_stats.physical_bytes, 0) AS physical_bytes,
			COALESCE(version_stats.referenced_versions, 0) AS referenced_versions,
			COALESCE(version_stats.current_versions, 0) AS current_versions,
			storage_data_set.created_at,
			storage_data_set.updated_at
		FROM storage_data_sets AS storage_data_set
		JOIN buckets AS bucket ON bucket.id = storage_data_set.bucket_id
		LEFT JOIN (
			SELECT
				storage_copy.storage_data_set_id,
				COUNT(*) AS committed_copies,
				SUM(CASE
					WHEN storage_data_set.status IN ('ready', 'draining')
					  AND storage_copy.provider_id IS NOT NULL AND storage_copy.provider_id <> ''
					  AND storage_data_set.data_set_id IS NOT NULL AND storage_data_set.data_set_id <> ''
					  AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> ''
					  AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> ''
					THEN 1 ELSE 0 END) AS readable_copies,
				SUM(storage_upload.content_size) AS physical_bytes
			FROM storage_upload_copies AS storage_copy
			JOIN storage_uploads AS storage_upload ON storage_upload.id = storage_copy.upload_id
			JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
			WHERE storage_copy.status = 'committed'
			GROUP BY storage_copy.storage_data_set_id
		) AS copy_stats ON copy_stats.storage_data_set_id = storage_data_set.id
		LEFT JOIN (
			SELECT
				storage_copy.storage_data_set_id,
				COUNT(DISTINCT object_version.version_id) AS referenced_versions,
				COUNT(DISTINCT CASE WHEN object_version.is_current THEN object_version.version_id END) AS current_versions
			FROM storage_upload_copies AS storage_copy
			JOIN object_versions AS object_version ON object_version.storage_upload_id = storage_copy.upload_id
			WHERE storage_copy.status = 'committed'
			  AND object_version.is_delete_marker = FALSE
			GROUP BY storage_copy.storage_data_set_id
		) AS version_stats ON version_stats.storage_data_set_id = storage_data_set.id
		WHERE (? = 0 OR storage_data_set.bucket_id = ?)
		ORDER BY bucket.name ASC, storage_data_set.copy_index ASC`
	if err := r.db.NewRaw(query, bucketID, bucketID).Scan(ctx, &summaries); err != nil {
		return nil, fmt.Errorf("listing storage data set summaries: %w", err)
	}
	return summaries, nil
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

func (r *BunStorageUploadRepo) GetDataSetBindingByID(ctx context.Context, id int64) (*model.StorageDataSet, error) {
	binding := new(model.StorageDataSet)
	err := r.db.NewSelect().
		Model(binding).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage data set binding by id: %w", err)
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

func (r *BunStorageUploadRepo) MarkDataSetDraining(ctx context.Context, id int64, lastError string) error {
	_, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusDraining).
		Set("last_error = ?", lastError).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage data set draining: %w", err)
	}
	return nil
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

func (r *BunStorageUploadRepo) MarkDataSetUnavailable(ctx context.Context, id int64, lastError string) error {
	_, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusUnavailable).
		Set("last_error = ?", lastError).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage data set unavailable: %w", err)
	}
	return nil
}

func (r *BunStorageUploadRepo) DiscardFailedDataSetCandidate(ctx context.Context, uploadID int64, copyIndex int, storageDataSetID int64) error {
	if uploadID <= 0 || copyIndex < 0 || storageDataSetID <= 0 {
		return fmt.Errorf("invalid failed storage data set candidate: %w", ErrInvalidInput)
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		candidates, err := db.NewSelect().
			Model((*model.StorageDataSet)(nil)).
			Where("id = ?", storageDataSetID).
			Where("created_by_upload_id = ?", uploadID).
			Where("status = ?", model.StorageDataSetStatusFailed).
			Where("(data_set_id IS NULL OR data_set_id = '')").
			Count(ctx)
		if err != nil {
			return fmt.Errorf("checking failed storage data set candidate: %w", err)
		}
		if candidates == 0 {
			return nil
		}
		if _, err := db.NewDelete().
			Model((*model.StorageUploadCopy)(nil)).
			Where("upload_id = ?", uploadID).
			Where("copy_index = ?", copyIndex).
			Where("storage_data_set_id = ?", storageDataSetID).
			Where("status = ?", model.StorageUploadCopyStatusFailed).
			Exec(ctx); err != nil {
			return fmt.Errorf("deleting failed storage upload copy candidate: %w", err)
		}
		refs, err := db.NewSelect().
			Model((*model.StorageUploadCopy)(nil)).
			Where("storage_data_set_id = ?", storageDataSetID).
			Count(ctx)
		if err != nil {
			return fmt.Errorf("checking failed storage data set candidate references: %w", err)
		}
		if refs > 0 {
			return nil
		}
		if _, err := db.NewDelete().
			Model((*model.StorageDataSet)(nil)).
			Where("id = ?", storageDataSetID).
			Where("created_by_upload_id = ?", uploadID).
			Where("status = ?", model.StorageDataSetStatusFailed).
			Where("(data_set_id IS NULL OR data_set_id = '')").
			Exec(ctx); err != nil {
			return fmt.Errorf("deleting failed storage data set candidate: %w", err)
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) CreateUploadCopiesForBindings(ctx context.Context, uploadID int64, copies []UploadCopyBindingInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		for _, input := range copies {
			if input.ProviderID.IsZero() {
				return fmt.Errorf("providerID is required: %w", ErrInvalidInput)
			}
			providerID := input.ProviderID
			isNewDataSet, err := storageDataSetCreatedByUpload(ctx, db, input.StorageDataSetID, uploadID)
			if err != nil {
				return err
			}
			copyRow := &model.StorageUploadCopy{
				UploadID:         uploadID,
				CopyIndex:        input.CopyIndex,
				ProviderID:       &providerID,
				TransferMethod:   input.TransferMethod,
				Status:           model.StorageUploadCopyStatusPending,
				StorageDataSetID: &input.StorageDataSetID,
				IsNewDataSet:     isNewDataSet,
			}
			if _, err := db.NewInsert().
				Model(copyRow).
				On("CONFLICT (upload_id, copy_index) DO NOTHING").
				Exec(ctx); err != nil {
				return fmt.Errorf("creating storage upload copy row: %w", err)
			}
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
		transferMethod, err := uploadCopyTransferMethod(ctx, db, input.UploadID, input.CopyIndex)
		if err != nil {
			return err
		}
		if transferMethod == model.StorageCopyTransferMethodIngress {
			if err := updateUploadIngressReady(ctx, db, input.UploadID, input.PieceCID, now); err != nil {
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
	if input.UploadID <= 0 || input.CopyIndex < 0 || input.PieceCID == "" || input.PieceID == nil || input.RetrievalURL == "" {
		return fmt.Errorf("marking storage upload copy committed: %w", ErrInvalidInput)
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		isNewDataSet, err := uploadCopyDataSetCreatedByUpload(ctx, db, input.UploadID, input.CopyIndex)
		if err != nil {
			return err
		}
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("status = ?", model.StorageUploadCopyStatusCommitted).
			Set("piece_id = COALESCE(?, piece_id)", input.PieceID).
			Set("retrieval_url = COALESCE(?, retrieval_url)", nullableString(input.RetrievalURL)).
			Set("is_new_data_set = ?", isNewDataSet).
			Set("commit_extra_data_hex = COALESCE(?, commit_extra_data_hex)", nullableString(input.CommitExtraDataHex)).
			Set("commit_transaction_id = COALESCE(?, commit_transaction_id)", nullableString(input.CommitTransactionID)).
			Set("last_error = NULL").
			Set("updated_at = ?", now).
			Where("upload_id = ? AND copy_index = ?", input.UploadID, input.CopyIndex).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload copy committed: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("marking storage upload copy committed: %w", ErrNotFound)
		}
		if err := updateUploadReadable(ctx, db, input.UploadID, input.PieceCID, now); err != nil {
			return err
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
		readableCount, err := countReadableCommittedCopies(ctx, db, uploadID)
		if err != nil {
			return err
		}
		if readableCount == 0 {
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
				Set("status = ?", model.StorageUploadStatusReadable).
				Set("error_message = ?", lastError).
				Set("updated_at = ?", now).
				Where("id = ?", uploadID).
				Where("status IN (?)", bun.List([]model.StorageUploadStatus{
					model.StorageUploadStatusReadable,
				})).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("marking storage upload readable after copy failure: %w", err)
			}
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) BindReadableUploadForContent(ctx context.Context, input BindReadableUploadInput) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		upload := new(model.StorageUpload)
		if err := db.NewSelect().Model(upload).Where("id = ?", input.UploadID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for readable bind: %w", err)
		}
		if err := requireReadableCommittedCopy(ctx, db, input.UploadID); err != nil {
			return err
		}
		now := time.Now()
		if err := updateUploadReadable(ctx, db, input.UploadID, derefString(upload.PieceCID), now); err != nil {
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
						  AND active_upload.status IN ('running', 'ingress_ready', 'readable')
					)
					AND NOT EXISTS (
						SELECT 1 FROM tasks AS active_task
						WHERE active_task.ref_type = 'object'
						  AND active_task.ref_version_id = object_versions.version_id
						  AND active_task.type = ?
						  AND active_task.status IN (?)
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
			model.TaskTypeUpload, bun.List(activeTaskStatuses()),
		).Scan(ctx, &refs)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("binding readable upload for content: %w", err)
		}
		return nil
	})
	return refs, err
}

func (r *BunStorageUploadRepo) BindReadableUploadForVersion(ctx context.Context, input BindReadableUploadForVersionInput) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		upload := new(model.StorageUpload)
		if err := db.NewSelect().Model(upload).Where("id = ?", input.UploadID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for version readable bind: %w", err)
		}
		if err := requireReadableCommittedCopy(ctx, db, input.UploadID); err != nil {
			return err
		}
		now := time.Now()
		if err := updateUploadReadable(ctx, db, input.UploadID, derefString(upload.PieceCID), now); err != nil {
			return err
		}
		query := `UPDATE object_versions
			SET storage_upload_id = ?, state = ?, failed_at_state = NULL, last_error = NULL, updated_at = ?
			WHERE version_id = ? AND bucket_id = ? AND size = ? AND checksum = ? AND state = ?
			  AND NOT EXISTS (
				SELECT 1 FROM storage_uploads AS active_upload
				WHERE active_upload.source_version_id = object_versions.version_id
				  AND active_upload.id <> ?
				  AND active_upload.status IN ('running', 'ingress_ready', 'readable')
			  )
			RETURNING object_id, version_id`
		err := db.NewRaw(query,
			input.UploadID, model.ObjectStateReplicating, now,
			input.VersionID, input.BucketID, input.ContentSize, input.Checksum, model.ObjectStateUploading,
			input.UploadID,
		).Scan(ctx, &refs)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("binding readable upload for version: %w", err)
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

func (r *BunStorageUploadRepo) FinalizeUploadIfTargetCopiesMet(ctx context.Context, input FinalizeUploadInput) (bool, []ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	finalized := false
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		upload := new(model.StorageUpload)
		if err := db.NewSelect().Model(upload).Where("id = ?", input.UploadID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return fmt.Errorf("selecting storage upload for finalization: %w", err)
		}
		readable, err := countReadableCommittedCopies(ctx, db, input.UploadID)
		if err != nil {
			return err
		}
		if upload.RequestedCopies <= 0 || readable < upload.RequestedCopies {
			return nil
		}
		now := time.Now()
		_, err = db.NewUpdate().
			Model((*model.StorageUpload)(nil)).
			Set("status = ?", model.StorageUploadStatusComplete).
			Set("accepted_at = COALESCE(accepted_at, ?)", now).
			Set("accept_error = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", input.UploadID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload complete: %w", err)
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

func countReadableCommittedCopies(ctx context.Context, db bun.IDB, uploadID int64) (int, error) {
	var row struct {
		Count int `bun:"count"`
	}
	err := db.NewRaw(`SELECT COUNT(*) AS count
		FROM storage_upload_copies AS storage_copy
		JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		WHERE storage_copy.upload_id = ?
		  AND storage_copy.status = ?
		  AND storage_copy.storage_data_set_id IS NOT NULL
		  AND storage_copy.provider_id IS NOT NULL AND storage_copy.provider_id <> ''
		  AND storage_data_set.data_set_id IS NOT NULL AND storage_data_set.data_set_id <> ''
		  AND storage_data_set.status IN ('ready', 'draining')
		  AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> ''
		  AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> ''`,
		uploadID, model.StorageUploadCopyStatusCommitted,
	).Scan(ctx, &row)
	if err != nil {
		return 0, fmt.Errorf("counting readable storage upload copies: %w", err)
	}
	return row.Count, nil
}

func requireReadableCommittedCopy(ctx context.Context, db bun.IDB, uploadID int64) error {
	count, err := countReadableCommittedCopies(ctx, db, uploadID)
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("storage upload %d has no readable committed copy: %w", uploadID, ErrNotFound)
	}
	return nil
}

func uploadCopyTransferMethod(ctx context.Context, db bun.IDB, uploadID int64, copyIndex int) (model.StorageCopyTransferMethod, error) {
	var row struct {
		TransferMethod model.StorageCopyTransferMethod `bun:"transfer_method"`
	}
	err := db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		Column("transfer_method").
		Where("upload_id = ? AND copy_index = ?", uploadID, copyIndex).
		Scan(ctx, &row)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("loading storage upload copy transfer method: %w", err)
	}
	return row.TransferMethod, nil
}

func uploadCopyDataSetCreatedByUpload(ctx context.Context, db bun.IDB, uploadID int64, copyIndex int) (bool, error) {
	var row struct {
		IsNewDataSet bool `bun:"is_new_data_set"`
	}
	err := db.NewRaw(`SELECT CASE
			WHEN storage_data_set.created_by_upload_id = storage_copy.upload_id THEN TRUE
			ELSE FALSE
		END AS is_new_data_set
		FROM storage_upload_copies AS storage_copy
		LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		WHERE storage_copy.upload_id = ? AND storage_copy.copy_index = ?`,
		uploadID, copyIndex,
	).Scan(ctx, &row)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("checking storage upload copy data set origin: %w", err)
	}
	return row.IsNewDataSet, nil
}

func storageDataSetCreatedByUpload(ctx context.Context, db bun.IDB, storageDataSetID int64, uploadID int64) (bool, error) {
	count, err := db.NewSelect().
		Model((*model.StorageDataSet)(nil)).
		Where("id = ? AND created_by_upload_id = ?", storageDataSetID, uploadID).
		Count(ctx)
	if err != nil {
		return false, fmt.Errorf("checking storage data set origin: %w", err)
	}
	return count > 0, nil
}

func updateUploadIngressReady(ctx context.Context, db bun.IDB, uploadID int64, pieceCID string, now time.Time) error {
	_, err := db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("status = ?", model.StorageUploadStatusIngressReady).
		Set("piece_cid = COALESCE(?, piece_cid)", nullableString(pieceCID)).
		Set("ingress_bytes_transferred = content_size").
		Set("progress_updated_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ?", uploadID).
		Where("status IN (?)", bun.List([]model.StorageUploadStatus{
			model.StorageUploadStatusRunning,
			model.StorageUploadStatusIngressReady,
			model.StorageUploadStatusFailed,
		})).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage upload ingress ready: %w", err)
	}
	return nil
}

func updateUploadReadable(ctx context.Context, db bun.IDB, uploadID int64, pieceCID string, now time.Time) error {
	_, err := db.NewUpdate().
		Model((*model.StorageUpload)(nil)).
		Set("status = ?", model.StorageUploadStatusReadable).
		Set("piece_cid = COALESCE(?, piece_cid)", nullableString(pieceCID)).
		Set("updated_at = ?", now).
		Where("id = ?", uploadID).
		Where("status IN (?)", bun.List([]model.StorageUploadStatus{
			model.StorageUploadStatusRunning,
			model.StorageUploadStatusIngressReady,
			model.StorageUploadStatusReadable,
			model.StorageUploadStatusFailed,
		})).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage upload readable: %w", err)
	}
	return nil
}

func completeUploadTasksForVersion(ctx context.Context, db bun.IDB, versionID string, now time.Time) error {
	if versionID == "" {
		return nil
	}
	_, err := db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusCompleted).
		Set("completed_at = ?", now).
		Set("last_error = NULL").
		Set("wait_reason = NULL").
		Set("status_message = NULL").
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("ref_type = ? AND ref_version_id = ? AND type = ?", "object", versionID, model.TaskTypeUpload).
		Where("status IN (?)", bun.List(activeTaskStatuses())).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing upload tasks for bound version: %w", err)
	}
	return nil
}

func activeUploadStatuses() []model.StorageUploadStatus {
	return []model.StorageUploadStatus{
		model.StorageUploadStatusRunning,
		model.StorageUploadStatusIngressReady,
		model.StorageUploadStatusReadable,
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
