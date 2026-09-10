package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/types"
	"github.com/uptrace/bun"
)

type BunStorageContentRepo struct {
	db bun.IDB
}

var _ StorageContentRepository = (*BunStorageContentRepo)(nil)

// EnsureContent returns the content row for one byte payload in a bucket,
// creating it if this is the first time those bytes are written.
// A uniqueness conflict returns the existing row without changing its frozen
// requested-copy target.
func (r *BunStorageContentRepo) EnsureContent(ctx context.Context, input EnsureContentInput) (*model.StorageContent, error) {
	if input.BucketID <= 0 || input.ContentSize < 0 || !validStorageContentChecksum(input.Checksum) {
		return nil, fmt.Errorf("ensuring storage content: %w", ErrInvalidInput)
	}
	requestedCopies := input.RequestedCopies
	if !model.ValidStorageCopies(requestedCopies) {
		return nil, fmt.Errorf("requested copies must be between %d and %d, got %d", model.StorageCopiesMin, model.StorageCopiesMax, requestedCopies)
	}
	content := &model.StorageContent{
		BucketID:        input.BucketID,
		Checksum:        input.Checksum,
		ContentSize:     input.ContentSize,
		RequestedCopies: requestedCopies,
	}
	if _, err := r.db.NewInsert().Model(content).Exec(ctx); err != nil {
		if isUniqueViolation(err) {
			existing, selectErr := r.findContentByBytes(ctx, input.BucketID, input.Checksum, input.ContentSize)
			if selectErr != nil {
				return nil, selectErr
			}
			if existing != nil {
				return existing, nil
			}
		}
		return nil, fmt.Errorf("ensuring storage content: %w", err)
	}
	return content, nil
}

func validStorageContentChecksum(checksum string) bool {
	if len(checksum) != 64 {
		return false
	}
	for i := range len(checksum) {
		if (checksum[i] < '0' || checksum[i] > '9') && (checksum[i] < 'a' || checksum[i] > 'f') {
			return false
		}
	}
	return true
}

// findContentByBytes resolves the content identity directly. Deduplication used
// to require scanning object versions by (bucket_id, size, checksum) and then
// joining uploads; the unique key makes it a single lookup.
func (r *BunStorageContentRepo) findContentByBytes(ctx context.Context, bucketID int64, checksum string, size int64) (*model.StorageContent, error) {
	content := new(model.StorageContent)
	err := r.db.NewSelect().
		Model(content).
		Where("bucket_id = ? AND checksum = ? AND content_size = ?", bucketID, checksum, size).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage content by bytes: %w", err)
	}
	return content, nil
}

func (r *BunStorageContentRepo) GetByID(ctx context.Context, contentID int64) (*model.StorageContent, error) {
	upload := new(model.StorageContent)
	err := r.db.NewSelect().
		Model(upload).
		Where("id = ?", contentID).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage upload: %w", err)
	}
	return upload, nil
}

func (r *BunStorageContentRepo) GetByIDs(ctx context.Context, contentIDs []int64) (map[int64]model.StorageContent, error) {
	uploadsByID := make(map[int64]model.StorageContent, len(contentIDs))
	if len(contentIDs) == 0 {
		return uploadsByID, nil
	}
	var uploads []model.StorageContent
	if err := r.db.NewSelect().
		Model(&uploads).
		Where("id IN (?)", bun.List(contentIDs)).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("selecting storage uploads by id: %w", err)
	}
	for _, upload := range uploads {
		uploadsByID[upload.ID] = upload
	}
	return uploadsByID, nil
}

// BeginIngressStoreProgress starts a fresh, fenced progress attempt on the
// ingress copy. The caller runs this in the same transaction as the task
// checkpoint that authorizes the provider request.
func (r *BunStorageContentRepo) BeginIngressStoreProgress(ctx context.Context, input BeginIngressStoreProgressInput) (*model.StorageCopy, error) {
	if input.CopyID < 1 || input.Generation < 1 || input.TaskID < 1 || input.Attempt < 1 {
		return nil, fmt.Errorf("beginning ingress store progress: %w", ErrInvalidInput)
	}
	now := time.Now()
	copyRow := new(model.StorageCopy)
	err := r.db.NewUpdate().
		Model(copyRow).
		Set("ingress_store_attempt = ?", input.Attempt).
		Set("ingress_bytes_transferred = 0").
		Set("progress_updated_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ? AND work_generation = ? AND active_task_id = ?", input.CopyID, input.Generation, input.TaskID).
		Where("ingress_store_attempt = ?", input.Attempt-1).
		Where("status = ? AND transfer_method = ?", model.StorageCopyStatusPending, model.StorageCopyTransferMethodIngress).
		Returning("*").
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("beginning ingress store progress: %w", ErrConflict)
	}
	if err != nil {
		return nil, fmt.Errorf("beginning ingress store progress: %w", err)
	}
	return copyRow, nil
}

// GetIngressCopy returns the copy that performs the ingress transfer for this
// content, or nil when there is none. Progress lives there rather than on the
// content shared by every replica.
// ContentPipelineState derives how far a content has travelled from its copy
// rows. Nothing stores this: a stored column would be a second copy of facts the
// copies already own, and the two would drift.
func (r *BunStorageContentRepo) ContentPipelineState(ctx context.Context, contentID int64) (model.ObjectState, error) {
	var state string
	err := r.db.NewSelect().
		TableExpr("storage_contents AS storage_content").
		ColumnExpr(contentPipelineStateSQL()).
		Where("storage_content.id = ?", contentID).
		Scan(ctx, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return model.ObjectStateCached, nil
	}
	if err != nil {
		return "", fmt.Errorf("deriving content pipeline state: %w", err)
	}
	return model.ObjectState(state), nil
}

// RecordContentFailure stores the message explaining why this content could not
// be placed. Whether it counts as failed is derived from its copies; only the
// human-readable reason is persisted.
func (r *BunStorageContentRepo) RecordContentFailure(ctx context.Context, contentID int64, message string) error {
	if contentID < 1 {
		return fmt.Errorf("recording content failure: %w", ErrInvalidInput)
	}
	_, err := r.db.NewUpdate().
		Model((*model.StorageContent)(nil)).
		Set("error_message = ?", message).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", contentID).
		Where("accepted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("recording content failure: %w", err)
	}
	return nil
}

func (r *BunStorageContentRepo) GetIngressCopy(ctx context.Context, contentID int64) (*model.StorageCopy, error) {
	copyRow, err := r.ingressCopy(ctx, contentID)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return copyRow, err
}

func (r *BunStorageContentRepo) ingressCopy(ctx context.Context, contentID int64) (*model.StorageCopy, error) {
	copyRow := new(model.StorageCopy)
	err := r.db.NewSelect().
		Model(copyRow).
		Where("content_id = ? AND transfer_method = ?", contentID, model.StorageCopyTransferMethodIngress).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("ingress copy for content %d: %w", contentID, ErrNotFound)
		}
		return nil, fmt.Errorf("selecting ingress copy: %w", err)
	}
	return copyRow, nil
}

func (r *BunStorageContentRepo) RecordIngressStoreProgress(ctx context.Context, input RecordIngressStoreProgressInput) (*model.StorageCopy, error) {
	if input.CopyID < 1 || input.Generation < 1 || input.TaskID < 1 || input.Attempt < 1 || input.BytesUploaded < 0 {
		return nil, fmt.Errorf("recording ingress store progress: %w", ErrInvalidInput)
	}
	now := time.Now()
	// content_size is repeated on the copy and pinned there by a composite
	// foreign key, so the clamp stays a single-row update with no join.
	copyRow := new(model.StorageCopy)
	err := r.db.NewUpdate().
		Model(copyRow).
		Set("progress_updated_at = CASE WHEN ingress_bytes_transferred < content_size AND ? > ingress_bytes_transferred THEN ? ELSE progress_updated_at END", input.BytesUploaded, now).
		Set("ingress_bytes_transferred = CASE WHEN ? > content_size THEN content_size WHEN ? > ingress_bytes_transferred THEN ? ELSE ingress_bytes_transferred END", input.BytesUploaded, input.BytesUploaded, input.BytesUploaded).
		Where("id = ? AND work_generation = ? AND active_task_id = ?", input.CopyID, input.Generation, input.TaskID).
		Where("ingress_store_attempt = ?", input.Attempt).
		Where("status = ? AND transfer_method = ?", model.StorageCopyStatusPending, model.StorageCopyTransferMethodIngress).
		Returning("*").
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("recording ingress store progress: %w", ErrConflict)
	}
	if err != nil {
		return nil, fmt.Errorf("recording ingress store progress: %w", err)
	}
	return copyRow, nil
}

func (r *BunStorageContentRepo) GetUploadProvenance(ctx context.Context, contentID int64) (*StorageContentProvenance, error) {
	upload, err := r.GetByID(ctx, contentID)
	if err != nil || upload == nil {
		return nil, err
	}
	copies, err := r.ListCopies(ctx, contentID)
	if err != nil {
		return nil, err
	}
	ingress, err := r.GetIngressCopy(ctx, contentID)
	if err != nil {
		return nil, err
	}
	return &StorageContentProvenance{
		Upload: *upload,
		Copies: copies, IngressCopy: ingress,
	}, nil
}

func (r *BunStorageContentRepo) ListCopies(ctx context.Context, contentID int64) ([]model.StorageCopy, error) {
	var copies []model.StorageCopy
	query := `SELECT storage_copy.*, storage_data_set.data_set_id AS data_set_id,
			CASE WHEN storage_data_set.created_by_content_id = storage_copy.content_id
				THEN TRUE ELSE FALSE END AS is_new_data_set,
			active_commit_attempt.attempt_id AS commit_attempt_id,
			active_commit_attempt.attempted_at AS commit_attempted_at,
			active_commit_attempt.transaction_id AS commit_transaction_id,
			active_commit_attempt.submission_json AS commit_submission_json,
			active_commit_attempt.confirmed_transaction_id AS commit_confirmed_transaction_id,
			active_commit_attempt.attention_code AS commit_attention_code,
			active_commit_attempt.attention_at AS commit_attention_at
		FROM storage_copies AS storage_copy
		LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		LEFT JOIN storage_commit_attempts AS active_commit_attempt
		  ON active_commit_attempt.content_id = storage_copy.content_id
		 AND active_commit_attempt.storage_data_set_id = storage_copy.storage_data_set_id
		 AND active_commit_attempt.resolved_at IS NULL
		WHERE storage_copy.content_id = ?
		ORDER BY storage_copy.copy_index ASC`
	if err := r.db.NewRaw(query, contentID).Scan(ctx, &copies); err != nil {
		return nil, fmt.Errorf("listing storage upload copies: %w", err)
	}
	return copies, nil
}

// CountCurrentGenerationCopySlots counts logical replica slots, not physical
// generation rows.
func (r *BunStorageContentRepo) CountCurrentGenerationCopySlots(ctx context.Context, contentID int64) (int, error) {
	if contentID <= 0 {
		return 0, fmt.Errorf("counting current upload copy slots: %w", ErrInvalidInput)
	}
	var count int
	query := fmt.Sprintf(`SELECT COUNT(DISTINCT storage_copy.copy_index)
		FROM storage_copies AS storage_copy
		WHERE storage_copy.content_id = ? AND %s`, currentGenerationCopySQL("storage_copy"))
	if err := r.db.NewRaw(query, contentID).Scan(ctx, &count); err != nil {
		return 0, fmt.Errorf("counting current upload copy slots: %w", err)
	}
	return count, nil
}

func (r *BunStorageContentRepo) ListReadableCommittedCopies(ctx context.Context, contentID int64) ([]ReadableStorageCopy, error) {
	var copies []ReadableStorageCopy
	query := fmt.Sprintf(`SELECT
			storage_copy.content_id,
			storage_content.piece_cid,
			storage_copy.copy_index,
			storage_copy.provider_id,
			storage_data_set.data_set_id,
			storage_copy.piece_id,
			storage_copy.transfer_method,
			storage_copy.retrieval_url
		FROM storage_copies AS storage_copy
		JOIN storage_contents AS storage_content ON storage_content.id = storage_copy.content_id
		JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		WHERE storage_copy.content_id = ?
		  AND storage_content.piece_cid IS NOT NULL AND storage_content.piece_cid <> ''
		  AND %s`,
		readableCommittedCopyPredicateSQL("storage_copy", "storage_data_set"),
	)
	args := []any{contentID}
	query += " ORDER BY storage_copy.copy_index ASC"
	if err := r.db.NewRaw(query, args...).Scan(ctx, &copies); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("listing readable storage copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageContentRepo) HasReadableCommittedCopy(ctx context.Context, contentID int64) (bool, error) {
	count, err := countReadableReplicaSlots(ctx, r.db, contentID)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

type bucketStorageHealthSummaryRow struct {
	BucketID               int64      `bun:"bucket_id"`
	AbnormalDataSets       int        `bun:"abnormal_data_sets"`
	AffectedVersionsSeen   int        `bun:"affected_versions_seen"`
	LocalStatusNotReady    bool       `bun:"local_status_not_ready"`
	ObservationMissing     bool       `bun:"observation_missing"`
	ObservationStale       bool       `bun:"observation_stale"`
	ObservationUnavailable bool       `bun:"observation_unavailable"`
	ObservationDegraded    bool       `bun:"observation_degraded"`
	ObservationUnknown     bool       `bun:"observation_unknown"`
	LastCheckedAt          *time.Time `bun:"last_checked_at"`
}

// bucketStorageHealthAffectedVersionExistsSQL expects dataSetSourceAlias to expose bucket_id and data_set_id.
func bucketStorageHealthAffectedVersionExistsSQL(dataSetSourceAlias string) string {
	return fmt.Sprintf(`EXISTS (
					SELECT 1
					FROM storage_copies AS storage_copy
					JOIN object_versions AS object_version
					  ON object_version.content_id = storage_copy.content_id
					 AND object_version.bucket_id = %[1]s.bucket_id
					 AND object_version.is_delete_marker = FALSE
					WHERE storage_copy.storage_data_set_id = %[1]s.data_set_id
					  AND storage_copy.status = %[2]s
				)`, dataSetSourceAlias, storageHealthCommittedCopyStatusSQL())
}

func (r *BunStorageContentRepo) ListBucketStorageHealthSummaries(ctx context.Context, bucketID int64, staleBefore time.Time, affectedVersionCap int) ([]BucketStorageHealthSummary, error) {
	if affectedVersionCap < 1 {
		affectedVersionCap = 1
	}
	var rows []bucketStorageHealthSummaryRow
	dataSetBucketFilter := ""
	args := []any{staleBefore}
	if bucketID > 0 {
		dataSetBucketFilter = `
			  AND storage_data_set.bucket_id = ?`
		args = append(args, bucketID)
	}
	query := fmt.Sprintf(`WITH bucket_data_sets AS (
				SELECT
					storage_data_set.id AS data_set_id,
					storage_data_set.bucket_id,
				CASE WHEN storage_data_set.status NOT IN (%s) THEN 1 ELSE 0 END AS local_status_not_ready,
				CASE WHEN observation.local_data_set_id IS NULL THEN 1 ELSE 0 END AS observation_missing,
				CASE WHEN observation.local_data_set_id IS NOT NULL AND observation.last_checked_at < ? THEN 1 ELSE 0 END AS observation_stale,
				CASE WHEN observation.status = %s THEN 1 ELSE 0 END AS observation_unavailable,
				CASE WHEN observation.status = %s THEN 1 ELSE 0 END AS observation_degraded,
				CASE WHEN observation.status = %s THEN 1 ELSE 0 END AS observation_unknown,
				observation.last_checked_at
				FROM storage_data_sets AS storage_data_set
				LEFT JOIN observability_data_set_states AS observation ON observation.local_data_set_id = storage_data_set.id
				WHERE 1 = 1
	%s
			),
			bucket_observation AS (
				SELECT
					bucket_id,
					COALESCE(MAX(observation_stale), 0) AS observation_stale,
					MIN(last_checked_at) AS last_checked_at
				FROM bucket_data_sets
				GROUP BY bucket_id
			),
			abnormal_data_sets AS (
				SELECT *
				FROM bucket_data_sets
				WHERE local_status_not_ready = 1
				  OR observation_missing = 1
				  OR observation_stale = 1
				  OR observation_unavailable = 1
				  OR observation_degraded = 1
				  OR observation_unknown = 1
			),
			bucket_abnormal AS (
				SELECT
					bucket_id,
					COUNT(*) AS abnormal_data_sets
				FROM abnormal_data_sets
				GROUP BY bucket_id
			),
			affected_data_sets AS (
				SELECT
					abnormal_data_set.*
				FROM abnormal_data_sets AS abnormal_data_set
				WHERE %s
			),
			affected_bucket_summary AS (
				SELECT
					bucket_id,
					COUNT(*) AS affected_data_sets,
					COALESCE(MAX(local_status_not_ready), 0) AS local_status_not_ready,
					COALESCE(MAX(observation_missing), 0) AS observation_missing,
					COALESCE(MAX(observation_stale), 0) AS observation_stale,
					COALESCE(MAX(observation_unavailable), 0) AS observation_unavailable,
					COALESCE(MAX(observation_degraded), 0) AS observation_degraded,
					COALESCE(MAX(observation_unknown), 0) AS observation_unknown,
					MIN(last_checked_at) AS last_checked_at
				FROM affected_data_sets
				GROUP BY bucket_id
			)
			SELECT
				bucket_observation.bucket_id,
				COALESCE(bucket_abnormal.abnormal_data_sets, 0) AS abnormal_data_sets,
				COALESCE((
					SELECT COUNT(*)
					FROM (
						SELECT DISTINCT object_version.version_id
						FROM affected_data_sets AS affected_data_set
						JOIN storage_copies AS storage_copy
						  ON storage_copy.storage_data_set_id = affected_data_set.data_set_id
						JOIN object_versions AS object_version
						  ON object_version.content_id = storage_copy.content_id
						 AND object_version.bucket_id = affected_data_set.bucket_id
						 AND object_version.is_delete_marker = FALSE
						WHERE affected_data_set.bucket_id = bucket_observation.bucket_id
						  AND storage_copy.status = %s
						LIMIT ?
					) AS capped_affected_versions
				), 0) AS affected_versions_seen,
				COALESCE(affected_bucket_summary.local_status_not_ready, 0) > 0 AS local_status_not_ready,
				COALESCE(affected_bucket_summary.observation_missing, 0) > 0 AS observation_missing,
				(CASE
					WHEN COALESCE(affected_bucket_summary.affected_data_sets, 0) > 0
						THEN COALESCE(affected_bucket_summary.observation_stale, 0)
					ELSE bucket_observation.observation_stale
				END) > 0 AS observation_stale,
				COALESCE(affected_bucket_summary.observation_unavailable, 0) > 0 AS observation_unavailable,
				COALESCE(affected_bucket_summary.observation_degraded, 0) > 0 AS observation_degraded,
				COALESCE(affected_bucket_summary.observation_unknown, 0) > 0 AS observation_unknown,
				CASE
					WHEN COALESCE(affected_bucket_summary.affected_data_sets, 0) > 0
						THEN affected_bucket_summary.last_checked_at
					ELSE bucket_observation.last_checked_at
				END AS last_checked_at
			FROM bucket_observation
			LEFT JOIN bucket_abnormal ON bucket_abnormal.bucket_id = bucket_observation.bucket_id
			LEFT JOIN affected_bucket_summary ON affected_bucket_summary.bucket_id = bucket_observation.bucket_id
			ORDER BY bucket_observation.bucket_id ASC`,
		storageHealthReadyDataSetStatusListSQL(),
		storageHealthUnavailableObservationStatusSQL(),
		storageHealthDegradedObservationStatusSQL(),
		storageHealthUnknownObservationStatusSQL(),
		dataSetBucketFilter,
		bucketStorageHealthAffectedVersionExistsSQL("abnormal_data_set"),
		storageHealthCommittedCopyStatusSQL(),
	)
	args = append(args, affectedVersionCap+1)
	if err := r.db.NewRaw(query, args...).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing bucket storage health summaries: %w", err)
	}
	reasonCodes, err := r.listBucketStorageHealthReasonCodes(ctx, bucketID, staleBefore)
	if err != nil {
		return nil, err
	}
	summaries := make([]BucketStorageHealthSummary, 0, len(rows))
	for _, row := range rows {
		affectedVersions := row.AffectedVersionsSeen
		exceedsCap := affectedVersions > affectedVersionCap
		if exceedsCap {
			affectedVersions = affectedVersionCap
		}
		reasons := reasonCodes[row.BucketID]
		if reasons == nil {
			reasons = []observability.ReasonCode{}
		}
		summaries = append(summaries, BucketStorageHealthSummary{
			BucketID:                   row.BucketID,
			AbnormalDataSets:           row.AbnormalDataSets,
			AffectedVersionsCapped:     affectedVersions,
			AffectedVersionsCap:        affectedVersionCap,
			AffectedVersionsExceedsCap: exceedsCap,
			LocalStatusNotReady:        row.LocalStatusNotReady,
			ObservationMissing:         row.ObservationMissing,
			ObservationStale:           row.ObservationStale,
			ObservationUnavailable:     row.ObservationUnavailable,
			ObservationDegraded:        row.ObservationDegraded,
			ObservationUnknown:         row.ObservationUnknown,
			ReasonCodes:                reasons,
			LastCheckedAt:              row.LastCheckedAt,
		})
	}
	return summaries, nil
}

type bucketStorageHealthReasonCodeRow struct {
	BucketID    int64                      `bun:"bucket_id"`
	LocalStatus model.StorageDataSetStatus `bun:"local_status"`
	ReasonCodes []observability.ReasonCode `bun:"reason_codes"`
}

func (r *BunStorageContentRepo) listBucketStorageHealthReasonCodes(ctx context.Context, bucketID int64, staleBefore time.Time) (map[int64][]observability.ReasonCode, error) {
	dataSetBucketFilter := ""
	args := make([]any, 0, 2)
	if bucketID > 0 {
		dataSetBucketFilter = `
			  AND storage_data_set.bucket_id = ?`
		args = append(args, bucketID)
	}
	query := fmt.Sprintf(`WITH abnormal_data_sets AS (
			SELECT
				storage_data_set.id AS data_set_id,
				storage_data_set.bucket_id,
				storage_data_set.status AS local_status,
				COALESCE(observation.reason_codes, %s) AS reason_codes
			FROM storage_data_sets AS storage_data_set
			LEFT JOIN observability_data_set_states AS observation ON observation.local_data_set_id = storage_data_set.id
			WHERE 1 = 1
%s
			  AND (
				  storage_data_set.status NOT IN (%s)
				  OR observation.local_data_set_id IS NULL
				  OR observation.status IN (%s)
				  OR observation.last_checked_at < ?
			  )
		)
		SELECT DISTINCT
				abnormal_data_set.bucket_id,
				abnormal_data_set.local_status,
				abnormal_data_set.reason_codes
		FROM abnormal_data_sets AS abnormal_data_set
		WHERE %s
		ORDER BY abnormal_data_set.bucket_id ASC`,
		storageHealthEmptyJSONArraySQL(r.db),
		dataSetBucketFilter,
		storageHealthReadyDataSetStatusListSQL(),
		storageHealthAbnormalObservationStatusListSQL(),
		bucketStorageHealthAffectedVersionExistsSQL("abnormal_data_set"),
	)
	args = append(args, staleBefore)
	var rows []bucketStorageHealthReasonCodeRow
	if err := r.db.NewRaw(query, args...).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing bucket storage health reason codes: %w", err)
	}
	out := make(map[int64][]observability.ReasonCode)
	for _, row := range rows {
		if row.LocalStatus != model.StorageDataSetStatusReady && row.LocalStatus != model.StorageDataSetStatusDraining {
			out[row.BucketID] = observability.AppendReasonCode(out[row.BucketID], observability.ReasonLocalStatusNotReady)
		}
		for _, reason := range row.ReasonCodes {
			out[row.BucketID] = observability.AppendReasonCode(out[row.BucketID], reason)
		}
	}
	return out, nil
}

// ListDataSetBindings returns every generation, including retired ones, because
// callers need the full provider history as well as the current write targets.
func (r *BunStorageContentRepo) ListDataSetBindings(ctx context.Context, bucketID int64) ([]model.StorageDataSet, error) {
	var bindings []model.StorageDataSet
	if err := r.db.NewSelect().
		Model(&bindings).
		Where("bucket_id = ?", bucketID).
		OrderExpr("copy_index ASC, generation ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing storage data set bindings: %w", err)
	}
	return bindings, nil
}

func (r *BunStorageContentRepo) ListDataSetSummaries(ctx context.Context, bucketID int64) ([]StorageDataSetSummary, error) {
	var summaries []StorageDataSetSummary
	query := fmt.Sprintf(`SELECT
			storage_data_set.id,
			storage_data_set.bucket_id,
			bucket.name AS bucket_name,
			storage_data_set.copy_index,
			storage_data_set.generation,
			storage_data_set.is_current,
			storage_data_set.provider_id,
			storage_data_set.data_set_id,
			storage_data_set.client_data_set_id,
			storage_data_set.status,
			storage_data_set.created_by_content_id,
			storage_data_set.last_used_content_id,
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
					WHEN storage_data_set.status IN (%s)
					  AND storage_copy.provider_id <> ''
					  AND storage_data_set.data_set_id IS NOT NULL AND storage_data_set.data_set_id <> ''
					  AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> ''
					  AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> ''
					THEN 1 ELSE 0 END) AS readable_copies,
				SUM(storage_content.content_size) AS physical_bytes
			FROM storage_copies AS storage_copy
			JOIN storage_contents AS storage_content ON storage_content.id = storage_copy.content_id
			JOIN storage_data_sets AS storage_data_set
			  ON storage_data_set.id = storage_copy.storage_data_set_id
			 AND storage_data_set.bucket_id = storage_content.bucket_id
			WHERE storage_copy.status = %s
			GROUP BY storage_copy.storage_data_set_id
		) AS copy_stats ON copy_stats.storage_data_set_id = storage_data_set.id
		LEFT JOIN (
			SELECT
				storage_copy.storage_data_set_id,
				COUNT(DISTINCT object_version.version_id) AS referenced_versions,
				COUNT(DISTINCT CASE WHEN referencing_object.current_version_id = object_version.version_id THEN object_version.version_id END) AS current_versions
			FROM storage_copies AS storage_copy
			JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
			JOIN object_versions AS object_version
			  ON object_version.content_id = storage_copy.content_id
			 AND object_version.bucket_id = storage_data_set.bucket_id
			JOIN objects AS referencing_object ON referencing_object.id = object_version.object_id
			WHERE storage_copy.status = %s
			  AND object_version.is_delete_marker = FALSE
			GROUP BY storage_copy.storage_data_set_id
		) AS version_stats ON version_stats.storage_data_set_id = storage_data_set.id
		WHERE (? = 0 OR storage_data_set.bucket_id = ?)
		ORDER BY bucket.name ASC, storage_data_set.copy_index ASC, storage_data_set.generation ASC`,
		storageHealthReadyDataSetStatusListSQL(),
		storageHealthCommittedCopyStatusSQL(),
		storageHealthCommittedCopyStatusSQL(),
	)
	if err := r.db.NewRaw(query, bucketID, bucketID).Scan(ctx, &summaries); err != nil {
		return nil, fmt.Errorf("listing storage data set summaries: %w", err)
	}
	return summaries, nil
}

// GetDataSetBindingByCopyIndex returns the generation that currently owns the
// slot. Historical generations stay readable but never receive new writes.
func (r *BunStorageContentRepo) GetDataSetBindingByCopyIndex(ctx context.Context, bucketID int64, copyIndex int) (*model.StorageDataSet, error) {
	binding := new(model.StorageDataSet)
	err := r.db.NewSelect().
		Model(binding).
		Where("bucket_id = ? AND copy_index = ? AND is_current", bucketID, copyIndex).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage data set binding: %w", err)
	}
	return binding, nil
}

func (r *BunStorageContentRepo) GetDataSetBindingByID(ctx context.Context, id int64) (*model.StorageDataSet, error) {
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

func (r *BunStorageContentRepo) EnsureDataSetBinding(ctx context.Context, input EnsureDataSetBindingInput) (*model.StorageDataSet, error) {
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

func (r *BunStorageContentRepo) MarkDataSetCreating(ctx context.Context, input MarkDataSetCreatingInput) error {
	now := time.Now()
	_, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusCreating).
		Set("create_transaction_id = ?", nullableString(input.TransactionID)).
		Set("create_status_url = ?", nullableString(input.StatusURL)).
		Set("client_data_set_id = ?", input.ClientDataSetID).
		Set("last_used_content_id = ?", nullableInt64(input.ContentID)).
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", input.ID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage data set creating: %w", err)
	}
	return nil
}

func (r *BunStorageContentRepo) MarkDataSetReady(ctx context.Context, input MarkDataSetReadyInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		return markDataSetReady(ctx, db, input.ID, input.ContentID, input.DataSetID, input.ClientDataSetID)
	})
}

func (r *BunStorageContentRepo) BackfillClientDataSetID(ctx context.Context, input BackfillClientDataSetIDInput) error {
	if input.ID <= 0 || input.DataSetID.IsZero() {
		return fmt.Errorf("backfilling storage client data set ID: %w", ErrInvalidInput)
	}
	res, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("client_data_set_id = ?", input.ClientDataSetID).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", input.ID).
		Where("data_set_id = ?", input.DataSetID).
		Where("client_data_set_id IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("backfilling storage client data set ID: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking storage client data set ID backfill: %w", err)
	}
	if rows > 0 {
		return nil
	}
	var current struct {
		DataSetID       *types.OnChainID `bun:"data_set_id"`
		ClientDataSetID *types.OnChainID `bun:"client_data_set_id"`
	}
	err = r.db.NewSelect().
		Table("storage_data_sets").
		Column("data_set_id", "client_data_set_id").
		Where("id = ?", input.ID).
		Scan(ctx, &current)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("backfilling storage client data set ID: %w", ErrNotFound)
		}
		return fmt.Errorf("checking storage client data set ID: %w", err)
	}
	if current.DataSetID == nil || !current.DataSetID.Equal(input.DataSetID) ||
		current.ClientDataSetID == nil || !current.ClientDataSetID.Equal(input.ClientDataSetID) {
		return fmt.Errorf("backfilling storage client data set ID: %w", ErrConflict)
	}
	return nil
}

func (r *BunStorageContentRepo) MarkDataSetDraining(ctx context.Context, id int64, lastError string) error {
	res, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusDraining).
		Set("is_current = ?", false).
		Set("last_error = ?", lastError).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", id).
		Where("status IN (?)", bun.List([]model.StorageDataSetStatus{
			model.StorageDataSetStatusReady,
			model.StorageDataSetStatusDraining,
		})).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage data set draining: %w", err)
	}
	return requireDataSetStatusUpdate(ctx, r.db, id, res, "marking storage data set draining")
}

// MarkDataSetFailed records why a generation could not be created and gives up
// its slot. Keeping is_current would leave the failed row owning the bucket's
// replica slot with nothing able to release it, which stalls provisioning
// permanently; the slot has to be free for the next generation to take it.
func (r *BunStorageContentRepo) MarkDataSetFailed(ctx context.Context, id int64, lastError string) error {
	res, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusFailed).
		Set("is_current = ?", false).
		Set("last_error = ?", lastError).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", id).
		Where("status IN (?)", bun.List([]model.StorageDataSetStatus{
			model.StorageDataSetStatusPending,
			model.StorageDataSetStatusCreating,
			model.StorageDataSetStatusFailed,
		})).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage data set failed: %w", err)
	}
	return requireDataSetStatusUpdate(ctx, r.db, id, res, "marking storage data set failed")
}

func requireDataSetStatusUpdate(ctx context.Context, db bun.IDB, id int64, result sql.Result, operation string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: reading affected rows: %w", operation, err)
	}
	if rows > 0 {
		return nil
	}
	count, err := db.NewSelect().
		Model((*model.StorageDataSet)(nil)).
		Where("id = ?", id).
		Count(ctx)
	if err != nil {
		return fmt.Errorf("%s: checking data set: %w", operation, err)
	}
	if count == 0 {
		return fmt.Errorf("%s: %w", operation, ErrNotFound)
	}
	return fmt.Errorf("%s: data set state changed: %w", operation, ErrConflict)
}

// RetireRejectedDataSet ends a generation whose creation the chain refused, so
// the provider it had reserved becomes available to the bucket again. Without
// this a rejected creation leaks that provider permanently: the unique partial
// index on (bucket_id, provider_id) covers every non-retired row, and a failed
// generation can neither be retired by the replacement gate nor replaced.
//
// The row and its copies are kept. Retirement is the honest end state here —
// the generation holds nothing and owes nothing, and that is provable — while
// deleting it would erase that the bucket ever tried this provider.
//
// The caller must have proof that no data set was created. A rejected
// transaction is proof; an unknown outcome is not, because a creation can be
// submitted and the process can die before the callback records it. Missing
// identity columns are not proof on their own, so the only guard here is the
// one fact that settles it from this side: no data set identity was recorded.
func (r *BunStorageContentRepo) RetireRejectedDataSet(ctx context.Context, storageDataSetID int64) (bool, error) {
	if storageDataSetID <= 0 {
		return false, fmt.Errorf("retiring rejected storage data set: %w", ErrInvalidInput)
	}
	res, err := r.db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusRetired).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", storageDataSetID).
		Where("status = ?", model.StorageDataSetStatusFailed).
		Where("is_current = ?", false).
		Where("(data_set_id IS NULL OR data_set_id = '')").
		Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("retiring rejected storage data set: %w", err)
	}
	rows, _ := res.RowsAffected()
	return rows == 1, nil
}

func (r *BunStorageContentRepo) CreateUploadCopiesForBindings(ctx context.Context, contentID int64, copies []UploadCopyBindingInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		for _, input := range copies {
			if input.ProviderID.IsZero() {
				return fmt.Errorf("providerID is required: %w", ErrInvalidInput)
			}
			binding := new(model.StorageDataSet)
			if err := db.NewSelect().
				Model(binding).
				Where("id = ?", input.StorageDataSetID).
				Scan(ctx); err != nil {
				return fmt.Errorf("loading storage data set for bound copy: %w", err)
			}
			if binding.CopyIndex != input.CopyIndex || binding.ProviderID.String() != input.ProviderID.String() {
				return fmt.Errorf("storage copy binding identity does not match data set: %w", ErrConflict)
			}
			var content struct {
				BucketID    int64 `bun:"bucket_id"`
				ContentSize int64 `bun:"content_size"`
			}
			if err := db.NewSelect().
				Model((*model.StorageContent)(nil)).
				Column("bucket_id", "content_size").
				Where("id = ?", contentID).
				Scan(ctx, &content); err != nil {
				return fmt.Errorf("loading storage content for bound copy: %w", err)
			}
			uploadBucketID := content.BucketID
			if binding.BucketID != uploadBucketID {
				return fmt.Errorf("storage copy upload and data set belong to different buckets: %w", ErrConflict)
			}
			copyRow := &model.StorageCopy{
				ContentID:        contentID,
				BucketID:         binding.BucketID,
				ContentSize:      content.ContentSize,
				CopyIndex:        input.CopyIndex,
				ProviderID:       binding.ProviderID,
				TransferMethod:   input.TransferMethod,
				Status:           model.StorageCopyStatusPending,
				StorageDataSetID: input.StorageDataSetID,
			}
			// Copies are unique per concrete data set so one upload can hold both
			// generations of a slot while a replacement migrates.
			if _, err := db.NewInsert().
				Model(copyRow).
				On("CONFLICT (content_id, storage_data_set_id) DO NOTHING").
				Exec(ctx); err != nil {
				return fmt.Errorf("creating storage upload copy row: %w", err)
			}
		}
		return nil
	})
}

// GetUploadCopy resolves a slot to the copy on its current generation, so a
// task that carries only (upload, slot) can never address a replaced or a
// not-yet-activated generation.
func (r *BunStorageContentRepo) GetUploadCopy(ctx context.Context, contentID int64, copyIndex int) (*model.StorageCopy, error) {
	copyRow := new(model.StorageCopy)
	q := r.db.NewSelect().Model(copyRow)
	projectActiveCommitAttempt(q, "storage_copy")
	err := q.
		Where("storage_copy.content_id = ? AND storage_copy.copy_index = ?", contentID, copyIndex).
		Where(currentGenerationCopySQL("storage_copy")).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage upload copy: %w", err)
	}
	return copyRow, nil
}

// GetUploadCopyForDataSet addresses one concrete generation, which is how
// replacement work targets the new provider while the old one still exists.
func (r *BunStorageContentRepo) GetUploadCopyForDataSet(ctx context.Context, contentID, storageDataSetID int64) (*model.StorageCopy, error) {
	if contentID <= 0 || storageDataSetID <= 0 {
		return nil, fmt.Errorf("selecting storage upload copy for data set: %w", ErrInvalidInput)
	}
	copyRow := new(model.StorageCopy)
	q := r.db.NewSelect().Model(copyRow)
	projectActiveCommitAttempt(q, "storage_copy")
	err := q.
		Where("storage_copy.content_id = ? AND storage_copy.storage_data_set_id = ?", contentID, storageDataSetID).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage upload copy for data set: %w", err)
	}
	return copyRow, nil
}

func (r *BunStorageContentRepo) GetUploadCopyByID(ctx context.Context, id int64) (*model.StorageCopy, error) {
	copyRow := new(model.StorageCopy)
	q := r.db.NewSelect().Model(copyRow)
	projectActiveCommitAttempt(q, "storage_copy")
	err := q.Where("storage_copy.id = ?", id).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting storage upload copy by id: %w", err)
	}
	return copyRow, nil
}

func selectLiveObjectVersionForStorageContent(
	ctx context.Context,
	db bun.IDB,
	upload *model.StorageContent,
	excludedVersionIDs []string,
) (*model.ObjectVersion, error) {
	if upload == nil || upload.ID <= 0 {
		return nil, fmt.Errorf("selecting live storage upload version: %w", ErrInvalidInput)
	}
	version := new(model.ObjectVersion)
	q := db.NewSelect().
		Model(version).
		Where("is_delete_marker = ?", false).
		Where("content_id = ?", upload.ID)
	if len(excludedVersionIDs) > 0 {
		q = q.Where("version_id NOT IN (?)", bun.List(excludedVersionIDs))
	}
	// Cache residency is a property of the content, not of any one version, so
	// every candidate here shares it and it cannot break the tie.
	err := q.
		OrderExpr("CASE WHEN EXISTS (SELECT 1 FROM objects AS pointer WHERE pointer.id = object_version.object_id AND pointer.current_version_id = object_version.version_id) THEN 0 ELSE 1 END ASC").
		OrderExpr("created_at DESC").
		OrderExpr("version_id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting live storage upload version: %w", err)
	}
	return version, nil
}

func (r *BunStorageContentRepo) ListIncompleteCopiesForDataSet(ctx context.Context, storageDataSetID int64) ([]model.StorageCopy, error) {
	if storageDataSetID < 1 {
		return nil, ErrInvalidInput
	}
	var copies []model.StorageCopy
	q := r.db.NewSelect().Model(&copies)
	projectActiveCommitAttempt(q, "storage_copy")
	err := q.
		Join("JOIN storage_contents AS storage_content ON storage_content.id = storage_copy.content_id").
		Where("storage_copy.storage_data_set_id = ?", storageDataSetID).
		Where("storage_copy.status IN (?)", bun.List([]model.StorageCopyStatus{
			model.StorageCopyStatusPending,
			model.StorageCopyStatusPieceReady,
			model.StorageCopyStatusCommitting,
		})).
		Where(`EXISTS (
			SELECT 1 FROM object_versions AS pending_version
			WHERE pending_version.is_delete_marker = ?
			  AND `+objectVersionReferencesStorageContentSQL("pending_version", "storage_content")+`
		)`, false).
		OrderExpr("storage_copy.id ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing incomplete data set copies: %w", err)
	}
	return copies, nil
}

func (r *BunStorageContentRepo) NextFinalizableCopyForDataSet(ctx context.Context, storageDataSetID int64) (*model.StorageCopy, error) {
	copyRow := new(model.StorageCopy)
	query := fmt.Sprintf(`SELECT storage_copy.*
		FROM storage_copies AS storage_copy
		JOIN storage_contents AS storage_content ON storage_content.id = storage_copy.content_id
		WHERE storage_copy.storage_data_set_id = ?
		  AND storage_copy.status = ?
		  AND storage_content.accepted_at IS NULL
		  AND storage_content.requested_copies > 0
		  AND EXISTS (
			SELECT 1 FROM object_versions AS pending_version
			WHERE pending_version.content_id = storage_content.id
			  AND pending_version.is_delete_marker = FALSE
		  )
		  AND (
			SELECT COUNT(DISTINCT readable_data_set.copy_index)
			FROM storage_copies AS readable_copy
			JOIN storage_data_sets AS readable_data_set ON readable_data_set.id = readable_copy.storage_data_set_id
			WHERE readable_copy.content_id = storage_content.id
			  AND %s
		  ) >= storage_content.requested_copies
		ORDER BY storage_copy.id ASC
		LIMIT 1`, readableCommittedCopyPredicateWithDataSetStatusSQL(
		"readable_copy",
		"readable_data_set",
		fmt.Sprintf("(readable_data_set.status IN (%s) OR readable_data_set.id = ?)",
			storageHealthReadyDataSetStatusListSQL()),
	))
	err := r.db.NewRaw(
		query,
		storageDataSetID,
		model.StorageCopyStatusCommitted,
		storageDataSetID,
	).Scan(ctx, copyRow)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting next finalizable data set copy: %w", err)
	}
	return copyRow, nil
}

func (r *BunStorageContentRepo) ListIncompleteReadableUploads(
	ctx context.Context,
	afterID int64,
	limit int,
) ([]IncompleteReadableUpload, error) {
	var uploads []model.StorageContent
	q := r.db.NewSelect().
		Model(&uploads).
		Where("accepted_at IS NULL").
		Where("id > ?", afterID).
		Where(`EXISTS (
			SELECT 1 FROM object_versions AS live_version
			WHERE live_version.is_delete_marker = ?
			  AND live_version.state = ?
			  AND `+objectVersionReferencesStorageContentSQL("live_version", "storage_content")+`
		)`, false, model.ObjectStateStored).
		OrderExpr("id ASC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing incomplete readable storage uploads: %w", err)
	}

	items := make([]IncompleteReadableUpload, 0, len(uploads))
	for i := range uploads {
		version := new(model.ObjectVersion)
		err := r.db.NewSelect().
			Model(version).
			Where("is_delete_marker = ?", false).
			Where("state = ?", model.ObjectStateStored).
			Where("content_id = ?", uploads[i].ID).
			OrderExpr("in_cache DESC").
			OrderExpr("CASE WHEN EXISTS (SELECT 1 FROM objects AS pointer WHERE pointer.id = object_version.object_id AND pointer.current_version_id = object_version.version_id) THEN 0 ELSE 1 END ASC").
			OrderExpr("created_at DESC").
			OrderExpr("version_id DESC").
			Limit(1).
			Scan(ctx)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, fmt.Errorf("selecting durable version for incomplete readable upload %d: %w", uploads[i].ID, err)
		}
		items = append(items, IncompleteReadableUpload{
			Upload:  uploads[i],
			Version: *version,
		})
	}
	return items, nil
}

func (r *BunStorageContentRepo) MarkUploadCopyPieceReady(ctx context.Context, input MarkUploadCopyPieceReadyInput) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockStorageContentForCopyMutation(ctx, db, input.ContentID); err != nil {
			return fmt.Errorf("locking storage upload for piece-ready copy: %w", err)
		}
		copyID, err := slotCopyTarget(ctx, db, input.StorageCopyID, input.ContentID, input.CopyIndex)
		if err != nil {
			return err
		}
		now := time.Now()
		q := db.NewUpdate().
			Model((*model.StorageCopy)(nil)).
			Set("status = ?", model.StorageCopyStatusPieceReady).
			Set("piece_id = COALESCE(?, piece_id)", input.PieceID).
			Set("retrieval_url = COALESCE(?, retrieval_url)", nullableString(input.RetrievalURL)).
			Set("commit_extra_data_hex = COALESCE(?, commit_extra_data_hex)", nullableString(input.CommitExtraDataHex)).
			Set("last_error = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", copyID).
			Where("status NOT IN (?, ?)", model.StorageCopyStatusCommitted, model.StorageCopyStatusCommitting)
		if input.PieceCID != "" {
			q = q.Where(`EXISTS (
				SELECT 1 FROM storage_contents AS evidence_upload
				WHERE evidence_upload.id = ?
				  AND (evidence_upload.piece_cid IS NULL OR evidence_upload.piece_cid = '' OR evidence_upload.piece_cid = ?)
			)`, input.ContentID, input.PieceCID)
		}
		if input.PieceID != nil {
			q = q.Where("(piece_id IS NULL OR piece_id = ?)", input.PieceID)
		}
		if input.RetrievalURL != "" {
			q = q.Where("(retrieval_url IS NULL OR retrieval_url = '' OR retrieval_url = ?)", input.RetrievalURL)
		}
		if input.CommitExtraDataHex != "" {
			q = q.Where("(commit_extra_data_hex IS NULL OR commit_extra_data_hex = '' OR commit_extra_data_hex = ?)", input.CommitExtraDataHex)
		}
		if input.RequireEligibleCopy {
			q = q.
				Where("status <> ?", model.StorageCopyStatusFailed).
				Where(liveObjectVersionExistsForUploadSQL(), input.ContentID, false)
		}
		res, err := q.Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload copy piece ready: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			var status model.StorageCopyStatus
			if err := db.NewSelect().Model((*model.StorageCopy)(nil)).
				Column("status").Where("id = ?", copyID).Scan(ctx, &status); err != nil {
				return fmt.Errorf("loading storage upload copy after piece evidence conflict: %w", err)
			}
			if status == model.StorageCopyStatusCommitting {
				compatible := db.NewSelect().
					Model((*model.StorageCopy)(nil)).
					Where("id = ?", copyID).
					Where("status = ?", model.StorageCopyStatusCommitting).
					Where(attemptedStorageCommitSQL("storage_copy"))
				if input.PieceCID != "" {
					compatible = compatible.Where(`EXISTS (
						SELECT 1 FROM storage_contents AS evidence_upload
						WHERE evidence_upload.id = ?
						  AND evidence_upload.piece_cid = ?
					)`, input.ContentID, input.PieceCID)
				}
				if input.PieceID != nil {
					compatible = compatible.Where("piece_id = ?", input.PieceID)
				}
				if input.RetrievalURL != "" {
					compatible = compatible.Where("retrieval_url = ?", input.RetrievalURL)
				}
				if input.CommitExtraDataHex != "" {
					compatible = compatible.Where("commit_extra_data_hex = ?", input.CommitExtraDataHex)
				}
				count, err := compatible.Count(ctx)
				if err != nil {
					return fmt.Errorf("checking idempotent piece evidence for committing copy: %w", err)
				}
				if count == 1 {
					return nil
				}
			}
			// A late piece-ready result cannot regress a committed copy. Treat that
			// stale observation as a harmless no-op; every other zero-row result is
			// conflicting monotonic evidence and must stop before Commit.
			if status == model.StorageCopyStatusCommitted && !input.RequireEligibleCopy {
				return nil
			}
			return fmt.Errorf("marking storage upload copy piece ready: %w", ErrConflict)
		}
		transferMethod, err := uploadCopyTransferMethod(ctx, db, input.ContentID, input.CopyIndex)
		if err != nil {
			return err
		}
		if transferMethod == model.StorageCopyTransferMethodIngress {
			if err := updateUploadIngressReady(ctx, db, input.ContentID, input.PieceCID, now); err != nil {
				return err
			}
		} else {
			if _, err := db.NewUpdate().
				Model((*model.StorageContent)(nil)).
				Set("piece_cid = COALESCE(?, piece_cid)", nullableString(input.PieceCID)).
				Set("updated_at = ?", now).
				Where("id = ?", input.ContentID).
				Exec(ctx); err != nil {
				return fmt.Errorf("recording storage upload piece CID: %w", err)
			}
		}
		// A pull that produced this piece is finished. Resolving it here, in the
		// same transaction, is what frees the copy's unresolved slot so a later
		// retry can ask a different source.
		if input.PullAttemptID != "" {
			if err := resolvePullAttempt(ctx, db, input.PullAttemptID, now); err != nil {
				return err
			}
		}
		return nil
	})
}

// resolvePullAttempt marks one request finished without changing its status: a
// resolved "attempted" row is the record of a request that succeeded.
func resolvePullAttempt(ctx context.Context, db bun.IDB, attemptID string, now time.Time) error {
	res, err := db.NewUpdate().
		Model((*storagepull.Attempt)(nil)).
		Set("resolved_at = COALESCE(resolved_at, ?)", now).
		Set("updated_at = ?", now).
		Where("attempt_id = ? AND status = ?", attemptID, storagepull.AttemptStatusAttempted).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("resolving storage pull attempt: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return fmt.Errorf("resolving storage pull attempt %s: %w", attemptID, ErrConflict)
	}
	return nil
}

func (r *BunStorageContentRepo) MarkUploadCopyCommitted(ctx context.Context, input MarkUploadCopyCommittedInput) error {
	if input.ContentID <= 0 || input.CopyIndex < 0 || input.PieceCID == "" || input.PieceID == nil || input.RetrievalURL == "" ||
		input.CommitAttemptID == "" || input.StorageCopyID <= 0 || input.CommitExtraDataHex == "" ||
		input.CommitTransactionID == "" || input.CommitConfirmedTransactionID == "" {
		return fmt.Errorf("marking storage upload copy committed: %w", ErrInvalidInput)
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		initial := new(model.StorageCopy)
		if err := db.NewSelect().
			Model(initial).
			Where("id = ?", input.StorageCopyID).
			Where("content_id = ?", input.ContentID).
			Where("copy_index = ?", input.CopyIndex).
			Scan(ctx); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("loading storage commit copy: %w", ErrNotFound)
			}
			return fmt.Errorf("loading storage commit copy: %w", err)
		}
		copyID, _, err := lockCommitCopyFamily(ctx, db, storagecommit.CopyIdentity{
			StorageCopyID:    input.StorageCopyID,
			ContentID:        input.ContentID,
			CopyIndex:        input.CopyIndex,
			StorageDataSetID: initial.StorageDataSetID,
			// Settling an attempted commit deliberately skips the owner
			// eligibility check: the piece already reached the provider, so a
			// deleted owner must not stop the copy from being recorded.
			RequireEligibleCopy: false,
		})
		if err != nil {
			return err
		}
		now := time.Now()
		{
			copyIdentity := new(model.StorageCopy)
			if err := db.NewSelect().
				Model(copyIdentity).
				Where("id = ?", copyID).
				Scan(ctx); err != nil {
				return fmt.Errorf("loading confirmed storage commit identity: %w", err)
			}
			identity := storagecommit.CopyIdentity{
				StorageCopyID:    copyID,
				ContentID:        input.ContentID,
				CopyIndex:        input.CopyIndex,
				StorageDataSetID: copyIdentity.StorageDataSetID,
			}
			attempt, err := loadCommitAttempt(ctx, db, identity, input.CommitAttemptID)
			if err != nil {
				return err
			}
			if attempt.Status == storagecommit.AttemptStatusConfirmed {
				if attempt.ResolvedAt == nil || derefString(attempt.ExtraDataHex) != input.CommitExtraDataHex ||
					derefString(attempt.TransactionID) != input.CommitTransactionID ||
					derefString(attempt.ConfirmedTransactionID) != input.CommitConfirmedTransactionID {
					return fmt.Errorf("replaying confirmed storage commit with conflicting evidence: %w", ErrConflict)
				}
				matchingCopyCount, countErr := db.NewSelect().
					Model((*model.StorageCopy)(nil)).
					Where("id = ?", copyID).
					Where("status = ?", model.StorageCopyStatusCommitted).
					Where("piece_id = ?", input.PieceID).
					Where("retrieval_url = ?", input.RetrievalURL).
					Where("commit_extra_data_hex = ?", input.CommitExtraDataHex).
					Where(`EXISTS (
						SELECT 1 FROM storage_contents AS evidence_upload
						WHERE evidence_upload.id = ? AND evidence_upload.piece_cid = ?
					)`, input.ContentID, input.PieceCID).
					Count(ctx)
				if countErr != nil {
					return fmt.Errorf("checking confirmed storage commit projection: %w", countErr)
				}
				if matchingCopyCount != 1 {
					return fmt.Errorf("replaying confirmed storage commit with conflicting projection: %w", ErrConflict)
				}
				return updateUploadReadable(ctx, db, input.ContentID, input.PieceCID, now)
			}
			if attempt.Status != storagecommit.AttemptStatusAttempted || attempt.ResolvedAt != nil {
				return fmt.Errorf("confirming resolved storage commit attempt: %w", ErrConflict)
			}
			attemptQuery := db.NewUpdate().
				Model((*storagecommit.Attempt)(nil)).
				Set("status = ?", storagecommit.AttemptStatusConfirmed).
				Set("transaction_id = COALESCE(transaction_id, ?)", input.CommitTransactionID).
				Set("confirmed_transaction_id = ?", input.CommitConfirmedTransactionID).
				Set("resolved_at = ?", now).
				Set("updated_at = ?", now).
				Where("attempt_id = ?", input.CommitAttemptID).
				Where("content_id = ? AND storage_data_set_id = ?", input.ContentID, copyIdentity.StorageDataSetID).
				Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusAttempted).
				Where("(transaction_id IS NULL OR transaction_id = ?)", input.CommitTransactionID)
			if input.CommitExtraDataHex != "" {
				attemptQuery = attemptQuery.Where("extra_data_hex = ?", input.CommitExtraDataHex)
			}
			res, err := attemptQuery.Exec(ctx)
			if err != nil {
				return fmt.Errorf("confirming storage commit attempt: %w", err)
			}
			if rows, _ := res.RowsAffected(); rows != 1 {
				return fmt.Errorf("confirming storage commit attempt: %w", ErrConflict)
			}
		}
		// The attempt was set to confirmed just above, so the projection can
		// name it; the composite foreign key refuses any other status.
		q := db.NewUpdate().
			Model((*model.StorageCopy)(nil)).
			Set("status = ?", model.StorageCopyStatusCommitted).
			Set("piece_id = COALESCE(?, piece_id)", input.PieceID).
			Set("retrieval_url = COALESCE(?, retrieval_url)", nullableString(input.RetrievalURL)).
			Set("commit_extra_data_hex = COALESCE(?, commit_extra_data_hex)", nullableString(input.CommitExtraDataHex)).
			Set("confirmed_attempt_id = ?", input.CommitAttemptID).
			Set("confirmed_attempt_status = ?", string(storagecommit.AttemptStatusConfirmed)).
			Set("last_error = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", copyID)
		q = q.Where(`EXISTS (
			SELECT 1 FROM storage_contents AS evidence_upload
			WHERE evidence_upload.id = ?
			  AND (evidence_upload.piece_cid IS NULL OR evidence_upload.piece_cid = '' OR evidence_upload.piece_cid = ?)
		)`, input.ContentID, input.PieceCID)
		if input.PieceID != nil {
			q = q.Where("(piece_id IS NULL OR piece_id = ?)", input.PieceID)
		}
		if input.RetrievalURL != "" {
			q = q.Where("(retrieval_url IS NULL OR retrieval_url = '' OR retrieval_url = ?)", input.RetrievalURL)
		}
		if input.CommitExtraDataHex != "" {
			q = q.Where("(commit_extra_data_hex IS NULL OR commit_extra_data_hex = '' OR commit_extra_data_hex = ?)", input.CommitExtraDataHex)
		}
		q = q.
			Set("commit_ready_at = NULL").
			Where("status = ?", model.StorageCopyStatusCommitting)
		res, err := q.Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload copy committed: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			if input.RequireEligibleCopy {
				return fmt.Errorf("marking storage upload copy committed: %w", ErrConflict)
			}
			return fmt.Errorf("marking storage upload copy committed: %w", ErrNotFound)
		}
		if err := updateUploadReadable(ctx, db, input.ContentID, input.PieceCID, now); err != nil {
			return err
		}
		return nil
	})
}

func liveObjectVersionExistsForUploadSQL() string {
	return `EXISTS (
		SELECT 1
		FROM storage_contents AS guarded_content
		JOIN object_versions AS live_version
		  ON ` + objectVersionReferencesStorageContentSQL("live_version", "guarded_content") + `
		WHERE guarded_content.id = ?
		  AND live_version.is_delete_marker = ?
	)`
}

func (r *BunStorageContentRepo) MarkUploadCopyFailed(ctx context.Context, input MarkUploadCopyFailedInput) error {
	contentID, copyIndex, lastError := input.ContentID, input.CopyIndex, input.LastError
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockStorageContentForCopyMutation(ctx, db, contentID); err != nil {
			return fmt.Errorf("locking storage upload for failed copy: %w", err)
		}
		copyID, err := slotCopyTarget(ctx, db, input.StorageCopyID, contentID, copyIndex)
		if err != nil {
			return err
		}
		now := time.Now()
		if input.PullAttemptID != "" {
			result, err := db.NewUpdate().
				Model((*storagepull.Attempt)(nil)).
				Set("status = ?", storagepull.AttemptStatusAbandoned).
				Set("last_error = ?", lastError).
				Set("resolved_at = ?", now).
				Set("updated_at = ?", now).
				Where("attempt_id = ?", input.PullAttemptID).
				Where("content_id = ?", contentID).
				Where(`storage_data_set_id = (
					SELECT storage_data_set_id FROM storage_copies WHERE id = ?
				)`, copyID).
				Where("status = ? AND resolved_at IS NULL", storagepull.AttemptStatusAttempted).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("abandoning failed storage pull: %w", err)
			}
			rows, _ := result.RowsAffected()
			if rows == 0 {
				count, err := db.NewSelect().
					Model((*storagepull.Attempt)(nil)).
					Where("attempt_id = ?", input.PullAttemptID).
					Where("content_id = ?", contentID).
					Where(`storage_data_set_id = (
						SELECT storage_data_set_id FROM storage_copies WHERE id = ?
					)`, copyID).
					Where("status = ? AND resolved_at IS NOT NULL", storagepull.AttemptStatusAbandoned).
					Count(ctx)
				if err != nil {
					return fmt.Errorf("checking abandoned storage pull: %w", err)
				}
				if count != 1 {
					return fmt.Errorf("abandoning failed storage pull: %w", ErrConflict)
				}
			}
		}
		if _, err := db.NewUpdate().
			Model((*storagecommit.Attempt)(nil)).
			Set("status = ?", storagecommit.AttemptStatusReleased).
			Set("release_reason = ?", string(storagecommit.ReleaseOwnerTerminal)).
			Set("resolved_at = ?", now).
			Set("updated_at = ?", now).
			Where("content_id = ?", contentID).
			Where(`storage_data_set_id = (
				SELECT storage_data_set_id FROM storage_copies WHERE id = ?
			)`, copyID).
			Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusReserved).
			Exec(ctx); err != nil {
			return fmt.Errorf("releasing failed storage copy reservation: %w", err)
		}
		res, err := db.NewUpdate().
			Model((*model.StorageCopy)(nil)).
			Set("status = ?", model.StorageCopyStatusFailed).
			Set("commit_ready_at = NULL").
			Set("commit_extra_data_hex = NULL").
			Set("last_error = ?", lastError).
			Set("updated_at = ?", now).
			Where("id = ?", copyID).
			Where("status <> ?", model.StorageCopyStatusCommitted).
			Where(`NOT EXISTS (
				SELECT 1 FROM storage_commit_attempts AS unresolved_attempt
				WHERE unresolved_attempt.content_id = storage_copy.content_id
				  AND unresolved_attempt.storage_data_set_id = storage_copy.storage_data_set_id
				  AND unresolved_attempt.resolved_at IS NULL
			)`).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload copy failed: %w", err)
		}
		rows, rowsErr := res.RowsAffected()
		if rowsErr != nil {
			return fmt.Errorf("marking storage upload copy failed: reading affected rows: %w", rowsErr)
		}
		if rows == 0 {
			submittedCount, countErr := db.NewSelect().
				Model((*model.StorageCopy)(nil)).
				Where("id = ?", copyID).
				Where(attemptedStorageCommitSQL("storage_copy")).
				Count(ctx)
			if countErr != nil {
				return fmt.Errorf("checking submitted storage commit before failure: %w", countErr)
			}
			if submittedCount > 0 {
				return fmt.Errorf("marking storage upload copy failed: submitted commit is still recoverable: %w", ErrConflict)
			}
			return nil
		}
		readableCount, err := countReadableReplicaSlots(ctx, db, contentID)
		if err != nil {
			return err
		}
		submittedCount, err := countSubmittedCommitCopies(ctx, db, contentID)
		if err != nil {
			return err
		}
		viableCount, err := db.NewSelect().
			Model((*model.StorageCopy)(nil)).
			Where("content_id = ?", contentID).
			Where("status <> ?", model.StorageCopyStatusFailed).
			Count(ctx)
		if err != nil {
			return fmt.Errorf("counting viable storage upload copies: %w", err)
		}
		if readableCount == 0 && submittedCount == 0 && viableCount == 0 {
			_, err = db.NewUpdate().
				Model((*model.StorageContent)(nil)).
				Set("error_message = ?", lastError).
				Set("updated_at = ?", now).
				Where("id = ?", contentID).
				Where("accepted_at IS NULL").
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("marking storage upload failed: %w", err)
			}
		} else if readableCount > 0 {
			_, err = db.NewUpdate().
				Model((*model.StorageContent)(nil)).
				Set("error_message = ?", lastError).
				Set("updated_at = ?", now).
				Where("id = ?", contentID).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("marking storage upload readable after copy failure: %w", err)
			}
		}
		return nil
	})
}

func (r *BunStorageContentRepo) ReopenFailedUploadCopy(ctx context.Context, copyID int64) error {
	if copyID < 1 {
		return ErrInvalidInput
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		return reopenFailedUploadCopy(ctx, db, copyID)
	})
}

func reopenFailedUploadCopy(ctx context.Context, db bun.IDB, copyID int64) error {
	now := time.Now()
	result, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("status = ?", model.StorageCopyStatusPending).
		Set("piece_id = NULL").
		Set("retrieval_url = NULL").
		Set("commit_extra_data_hex = NULL").
		Set("commit_ready_at = NULL").
		Set("confirmed_attempt_id = NULL").
		Set("confirmed_attempt_status = NULL").
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", copyID).
		Where("status = ?", model.StorageCopyStatusFailed).
		Where("active_task_id IS NULL").
		Where(`NOT EXISTS (
			SELECT 1 FROM storage_commit_attempts AS unresolved_attempt
			WHERE unresolved_attempt.content_id = storage_copy.content_id
			  AND unresolved_attempt.storage_data_set_id = storage_copy.storage_data_set_id
			  AND unresolved_attempt.resolved_at IS NULL
		)`).
		Exec(ctx)
	if err := requireRows(result, err, "reopening failed storage upload copy"); err != nil {
		return err
	}
	// The next attempt may choose a different source, so the request this copy
	// already sent is abandoned rather than reused. Its unresolved slot has to
	// be freed inside this transaction or the next reservation is refused.
	if _, err := db.NewUpdate().
		Model((*storagepull.Attempt)(nil)).
		Set("status = ?", storagepull.AttemptStatusAbandoned).
		Set("resolved_at = ?", now).
		Set("updated_at = ?", now).
		Where("resolved_at IS NULL").
		Where(`(content_id, storage_data_set_id) IN (
			SELECT reopened_copy.content_id, reopened_copy.storage_data_set_id
			FROM storage_copies AS reopened_copy WHERE reopened_copy.id = ?
		)`, copyID).
		Exec(ctx); err != nil {
		return fmt.Errorf("abandoning storage pull attempt: %w", err)
	}
	return nil
}

func countSubmittedCommitCopies(ctx context.Context, db bun.IDB, contentID int64) (int, error) {
	count, err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Where("content_id = ?", contentID).
		Where(attemptedStorageCommitSQL("storage_copy")).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting submitted storage commits: %w", err)
	}
	return count, nil
}

// selectObjectVersionRefsForContent lists the live data versions backed by one
// content. Pipeline position is derived from the copies now, so callers that
// used to rewrite object_versions here only need to know which versions the
// change reached.
func selectObjectVersionRefsForContent(ctx context.Context, db bun.IDB, contentID int64, versionID string) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	q := db.NewSelect().
		Table("object_versions").
		Column("object_id", "version_id", "content_id").
		Where("content_id = ?", contentID).
		Where("is_delete_marker = ?", false)
	if versionID != "" {
		q = q.Where("version_id = ?", versionID)
	}
	if err := q.Scan(ctx, &refs); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return refs, nil
}

// BindReadableUploadForContent records that one content became readable at a
// provider. Versions already point at the content from creation, so this only
// settles the content row and reports the versions it backs.
func (r *BunStorageContentRepo) BindReadableUploadForContent(ctx context.Context, input BindReadableUploadInput) ([]ObjectVersionRef, error) {
	return r.bindReadableContent(ctx, input.ContentID, input.BucketID, "")
}

// BindReadableUploadForVersion is BindReadableUploadForContent scoped to the
// refs of a single version.
func (r *BunStorageContentRepo) BindReadableUploadForVersion(ctx context.Context, input BindReadableUploadForVersionInput) ([]ObjectVersionRef, error) {
	return r.bindReadableContent(ctx, input.ContentID, input.BucketID, input.VersionID)
}

func (r *BunStorageContentRepo) bindReadableContent(ctx context.Context, contentID, bucketID int64, versionID string) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		content, err := lockStorageContentForObjectState(ctx, db, contentID, model.ObjectStateReplicating)
		if err != nil {
			return fmt.Errorf("locking storage content for readable bind: %w", err)
		}
		if content.BucketID != bucketID {
			return fmt.Errorf("storage content %d belongs to bucket %d, not %d: %w", content.ID, content.BucketID, bucketID, ErrConflict)
		}
		if err := updateUploadReadable(ctx, db, contentID, derefString(content.PieceCID), time.Now()); err != nil {
			return err
		}
		refs, err = selectObjectVersionRefsForContent(ctx, db, contentID, versionID)
		if err != nil {
			return fmt.Errorf("listing object versions for readable content: %w", err)
		}
		return nil
	})
	return refs, err
}

func (r *BunStorageContentRepo) FinalizeUploadIfTargetCopiesMet(ctx context.Context, input FinalizeUploadInput) (bool, []ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	finalized := false
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		var bucketID int64
		if err := db.NewSelect().
			Model((*model.StorageContent)(nil)).
			Column("bucket_id").
			Where("id = ?", input.ContentID).
			Scan(ctx, &bucketID); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("loading storage upload %d: %w", input.ContentID, ErrNotFound)
			}
			return fmt.Errorf("loading storage upload bucket: %w", err)
		}
		bucket, err := lockBucketByID(ctx, db, bucketID)
		if err != nil {
			return err
		}
		if bucket == nil {
			return fmt.Errorf("storage upload %d bucket not found: %w", input.ContentID, ErrNotFound)
		}
		locked, err := lockStorageContentsByID(ctx, db, []int64{input.ContentID})
		if err != nil {
			return fmt.Errorf("locking storage upload for finalization: %w", err)
		}
		upload := locked[input.ContentID]
		if upload == nil {
			return fmt.Errorf("storage upload %d cannot be finalized: %w", input.ContentID, ErrConflict)
		}
		readable, err := countReadableReplicaSlots(ctx, db, input.ContentID)
		if err != nil {
			return err
		}
		minimum := minimumDurableCopiesForUpload(bucket, upload.RequestedCopies)
		if minimum <= 0 || readable < minimum {
			return nil
		}
		now := time.Now()
		// Reaching the durability minimum is a fact about the copies, so no
		// version row changes; the refs only tell callers who is affected.
		refs, err = selectObjectVersionRefsForContent(ctx, db, input.ContentID, "")
		if err != nil {
			return fmt.Errorf("listing durable object versions: %w", err)
		}
		if upload.RequestedCopies <= 0 || readable < upload.RequestedCopies {
			return nil
		}
		_, err = db.NewUpdate().
			Model((*model.StorageContent)(nil)).
			Set("accepted_at = COALESCE(accepted_at, ?)", now).
			Set("updated_at = ?", now).
			Where("id = ?", input.ContentID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage upload complete: %w", err)
		}
		finalized = true
		return nil
	})
	return finalized, refs, err
}

func minimumDurableCopiesForUpload(bucket *model.Bucket, requestedCopies int) int {
	if requestedCopies <= 0 {
		return 0
	}
	if bucket == nil || bucket.MinimumDurableCopies >= requestedCopies {
		return requestedCopies
	}
	return bucket.MinimumDurableCopies
}

func (r *BunStorageContentRepo) runMaybeTx(ctx context.Context, fn func(bun.IDB) error) error {
	return runMaybeTx(ctx, r.db, fn)
}

// Generations are numbered per replica slot and never reused, so a retired
// generation stays distinguishable from the one that replaced it.
func nextDataSetGeneration(ctx context.Context, db bun.IDB, bucketID int64, copyIndex int) (int64, error) {
	var generation int64
	err := db.NewRaw(
		`SELECT COALESCE(MAX(generation), 0) + 1 FROM storage_data_sets WHERE bucket_id = ? AND copy_index = ?`,
		bucketID, copyIndex,
	).Scan(ctx, &generation)
	if err != nil {
		return 0, fmt.Errorf("selecting next storage data set generation: %w", err)
	}
	return generation, nil
}

func ensureDataSetBinding(ctx context.Context, db bun.IDB, input EnsureDataSetBindingInput) (*model.StorageDataSet, error) {
	if input.BucketID == 0 || input.ProviderID.IsZero() || input.CopyIndex < 0 {
		return nil, fmt.Errorf("invalid storage data set binding input: %w", ErrInvalidInput)
	}
	// A provider remains reserved until its old data set is verifiably retired.
	// The current-slot rule is intentionally narrower so replacement can keep a
	// draining source and a current target in the same logical slot.
	existingByProvider := new(model.StorageDataSet)
	err := db.NewSelect().
		Model(existingByProvider).
		Where("bucket_id = ? AND provider_id = ? AND status <> ?", input.BucketID, input.ProviderID, model.StorageDataSetStatusRetired).
		Scan(ctx)
	if err == nil {
		if !existingByProvider.IsCurrent {
			return nil, fmt.Errorf("provider %s remains reserved by non-retired data set generation %d: %w", input.ProviderID, existingByProvider.ID, ErrAlreadyExists)
		}
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
		Where("bucket_id = ? AND copy_index = ? AND is_current", input.BucketID, input.CopyIndex).
		Scan(ctx)
	if err == nil {
		return nil, fmt.Errorf("copy_index %d already bound to provider %s: %w", input.CopyIndex, existingByIndex.ProviderID, ErrAlreadyExists)
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("selecting storage data set by copy index: %w", err)
	}
	generation, err := nextDataSetGeneration(ctx, db, input.BucketID, input.CopyIndex)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	binding := &model.StorageDataSet{
		BucketID:           input.BucketID,
		ProviderID:         input.ProviderID,
		CopyIndex:          input.CopyIndex,
		Generation:         generation,
		IsCurrent:          true,
		Status:             model.StorageDataSetStatusPending,
		CreatedByContentID: nullableInt64(input.CreatedByContentID),
		LastUsedContentID:  nullableInt64(input.CreatedByContentID),
		CreatedAt:          now,
		UpdatedAt:          now,
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
			Where("bucket_id = ? AND provider_id = ? AND status <> ?", input.BucketID, input.ProviderID, model.StorageDataSetStatusRetired).
			Scan(ctx)
		if selectErr == nil {
			if !existing.IsCurrent {
				return nil, fmt.Errorf("provider %s remains reserved by non-retired data set generation %d: %w", input.ProviderID, existing.ID, ErrAlreadyExists)
			}
			if existing.CopyIndex == input.CopyIndex {
				return existing, nil
			}
			return nil, fmt.Errorf("provider %s already bound to copy_index %d: %w", input.ProviderID, existing.CopyIndex, ErrAlreadyExists)
		}
		if selectErr != nil && selectErr != sql.ErrNoRows {
			return nil, fmt.Errorf("selecting storage data set after conflict: %w", selectErr)
		}
		return nil, fmt.Errorf("storage data set binding already exists: %w", ErrAlreadyExists)
	}
	return binding, nil
}

func markDataSetReady(ctx context.Context, db bun.IDB, id int64, contentID int64, dataSetID types.OnChainID, clientDataSetID *types.OnChainID) error {
	if dataSetID.IsZero() {
		return fmt.Errorf("dataSetID is required: %w", ErrInvalidInput)
	}
	res, err := db.NewUpdate().
		Model((*model.StorageDataSet)(nil)).
		Set("status = ?", model.StorageDataSetStatusReady).
		Set("data_set_id = ?", dataSetID).
		Set("client_data_set_id = COALESCE(?, client_data_set_id)", clientDataSetID).
		Set("last_used_content_id = ?", nullableInt64(contentID)).
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

// The result counts logical replica slots, so several data set generations of
// one slot never inflate an upload's durability.
func countReadableReplicaSlots(ctx context.Context, db bun.IDB, contentID int64) (int, error) {
	var row struct {
		Count int `bun:"count"`
	}
	err := db.NewRaw(fmt.Sprintf(`SELECT %s AS count`,
		distinctReadableSlotCountSQL("storage_copy", "storage_data_set", "?"),
	),
		contentID,
	).Scan(ctx, &row)
	if err != nil {
		return 0, fmt.Errorf("counting readable replica slots: %w", err)
	}
	return row.Count, nil
}

func requireReadableCommittedCopy(ctx context.Context, db bun.IDB, contentID int64) error {
	count, err := countReadableReplicaSlots(ctx, db, contentID)
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("storage upload %d has no readable committed copy: %w", contentID, ErrNotFound)
	}
	return nil
}

// Addressing a copy by slot alone became ambiguous once a slot can own several
// generations, so every write resolves to one concrete row first. Returning
// zero means the slot has no copy yet; an ambiguous slot is a conflict rather
// than a silent multi-row update.
// slotCopyTarget picks the concrete copy a mutation must touch. Zero means the
// slot has no copy, which every caller already handles as "no rows updated".
func slotCopyTarget(ctx context.Context, db bun.IDB, copyID, contentID int64, copyIndex int) (int64, error) {
	if copyID > 0 {
		return copyID, nil
	}
	return resolveSlotCopyID(ctx, db, contentID, copyIndex)
}

func resolveSlotCopyID(ctx context.Context, db bun.IDB, contentID int64, copyIndex int) (int64, error) {
	var ids []int64
	err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Column("id").
		Where("storage_copy.content_id = ? AND storage_copy.copy_index = ?", contentID, copyIndex).
		Where(currentGenerationCopySQL("storage_copy")).
		Scan(ctx, &ids)
	if err != nil {
		return 0, fmt.Errorf("resolving storage upload copy for slot: %w", err)
	}
	switch len(ids) {
	case 0:
		return 0, nil
	case 1:
		return ids[0], nil
	default:
		return 0, fmt.Errorf(
			"storage upload %d replica slot %d matches %d copies: %w",
			contentID, copyIndex, len(ids), ErrConflict,
		)
	}
}

func uploadCopyTransferMethod(ctx context.Context, db bun.IDB, contentID int64, copyIndex int) (model.StorageCopyTransferMethod, error) {
	var row struct {
		TransferMethod model.StorageCopyTransferMethod `bun:"transfer_method"`
	}
	err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Column("transfer_method").
		Where("storage_copy.content_id = ? AND storage_copy.copy_index = ?", contentID, copyIndex).
		Where(currentGenerationCopySQL("storage_copy")).
		Limit(1).
		Scan(ctx, &row)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("loading storage upload copy transfer method: %w", err)
	}
	return row.TransferMethod, nil
}

func updateUploadIngressReady(ctx context.Context, db bun.IDB, contentID int64, pieceCID string, now time.Time) error {
	_, err := db.NewUpdate().
		Model((*model.StorageContent)(nil)).
		Set("piece_cid = COALESCE(?, piece_cid)", nullableString(pieceCID)).
		Set("error_message = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", contentID).
		Where("accepted_at IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage upload ingress ready: %w", err)
	}
	return nil
}

func updateUploadReadable(ctx context.Context, db bun.IDB, contentID int64, pieceCID string, now time.Time) error {
	_, err := db.NewUpdate().
		Model((*model.StorageContent)(nil)).
		Set("piece_cid = COALESCE(?, piece_cid)", nullableString(pieceCID)).
		Set("error_message = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", contentID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking storage upload readable: %w", err)
	}
	return nil
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
