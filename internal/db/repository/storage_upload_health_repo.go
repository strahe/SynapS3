package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

func bucketStorageHealthDataSetCTE(db bun.IDB) string {
	return fmt.Sprintf(`bucket_data_sets AS (
		SELECT
			storage_data_set.id AS local_data_set_id,
			storage_data_set.bucket_id,
			storage_data_set.copy_index,
			storage_data_set.provider_id,
			storage_data_set.data_set_id,
			storage_data_set.client_data_set_id,
			storage_data_set.status AS local_status,
			observation.status AS observation_status,
			observation.local_data_set_id IS NULL AS observation_missing,
			observation.local_data_set_id IS NOT NULL AND observation.last_checked_at < ? AS observation_stale,
			COALESCE(observation.reason_codes, %s) AS reason_codes,
			observation.last_checked_at,
			observation.last_error
		FROM storage_data_sets AS storage_data_set
		LEFT JOIN observability_data_set_states AS observation ON observation.local_data_set_id = storage_data_set.id
		WHERE storage_data_set.bucket_id = ?
	),
	abnormal_data_sets AS (
		SELECT *
		FROM bucket_data_sets
		WHERE local_status NOT IN (%s)
		   OR observation_missing
		   OR observation_stale
		   OR observation_status IN (%s)
	)`, storageHealthEmptyJSONArraySQL(db), storageHealthReadyDataSetStatusListSQL(), storageHealthAbnormalObservationStatusListSQL())
}

type affectedVersionRow struct {
	VersionID       string             `bun:"version_id"`
	ObjectID        int64              `bun:"object_id"`
	BucketID        int64              `bun:"bucket_id"`
	Key             string             `bun:"key"`
	Size            int64              `bun:"size"`
	ETag            string             `bun:"e_tag"`
	Checksum        string             `bun:"checksum"`
	ContentType     string             `bun:"content_type"`
	CacheKey        string             `bun:"cache_key"`
	StorageUploadID *int64             `bun:"storage_upload_id"`
	InCache         bool               `bun:"in_cache"`
	IsCurrent       bool               `bun:"is_current"`
	State           model.ObjectState  `bun:"state"`
	FailedAtState   *model.ObjectState `bun:"failed_at_state"`
	LastError       *string            `bun:"last_error"`
	CreatedAt       time.Time          `bun:"created_at"`
	UpdatedAt       time.Time          `bun:"updated_at"`
}

type affectedVersionRiskDataSetRow struct {
	VersionID string `bun:"version_id"`
	BucketStorageHealthRiskDataSet
}

type affectedVersionReadableCountRow struct {
	VersionID string `bun:"version_id"`
	Count     int    `bun:"readable_alternative_count"`
}

func (r *BunStorageUploadRepo) ListBucketStorageHealthAffectedVersions(ctx context.Context, input BucketStorageHealthAffectedVersionsInput) (BucketStorageHealthAffectedVersionPage, error) {
	if input.BucketID <= 0 {
		return BucketStorageHealthAffectedVersionPage{}, fmt.Errorf("bucket_id is required: %w", ErrInvalidInput)
	}
	if err := validateBucketStorageHealthAffectedVersionMarkers(input); err != nil {
		return BucketStorageHealthAffectedVersionPage{}, err
	}
	limit := input.Limit
	if limit < 1 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}

	rows, err := r.listBucketStorageHealthAffectedVersionRows(ctx, input, limit+1)
	if err != nil {
		return BucketStorageHealthAffectedVersionPage{}, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	versionIDs := make([]string, 0, len(rows))
	out := make([]BucketStorageHealthAffectedVersion, 0, len(rows))
	for _, row := range rows {
		versionIDs = append(versionIDs, row.VersionID)
		out = append(out, BucketStorageHealthAffectedVersion{
			Version: affectedVersionRowObjectVersion(row),
		})
	}
	riskDataSets, err := r.listBucketStorageHealthRiskDataSetsForVersions(ctx, input, versionIDs)
	if err != nil {
		return BucketStorageHealthAffectedVersionPage{}, err
	}
	readableCounts, err := r.listBucketStorageHealthReadableAlternativeCounts(ctx, input, versionIDs)
	if err != nil {
		return BucketStorageHealthAffectedVersionPage{}, err
	}
	for i := range out {
		out[i].RiskDataSets = riskDataSets[out[i].Version.VersionID]
		out[i].ReadableAlternativeCount = readableCounts[out[i].Version.VersionID]
	}

	page := BucketStorageHealthAffectedVersionPage{
		Versions: out,
		HasMore:  hasMore,
	}
	if hasMore && len(out) > 0 {
		last := out[len(out)-1].Version
		page.NextKeyMarker = last.Key
		page.NextVersionIDMarker = last.VersionID
		page.NextCreatedAtMarker = last.CreatedAt
	}
	return page, nil
}

func (r *BunStorageUploadRepo) listBucketStorageHealthAffectedVersionRows(ctx context.Context, input BucketStorageHealthAffectedVersionsInput, limit int) ([]affectedVersionRow, error) {
	filters, args := bucketStorageHealthAffectedVersionObjectFilters(r.db, input)
	args = append([]interface{}{input.StaleBefore, input.BucketID, input.BucketID}, args...)
	dataSetFilter, dataSetArgs := bucketStorageHealthAffectedVersionDataSetFilter(input)
	args = append(args, dataSetArgs...)
	args = append(args, limit)
	keyOrder := keyOrderExpr(r.db, "object_version.key")
	query := fmt.Sprintf(`WITH %s
		SELECT
			object_version.version_id,
			object_version.object_id,
			object_version.bucket_id,
			object_version.key,
			object_version.size,
			object_version.e_tag,
			object_version.checksum,
			object_version.content_type,
			object_version.cache_key,
			object_version.storage_upload_id,
			object_version.in_cache,
			object_version.is_current,
			object_version.state,
			object_version.failed_at_state,
			object_version.last_error,
			object_version.created_at,
			object_version.updated_at
		FROM object_versions AS object_version
		WHERE object_version.bucket_id = ?
		  AND object_version.is_delete_marker = FALSE
%s
		  AND EXISTS (
				SELECT 1
				FROM storage_upload_copies AS storage_copy
				JOIN abnormal_data_sets AS abnormal_data_set
				  ON abnormal_data_set.local_data_set_id = storage_copy.storage_data_set_id
				WHERE storage_copy.upload_id = object_version.storage_upload_id
				  AND storage_copy.status = %s
%s
		  )
		ORDER BY %s ASC, object_version.created_at DESC, object_version.version_id DESC
		LIMIT ?`, bucketStorageHealthDataSetCTE(r.db), filters, storageHealthCommittedCopyStatusSQL(), dataSetFilter, keyOrder)
	var rows []affectedVersionRow
	if err := r.db.NewRaw(query, args...).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing bucket storage health affected versions: %w", err)
	}
	return rows, nil
}

func (r *BunStorageUploadRepo) listBucketStorageHealthRiskDataSetsForVersions(ctx context.Context, input BucketStorageHealthAffectedVersionsInput, versionIDs []string) (map[string][]BucketStorageHealthRiskDataSet, error) {
	out := make(map[string][]BucketStorageHealthRiskDataSet, len(versionIDs))
	if len(versionIDs) == 0 {
		return out, nil
	}
	dataSetFilter := ""
	args := []interface{}{input.StaleBefore, input.BucketID, input.BucketID, bun.List(versionIDs)}
	if input.LocalDataSetID > 0 {
		dataSetFilter = `
		  AND abnormal_data_set.local_data_set_id = ?`
		args = append(args, input.LocalDataSetID)
	}
	query := fmt.Sprintf(`WITH %s
		SELECT DISTINCT
			object_version.version_id,
			abnormal_data_set.local_data_set_id,
			abnormal_data_set.bucket_id,
			abnormal_data_set.copy_index,
			abnormal_data_set.provider_id,
			abnormal_data_set.data_set_id,
			abnormal_data_set.client_data_set_id,
			abnormal_data_set.local_status,
			abnormal_data_set.observation_status,
			abnormal_data_set.observation_missing,
			abnormal_data_set.observation_stale,
			abnormal_data_set.reason_codes,
			abnormal_data_set.last_checked_at,
			abnormal_data_set.last_error
		FROM abnormal_data_sets AS abnormal_data_set
		JOIN storage_upload_copies AS storage_copy ON storage_copy.storage_data_set_id = abnormal_data_set.local_data_set_id
		JOIN object_versions AS object_version
		  ON object_version.storage_upload_id = storage_copy.upload_id
		 AND object_version.bucket_id = ?
		 AND object_version.is_delete_marker = FALSE
		WHERE object_version.version_id IN (?)
		  AND storage_copy.status = %s
%s
		ORDER BY object_version.version_id ASC, abnormal_data_set.copy_index ASC, abnormal_data_set.local_data_set_id ASC`, bucketStorageHealthDataSetCTE(r.db), storageHealthCommittedCopyStatusSQL(), dataSetFilter)
	var rows []affectedVersionRiskDataSetRow
	if err := r.db.NewRaw(query, args...).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing bucket storage health risk data sets: %w", err)
	}
	for _, row := range rows {
		out[row.VersionID] = append(out[row.VersionID], row.BucketStorageHealthRiskDataSet)
	}
	return out, nil
}

func (r *BunStorageUploadRepo) listBucketStorageHealthReadableAlternativeCounts(ctx context.Context, input BucketStorageHealthAffectedVersionsInput, versionIDs []string) (map[string]int, error) {
	out := make(map[string]int, len(versionIDs))
	if len(versionIDs) == 0 {
		return out, nil
	}
	query := fmt.Sprintf(`SELECT
			object_version.version_id,
			COUNT(*) AS readable_alternative_count
		FROM object_versions AS object_version
		JOIN storage_upload_copies AS readable_copy ON readable_copy.upload_id = object_version.storage_upload_id
		JOIN storage_uploads AS readable_upload
		  ON readable_upload.id = readable_copy.upload_id
		 AND readable_upload.bucket_id = object_version.bucket_id
		JOIN storage_data_sets AS readable_data_set
		  ON readable_data_set.id = readable_copy.storage_data_set_id
		 AND readable_data_set.bucket_id = object_version.bucket_id
		JOIN observability_data_set_states AS readable_observation ON readable_observation.local_data_set_id = readable_data_set.id
		WHERE object_version.bucket_id = ?
		  AND object_version.version_id IN (?)
		  AND %s
		GROUP BY object_version.version_id`, readableCommittedStorageCopySQL())
	var rows []affectedVersionReadableCountRow
	if err := r.db.NewRaw(query, input.BucketID, bun.List(versionIDs), input.StaleBefore).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing bucket storage health readable alternative counts: %w", err)
	}
	for _, row := range rows {
		out[row.VersionID] = row.Count
	}
	return out, nil
}

func bucketStorageHealthAffectedVersionObjectFilters(db bun.IDB, input BucketStorageHealthAffectedVersionsInput) (string, []interface{}) {
	var filters strings.Builder
	args := make([]interface{}, 0, 8)
	if input.Key != "" {
		filters.WriteString(`
		  AND object_version.key = ?`)
		args = append(args, input.Key)
	} else if input.Prefix != "" {
		condition, prefixArgs := caseSensitivePrefixSQL(db, "object_version.key", input.Prefix)
		filters.WriteString(`
		  AND `)
		filters.WriteString(condition)
		args = append(args, prefixArgs...)
	}
	if input.KeyMarker != "" {
		filters.WriteString(`
		  AND (
				`)
		filters.WriteString(keyComparisonSQL(db, "object_version.key", ">"))
		filters.WriteString(`
				OR (
					`)
		filters.WriteString(keyComparisonSQL(db, "object_version.key", "="))
		filters.WriteString(`
					AND (
						object_version.created_at < ?
						OR (
							object_version.created_at = ?
							AND object_version.version_id < ?
						)
					)
				)
		  )`)
		args = append(args, input.KeyMarker, input.KeyMarker, input.CreatedAtMarker, input.CreatedAtMarker, input.VersionIDMarker)
	}
	return filters.String(), args
}

func bucketStorageHealthAffectedVersionDataSetFilter(input BucketStorageHealthAffectedVersionsInput) (string, []interface{}) {
	if input.LocalDataSetID <= 0 {
		return "", nil
	}
	return `
				  AND abnormal_data_set.local_data_set_id = ?`, []interface{}{input.LocalDataSetID}
}

func validateBucketStorageHealthAffectedVersionMarkers(input BucketStorageHealthAffectedVersionsInput) error {
	hasKey := input.KeyMarker != ""
	hasVersion := input.VersionIDMarker != ""
	hasCreatedAt := !input.CreatedAtMarker.IsZero()
	if hasKey == hasVersion && hasVersion == hasCreatedAt {
		return nil
	}
	return fmt.Errorf("key_marker, version_marker, and created_at_marker must be provided together: %w", ErrInvalidInput)
}

// This records locally known readable committed copies; it is not a data-safety guarantee.
func readableCommittedStorageCopySQL() string {
	return fmt.Sprintf(`readable_upload.piece_cid IS NOT NULL AND readable_upload.piece_cid <> ''
		  AND readable_copy.status = %s
		  AND readable_copy.storage_data_set_id IS NOT NULL
		  AND readable_copy.provider_id IS NOT NULL AND readable_copy.provider_id <> ''
		  AND readable_data_set.data_set_id IS NOT NULL AND readable_data_set.data_set_id <> ''
		  AND readable_data_set.status IN (%s)
		  AND readable_observation.status = %s
		  AND readable_observation.last_checked_at >= ?
		  AND readable_copy.piece_id IS NOT NULL AND readable_copy.piece_id <> ''
		  AND readable_copy.retrieval_url IS NOT NULL AND readable_copy.retrieval_url <> ''`,
		storageHealthCommittedCopyStatusSQL(),
		storageHealthReadyDataSetStatusListSQL(),
		storageHealthAvailableObservationStatusSQL(),
	)
}

func affectedVersionRowObjectVersion(row affectedVersionRow) model.ObjectVersion {
	// Keep this mapper aligned with the affected version SELECT row.
	return model.ObjectVersion{
		VersionID:       row.VersionID,
		ObjectID:        row.ObjectID,
		BucketID:        row.BucketID,
		Key:             row.Key,
		Size:            row.Size,
		ETag:            row.ETag,
		Checksum:        row.Checksum,
		ContentType:     row.ContentType,
		CacheKey:        row.CacheKey,
		StorageUploadID: row.StorageUploadID,
		InCache:         row.InCache,
		IsCurrent:       row.IsCurrent,
		State:           row.State,
		FailedAtState:   row.FailedAtState,
		LastError:       row.LastError,
		CreatedAt:       row.CreatedAt,
		UpdatedAt:       row.UpdatedAt,
	}
}
