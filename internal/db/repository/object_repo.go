package repository

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// BunObjectRepo implements ObjectRepository using Bun ORM.
type BunObjectRepo struct {
	db bun.IDB
}

var _ ObjectRepository = (*BunObjectRepo)(nil)

func (r *BunObjectRepo) CreateVersionAndSetCurrent(ctx context.Context, version *model.ObjectVersion) (int64, error) {
	var objectID int64
	_, canRestartTx := r.db.(*bun.DB)
	for attempt := 0; ; attempt++ {
		err := r.runMaybeTx(ctx, func(db bun.IDB) error {
			id, err := createVersionAndSetCurrent(ctx, db, version)
			objectID = id
			return err
		})
		if err == nil {
			return objectID, nil
		}
		if !shouldRetryObjectWrite(err, canRestartTx) || attempt >= 19 {
			return 0, err
		}
		delay := time.Duration(attempt+1) * 25 * time.Millisecond
		if delay > 200*time.Millisecond {
			delay = 200 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *BunObjectRepo) CreateVersionAndSetCurrentIfChanged(ctx context.Context, version *model.ObjectVersion) (ObjectVersionWriteResult, error) {
	var result ObjectVersionWriteResult
	_, canRestartTx := r.db.(*bun.DB)
	for attempt := 0; ; attempt++ {
		err := r.runMaybeTx(ctx, func(db bun.IDB) error {
			writeResult, err := createVersionAndSetCurrentIfChanged(ctx, db, version)
			result = writeResult
			return err
		})
		if err == nil {
			return result, nil
		}
		if !shouldRetryObjectWrite(err, canRestartTx) || attempt >= 19 {
			return ObjectVersionWriteResult{}, err
		}
		delay := time.Duration(attempt+1) * 25 * time.Millisecond
		if delay > 200*time.Millisecond {
			delay = 200 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ObjectVersionWriteResult{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *BunObjectRepo) GetObjectByID(ctx context.Context, id int64) (*model.Object, error) {
	obj := new(model.Object)
	err := r.db.NewSelect().
		Model(obj).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting object by ID: %w", err)
	}
	return obj, nil
}

func (r *BunObjectRepo) GetObjectByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.Object, error) {
	obj := new(model.Object)
	err := r.db.NewSelect().
		Model(obj).
		Where("bucket_id = ? AND key = ?", bucketID, key).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting object by bucket+key: %w", err)
	}
	return obj, nil
}

func (r *BunObjectRepo) GetCurrentVersionByObjectID(ctx context.Context, objectID int64) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := r.db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.object_id = ? AND object_version.is_current = ?", objectID, true).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting current object version by object ID: %w", err)
	}
	return version, nil
}

func (r *BunObjectRepo) GetCurrentVersionByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := r.db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.bucket_id = ? AND object_version.key = ? AND object_version.is_current = ?", bucketID, key, true).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting current object version by bucket+key: %w", err)
	}
	return version, nil
}

func (r *BunObjectRepo) GetVersionByID(ctx context.Context, versionID string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := r.db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.version_id = ?", versionID).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting object version by ID: %w", err)
	}
	return version, nil
}

func (r *BunObjectRepo) GetVersionByBucketKeyAndID(ctx context.Context, bucketID int64, key string, versionID string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := r.db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.bucket_id = ? AND object_version.key = ? AND object_version.version_id = ?", bucketID, key, versionID).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting object version by bucket+key+ID: %w", err)
	}
	return version, nil
}

func (r *BunObjectRepo) FindReusableStoredVersion(ctx context.Context, bucketID int64, size int64, checksum string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := r.db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.bucket_id = ? AND object_version.size = ? AND object_version.checksum = ?", bucketID, size, checksum).
		Where("object_version.state IN (?)", bun.List([]model.ObjectState{model.ObjectStateStored, model.ObjectStateCacheEvicted})).
		Where("storage_upload.status = ?", model.StorageUploadStatusComplete).
		Where(usableCopyExistsSQL("object_version.storage_upload_id")).
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("finding reusable stored object version: %w", err)
	}
	return version, nil
}

func (r *BunObjectRepo) FindReusableActiveUploadVersion(ctx context.Context, bucketID int64, size int64, checksum string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	err := r.db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version").
		ColumnExpr("object_version.*").
		Join("JOIN tasks AS task ON task.ref_type = ? AND task.ref_version_id = object_version.version_id", "object").
		Where("object_version.bucket_id = ? AND object_version.size = ? AND object_version.checksum = ?", bucketID, size, checksum).
		Where("object_version.state IN (?)", bun.List([]model.ObjectState{model.ObjectStateCached, model.ObjectStateUploading})).
		Where("object_version.in_cache = ?", true).
		Where("task.type = ?", model.TaskTypeUpload).
		Where("task.status IN (?)", bun.List([]model.TaskStatus{model.TaskStatusPending, model.TaskStatusRunning})).
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("finding reusable active upload object version: %w", err)
	}
	return version, nil
}

func (r *BunObjectRepo) ListCurrentVersionsByBucket(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]model.ObjectVersion, error) {
	return r.listCurrentVersionsByBucket(ctx, bucketID, prefix, afterKey, false, maxKeys)
}

func (r *BunObjectRepo) ListCurrentVersionsByBucketAtOrAfter(ctx context.Context, bucketID int64, prefix string, fromKey string, maxKeys int) ([]model.ObjectVersion, error) {
	return r.listCurrentVersionsByBucket(ctx, bucketID, prefix, fromKey, true, maxKeys)
}

func (r *BunObjectRepo) listCurrentVersionsByBucket(ctx context.Context, bucketID int64, prefix string, keyBoundary string, includeBoundary bool, maxKeys int) ([]model.ObjectVersion, error) {
	var versions []model.ObjectVersion
	q := r.db.NewSelect().
		Model(&versions).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version").
		Where("object_version.bucket_id = ? AND object_version.is_current = ?", bucketID, true).
		OrderExpr("object_version.key ASC")

	if prefix != "" {
		escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
		q = q.Where("object_version.key LIKE ? ESCAPE '\\'", escaped+"%")
	}
	if keyBoundary != "" {
		if includeBoundary {
			q = q.Where("object_version.key >= ?", keyBoundary)
		} else {
			q = q.Where("object_version.key > ?", keyBoundary)
		}
	}
	if maxKeys > 0 {
		q = q.Limit(maxKeys)
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing current object versions: %w", err)
	}
	return versions, nil
}

func (r *BunObjectRepo) ListVersionsByBucket(ctx context.Context, bucketID int64, prefix string, keyMarker string, versionIDMarker string, maxKeys int) ([]ObjectVersionListItem, error) {
	var rows []ObjectVersionListItem
	q := r.db.NewSelect().
		Model(&rows).
		ModelTableExpr("object_versions AS object_version").
		Where("object_version.bucket_id = ?", bucketID).
		OrderExpr("object_version.key ASC").
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC")
	q = withObjectVersionStorageColumns(q, "object_version")

	if prefix != "" {
		escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
		q = q.Where("object_version.key LIKE ? ESCAPE '\\'", escaped+"%")
	}
	if keyMarker != "" {
		if versionIDMarker == "" {
			q = q.Where("object_version.key > ?", keyMarker)
		} else {
			marker, err := r.GetVersionByBucketKeyAndID(ctx, bucketID, keyMarker, versionIDMarker)
			if err != nil {
				return nil, err
			}
			if marker == nil {
				q = q.Where("object_version.key > ?", keyMarker)
			} else {
				q = q.Where("(object_version.key > ?) OR (object_version.key = ? AND (object_version.created_at < ? OR (object_version.created_at = ? AND object_version.version_id < ?)))",
					keyMarker, keyMarker, marker.CreatedAt, marker.CreatedAt, marker.VersionID)
			}
		}
	}
	if maxKeys > 0 {
		q = q.Limit(maxKeys)
	}

	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing object versions: %w", err)
	}
	return rows, nil
}

func (r *BunObjectRepo) ListVersionsByKey(ctx context.Context, bucketID int64, key string, afterVersionID string, maxKeys int) ([]ObjectVersionListItem, error) {
	var rows []ObjectVersionListItem
	q := r.db.NewSelect().
		Model(&rows).
		ModelTableExpr("object_versions AS object_version").
		Where("object_version.bucket_id = ? AND object_version.key = ?", bucketID, key).
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC")
	q = withObjectVersionStorageColumns(q, "object_version")

	if afterVersionID != "" {
		marker, err := r.GetVersionByBucketKeyAndID(ctx, bucketID, key, afterVersionID)
		if err != nil {
			return nil, err
		}
		if marker != nil {
			q = q.Where("(object_version.created_at < ? OR (object_version.created_at = ? AND object_version.version_id < ?))",
				marker.CreatedAt, marker.CreatedAt, marker.VersionID)
		}
	}
	if maxKeys > 0 {
		q = q.Limit(maxKeys)
	}

	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing object versions by key: %w", err)
	}
	return rows, nil
}

func (r *BunObjectRepo) UpdateVersionState(ctx context.Context, versionID string, from, to model.ObjectState) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		return updateVersionState(ctx, db, versionID, from, to)
	})
}

func (r *BunObjectRepo) UpdateVersionStateToFailed(ctx context.Context, versionID string, from model.ObjectState, lastError string) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.ObjectVersion)(nil)).
			Set("state = ?", model.ObjectStateFailed).
			Set("failed_at_state = ?", from).
			Set("last_error = ?", lastError).
			Set("updated_at = ?", now).
			Where("version_id = ? AND state = ?", versionID, from).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("updating object version state to failed: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("state transition %s→failed failed: version %s not in expected state", from, versionID)
		}
		return nil
	})
}

func (r *BunObjectRepo) SetVersionCachePresence(ctx context.Context, versionID string, inCache bool) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		return setVersionCachePresence(ctx, db, versionID, inCache)
	})
}

func (r *BunObjectRepo) SetVersionStorageUploadAndTransition(ctx context.Context, versionID string, storageUploadID int64, from, to model.ObjectState) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		query := `UPDATE object_versions
			SET storage_upload_id = ?, state = ?, updated_at = ?
			WHERE version_id = ? AND state = ?
			  AND EXISTS (
				SELECT 1 FROM storage_uploads
				WHERE id = ? AND status = ?
			  )
			  AND ` + usableCopyExistsSQL("?")
		res, err := db.NewRaw(query, storageUploadID, to, now, versionID, from, storageUploadID, model.StorageUploadStatusComplete, storageUploadID).Exec(ctx)
		if err != nil {
			return fmt.Errorf("setting version storage upload and transitioning state: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("SetVersionStorageUploadAndTransition %s→%s failed: version %s not in expected state or upload not usable", from, to, versionID)
		}
		return nil
	})
}

func (r *BunObjectRepo) FailUploadingContentFollowers(ctx context.Context, bucketID int64, size int64, checksum string, leaderVersionID string, lastError string) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		query := `UPDATE object_versions SET state = ?, failed_at_state = ?, last_error = ?, updated_at = ? WHERE bucket_id = ? AND size = ? AND checksum = ? AND state = ? AND (version_id = ? OR NOT EXISTS (SELECT 1 FROM tasks WHERE tasks.ref_type = ? AND tasks.ref_version_id = object_versions.version_id AND tasks.type = ? AND tasks.status IN (?, ?))) RETURNING object_id, version_id`
		err := db.NewRaw(query,
			model.ObjectStateFailed,
			model.ObjectStateUploading,
			lastError,
			now,
			bucketID,
			size,
			checksum,
			model.ObjectStateUploading,
			leaderVersionID,
			"object",
			model.TaskTypeUpload,
			model.TaskStatusPending,
			model.TaskStatusRunning,
		).Scan(ctx, &refs)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no uploading object versions matched content: %w", ErrNotFound)
			}
			return fmt.Errorf("failing uploading object versions: %w", err)
		}
		if len(refs) == 0 {
			return fmt.Errorf("no uploading object versions matched content: %w", ErrNotFound)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

func (r *BunObjectRepo) ListVersionsByState(ctx context.Context, state model.ObjectState, limit int) ([]model.ObjectVersion, error) {
	var versions []model.ObjectVersion
	q := r.db.NewSelect().
		Model(&versions).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version").
		Where("object_version.state = ?", state).
		OrderExpr("object_version.updated_at ASC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing object versions by state: %w", err)
	}
	return versions, nil
}

func (r *BunObjectRepo) ResetStaleVersionStates(ctx context.Context, fromState, toState model.ObjectState, staleBefore time.Time) (int, error) {
	var reset int
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		versionIDs, err := resetStaleVersions(ctx, db, fromState, toState, staleBefore)
		if err != nil {
			return err
		}
		reset = len(versionIDs)
		return nil
	})
	if err != nil {
		return reset, err
	}
	return reset, nil
}

// CountByState returns current object counts grouped by state.
func (r *BunObjectRepo) CountByState(ctx context.Context) ([]ObjectStateCount, error) {
	var counts []ObjectStateCount
	err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		ColumnExpr("state, COUNT(*) AS count").
		Where("is_current = ?", true).
		GroupExpr("state").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting current object versions by state: %w", err)
	}
	return counts, nil
}

// TotalSize returns the sum of all current object sizes in bytes.
func (r *BunObjectRepo) TotalSize(ctx context.Context) (int64, error) {
	var total int64
	err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		ColumnExpr("COALESCE(SUM(size), 0)").
		Where("is_current = ?", true).
		Scan(ctx, &total)
	if err != nil {
		return 0, fmt.Errorf("computing total current object size: %w", err)
	}
	return total, nil
}

// CountByBucket returns the number of current objects in a bucket.
func (r *BunObjectRepo) CountByBucket(ctx context.Context, bucketID int64) (int64, error) {
	count, err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("bucket_id = ? AND is_current = ?", bucketID, true).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting current objects in bucket %d: %w", bucketID, err)
	}
	return int64(count), nil
}

// TotalSizeByBucket returns the sum of current object sizes in a bucket.
func (r *BunObjectRepo) TotalSizeByBucket(ctx context.Context, bucketID int64) (int64, error) {
	var total int64
	err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		ColumnExpr("COALESCE(SUM(size), 0)").
		Where("bucket_id = ? AND is_current = ?", bucketID, true).
		Scan(ctx, &total)
	if err != nil {
		return 0, fmt.Errorf("computing total current size for bucket %d: %w", bucketID, err)
	}
	return total, nil
}

// AggregateByBucket returns current object count and total size for all buckets in a single query.
func (r *BunObjectRepo) AggregateByBucket(ctx context.Context) (map[int64]BucketObjectStats, error) {
	var rows []struct {
		BucketID  int64 `bun:"bucket_id"`
		Count     int64 `bun:"count"`
		TotalSize int64 `bun:"total_size"`
	}
	err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		ColumnExpr("bucket_id").
		ColumnExpr("COUNT(*) AS count").
		ColumnExpr("COALESCE(SUM(size), 0) AS total_size").
		Where("is_current = ?", true).
		GroupExpr("bucket_id").
		Scan(ctx, &rows)
	if err != nil {
		return nil, fmt.Errorf("aggregating current objects by bucket: %w", err)
	}
	stats := make(map[int64]BucketObjectStats, len(rows))
	for _, row := range rows {
		stats[row.BucketID] = BucketObjectStats{Count: row.Count, TotalSize: row.TotalSize}
	}
	return stats, nil
}

func (r *BunObjectRepo) runMaybeTx(ctx context.Context, fn func(bun.IDB) error) error {
	if db, ok := r.db.(*bun.DB); ok {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(tx)
		})
	}
	return fn(r.db)
}

func withObjectVersionStorageColumns(q *bun.SelectQuery, alias string) *bun.SelectQuery {
	return q.
		ColumnExpr(alias + ".*").
		ColumnExpr("storage_upload.piece_cid AS piece_cid").
		ColumnExpr("CASE WHEN " + alias + ".state IN ('stored', 'cache_evicted') AND storage_upload.status = 'complete' THEN TRUE ELSE FALSE END AS in_filecoin").
		Join("LEFT JOIN storage_uploads AS storage_upload ON storage_upload.id = " + alias + ".storage_upload_id")
}

func usableCopyExistsSQL(uploadIDExpr string) string {
	return "EXISTS (SELECT 1 FROM storage_upload_copies AS storage_copy WHERE storage_copy.upload_id = " + uploadIDExpr + " AND storage_copy.storage_data_set_id IS NOT NULL AND storage_copy.provider_id IS NOT NULL AND storage_copy.provider_id <> '' AND storage_copy.data_set_id IS NOT NULL AND storage_copy.data_set_id <> '' AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> '' AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> '')"
}

func createVersionAndSetCurrentIfChanged(ctx context.Context, db bun.IDB, version *model.ObjectVersion) (ObjectVersionWriteResult, error) {
	normalizeObjectVersion(version)
	if err := lockCurrentObjectIfExists(ctx, db, version.BucketID, version.Key); err != nil {
		return ObjectVersionWriteResult{}, err
	}

	existing, err := selectObjectByBucketAndKey(ctx, db, version.BucketID, version.Key)
	if err != nil && err != sql.ErrNoRows {
		return ObjectVersionWriteResult{}, fmt.Errorf("checking existing object: %w", err)
	}
	if err == nil {
		current, currentErr := selectCurrentVersionByObjectID(ctx, db, existing.ID)
		if currentErr != nil && currentErr != sql.ErrNoRows {
			return ObjectVersionWriteResult{}, fmt.Errorf("checking current version: %w", currentErr)
		}
		if currentErr == nil && objectVersionMatchesVersion(current, version) {
			return ObjectVersionWriteResult{
				ObjectID:  existing.ID,
				VersionID: current.VersionID,
				ETag:      current.ETag,
				Created:   false,
			}, nil
		}
	}

	result, err := createVersionAndSetCurrentFromExisting(ctx, db, version, existing, err)
	if err != nil {
		return ObjectVersionWriteResult{}, err
	}
	return result, nil
}

func createVersionAndSetCurrent(ctx context.Context, db bun.IDB, version *model.ObjectVersion) (int64, error) {
	if err := lockCurrentObjectIfExists(ctx, db, version.BucketID, version.Key); err != nil {
		return 0, err
	}
	existing, err := selectObjectByBucketAndKey(ctx, db, version.BucketID, version.Key)
	if err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("checking existing object: %w", err)
	}

	result, err := createVersionAndSetCurrentFromExisting(ctx, db, version, existing, err)
	if err != nil {
		return 0, err
	}
	return result.ObjectID, nil
}

func createVersionAndSetCurrentFromExisting(ctx context.Context, db bun.IDB, version *model.ObjectVersion, existing *model.Object, existingErr error) (ObjectVersionWriteResult, error) {
	normalizeObjectVersion(version)
	now := time.Now()

	if existingErr == sql.ErrNoRows {
		obj := objectIdentityFromVersion(version)
		obj.CreatedAt = now
		obj.UpdatedAt = now
		if _, insertErr := db.NewInsert().Model(obj).Exec(ctx); insertErr != nil {
			if !isUniqueViolation(insertErr) {
				return ObjectVersionWriteResult{}, fmt.Errorf("inserting new object: %w", insertErr)
			}
			return ObjectVersionWriteResult{}, fmt.Errorf("%w: inserting new object: %w", errConcurrentObjectCreate, insertErr)
		}

		version.ObjectID = obj.ID
		version.IsCurrent = true
		if version.CreatedAt.IsZero() {
			version.CreatedAt = now
		}
		if version.UpdatedAt.IsZero() {
			version.UpdatedAt = now
		}
		if _, insertErr := db.NewInsert().Model(version).Exec(ctx); insertErr != nil {
			return ObjectVersionWriteResult{}, fmt.Errorf("inserting object version: %w", insertErr)
		}
		return ObjectVersionWriteResult{
			ObjectID:  obj.ID,
			VersionID: version.VersionID,
			ETag:      version.ETag,
			Created:   true,
		}, nil
	}

	version.ObjectID = existing.ID
	version.IsCurrent = true
	if version.CreatedAt.IsZero() {
		version.CreatedAt = now
	}
	if version.UpdatedAt.IsZero() {
		version.UpdatedAt = now
	}
	if _, err := db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("is_current = ?", false).
		Where("object_id = ? AND is_current = ?", existing.ID, true).
		Exec(ctx); err != nil {
		return ObjectVersionWriteResult{}, fmt.Errorf("clearing previous current version: %w", err)
	}
	if _, insertErr := db.NewInsert().Model(version).Exec(ctx); insertErr != nil {
		return ObjectVersionWriteResult{}, fmt.Errorf("inserting object version: %w", insertErr)
	}
	if _, updateErr := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("updated_at = ?", now).
		Where("id = ?", existing.ID).
		Exec(ctx); updateErr != nil {
		return ObjectVersionWriteResult{}, fmt.Errorf("updating object identity timestamp: %w", updateErr)
	}
	return ObjectVersionWriteResult{
		ObjectID:  existing.ID,
		VersionID: version.VersionID,
		ETag:      version.ETag,
		Created:   true,
	}, nil
}

func normalizeObjectVersion(version *model.ObjectVersion) {
	version.FailedAtState = nil
	version.LastError = nil
	if version.State == "" {
		version.State = model.ObjectStateCached
	}
	if version.State != model.ObjectStateCacheEvicted {
		version.InCache = true
	}
	if version.ContentType == "" {
		version.ContentType = "application/octet-stream"
	}
}

func selectObjectByBucketAndKey(ctx context.Context, db bun.IDB, bucketID int64, key string) (*model.Object, error) {
	obj := new(model.Object)
	err := db.NewSelect().
		Model(obj).
		Where("bucket_id = ? AND key = ?", bucketID, key).
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func selectCurrentVersionByObjectID(ctx context.Context, db bun.IDB, objectID int64) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.object_id = ? AND object_version.is_current = ?", objectID, true).Scan(ctx)
	if err != nil {
		return nil, err
	}
	return version, nil
}

func lockCurrentObjectIfExists(ctx context.Context, db bun.IDB, bucketID int64, key string) error {
	if _, err := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("updated_at = updated_at").
		Where("bucket_id = ? AND key = ?", bucketID, key).
		Exec(ctx); err != nil {
		return fmt.Errorf("locking current object: %w", err)
	}
	return nil
}

func objectVersionMatchesVersion(current *model.ObjectVersion, version *model.ObjectVersion) bool {
	if current == nil || current.State == model.ObjectStateFailed {
		return false
	}
	return current.Size == version.Size &&
		current.ETag == version.ETag &&
		current.Checksum == version.Checksum &&
		current.ContentType == version.ContentType &&
		maps.Equal(current.Metadata, version.Metadata)
}

func updateVersionState(ctx context.Context, db bun.IDB, versionID string, from, to model.ObjectState) error {
	now := time.Now()
	q := db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("state = ?", to).
		Set("updated_at = ?", now).
		Where("version_id = ? AND state = ?", versionID, from)
	q = applyCacheLocationForState(q, to)
	if from == model.ObjectStateFailed {
		q = q.Set("failed_at_state = NULL")
		q = q.Set("last_error = NULL")
	}
	res, err := q.Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating object version state: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("state transition %s→%s failed: version %s not in expected state", from, to, versionID)
	}
	return nil
}

func resetStaleVersions(ctx context.Context, db bun.IDB, fromState, toState model.ObjectState, staleBefore time.Time) ([]string, error) {
	now := time.Now()
	var rows []struct {
		VersionID string `bun:"version_id"`
	}
	cacheColumn := cacheLocationSQLForState(toState)
	query := `UPDATE object_versions SET state = ?, updated_at = ?` + cacheColumn + ` WHERE state = ? AND updated_at < ? RETURNING version_id`
	args := []interface{}{toState, now, fromState, staleBefore}
	if fromState == model.ObjectStateFailed {
		query = `UPDATE object_versions SET state = ?, failed_at_state = NULL, last_error = NULL, updated_at = ?` + cacheColumn + ` WHERE state = ? AND updated_at < ? RETURNING version_id`
	}
	if err := db.NewRaw(query, args...).Scan(ctx, &rows); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("resetting stale object versions: %w", err)
	}
	versionIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		versionIDs = append(versionIDs, row.VersionID)
	}
	return versionIDs, nil
}

func objectIdentityFromVersion(version *model.ObjectVersion) *model.Object {
	return &model.Object{
		BucketID: version.BucketID,
		Key:      version.Key,
	}
}

func setVersionCachePresence(ctx context.Context, db bun.IDB, versionID string, inCache bool) error {
	res, err := db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("in_cache = ?", inCache).
		Where("version_id = ?", versionID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("setting version cache presence: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("setting version cache presence: version %s not found", versionID)
	}
	return nil
}

func applyCacheLocationForState(q *bun.UpdateQuery, state model.ObjectState) *bun.UpdateQuery {
	switch state {
	case model.ObjectStateCached:
		return q.Set("in_cache = ?", true)
	case model.ObjectStateCacheEvicted:
		return q.Set("in_cache = ?", false)
	default:
		return q
	}
}

func cacheLocationSQLForState(state model.ObjectState) string {
	switch state {
	case model.ObjectStateCached:
		return ", in_cache = TRUE"
	case model.ObjectStateCacheEvicted:
		return ", in_cache = FALSE"
	default:
		return ""
	}
}
