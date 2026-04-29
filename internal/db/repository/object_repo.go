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

func (r *BunObjectRepo) GetByID(ctx context.Context, id int64) (*model.Object, error) {
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

func (r *BunObjectRepo) GetByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.Object, error) {
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

func (r *BunObjectRepo) GetVersionByID(ctx context.Context, versionID string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	err := r.db.NewSelect().
		Model(version).
		Where("version_id = ?", versionID).
		Scan(ctx)
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
	err := r.db.NewSelect().
		Model(version).
		Where("bucket_id = ? AND key = ? AND version_id = ?", bucketID, key, versionID).
		Scan(ctx)
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
	err := r.db.NewSelect().
		Model(version).
		Where("bucket_id = ? AND size = ? AND checksum = ?", bucketID, size, checksum).
		Where("state IN (?)", bun.List([]model.ObjectState{model.ObjectStateStored, model.ObjectStateCacheEvicted})).
		Where("piece_cid IS NOT NULL AND piece_cid <> ''").
		Where("retrieval_url IS NOT NULL AND retrieval_url <> ''").
		OrderExpr("created_at DESC").
		OrderExpr("version_id DESC").
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

func (r *BunObjectRepo) ListByBucket(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]model.Object, error) {
	return r.listByBucket(ctx, bucketID, prefix, afterKey, false, maxKeys)
}

func (r *BunObjectRepo) ListByBucketAtOrAfter(ctx context.Context, bucketID int64, prefix string, fromKey string, maxKeys int) ([]model.Object, error) {
	return r.listByBucket(ctx, bucketID, prefix, fromKey, true, maxKeys)
}

func (r *BunObjectRepo) listByBucket(ctx context.Context, bucketID int64, prefix string, keyBoundary string, includeBoundary bool, maxKeys int) ([]model.Object, error) {
	var objects []model.Object
	q := r.db.NewSelect().
		Model(&objects).
		Where("bucket_id = ?", bucketID).
		OrderExpr("key ASC")

	if prefix != "" {
		escaped := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(prefix)
		q = q.Where("key LIKE ? ESCAPE '\\'", escaped+"%")
	}
	if keyBoundary != "" {
		if includeBoundary {
			q = q.Where("key >= ?", keyBoundary)
		} else {
			q = q.Where("key > ?", keyBoundary)
		}
	}
	if maxKeys > 0 {
		q = q.Limit(maxKeys)
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing objects: %w", err)
	}
	return objects, nil
}

func (r *BunObjectRepo) ListVersionsByBucket(ctx context.Context, bucketID int64, prefix string, keyMarker string, versionIDMarker string, maxKeys int) ([]ObjectVersionListItem, error) {
	var rows []ObjectVersionListItem
	q := r.db.NewSelect().
		Model(&rows).
		ModelTableExpr("object_versions AS object_version").
		ColumnExpr("object_version.*").
		ColumnExpr("object.current_version_id AS current_version_id").
		Join("JOIN objects AS object ON object.id = object_version.object_id").
		Where("object_version.bucket_id = ?", bucketID).
		OrderExpr("object_version.key ASC").
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC")

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
		ColumnExpr("object_version.*").
		ColumnExpr("object.current_version_id AS current_version_id").
		Join("JOIN objects AS object ON object.id = object_version.object_id").
		Where("object_version.bucket_id = ? AND object_version.key = ?", bucketID, key).
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC")

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

		_, err = db.NewUpdate().
			Model((*model.Object)(nil)).
			Set("state = ?", model.ObjectStateFailed).
			Set("failed_at_state = ?", from).
			Set("last_error = ?", lastError).
			Set("updated_at = ?", now).
			Where("current_version_id = ?", versionID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("mirroring failed state to current object: %w", err)
		}
		return nil
	})
}

func (r *BunObjectRepo) SetVersionStorageInfoAndTransition(ctx context.Context, versionID string, pieceCID string, retrievalURL string, from, to model.ObjectState) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.ObjectVersion)(nil)).
			Set("piece_cid = ?", pieceCID).
			Set("retrieval_url = ?", retrievalURL).
			Set("state = ?", to).
			Set("updated_at = ?", now).
			Where("version_id = ? AND state = ?", versionID, from).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("setting version storage info and transitioning state: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("SetVersionStorageInfoAndTransition %s→%s failed: version %s not in expected state", from, to, versionID)
		}

		_, err = db.NewUpdate().
			Model((*model.Object)(nil)).
			Set("piece_cid = ?", pieceCID).
			Set("retrieval_url = ?", retrievalURL).
			Set("state = ?", to).
			Set("updated_at = ?", now).
			Where("current_version_id = ?", versionID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("mirroring storage info to current object: %w", err)
		}
		return nil
	})
}

func (r *BunObjectRepo) SetStorageInfoForUploadingContent(ctx context.Context, bucketID int64, size int64, checksum string, pieceCID string, retrievalURL string) ([]ObjectVersionRef, error) {
	var refs []ObjectVersionRef
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		updated, err := setStorageInfoForUploadingContent(ctx, db, bucketID, size, checksum, pieceCID, retrievalURL)
		if err != nil {
			return err
		}
		refs = updated
		if len(refs) == 0 {
			return fmt.Errorf("no uploading object versions matched content: %w", ErrNotFound)
		}

		versionIDs := make([]string, 0, len(refs))
		for _, ref := range refs {
			versionIDs = append(versionIDs, ref.VersionID)
		}

		now := time.Now()
		_, err = db.NewUpdate().
			Model((*model.Object)(nil)).
			Set("piece_cid = ?", pieceCID).
			Set("retrieval_url = ?", retrievalURL).
			Set("state = ?", model.ObjectStateStored).
			Set("updated_at = ?", now).
			Where("current_version_id IN (?)", bun.List(versionIDs)).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("mirroring content storage info to current objects: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
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

		versionIDs := make([]string, 0, len(refs))
		for _, ref := range refs {
			versionIDs = append(versionIDs, ref.VersionID)
		}

		_, err = db.NewUpdate().
			Model((*model.Object)(nil)).
			Set("state = ?", model.ObjectStateFailed).
			Set("failed_at_state = ?", model.ObjectStateUploading).
			Set("last_error = ?", lastError).
			Set("updated_at = ?", now).
			Where("current_version_id IN (?)", bun.List(versionIDs)).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("mirroring failed content state to current objects: %w", err)
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
		Where("state = ?", state).
		OrderExpr("updated_at ASC")
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
		if reset == 0 {
			return nil
		}

		now := time.Now()
		mirror := db.NewUpdate().
			Model((*model.Object)(nil)).
			Set("state = ?", toState).
			Set("updated_at = ?", now).
			Where("current_version_id IN (?)", bun.List(versionIDs))
		if fromState == model.ObjectStateFailed {
			mirror = mirror.Set("failed_at_state = NULL")
		}
		if _, err := mirror.Exec(ctx); err != nil {
			return fmt.Errorf("mirroring stale version reset to current objects: %w", err)
		}
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
		Model((*model.Object)(nil)).
		ColumnExpr("state, COUNT(*) AS count").
		GroupExpr("state").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting objects by state: %w", err)
	}
	return counts, nil
}

// TotalSize returns the sum of all current object sizes in bytes.
func (r *BunObjectRepo) TotalSize(ctx context.Context) (int64, error) {
	var total int64
	err := r.db.NewSelect().
		Model((*model.Object)(nil)).
		ColumnExpr("COALESCE(SUM(size), 0)").
		Scan(ctx, &total)
	if err != nil {
		return 0, fmt.Errorf("computing total object size: %w", err)
	}
	return total, nil
}

// CountByBucket returns the number of current objects in a bucket.
func (r *BunObjectRepo) CountByBucket(ctx context.Context, bucketID int64) (int64, error) {
	count, err := r.db.NewSelect().
		Model((*model.Object)(nil)).
		Where("bucket_id = ?", bucketID).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting objects in bucket %d: %w", bucketID, err)
	}
	return int64(count), nil
}

// TotalSizeByBucket returns the sum of current object sizes in a bucket.
func (r *BunObjectRepo) TotalSizeByBucket(ctx context.Context, bucketID int64) (int64, error) {
	var total int64
	err := r.db.NewSelect().
		Model((*model.Object)(nil)).
		ColumnExpr("COALESCE(SUM(size), 0)").
		Where("bucket_id = ?", bucketID).
		Scan(ctx, &total)
	if err != nil {
		return 0, fmt.Errorf("computing total size for bucket %d: %w", bucketID, err)
	}
	return total, nil
}

// AggregateByBucket returns current object count and total size for all buckets.
func (r *BunObjectRepo) AggregateByBucket(ctx context.Context) (map[int64]BucketObjectStats, error) {
	var rows []struct {
		BucketID  int64 `bun:"bucket_id"`
		Count     int64 `bun:"count"`
		TotalSize int64 `bun:"total_size"`
	}
	err := r.db.NewSelect().
		Model((*model.Object)(nil)).
		ColumnExpr("bucket_id").
		ColumnExpr("COUNT(*) AS count").
		ColumnExpr("COALESCE(SUM(size), 0) AS total_size").
		GroupExpr("bucket_id").
		Scan(ctx, &rows)
	if err != nil {
		return nil, fmt.Errorf("aggregating objects by bucket: %w", err)
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

func createVersionAndSetCurrentIfChanged(ctx context.Context, db bun.IDB, version *model.ObjectVersion) (ObjectVersionWriteResult, error) {
	normalizeObjectVersion(version)
	if err := lockCurrentObjectIfExists(ctx, db, version.BucketID, version.Key); err != nil {
		return ObjectVersionWriteResult{}, err
	}

	existing, err := selectObjectByBucketAndKey(ctx, db, version.BucketID, version.Key)
	if err != nil && err != sql.ErrNoRows {
		return ObjectVersionWriteResult{}, fmt.Errorf("checking existing object: %w", err)
	}
	if err == nil && objectSnapshotMatchesVersion(existing, version) {
		return ObjectVersionWriteResult{
			ObjectID:  existing.ID,
			VersionID: existing.CurrentVersionID,
			ETag:      existing.ETag,
			Created:   false,
		}, nil
	}

	result, err := createVersionAndSetCurrentFromExisting(ctx, db, version, existing, err)
	if err != nil {
		return ObjectVersionWriteResult{}, err
	}
	return result, nil
}

func createVersionAndSetCurrent(ctx context.Context, db bun.IDB, version *model.ObjectVersion) (int64, error) {
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
		obj := objectFromVersion(version)
		obj.CreatedAt = now
		obj.UpdatedAt = now
		if _, insertErr := db.NewInsert().Model(obj).Exec(ctx); insertErr != nil {
			if !isUniqueViolation(insertErr) {
				return ObjectVersionWriteResult{}, fmt.Errorf("inserting new object: %w", insertErr)
			}
			return ObjectVersionWriteResult{}, fmt.Errorf("%w: inserting new object: %w", errConcurrentObjectCreate, insertErr)
		} else {
			version.ObjectID = obj.ID
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
	}

	version.ObjectID = existing.ID
	if version.CreatedAt.IsZero() {
		version.CreatedAt = now
	}
	if version.UpdatedAt.IsZero() {
		version.UpdatedAt = now
	}
	if _, insertErr := db.NewInsert().Model(version).Exec(ctx); insertErr != nil {
		return ObjectVersionWriteResult{}, fmt.Errorf("inserting object version: %w", insertErr)
	}

	_, updateErr := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("current_version_id = ?", version.VersionID).
		Set("size = ?", version.Size).
		Set("e_tag = ?", version.ETag).
		Set("checksum = ?", version.Checksum).
		Set("content_type = ?", version.ContentType).
		Set("metadata = ?", version.Metadata).
		Set("cache_key = ?", version.CacheKey).
		Set("piece_cid = ?", version.PieceCID).
		Set("retrieval_url = ?", version.RetrievalURL).
		Set("state = ?", version.State).
		Set("failed_at_state = NULL").
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", existing.ID).
		Exec(ctx)
	if updateErr != nil {
		return ObjectVersionWriteResult{}, fmt.Errorf("updating current object snapshot: %w", updateErr)
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

func objectSnapshotMatchesVersion(obj *model.Object, version *model.ObjectVersion) bool {
	if obj == nil || obj.State == model.ObjectStateFailed {
		return false
	}
	return obj.Size == version.Size &&
		obj.ETag == version.ETag &&
		obj.Checksum == version.Checksum &&
		obj.ContentType == version.ContentType &&
		maps.Equal(obj.Metadata, version.Metadata)
}

func updateVersionState(ctx context.Context, db bun.IDB, versionID string, from, to model.ObjectState) error {
	now := time.Now()
	q := db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("state = ?", to).
		Set("updated_at = ?", now).
		Where("version_id = ? AND state = ?", versionID, from)
	if from == model.ObjectStateFailed {
		q = q.Set("failed_at_state = NULL")
	}
	res, err := q.Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating object version state: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("state transition %s→%s failed: version %s not in expected state", from, to, versionID)
	}

	mirror := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("state = ?", to).
		Set("updated_at = ?", now).
		Where("current_version_id = ?", versionID)
	if from == model.ObjectStateFailed {
		mirror = mirror.Set("failed_at_state = NULL")
	}
	if _, err := mirror.Exec(ctx); err != nil {
		return fmt.Errorf("mirroring version state to current object: %w", err)
	}
	return nil
}

func resetStaleVersions(ctx context.Context, db bun.IDB, fromState, toState model.ObjectState, staleBefore time.Time) ([]string, error) {
	now := time.Now()
	var rows []struct {
		VersionID string `bun:"version_id"`
	}
	query := `UPDATE object_versions SET state = ?, updated_at = ? WHERE state = ? AND updated_at < ? RETURNING version_id`
	args := []interface{}{toState, now, fromState, staleBefore}
	if fromState == model.ObjectStateFailed {
		query = `UPDATE object_versions SET state = ?, failed_at_state = NULL, updated_at = ? WHERE state = ? AND updated_at < ? RETURNING version_id`
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

func setStorageInfoForUploadingContent(ctx context.Context, db bun.IDB, bucketID int64, size int64, checksum string, pieceCID string, retrievalURL string) ([]ObjectVersionRef, error) {
	now := time.Now()
	var refs []ObjectVersionRef
	query := `UPDATE object_versions
		SET piece_cid = ?, retrieval_url = ?, state = ?, updated_at = ?
		WHERE bucket_id = ? AND size = ? AND checksum = ? AND state = ?
		RETURNING object_id, version_id`
	err := db.NewRaw(query,
		pieceCID, retrievalURL, model.ObjectStateStored, now,
		bucketID, size, checksum, model.ObjectStateUploading,
	).Scan(ctx, &refs)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("setting storage info for uploading content: %w", err)
	}
	return refs, nil
}

func objectFromVersion(version *model.ObjectVersion) *model.Object {
	return &model.Object{
		BucketID:         version.BucketID,
		Key:              version.Key,
		CurrentVersionID: version.VersionID,
		Size:             version.Size,
		ETag:             version.ETag,
		Checksum:         version.Checksum,
		ContentType:      version.ContentType,
		Metadata:         version.Metadata,
		CacheKey:         version.CacheKey,
		PieceCID:         version.PieceCID,
		RetrievalURL:     version.RetrievalURL,
		State:            version.State,
		FailedAtState:    version.FailedAtState,
		LastError:        version.LastError,
	}
}
