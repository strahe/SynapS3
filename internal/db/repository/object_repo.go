package repository

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
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

func (r *BunObjectRepo) CreateDeleteMarkerAndSetCurrent(ctx context.Context, bucketID int64, key string, versionID string) (*model.ObjectVersion, error) {
	if bucketID == 0 || key == "" || versionID == "" {
		return nil, fmt.Errorf("creating delete marker: %w", ErrInvalidInput)
	}
	marker := &model.ObjectVersion{
		VersionID:      versionID,
		BucketID:       bucketID,
		Key:            key,
		Size:           0,
		ETag:           "",
		Checksum:       "",
		ContentType:    "",
		CacheKey:       "",
		InCache:        false,
		IsDeleteMarker: true,
		State:          model.ObjectStateCached,
	}

	_, canRestartTx := r.db.(*bun.DB)
	for attempt := 0; ; attempt++ {
		err := r.runMaybeTx(ctx, func(db bun.IDB) error {
			return createDeleteMarkerAndSetCurrent(ctx, db, marker)
		})
		if err == nil {
			return marker, nil
		}
		if !shouldRetryObjectWrite(err, canRestartTx) || attempt >= 19 {
			return nil, err
		}
		delay := time.Duration(attempt+1) * 25 * time.Millisecond
		if delay > 200*time.Millisecond {
			delay = 200 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *BunObjectRepo) DeleteMarkerVersion(ctx context.Context, bucketID int64, key string, versionID string) error {
	if bucketID == 0 || key == "" || versionID == "" {
		return fmt.Errorf("deleting marker version: %w", ErrInvalidInput)
	}
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		return deleteMarkerVersion(ctx, db, bucketID, key, versionID)
	})
}

func (r *BunObjectRepo) DeleteObjectVersionPermanently(ctx context.Context, input DeleteObjectVersionInput) (DeleteObjectVersionResult, error) {
	var result DeleteObjectVersionResult
	if input.BucketID == 0 || input.Key == "" || input.VersionID == "" {
		return result, fmt.Errorf("permanently deleting object version: %w", ErrInvalidInput)
	}
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockCurrentObjectIfExists(ctx, db, input.BucketID, input.Key); err != nil {
			return err
		}
		version, err := selectVersionByBucketKeyAndID(ctx, db, input.BucketID, input.Key, input.VersionID)
		if err != nil {
			return err
		}
		if version == nil {
			return ErrNotFound
		}
		if version.IsDeleteMarker || !objectVersionPermanentDeleteStateAllowed(version.State) {
			return ErrConflict
		}
		if err := ensureObjectVersionHasNoActiveWork(ctx, db, version.VersionID); err != nil {
			return err
		}
		wasCurrent := version.IsCurrent
		objectID := version.ObjectID

		now := time.Now()
		deletion := &model.ObjectDeletion{
			BucketID:           version.BucketID,
			ObjectID:           version.ObjectID,
			Key:                version.Key,
			VersionID:          version.VersionID,
			CacheKey:           version.CacheKey,
			StorageUploadID:    version.StorageUploadID,
			Size:               version.Size,
			Checksum:           version.Checksum,
			CacheCleanupStatus: model.CacheCleanupStatusPending,
			CreatedAt:          now,
			UpdatedAt:          now,
			DeletedAt:          now,
		}
		if _, err := db.NewInsert().Model(deletion).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return ErrAlreadyExists
			}
			return fmt.Errorf("recording object deletion: %w", err)
		}
		result.DeletionID = deletion.ID
		result.CacheKey = version.CacheKey
		result.StorageUploadID = version.StorageUploadID

		if version.StorageUploadID != nil {
			cleanupTaskID, err := createStorageCleanupTaskForDeletedVersion(ctx, db, *version.StorageUploadID, version.VersionID, input.StorageCleanupMaxRetries)
			if err != nil {
				return err
			}
			result.StorageCleanupTaskID = cleanupTaskID
		}

		res, err := db.NewDelete().
			Model((*model.ObjectVersion)(nil)).
			Where("version_id = ?", version.VersionID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("deleting object version row: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return ErrNotFound
		}
		if wasCurrent {
			if err := promoteLatestVersionOrDeleteObject(ctx, db, objectID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return DeleteObjectVersionResult{}, fmt.Errorf("permanently deleting object version: %w", err)
	}
	return result, nil
}

func (r *BunObjectRepo) DeleteDeletedObjectPermanently(ctx context.Context, input DeleteDeletedObjectInput) (DeleteDeletedObjectResult, error) {
	var result DeleteDeletedObjectResult
	if input.BucketID == 0 || input.Key == "" || input.DeleteMarkerVersionID == "" {
		return result, fmt.Errorf("permanently deleting deleted object: %w", ErrInvalidInput)
	}
	result.Key = input.Key
	result.DeleteMarkerVersionID = input.DeleteMarkerVersionID

	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockCurrentObjectIfExists(ctx, db, input.BucketID, input.Key); err != nil {
			return err
		}
		current, err := selectCurrentVersionByBucketAndKey(ctx, db, input.BucketID, input.Key)
		if err != nil {
			return err
		}
		if current == nil {
			return ErrNotFound
		}
		if !current.IsDeleteMarker || current.VersionID != input.DeleteMarkerVersionID {
			return ErrConflict
		}

		versions, err := selectVersionsByObjectNewestFirst(ctx, db, current.ObjectID)
		if err != nil {
			return err
		}
		if len(versions) == 0 {
			return ErrNotFound
		}

		now := time.Now()
		deletions := make([]model.ObjectDeletion, 0, len(versions))
		deletedVersionIDsByUpload := make(map[int64][]string)
		for i := range versions {
			version := versions[i]
			if version.IsDeleteMarker {
				result.DeleteMarkersDeleted++
				continue
			}
			if !objectVersionPermanentDeleteStateAllowed(version.State) {
				return ErrConflict
			}
			if err := ensureObjectVersionHasNoActiveWork(ctx, db, version.VersionID); err != nil {
				return err
			}
			deletions = append(deletions, model.ObjectDeletion{
				BucketID:           version.BucketID,
				ObjectID:           version.ObjectID,
				Key:                version.Key,
				VersionID:          version.VersionID,
				CacheKey:           version.CacheKey,
				StorageUploadID:    version.StorageUploadID,
				Size:               version.Size,
				Checksum:           version.Checksum,
				CacheCleanupStatus: model.CacheCleanupStatusPending,
				CreatedAt:          now,
				UpdatedAt:          now,
				DeletedAt:          now,
			})
			result.DeletedVersions = append(result.DeletedVersions, DeletedObjectVersionSnapshot{
				VersionID: version.VersionID,
				CacheKey:  version.CacheKey,
			})
			result.DataVersionsDeleted++
			if version.StorageUploadID != nil {
				uploadID := *version.StorageUploadID
				deletedVersionIDsByUpload[uploadID] = append(deletedVersionIDsByUpload[uploadID], version.VersionID)
			}
		}

		if len(deletions) > 0 {
			if _, err := db.NewInsert().Model(&deletions).Exec(ctx); err != nil {
				if isUniqueViolation(err) {
					return ErrAlreadyExists
				}
				return fmt.Errorf("recording object deletions: %w", err)
			}
		}

		for uploadID, deletedVersionIDs := range deletedVersionIDsByUpload {
			cleanupTaskID, err := createStorageCleanupTaskForDeletedVersions(ctx, db, uploadID, deletedVersionIDs, input.StorageCleanupMaxRetries)
			if err != nil {
				return err
			}
			if cleanupTaskID != nil {
				result.StorageCleanupTaskIDs = append(result.StorageCleanupTaskIDs, *cleanupTaskID)
			}
		}

		if _, err := db.NewDelete().
			Model((*model.ObjectVersion)(nil)).
			Where("object_id = ?", current.ObjectID).
			Exec(ctx); err != nil {
			return fmt.Errorf("deleting deleted object versions: %w", err)
		}
		if _, err := db.NewDelete().
			Model((*model.Object)(nil)).
			Where("id = ?", current.ObjectID).
			Exec(ctx); err != nil {
			return fmt.Errorf("deleting object identity: %w", err)
		}
		return nil
	})
	if err != nil {
		return DeleteDeletedObjectResult{}, fmt.Errorf("permanently deleting deleted object: %w", err)
	}
	return result, nil
}

func (r *BunObjectRepo) UpdateObjectDeletionCacheCleanup(ctx context.Context, versionID string, status model.CacheCleanupStatus, cacheError string) error {
	if versionID == "" || status == "" {
		return fmt.Errorf("updating object deletion cache cleanup: %w", ErrInvalidInput)
	}
	now := time.Now()
	q := r.db.NewUpdate().
		Model((*model.ObjectDeletion)(nil)).
		Set("cache_cleanup_status = ?", status).
		Set("cache_cleaned_at = ?", now).
		Set("updated_at = ?", now)
	if cacheError == "" {
		q = q.Set("cache_error = NULL")
	} else {
		q = q.Set("cache_error = ?", cacheError)
	}
	res, err := q.Where("version_id = ?", versionID).Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating object deletion cache cleanup: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("updating object deletion cache cleanup: %w", ErrNotFound)
	}
	return nil
}

func (r *BunObjectRepo) RestoreCurrentDeleteMarkerStack(ctx context.Context, bucketID int64, key string, currentMarkerVersionID string) (*model.ObjectVersion, error) {
	if bucketID == 0 || key == "" || currentMarkerVersionID == "" {
		return nil, fmt.Errorf("restoring delete marker stack: %w", ErrInvalidInput)
	}
	var restored *model.ObjectVersion
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		version, err := restoreCurrentDeleteMarkerStack(ctx, db, bucketID, key, currentMarkerVersionID)
		restored = version
		return err
	})
	if err != nil {
		return nil, err
	}
	return restored, nil
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
		Where("object_version.is_delete_marker = ?", false).
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

func (r *BunObjectRepo) FindReusableReplicatingVersion(ctx context.Context, bucketID int64, size int64, checksum string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := r.db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.bucket_id = ? AND object_version.size = ? AND object_version.checksum = ?", bucketID, size, checksum).
		Where("object_version.is_delete_marker = ?", false).
		Where("object_version.state = ?", model.ObjectStateReplicating).
		Where(usableCopyExistsSQL("object_version.storage_upload_id")).
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("finding reusable replicating object version: %w", err)
	}
	return version, nil
}

func (r *BunObjectRepo) FindReusableActiveUploadVersion(ctx context.Context, bucketID int64, size int64, checksum string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	err := r.db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version").
		ColumnExpr("object_version.*").
		Where("object_version.bucket_id = ? AND object_version.size = ? AND object_version.checksum = ?", bucketID, size, checksum).
		Where("object_version.is_delete_marker = ?", false).
		Where("object_version.state IN (?)", bun.List([]model.ObjectState{model.ObjectStateCached, model.ObjectStateUploading, model.ObjectStateCommitting})).
		Where("object_version.in_cache = ?", true).
		Where(`(
			EXISTS (
				SELECT 1 FROM storage_uploads AS active_upload
				WHERE active_upload.source_version_id = object_version.version_id
				  AND active_upload.status IN (?)
				  AND (
					(object_version.state = ? AND active_upload.status IN (?, ?))
					OR (
						object_version.state = ?
						AND `+usableCopyExistsSQL("active_upload.id")+`
					)
				  )
			)
			OR (
				object_version.state IN (?, ?)
				AND EXISTS (
					SELECT 1 FROM tasks AS active_task
					WHERE active_task.ref_type = ?
					  AND active_task.ref_version_id = object_version.version_id
					  AND active_task.type = ?
					  AND active_task.status IN (?)
				)
			)
		)`,
			bun.List(activeUploadStatuses()),
			model.ObjectStateUploading, model.StorageUploadStatusRunning, model.StorageUploadStatusIngressReady,
			model.ObjectStateCommitting,
			model.ObjectStateCached, model.ObjectStateUploading,
			"object", model.TaskTypeUpload, bun.List(activeTaskStatuses()),
		).
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
		Where("object_version.is_delete_marker = ?", false).
		OrderExpr("object_version.key ASC")

	if prefix != "" {
		q = applyCaseSensitivePrefixFilter(r.db, q, "object_version.key", prefix)
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
		q = applyCaseSensitivePrefixFilter(r.db, q, "object_version.key", prefix)
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

func (r *BunObjectRepo) ListRecoverableDeleteMarkers(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]RecoverableDeleteMarker, error) {
	var markers []model.ObjectVersion
	q := r.db.NewSelect().
		Model(&markers).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version").
		Where("object_version.bucket_id = ?", bucketID).
		Where("object_version.is_current = ?", true).
		Where("object_version.is_delete_marker = ?", true).
		Where("EXISTS (SELECT 1 FROM object_versions AS data_version WHERE data_version.object_id = object_version.object_id AND data_version.is_delete_marker = ?)", false).
		OrderExpr("object_version.key ASC")

	if prefix != "" {
		q = applyCaseSensitivePrefixFilter(r.db, q, "object_version.key", prefix)
	}
	if afterKey != "" {
		q = q.Where("object_version.key > ?", afterKey)
	}
	if maxKeys > 0 {
		q = q.Limit(maxKeys)
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing recoverable delete markers: %w", err)
	}

	items := make([]RecoverableDeleteMarker, 0, len(markers))
	objectIDs := make([]int64, 0, len(markers))
	for _, marker := range markers {
		objectIDs = append(objectIDs, marker.ObjectID)
	}
	restoreVersions, err := selectLatestDataVersionsByObjectIDs(ctx, r.db, objectIDs)
	if err != nil {
		return nil, err
	}
	for _, marker := range markers {
		restoreVersion, ok := restoreVersions[marker.ObjectID]
		if !ok {
			continue
		}
		items = append(items, RecoverableDeleteMarker{
			Marker:         marker,
			RestoreVersion: restoreVersion,
		})
	}
	return items, nil
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
			Set("storage_upload_id = NULL").
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
		query := `UPDATE object_versions
			SET state = ?, failed_at_state = state, last_error = ?, updated_at = ?
			WHERE bucket_id = ? AND size = ? AND checksum = ? AND state IN (?, ?)
			  AND (
				version_id = ?
				OR NOT EXISTS (
					SELECT 1 FROM tasks
					WHERE tasks.ref_type = ?
					  AND tasks.ref_version_id = object_versions.version_id
					  AND tasks.type = ?
					  AND tasks.status IN (?)
				)
				AND NOT EXISTS (
					SELECT 1 FROM storage_uploads AS active_upload
					WHERE active_upload.source_version_id = object_versions.version_id
					  AND active_upload.status IN (?)
				)
			  )
			RETURNING object_id, version_id`
		err := db.NewRaw(query,
			model.ObjectStateFailed,
			lastError,
			now,
			bucketID,
			size,
			checksum,
			model.ObjectStateUploading,
			model.ObjectStateCommitting,
			leaderVersionID,
			"object",
			model.TaskTypeUpload,
			bun.List(activeTaskStatuses()),
			bun.List(activeUploadStatuses()),
		).Scan(ctx, &refs)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("no active upload object versions matched content: %w", ErrNotFound)
			}
			return fmt.Errorf("failing active upload object versions: %w", err)
		}
		if len(refs) == 0 {
			return fmt.Errorf("no active upload object versions matched content: %w", ErrNotFound)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

func (r *BunObjectRepo) ListVersionsByState(ctx context.Context, state model.ObjectState, limit int) ([]model.ObjectVersion, error) {
	return r.ListVersionsByStateAfter(ctx, state, time.Time{}, "", limit)
}

func (r *BunObjectRepo) ListVersionsByStateAfter(ctx context.Context, state model.ObjectState, afterUpdatedAt time.Time, afterVersionID string, limit int) ([]model.ObjectVersion, error) {
	var versions []model.ObjectVersion
	q := r.db.NewSelect().
		Model(&versions).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version").
		Where("object_version.state = ?", state).
		Where("object_version.is_delete_marker = ?", false).
		OrderExpr("object_version.updated_at ASC, object_version.version_id ASC")
	if !afterUpdatedAt.IsZero() || afterVersionID != "" {
		q = q.Where("(object_version.updated_at > ? OR (object_version.updated_at = ? AND object_version.version_id > ?))", afterUpdatedAt, afterUpdatedAt, afterVersionID)
	}
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
		Where("is_delete_marker = ?", false).
		GroupExpr("state").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting current object versions by state: %w", err)
	}
	return counts, nil
}

// AggregateByState returns current object counts and sizes grouped by state.
func (r *BunObjectRepo) AggregateByState(ctx context.Context) ([]ObjectStateAggregate, error) {
	var rows []ObjectStateAggregate
	err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		ColumnExpr("state, COUNT(*) AS count, COALESCE(SUM(size), 0) AS total_size").
		Where("is_current = ?", true).
		Where("is_delete_marker = ?", false).
		GroupExpr("state").
		Scan(ctx, &rows)
	if err != nil {
		return nil, fmt.Errorf("aggregating current object versions by state: %w", err)
	}
	return rows, nil
}

func (r *BunObjectRepo) CountOverviewAttention(ctx context.Context) (ObjectAttentionCount, error) {
	var count ObjectAttentionCount
	query := `WITH current_versions AS (
			SELECT version_id, storage_upload_id, state, in_cache
			FROM object_versions
			WHERE is_current = ?
			  AND is_delete_marker = ?
		),
		latest_upload_refs AS (
			SELECT source_upload.source_version_id, MAX(source_upload.id) AS latest_upload_id
			FROM storage_uploads AS source_upload
			JOIN current_versions AS current_version
			  ON current_version.version_id = source_upload.source_version_id
			 AND current_version.storage_upload_id IS NULL
			WHERE source_upload.source_version_id <> ''
			GROUP BY source_upload.source_version_id
		)
		SELECT
			COALESCE(SUM(CASE WHEN current_version.state = ? OR latest_upload.status IN (?, ?) THEN 1 ELSE 0 END), 0) AS needs_attention,
			COALESCE(SUM(CASE WHEN current_version.in_cache = ? AND NOT (current_version.state IN (?, ?, ?) AND ` + usableCopyExistsSQL("current_version.storage_upload_id") + `) THEN 1 ELSE 0 END), 0) AS unavailable
		FROM current_versions AS current_version
		LEFT JOIN latest_upload_refs AS latest_upload_ref
		  ON latest_upload_ref.source_version_id = current_version.version_id
		LEFT JOIN storage_uploads AS latest_upload
		  ON latest_upload.id = COALESCE(current_version.storage_upload_id, latest_upload_ref.latest_upload_id)`
	err := r.db.NewRaw(query,
		true,
		false,
		model.ObjectStateFailed,
		model.StorageUploadStatusFailed,
		model.StorageUploadStatusRejected,
		false,
		model.ObjectStateReplicating,
		model.ObjectStateStored,
		model.ObjectStateCacheEvicted,
	).Scan(ctx, &count)
	if err != nil {
		return ObjectAttentionCount{}, fmt.Errorf("counting overview object attention: %w", err)
	}
	return count, nil
}

// CountByBucket returns the number of current objects in a bucket.
func (r *BunObjectRepo) CountByBucket(ctx context.Context, bucketID int64) (int64, error) {
	count, err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("bucket_id = ? AND is_current = ?", bucketID, true).
		Where("is_delete_marker = ?", false).
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
		Where("is_delete_marker = ?", false).
		Scan(ctx, &total)
	if err != nil {
		return 0, fmt.Errorf("computing total current size for bucket %d: %w", bucketID, err)
	}
	return total, nil
}

// BucketStats returns current object count and total size for a single bucket.
func (r *BunObjectRepo) BucketStats(ctx context.Context, bucketID int64) (BucketObjectStats, error) {
	var stats BucketObjectStats
	err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		ColumnExpr("COUNT(*) AS count").
		ColumnExpr("COALESCE(SUM(size), 0) AS total_size").
		Where("bucket_id = ? AND is_current = ?", bucketID, true).
		Where("is_delete_marker = ?", false).
		Scan(ctx, &stats)
	if err != nil {
		return BucketObjectStats{}, fmt.Errorf("getting stats for bucket %d: %w", bucketID, err)
	}
	return stats, nil
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
		Where("is_delete_marker = ?", false).
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
		ColumnExpr("CASE WHEN " + alias + ".state IN ('replicating', 'stored', 'cache_evicted') AND " + usableCopyExistsSQL(alias+".storage_upload_id") + " THEN TRUE ELSE FALSE END AS in_filecoin").
		Join("LEFT JOIN storage_uploads AS storage_upload ON storage_upload.id = " + alias + ".storage_upload_id")
}

func usableCopyExistsSQL(uploadIDExpr string) string {
	return "EXISTS (SELECT 1 FROM storage_upload_copies AS storage_copy JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id WHERE storage_copy.upload_id = " + uploadIDExpr + " AND storage_copy.status = 'committed' AND storage_copy.storage_data_set_id IS NOT NULL AND storage_copy.provider_id IS NOT NULL AND storage_copy.provider_id <> '' AND storage_data_set.data_set_id IS NOT NULL AND storage_data_set.data_set_id <> '' AND storage_data_set.status IN ('ready', 'draining') AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> '' AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> '')"
}

func objectVersionPermanentDeleteStateAllowed(state model.ObjectState) bool {
	switch state {
	case model.ObjectStateCached, model.ObjectStateStored, model.ObjectStateCacheEvicted, model.ObjectStateFailed:
		return true
	default:
		return false
	}
}

func ensureObjectVersionHasNoActiveWork(ctx context.Context, db bun.IDB, versionID string) error {
	uploadCount, err := db.NewSelect().
		Model((*model.StorageUpload)(nil)).
		Where("source_version_id = ? AND status IN (?)", versionID, bun.List(activeUploadStatuses())).
		Count(ctx)
	if err != nil {
		return fmt.Errorf("checking active upload for permanent delete: %w", err)
	}
	if uploadCount > 0 {
		return ErrConflict
	}
	taskCount, err := db.NewSelect().
		Model((*model.Task)(nil)).
		Where("ref_type = ? AND ref_version_id = ? AND status IN (?)", "object", versionID, bun.List(activeTaskStatuses())).
		Count(ctx)
	if err != nil {
		return fmt.Errorf("checking active task for permanent delete: %w", err)
	}
	if taskCount > 0 {
		return ErrConflict
	}
	return nil
}

const defaultStorageCleanupMaxRetries = 5

func createStorageCleanupTaskForDeletedVersion(ctx context.Context, db bun.IDB, uploadID int64, deletedVersionID string, maxRetries *int) (*int64, error) {
	return createStorageCleanupTaskForDeletedVersions(ctx, db, uploadID, []string{deletedVersionID}, maxRetries)
}

func createStorageCleanupTaskForDeletedVersions(ctx context.Context, db bun.IDB, uploadID int64, deletedVersionIDs []string, maxRetries *int) (*int64, error) {
	if uploadID == 0 || len(deletedVersionIDs) == 0 {
		return nil, fmt.Errorf("creating storage cleanup task: %w", ErrInvalidInput)
	}
	copies, err := storageCleanupCopySnapshots(ctx, db, uploadID)
	if err != nil {
		return nil, err
	}
	if len(copies) == 0 {
		return nil, nil
	}

	idempotencyKey := fmt.Sprintf("storage_cleanup:%d", uploadID)
	task := new(model.Task)
	err = db.NewSelect().
		Model(task).
		Where("idempotency_key = ?", idempotencyKey).
		Scan(ctx)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("selecting storage cleanup task: %w", err)
	}
	if err == nil {
		return reuseStorageCleanupTask(ctx, db, task, uploadID, deletedVersionIDs, maxRetries)
	}

	now := time.Now()
	maxRetriesValue := storageCleanupMaxRetries(maxRetries)
	task = &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          uploadID,
		RefVersionID:   "",
		IdempotencyKey: idempotencyKey,
		Payload:        storageCleanupTaskPayload(uploadID, deletedVersionIDs),
		Status:         model.TaskStatusQueued,
		MaxRetries:     maxRetriesValue,
		ScheduledAt:    now,
	}
	res, err := db.NewInsert().
		Model(task).
		On("CONFLICT (idempotency_key) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating storage cleanup task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		task = new(model.Task)
		if err := db.NewSelect().
			Model(task).
			Where("idempotency_key = ?", idempotencyKey).
			Scan(ctx); err != nil {
			return nil, fmt.Errorf("selecting concurrent storage cleanup task: %w", err)
		}
		return reuseStorageCleanupTask(ctx, db, task, uploadID, deletedVersionIDs, maxRetries)
	}
	if maxRetriesValue == 0 {
		if _, err := db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("max_retries = 0").
			Where("id = ?", task.ID).
			Exec(ctx); err != nil {
			return nil, fmt.Errorf("setting storage cleanup task max retries: %w", err)
		}
	}
	for i := range copies {
		copies[i].TaskID = task.ID
		copies[i].CreatedAt = now
		copies[i].UpdatedAt = now
	}
	if _, err := db.NewInsert().Model(&copies).Exec(ctx); err != nil {
		return nil, fmt.Errorf("creating storage cleanup copy snapshots: %w", err)
	}
	return &task.ID, nil
}

func reuseStorageCleanupTask(ctx context.Context, db bun.IDB, task *model.Task, uploadID int64, deletedVersionIDs []string, maxRetries *int) (*int64, error) {
	if task == nil {
		return nil, fmt.Errorf("reusing storage cleanup task: %w", ErrInvalidInput)
	}
	if task.Status != model.TaskStatusCompleted {
		if taskStatusIsActive(task.Status) {
			if err := updateStorageCleanupTaskPayload(ctx, db, task, uploadID, deletedVersionIDs); err != nil {
				return nil, err
			}
		}
		return &task.ID, nil
	}

	remainingCopies, err := db.NewSelect().
		Model((*model.StorageCleanupCopy)(nil)).
		Where("task_id = ? AND status <> ?", task.ID, model.StorageCleanupCopyStatusRemoved).
		Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("checking retained storage cleanup copies: %w", err)
	}
	if remainingCopies == 0 {
		return &task.ID, nil
	}

	now := time.Now()
	res, err := db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusQueued).
		Set("retry_count = 0").
		Set("max_retries = ?", storageCleanupMaxRetries(maxRetries)).
		Set("payload = ?", mergeStorageCleanupTaskPayload(task.Payload, uploadID, deletedVersionIDs)).
		Set("scheduled_at = ?", now).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Set("completed_at = NULL").
		Set("last_error = NULL").
		Set("wait_reason = NULL").
		Set("status_message = NULL").
		Where("id = ? AND status = ?", task.ID, model.TaskStatusCompleted).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("requeueing retained storage cleanup task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return nil, fmt.Errorf("requeueing retained storage cleanup task %d: %w", task.ID, ErrConflict)
	}
	return &task.ID, nil
}

func updateStorageCleanupTaskPayload(ctx context.Context, db bun.IDB, task *model.Task, uploadID int64, deletedVersionIDs []string) error {
	payload := mergeStorageCleanupTaskPayload(task.Payload, uploadID, deletedVersionIDs)
	res, err := db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("payload = ?", payload).
		Where("id = ?", task.ID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating storage cleanup task payload: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("updating storage cleanup task payload %d: %w", task.ID, ErrNotFound)
	}
	return nil
}

func taskStatusIsActive(status model.TaskStatus) bool {
	for _, active := range activeTaskStatuses() {
		if status == active {
			return true
		}
	}
	return false
}

func storageCleanupMaxRetries(maxRetries *int) int {
	if maxRetries == nil {
		return defaultStorageCleanupMaxRetries
	}
	return *maxRetries
}

func storageCleanupTaskPayload(uploadID int64, deletedVersionIDs []string) map[string]interface{} {
	payload := map[string]interface{}{
		"storage_upload_id": uploadID,
	}
	if len(deletedVersionIDs) > 0 {
		payload["deleted_source_version"] = deletedVersionIDs[0]
	}
	payload["deleted_source_versions"] = deletedVersionIDs
	return payload
}

func mergeStorageCleanupTaskPayload(existing map[string]interface{}, uploadID int64, deletedVersionIDs []string) map[string]interface{} {
	merged := storageCleanupPayloadVersionIDs(existing)
	for _, versionID := range deletedVersionIDs {
		merged = appendUniqueString(merged, versionID)
	}
	return storageCleanupTaskPayload(uploadID, merged)
}

func storageCleanupPayloadVersionIDs(payload map[string]interface{}) []string {
	var out []string
	if payload == nil {
		return out
	}

	switch values := payload["deleted_source_versions"].(type) {
	case []string:
		for _, value := range values {
			out = appendUniqueString(out, value)
		}
	case []interface{}:
		for _, value := range values {
			text, ok := value.(string)
			if ok {
				out = appendUniqueString(out, text)
			}
		}
	}
	legacy, ok := payload["deleted_source_version"].(string)
	if ok {
		out = appendUniqueString(out, legacy)
	}
	return out
}

func appendUniqueString(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func storageCleanupCopySnapshots(ctx context.Context, db bun.IDB, uploadID int64) ([]model.StorageCleanupCopy, error) {
	var copies []model.StorageCleanupCopy
	err := db.NewRaw(`SELECT
			storage_copy.upload_id,
			storage_copy.copy_index,
			storage_copy.provider_id,
			storage_copy.storage_data_set_id,
			storage_data_set.data_set_id,
			storage_data_set.client_data_set_id,
			storage_copy.piece_id,
			storage_upload.piece_cid,
			storage_copy.retrieval_url,
			? AS status
		FROM storage_upload_copies AS storage_copy
		JOIN storage_uploads AS storage_upload ON storage_upload.id = storage_copy.upload_id
		LEFT JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		WHERE storage_copy.upload_id = ? AND storage_copy.status = ?
		ORDER BY storage_copy.copy_index ASC`,
		model.StorageCleanupCopyStatusPending,
		uploadID,
		model.StorageUploadCopyStatusCommitted,
	).Scan(ctx, &copies)
	if err != nil {
		return nil, fmt.Errorf("loading storage cleanup copy snapshots: %w", err)
	}
	return copies, nil
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

func createDeleteMarkerAndSetCurrent(ctx context.Context, db bun.IDB, marker *model.ObjectVersion) error {
	now := time.Now()
	if err := lockCurrentObjectIfExists(ctx, db, marker.BucketID, marker.Key); err != nil {
		return err
	}
	existing, err := selectObjectByBucketAndKey(ctx, db, marker.BucketID, marker.Key)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("checking existing object: %w", err)
	}
	if err == sql.ErrNoRows {
		obj := objectIdentityFromVersion(marker)
		obj.CreatedAt = now
		obj.UpdatedAt = now
		if _, insertErr := db.NewInsert().Model(obj).Exec(ctx); insertErr != nil {
			if !isUniqueViolation(insertErr) {
				return fmt.Errorf("inserting new object for delete marker: %w", insertErr)
			}
			return fmt.Errorf("%w: inserting new object for delete marker: %w", errConcurrentObjectCreate, insertErr)
		}
		marker.ObjectID = obj.ID
	} else {
		marker.ObjectID = existing.ID
		if _, updateErr := db.NewUpdate().
			Model((*model.Object)(nil)).
			Set("updated_at = ?", now).
			Where("id = ?", existing.ID).
			Exec(ctx); updateErr != nil {
			return fmt.Errorf("updating object identity timestamp: %w", updateErr)
		}
	}

	marker.IsCurrent = true
	marker.IsDeleteMarker = true
	marker.Size = 0
	marker.ETag = ""
	marker.Checksum = ""
	marker.ContentType = ""
	marker.CacheKey = ""
	marker.StorageUploadID = nil
	marker.InCache = false
	marker.State = model.ObjectStateCached
	marker.FailedAtState = nil
	marker.LastError = nil
	if marker.CreatedAt.IsZero() {
		marker.CreatedAt = now
	}
	if marker.UpdatedAt.IsZero() {
		marker.UpdatedAt = now
	}

	if _, err := db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("is_current = ?", false).
		Where("object_id = ? AND is_current = ?", marker.ObjectID, true).
		Exec(ctx); err != nil {
		return fmt.Errorf("clearing previous current version: %w", err)
	}
	if _, err := db.NewInsert().
		Model(marker).
		Column("version_id", "object_id", "bucket_id", "key", "size", "e_tag", "checksum", "content_type", "metadata", "cache_key", "storage_upload_id", "in_cache", "is_current", "is_delete_marker", "state", "failed_at_state", "last_error", "created_at", "updated_at").
		Value("content_type", "?", "").
		Value("in_cache", "?", false).
		Exec(ctx); err != nil {
		return fmt.Errorf("inserting delete marker: %w", err)
	}
	return nil
}

func deleteMarkerVersion(ctx context.Context, db bun.IDB, bucketID int64, key string, versionID string) error {
	if err := lockCurrentObjectIfExists(ctx, db, bucketID, key); err != nil {
		return err
	}
	version, err := selectVersionByBucketKeyAndID(ctx, db, bucketID, key, versionID)
	if err != nil {
		return err
	}
	if version == nil {
		return fmt.Errorf("deleting marker version: %w", ErrNotFound)
	}
	if !version.IsDeleteMarker {
		return fmt.Errorf("deleting marker version: %w", ErrInvalidInput)
	}

	if _, err := db.NewDelete().
		Model((*model.ObjectVersion)(nil)).
		Where("bucket_id = ? AND key = ? AND version_id = ?", bucketID, key, versionID).
		Exec(ctx); err != nil {
		return fmt.Errorf("deleting marker version: %w", err)
	}
	if !version.IsCurrent {
		return nil
	}
	return promoteLatestVersionOrDeleteObject(ctx, db, version.ObjectID)
}

func restoreCurrentDeleteMarkerStack(ctx context.Context, db bun.IDB, bucketID int64, key string, currentMarkerVersionID string) (*model.ObjectVersion, error) {
	if err := lockCurrentObjectIfExists(ctx, db, bucketID, key); err != nil {
		return nil, err
	}
	current, err := selectCurrentVersionByBucketAndKey(ctx, db, bucketID, key)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, fmt.Errorf("restoring delete marker stack: %w", ErrNotFound)
	}
	if !current.IsDeleteMarker || current.VersionID != currentMarkerVersionID {
		return nil, fmt.Errorf("restoring delete marker stack: %w", ErrConflict)
	}

	versions, err := selectVersionsByObjectNewestFirst(ctx, db, current.ObjectID)
	if err != nil {
		return nil, err
	}
	markerIDs := make([]string, 0)
	var restoreTarget *model.ObjectVersion
	for i := range versions {
		version := versions[i]
		if version.IsDeleteMarker {
			markerIDs = append(markerIDs, version.VersionID)
			continue
		}
		restoreTarget = &version
		break
	}
	if restoreTarget == nil {
		return nil, fmt.Errorf("restoring delete marker stack: %w", ErrNotFound)
	}
	if len(markerIDs) == 0 {
		return nil, fmt.Errorf("restoring delete marker stack: %w", ErrConflict)
	}

	if _, err := db.NewDelete().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ? AND version_id IN (?)", current.ObjectID, bun.List(markerIDs)).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("deleting marker stack: %w", err)
	}
	if _, err := db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("is_current = ?", true).
		Where("version_id = ?", restoreTarget.VersionID).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("restoring data version current flag: %w", err)
	}
	if _, err := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", current.ObjectID).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("updating object identity timestamp: %w", err)
	}
	restoreTarget.IsCurrent = true
	return restoreTarget, nil
}

func promoteLatestVersionOrDeleteObject(ctx context.Context, db bun.IDB, objectID int64) error {
	latest, err := selectLatestVersionByObjectID(ctx, db, objectID)
	if err != nil {
		return err
	}
	if latest == nil {
		if _, err := db.NewDelete().
			Model((*model.Object)(nil)).
			Where("id = ?", objectID).
			Exec(ctx); err != nil {
			return fmt.Errorf("deleting empty object identity: %w", err)
		}
		return nil
	}
	if _, err := db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("is_current = (version_id = ?)", latest.VersionID).
		Where("object_id = ?", objectID).
		Exec(ctx); err != nil {
		return fmt.Errorf("promoting latest version: %w", err)
	}
	if _, err := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", objectID).
		Exec(ctx); err != nil {
		return fmt.Errorf("updating object identity timestamp: %w", err)
	}
	return nil
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

func selectCurrentVersionByBucketAndKey(ctx context.Context, db bun.IDB, bucketID int64, key string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := db.NewSelect().
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

func selectVersionByBucketKeyAndID(ctx context.Context, db bun.IDB, bucketID int64, key string, versionID string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := db.NewSelect().
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

func selectLatestVersionByObjectID(ctx context.Context, db bun.IDB, objectID int64) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.object_id = ?", objectID).
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting latest object version: %w", err)
	}
	return version, nil
}

func selectLatestDataVersionsByObjectIDs(ctx context.Context, db bun.IDB, objectIDs []int64) (map[int64]model.ObjectVersion, error) {
	if len(objectIDs) == 0 {
		return map[int64]model.ObjectVersion{}, nil
	}

	var versions []model.ObjectVersion
	q := db.NewSelect().
		Model(&versions).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.object_id IN (?)", bun.List(objectIDs)).
		Where("object_version.is_delete_marker = ?", false).
		Where(`NOT EXISTS (
			SELECT 1 FROM object_versions AS newer_data_version
			WHERE newer_data_version.object_id = object_version.object_id
			  AND newer_data_version.is_delete_marker = ?
			  AND (
			    newer_data_version.created_at > object_version.created_at
			    OR (newer_data_version.created_at = object_version.created_at AND newer_data_version.version_id > object_version.version_id)
			  )
		)`, false).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("selecting latest data versions: %w", err)
	}

	byObjectID := make(map[int64]model.ObjectVersion, len(versions))
	for _, version := range versions {
		byObjectID[version.ObjectID] = version
	}
	return byObjectID, nil
}

func selectVersionsByObjectNewestFirst(ctx context.Context, db bun.IDB, objectID int64) ([]model.ObjectVersion, error) {
	var versions []model.ObjectVersion
	q := db.NewSelect().
		Model(&versions).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.object_id = ?", objectID).
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("selecting object versions by object ID: %w", err)
	}
	return versions, nil
}

func lockCurrentObjectIfExists(ctx context.Context, db bun.IDB, bucketID int64, key string) error {
	// Use an UPDATE as a cross-dialect row lock; SELECT FOR UPDATE is not supported by SQLite.
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
	if current == nil || current.State == model.ObjectStateFailed || current.IsDeleteMarker {
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
