package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
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
		delay := min(time.Duration(attempt+1)*25*time.Millisecond, 200*time.Millisecond)
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
		delay := min(time.Duration(attempt+1)*25*time.Millisecond, 200*time.Millisecond)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ObjectVersionWriteResult{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *BunObjectRepo) CreateRestoredVersionAndSetCurrent(ctx context.Context, version *model.ObjectVersion, sourceVersionID, expectedCurrentVersionID string) (int64, error) {
	if version == nil || version.BucketID == 0 || version.Key == "" || version.VersionID == "" || version.IsDeleteMarker || sourceVersionID == "" || expectedCurrentVersionID == "" {
		return 0, fmt.Errorf("creating restored object version: %w", ErrInvalidInput)
	}

	var objectID int64
	_, canRestartTx := r.db.(*bun.DB)
	for attempt := 0; ; attempt++ {
		err := r.runMaybeTx(ctx, func(db bun.IDB) error {
			id, err := createRestoredVersionAndSetCurrent(ctx, db, version, sourceVersionID, expectedCurrentVersionID)
			objectID = id
			return err
		})
		if err == nil {
			return objectID, nil
		}
		if !shouldRetryObjectWrite(err, canRestartTx) || attempt >= 19 {
			return 0, err
		}
		delay := min(time.Duration(attempt+1)*25*time.Millisecond, 200*time.Millisecond)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
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
		ContentType:    "",
		Metadata:       map[string]string{},
		IsDeleteMarker: true,
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
		delay := min(time.Duration(attempt+1)*25*time.Millisecond, 200*time.Millisecond)
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
		version, err := selectVersionByBucketKeyAndID(ctx, db, input.BucketID, input.Key, input.VersionID)
		if err != nil {
			return err
		}
		if version == nil {
			return ErrNotFound
		}
		preliminaryUploads, err := authoritativeStorageContentsForVersions(ctx, db, []*model.ObjectVersion{version})
		if err != nil {
			return err
		}
		lockedUploads, err := lockStorageContentsByID(ctx, db, sortedContentIDs(preliminaryUploads))
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return ErrPermanentDeleteStorageBusy
			}
			return err
		}
		if err := lockCurrentObjectIfExists(ctx, db, input.BucketID, input.Key); err != nil {
			return err
		}
		if err := lockObjectVersionsByID(ctx, db, []string{input.VersionID}); err != nil {
			return err
		}
		version, err = selectVersionByBucketKeyAndID(ctx, db, input.BucketID, input.Key, input.VersionID)
		if err != nil {
			return err
		}
		if version == nil {
			return ErrNotFound
		}
		if version.IsDeleteMarker {
			return ErrConflict
		}
		if err := objectVersionPermanentDeleteStateError(version.State); err != nil {
			return err
		}
		currentUploads, err := authoritativeStorageContentsForVersions(ctx, db, []*model.ObjectVersion{version})
		if err != nil {
			return err
		}
		if !sameContentIDs(preliminaryUploads, currentUploads) {
			return ErrPermanentDeleteStorageBusy
		}
		if err := prepareObjectVersionsForPermanentDelete(ctx, db, []*model.ObjectVersion{version}, lockedUploads); err != nil {
			return err
		}
		wasCurrent := version.IsCurrent
		objectID := version.ObjectID

		now := time.Now()
		deletion := &model.ObjectDeletion{
			BucketID:  version.BucketID,
			ObjectID:  version.ObjectID,
			Key:       version.Key,
			VersionID: version.VersionID,
			ContentID: version.ContentID,
			Size:      version.Size,
			DeletedAt: now,
		}
		if _, err := db.NewInsert().Model(deletion).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return ErrAlreadyExists
			}
			return fmt.Errorf("recording object deletion: %w", err)
		}
		result.DeletionID = deletion.ID
		result.ContentID = version.ContentID

		if version.ContentID != nil {
			cleanup, err := reserveStorageCleanupForDeletedVersions(ctx, db, *version.ContentID, []string{version.VersionID})
			if err != nil {
				return err
			}
			result.StorageCleanup = cleanup
		}

		if wasCurrent {
			if err := repointObjectAwayFromVersion(ctx, db, objectID, version.VersionID); err != nil {
				return err
			}
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
			if err := deleteObjectIdentityIfEmpty(ctx, db, objectID); err != nil {
				return err
			}
		}
		// Only ask once the row is gone: while this version still existed it
		// would count as a reference to its own content and the shared cache
		// file would never be released.
		if version.ContentID != nil {
			unreferenced, err := contentIsUnreferenced(ctx, db, *version.ContentID)
			if err != nil {
				return err
			}
			result.ContentUnreferenced = unreferenced
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
		versionIDs := make([]string, 0, len(versions))
		for i := range versions {
			versionIDs = append(versionIDs, versions[i].VersionID)
		}
		preliminaryDataVersions := dataObjectVersionPointers(versions)
		preliminaryUploads, err := authoritativeStorageContentsForVersions(ctx, db, preliminaryDataVersions)
		if err != nil {
			return err
		}
		lockedUploads, err := lockStorageContentsByID(ctx, db, sortedContentIDs(preliminaryUploads))
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return ErrPermanentDeleteStorageBusy
			}
			return err
		}
		if err := lockCurrentObjectIfExists(ctx, db, input.BucketID, input.Key); err != nil {
			return err
		}
		if err := lockObjectVersionsByID(ctx, db, versionIDs); err != nil {
			return err
		}
		current, err = selectCurrentVersionByBucketAndKey(ctx, db, input.BucketID, input.Key)
		if err != nil {
			return err
		}
		if current == nil {
			return ErrNotFound
		}
		if !current.IsDeleteMarker || current.VersionID != input.DeleteMarkerVersionID {
			return ErrConflict
		}
		versions, err = selectVersionsByObjectNewestFirst(ctx, db, current.ObjectID)
		if err != nil {
			return err
		}
		if !sameObjectVersionIDs(versions, versionIDs) {
			return ErrConflict
		}
		dataVersions := make([]*model.ObjectVersion, 0, len(versions))
		for i := range versions {
			if versions[i].IsDeleteMarker {
				continue
			}
			if err := objectVersionPermanentDeleteStateError(versions[i].State); err != nil {
				return err
			}
			dataVersions = append(dataVersions, &versions[i])
		}
		currentUploads, err := authoritativeStorageContentsForVersions(ctx, db, dataVersions)
		if err != nil {
			return err
		}
		if !sameContentIDs(preliminaryUploads, currentUploads) {
			return ErrPermanentDeleteStorageBusy
		}
		if err := prepareObjectVersionsForPermanentDelete(ctx, db, dataVersions, lockedUploads); err != nil {
			return err
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
			deletions = append(deletions, model.ObjectDeletion{
				BucketID:  version.BucketID,
				ObjectID:  version.ObjectID,
				Key:       version.Key,
				VersionID: version.VersionID,
				ContentID: version.ContentID,
				Size:      version.Size,
				DeletedAt: now,
			})
			result.DeletedVersions = append(result.DeletedVersions, DeletedObjectVersionSnapshot{
				VersionID: version.VersionID,
				ContentID: version.ContentID,
			})
			result.DataVersionsDeleted++
			if version.ContentID != nil {
				contentID := *version.ContentID
				deletedVersionIDsByUpload[contentID] = append(deletedVersionIDsByUpload[contentID], version.VersionID)
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

		for contentID, deletedVersionIDs := range deletedVersionIDsByUpload {
			cleanup, err := reserveStorageCleanupForDeletedVersions(ctx, db, contentID, deletedVersionIDs)
			if err != nil {
				return err
			}
			if cleanup != nil {
				result.StorageCleanups = append(result.StorageCleanups, *cleanup)
			}
		}

		// The object stops pointing at any version before the rows go; the
		// pointer's foreign key would otherwise refuse the delete.
		if _, err := db.NewUpdate().
			Model((*model.Object)(nil)).
			Set("current_version_id = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", current.ObjectID).
			Exec(ctx); err != nil {
			return fmt.Errorf("clearing object current version: %w", err)
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

		// Whether the cached bytes may go is answered only after the version
		// rows are gone, and inside this transaction, because a version of
		// another object can still name the same content.
		unreferenced := make(map[int64]bool, len(deletedVersionIDsByUpload))
		for contentID := range deletedVersionIDsByUpload {
			free, err := contentIsUnreferenced(ctx, db, contentID)
			if err != nil {
				return err
			}
			unreferenced[contentID] = free
		}
		for i := range result.DeletedVersions {
			if id := result.DeletedVersions[i].ContentID; id != nil {
				result.DeletedVersions[i].ContentUnreferenced = unreferenced[*id]
			}
		}
		return nil
	})
	if err != nil {
		return DeleteDeletedObjectResult{}, fmt.Errorf("permanently deleting deleted object: %w", err)
	}
	return result, nil
}

func (r *BunObjectRepo) ClearContentCachePresence(ctx context.Context, contentID int64) error {
	if contentID < 1 {
		return fmt.Errorf("clearing content cache presence: %w", ErrInvalidInput)
	}
	_, err := r.db.NewUpdate().
		Model((*model.ObjectCache)(nil)).
		Set("in_cache = ?", false).
		Set("cache_presence_generation = cache_presence_generation + 1").
		Set("updated_at = ?", time.Now()).
		Where("content_id = ?", contentID).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("clearing content cache presence: %w", err)
	}
	return nil
}

// contentIsUnreferenced reports whether any live object version still points at
// the content. Cache release and remote cleanup both hang off this answer, so it
// must be read inside the same transaction that removed the version.
func contentIsUnreferenced(ctx context.Context, db bun.IDB, contentID int64) (bool, error) {
	count, err := db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("content_id = ?", contentID).
		Count(ctx)
	if err != nil {
		return false, fmt.Errorf("counting live versions for content %d: %w", contentID, err)
	}
	return count == 0, nil
}

// ContentIsUnreferenced reports whether any live object version still points at
// the content, so callers outside a deletion transaction can decide whether
// releasing its cached bytes is safe.
func (r *BunObjectRepo) ContentIsUnreferenced(ctx context.Context, contentID int64) (bool, error) {
	if contentID <= 0 {
		return false, fmt.Errorf("checking content references: %w", ErrInvalidInput)
	}
	return contentIsUnreferenced(ctx, r.db, contentID)
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
	err := q.Where("object_version.object_id = ?", objectID).
		Where("current_object.current_version_id = object_version.version_id").Scan(ctx)
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
	err := q.Where("object_version.bucket_id = ? AND object_version.key = ?", bucketID, key).
		Where("current_object.current_version_id = object_version.version_id").Scan(ctx)
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

func (r *BunObjectRepo) ListCurrentVersionsByBucket(ctx context.Context, bucketID int64, prefix string, afterKey string, maxKeys int) ([]model.ObjectVersion, error) {
	return r.listCurrentVersionsByBucket(ctx, bucketID, prefix, afterKey, false, maxKeys)
}

func (r *BunObjectRepo) ListCurrentVersionsByBucketAtOrAfter(ctx context.Context, bucketID int64, prefix string, fromKey string, maxKeys int) ([]model.ObjectVersion, error) {
	return r.listCurrentVersionsByBucket(ctx, bucketID, prefix, fromKey, true, maxKeys)
}

func (r *BunObjectRepo) listCurrentVersionsByBucket(ctx context.Context, bucketID int64, prefix string, keyBoundary string, includeBoundary bool, maxKeys int) ([]model.ObjectVersion, error) {
	var versions []model.ObjectVersion
	// Listing walks the objects unique key and follows each pointer, so the
	// bucket+key ordering comes from objects rather than from a partial index
	// over every version.
	keyExpr := keyOrderExpr(r.db, "current_object.key")
	q := r.db.NewSelect().
		Model(&versions).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version").
		Where("current_object.bucket_id = ?", bucketID).
		Where("current_object.current_version_id = object_version.version_id").
		Where("object_version.is_delete_marker = ?", false).
		OrderExpr(keyExpr + " ASC")

	if prefix != "" {
		q = applyCaseSensitivePrefixFilter(r.db, q, "current_object.key", prefix)
	}
	if keyBoundary != "" {
		if includeBoundary {
			q = q.Where(keyComparisonSQL(r.db, "current_object.key", ">="), keyBoundary)
		} else {
			q = q.Where(keyComparisonSQL(r.db, "current_object.key", ">"), keyBoundary)
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
	keyExpr := keyOrderExpr(r.db, "object_version.key")
	q := r.db.NewSelect().
		Model(&rows).
		ModelTableExpr("object_versions AS object_version").
		Where("object_version.bucket_id = ?", bucketID).
		OrderExpr(keyExpr + " ASC").
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC")
	q = withObjectVersionStorageColumns(q, "object_version")

	if prefix != "" {
		q = applyCaseSensitivePrefixFilter(r.db, q, "object_version.key", prefix)
	}
	if keyMarker != "" {
		if versionIDMarker == "" {
			q = q.Where(keyComparisonSQL(r.db, "object_version.key", ">"), keyMarker)
		} else {
			marker, err := r.GetVersionByBucketKeyAndID(ctx, bucketID, keyMarker, versionIDMarker)
			if err != nil {
				return nil, err
			}
			if marker == nil {
				q = q.Where(keyComparisonSQL(r.db, "object_version.key", ">"), keyMarker)
			} else {
				q = q.Where("("+keyComparisonSQL(r.db, "object_version.key", ">")+") OR ("+keyComparisonSQL(r.db, "object_version.key", "=")+" AND (object_version.created_at < ? OR (object_version.created_at = ? AND object_version.version_id < ?)))",
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
	keyExpr := keyOrderExpr(r.db, "current_object.key")
	q := r.db.NewSelect().
		Model(&markers).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version").
		Where("current_object.bucket_id = ?", bucketID).
		Where("current_object.current_version_id = object_version.version_id").
		Where("object_version.is_delete_marker = ?", true).
		Where("EXISTS (SELECT 1 FROM object_versions AS data_version WHERE data_version.object_id = object_version.object_id AND data_version.is_delete_marker = ?)", false).
		OrderExpr(keyExpr + " ASC")

	if prefix != "" {
		q = applyCaseSensitivePrefixFilter(r.db, q, "current_object.key", prefix)
	}
	if afterKey != "" {
		q = q.Where(keyComparisonSQL(r.db, "current_object.key", ">"), afterKey)
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

func (r *BunObjectRepo) SetVersionCachePresence(ctx context.Context, versionID string, inCache bool) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		return setVersionCachePresence(ctx, db, versionID, inCache)
	})
}

// RecordContentCacheAccess advances LRU recency for one content payload without
// asserting that its bytes are present.
func (r *BunObjectRepo) RecordContentCacheAccess(ctx context.Context, contentID int64, accessedAt time.Time) error {
	return r.recordContentCacheAccess(ctx, contentID, accessedAt, false)
}

// RecordContentCacheCommit marks a freshly written cache file present and
// advances its recency in the same write.
func (r *BunObjectRepo) RecordContentCacheCommit(ctx context.Context, contentID int64, accessedAt time.Time) error {
	return r.recordContentCacheAccess(ctx, contentID, accessedAt, true)
}

func (r *BunObjectRepo) recordContentCacheAccess(ctx context.Context, contentID int64, accessedAt time.Time, present bool) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		if contentID < 1 {
			return nil
		}
		normalized := cacheeviction.NormalizeAccessTime(accessedAt)
		if present {
			return upsertContentCachePresence(ctx, db, contentID, true, &normalized)
		}
		// An access-only write must never claim the bytes are present: the
		// reader may have served them from a provider.
		if _, err := db.NewUpdate().
			Model((*model.ObjectCache)(nil)).
			Set(`cache_accessed_at = CASE
				WHEN cache_accessed_at IS NULL OR cache_accessed_at < ? THEN ?
				ELSE cache_accessed_at END`, normalized, normalized).
			Set("updated_at = ?", time.Now()).
			Where("content_id = ?", contentID).
			Exec(ctx); err != nil {
			return fmt.Errorf("recording content cache access: %w", err)
		}
		return nil
	})
}

func (r *BunObjectRepo) CountByState(ctx context.Context) ([]ObjectStateCount, error) {
	var counts []ObjectStateCount
	state := objectVersionStateSQL("object_version")
	err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		ColumnExpr(state+" AS state, COUNT(*) AS count").
		Join("LEFT JOIN storage_contents AS storage_content ON storage_content.id = object_version.content_id").
		Where(currentVersionPredicateSQL("object_version")).
		Where("object_version.is_delete_marker = ?", false).
		GroupExpr(state).
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting current object versions by state: %w", err)
	}
	return counts, nil
}

// AggregateByState returns current object counts and sizes grouped by state.
func (r *BunObjectRepo) AggregateByState(ctx context.Context) ([]ObjectStateAggregate, error) {
	var rows []ObjectStateAggregate
	state := objectVersionStateSQL("object_version")
	err := r.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		ColumnExpr(state+" AS state, COUNT(*) AS count, COALESCE(SUM(object_version.size), 0) AS total_size").
		Join("LEFT JOIN storage_contents AS storage_content ON storage_content.id = object_version.content_id").
		Where(currentVersionPredicateSQL("object_version")).
		Where("object_version.is_delete_marker = ?", false).
		GroupExpr(state).
		Scan(ctx, &rows)
	if err != nil {
		return nil, fmt.Errorf("aggregating current object versions by state: %w", err)
	}
	return rows, nil
}

func (r *BunObjectRepo) CountOverviewAttention(ctx context.Context) (ObjectAttentionCount, error) {
	var count ObjectAttentionCount
	// Pipeline position and cache residency are derived from the content's
	// copies and its cache entry, so the counts read those rather than columns
	// object_versions no longer owns.
	query := `WITH current_versions AS (
			SELECT object_version.version_id,
			       object_version.content_id,
			       ` + objectVersionStateSQL("object_version") + ` AS state,
			       COALESCE(cache_entry.in_cache, FALSE) AS in_cache
			FROM object_versions AS object_version
			LEFT JOIN storage_contents AS storage_content
			  ON storage_content.id = object_version.content_id
			LEFT JOIN object_cache AS cache_entry
			  ON cache_entry.content_id = object_version.content_id
			WHERE ` + currentVersionPredicateSQL("object_version") + `
			  AND object_version.is_delete_marker = ?
		)
		SELECT
			COALESCE(SUM(CASE WHEN current_version.state = ? OR version_content.error_message IS NOT NULL THEN 1 ELSE 0 END), 0) AS needs_attention,
			COALESCE(SUM(CASE WHEN current_version.in_cache = ? AND NOT ` + usableCopyExistsSQL("current_version.content_id") + ` THEN 1 ELSE 0 END), 0) AS unavailable
		FROM current_versions AS current_version
		LEFT JOIN storage_contents AS version_content
		  ON version_content.id = current_version.content_id`
	err := r.db.NewRaw(query,
		false,
		model.ObjectStateFailed,
		false,
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
		Where("bucket_id = ?", bucketID).
		Where(currentVersionPredicateSQL("object_version")).
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
		Where("bucket_id = ?", bucketID).
		Where(currentVersionPredicateSQL("object_version")).
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
		Where("bucket_id = ?", bucketID).
		Where(currentVersionPredicateSQL("object_version")).
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
		Where(currentVersionPredicateSQL("object_version")).
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

// withObjectVersionStorageColumns projects everything an object version no
// longer stores. Content identity lives on storage_contents, cache residency on
// object_cache, and pipeline position is a function of the copy rows. Every read
// that returns a model.ObjectVersion must go through here, or those fields come
// back as zero values rather than facts.
func withObjectVersionStorageColumns(q *bun.SelectQuery, alias string) *bun.SelectQuery {
	return q.
		ColumnExpr(alias + ".*").
		ColumnExpr("CASE WHEN current_object.current_version_id = " + alias + ".version_id THEN TRUE ELSE FALSE END AS is_current").
		ColumnExpr("COALESCE(storage_content.checksum, '') AS checksum").
		ColumnExpr("storage_content.piece_cid AS piece_cid").
		ColumnExpr("COALESCE(object_cache_entry.in_cache, FALSE) AS in_cache").
		ColumnExpr("object_cache_entry.cache_accessed_at AS cache_accessed_at").
		ColumnExpr("CASE WHEN " + usableCopyExistsSQL(alias+".content_id") + " THEN TRUE ELSE FALSE END AS in_filecoin").
		ColumnExpr(objectVersionStateSQL(alias) + " AS state").
		Join("JOIN objects AS current_object ON current_object.id = " + alias + ".object_id").
		Join("LEFT JOIN storage_contents AS storage_content ON storage_content.id = " + alias + ".content_id").
		Join("LEFT JOIN object_cache AS object_cache_entry ON object_cache_entry.content_id = " + alias + ".content_id")
}

// currentVersionPredicateSQL matches the one version an object currently serves.
// "Current" is the object's pointer, so it is asked of objects rather than of a
// flag repeated on every version.
func currentVersionPredicateSQL(alias string) string {
	return "EXISTS (SELECT 1 FROM objects AS current_pointer WHERE current_pointer.id = " + alias +
		".object_id AND current_pointer.current_version_id = " + alias + ".version_id)"
}

// objectVersionStateSQL derives how far a version's content has travelled. The
// value used to be a stored column that could disagree with the copies it
// summarised; deriving it makes that disagreement unrepresentable. A version
// with no content is a delete marker and has no pipeline to report.
func objectVersionStateSQL(alias string) string {
	return fmt.Sprintf("CASE WHEN %s.content_id IS NULL THEN '%s' ELSE (%s) END",
		alias, model.ObjectStateCached, contentPipelineStateSQL())
}

// contentPipelineStateSQL expects a storage_content alias in scope.
func contentPipelineStateSQL() string {
	contentID := "storage_content.id"
	readableSlots := distinctReadableSlotCountSQL("state_copy", "state_data_set", contentID)
	anyCopy := "EXISTS (SELECT 1 FROM storage_copies AS any_copy WHERE any_copy.content_id = " + contentID + ")"
	everyCopyFailed := "NOT EXISTS (SELECT 1 FROM storage_copies AS live_copy WHERE live_copy.content_id = " + contentID +
		" AND live_copy.status <> '" + string(model.StorageCopyStatusFailed) + "')"
	committing := "EXISTS (SELECT 1 FROM storage_copies AS committing_copy WHERE committing_copy.content_id = " + contentID +
		" AND committing_copy.status IN ('" + string(model.StorageCopyStatusPieceReady) + "', '" + string(model.StorageCopyStatusCommitting) + "'))"
	return fmt.Sprintf(`CASE
		WHEN %[1]s >= COALESCE(storage_content.requested_copies, 1) THEN '%[5]s'
		WHEN %[1]s >= 1 THEN '%[6]s'
		WHEN %[2]s AND %[3]s THEN '%[7]s'
		WHEN %[4]s THEN '%[8]s'
		WHEN %[2]s THEN '%[9]s'
		ELSE '%[10]s'
	END`,
		readableSlots, anyCopy, everyCopyFailed, committing,
		model.ObjectStateStored, model.ObjectStateReplicating, model.ObjectStateFailed,
		model.ObjectStateCommitting, model.ObjectStateUploading, model.ObjectStateCached,
	)
}

func usableCopyExistsSQL(contentIDExpr string) string {
	return "EXISTS (SELECT 1 FROM storage_copies AS storage_copy JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id WHERE storage_copy.content_id = " + contentIDExpr + " AND storage_copy.status = 'committed' AND storage_copy.provider_id <> '' AND storage_data_set.data_set_id IS NOT NULL AND storage_data_set.data_set_id <> '' AND storage_data_set.status IN ('ready', 'draining') AND storage_copy.piece_id IS NOT NULL AND storage_copy.piece_id <> '' AND storage_copy.retrieval_url IS NOT NULL AND storage_copy.retrieval_url <> '')"
}

func objectVersionPermanentDeleteStateError(state model.ObjectState) error {
	switch state {
	case model.ObjectStateCached,
		model.ObjectStateUploading,
		model.ObjectStateCommitting,
		model.ObjectStateReplicating,
		model.ObjectStateStored,
		model.ObjectStateFailed:
		return nil
	default:
		return ErrConflict
	}
}

func prepareObjectVersionsForPermanentDelete(
	ctx context.Context,
	db bun.IDB,
	versions []*model.ObjectVersion,
	uploadsByID map[int64]*model.StorageContent,
) error {
	if len(versions) == 0 {
		return nil
	}

	deletingVersionIDs := make([]string, 0, len(versions))
	for _, version := range versions {
		if version == nil || version.VersionID == "" {
			return fmt.Errorf("preparing permanent delete storage work: %w", ErrInvalidInput)
		}
		deletingVersionIDs = append(deletingVersionIDs, version.VersionID)
	}
	sort.Strings(deletingVersionIDs)

	contentIDs := sortedContentIDs(uploadsByID)
	var copies []model.StorageCopy
	if len(contentIDs) > 0 {
		query := db.NewSelect().Model(&copies)
		projectActiveCommitAttempt(query, "storage_copy")
		if err := query.
			Where("storage_copy.content_id IN (?)", bun.List(contentIDs)).
			OrderExpr("storage_copy.id ASC").
			Scan(ctx); err != nil {
			return fmt.Errorf("loading storage copies for permanent delete: %w", err)
		}
	}
	for i := range copies {
		res, err := db.NewUpdate().
			Model((*model.StorageCopy)(nil)).
			Set("updated_at = updated_at").
			Where("id = ?", copies[i].ID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("locking storage copy for permanent delete: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("locking storage copy %d for permanent delete: %w", copies[i].ID, ErrConflict)
		}
	}

	relatedVersionIDs := append([]string(nil), deletingVersionIDs...)
	var boundVersionIDs []string
	if len(contentIDs) > 0 {
		if err := db.NewSelect().
			Model((*model.ObjectVersion)(nil)).
			Column("version_id").
			Where("content_id IN (?)", bun.List(contentIDs)).
			Scan(ctx, &boundVersionIDs); err != nil {
			return fmt.Errorf("loading storage upload references for permanent delete: %w", err)
		}
	}
	for _, versionID := range boundVersionIDs {
		relatedVersionIDs = appendUniqueString(relatedVersionIDs, versionID)
	}
	sort.Strings(relatedVersionIDs)

	// Ingest is scheduled against the content, so a version cannot be removed
	// while work is in flight for either the version or the bytes it names.
	contentSubjectKeys := make([]string, 0, len(contentIDs))
	for _, contentID := range contentIDs {
		contentSubjectKeys = append(contentSubjectKeys, strconv.FormatInt(contentID, 10))
	}
	var relatedTasks []model.Task
	taskQuery := db.NewSelect().
		Model(&relatedTasks).
		Column("id", "status").
		OrderExpr("id ASC")
	if len(contentSubjectKeys) > 0 {
		taskQuery = taskQuery.Where(
			"(subject_type = ? AND subject_key IN (?)) OR (subject_type = ? AND subject_key IN (?))",
			"object_version", bun.List(relatedVersionIDs),
			"storage_content", bun.List(contentSubjectKeys),
		)
	} else {
		taskQuery = taskQuery.Where("subject_type = ? AND subject_key IN (?)", "object_version", bun.List(relatedVersionIDs))
	}
	if db.Dialect().Name() == dialect.PG {
		taskQuery = taskQuery.For("UPDATE")
	}
	if err := taskQuery.Scan(ctx); err != nil {
		return fmt.Errorf("loading storage tasks for permanent delete: %w", err)
	}
	for i := range relatedTasks {
		if relatedTasks[i].Status == model.TaskStatusPending || relatedTasks[i].Status == model.TaskStatusRunning {
			return ErrPermanentDeleteStorageBusy
		}
	}
	if len(contentIDs) == 0 {
		return nil
	}

	copiesByContentID := make(map[int64][]model.StorageCopy)
	for _, copyRow := range copies {
		copiesByContentID[copyRow.ContentID] = append(copiesByContentID[copyRow.ContentID], copyRow)
	}
	for _, contentID := range contentIDs {
		upload := uploadsByID[contentID]
		liveVersion, err := selectLiveObjectVersionForStorageContent(ctx, db, upload, deletingVersionIDs)
		if err != nil {
			return err
		}
		if liveVersion != nil {
			continue
		}
		for _, copyRow := range copiesByContentID[contentID] {
			if storageUploadCopyHasAttemptedCommit(copyRow) {
				return ErrPermanentDeleteStorageBusy
			}
			if !storageUploadCopyCanBeCancelledForPermanentDelete(copyRow) {
				continue
			}
			now := time.Now()
			if _, err := db.NewUpdate().
				Model((*storagecommit.Attempt)(nil)).
				Set("status = ?", storagecommit.AttemptStatusReleased).
				Set("release_reason = ?", string(storagecommit.ReleaseOwnerTerminal)).
				Set("resolved_at = ?", now).
				Set("updated_at = ?", now).
				Where("content_id = ? AND storage_data_set_id = ?", copyRow.ContentID, copyRow.StorageDataSetID).
				Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusReserved).
				Exec(ctx); err != nil {
				return fmt.Errorf("releasing storage reservation for permanent delete: %w", err)
			}
			res, err := db.NewUpdate().
				Model((*model.StorageCopy)(nil)).
				Set("status = ?", model.StorageCopyStatusFailed).
				Set("commit_ready_at = NULL").
				Set("commit_extra_data_hex = NULL").
				Set("last_error = ?", "cancelled because the last object version was permanently deleted").
				Set("updated_at = ?", now).
				Where("id = ?", copyRow.ID).
				Where("status IN (?)", bun.List([]model.StorageCopyStatus{
					model.StorageCopyStatusPending,
					model.StorageCopyStatusPieceReady,
					model.StorageCopyStatusCommitting,
				})).
				Where(`NOT EXISTS (
					SELECT 1 FROM storage_commit_attempts AS unresolved_attempt
					WHERE unresolved_attempt.content_id = storage_copy.content_id
					  AND unresolved_attempt.storage_data_set_id = storage_copy.storage_data_set_id
					  AND unresolved_attempt.resolved_at IS NULL
				)`).
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("cancelling storage copy for permanent delete: %w", err)
			}
			rows, _ := res.RowsAffected()
			if rows == 0 {
				return fmt.Errorf("cancelling storage copy %d for permanent delete: %w", copyRow.ID, ErrPermanentDeleteStorageBusy)
			}
		}
		if _, err := db.NewUpdate().
			Model((*model.StorageContent)(nil)).
			Set("error_message = ?", "the last object version referencing this content was permanently deleted").
			Set("updated_at = ?", time.Now()).
			Where("id = ?", contentID).
			Exec(ctx); err != nil {
			return fmt.Errorf("closing storage content for permanent delete: %w", err)
		}
	}
	return nil
}

func authoritativeStorageContentsForVersions(
	ctx context.Context,
	db bun.IDB,
	versions []*model.ObjectVersion,
) (map[int64]*model.StorageContent, error) {
	uploadsByID := make(map[int64]*model.StorageContent)
	for _, version := range versions {
		if version == nil || version.VersionID == "" {
			return nil, fmt.Errorf("loading permanent delete storage uploads: %w", ErrInvalidInput)
		}
		upload, err := authoritativeStorageContentForVersion(ctx, db, version)
		if err != nil {
			return nil, err
		}
		if upload == nil {
			continue
		}
		contentID := upload.ID
		version.ContentID = &contentID
		uploadsByID[upload.ID] = upload
	}
	return uploadsByID, nil
}

func dataObjectVersionPointers(versions []model.ObjectVersion) []*model.ObjectVersion {
	dataVersions := make([]*model.ObjectVersion, 0, len(versions))
	for i := range versions {
		if !versions[i].IsDeleteMarker {
			dataVersions = append(dataVersions, &versions[i])
		}
	}
	return dataVersions
}

func sameObjectVersionIDs(versions []model.ObjectVersion, expectedIDs []string) bool {
	if len(versions) != len(expectedIDs) {
		return false
	}
	actualIDs := make([]string, 0, len(versions))
	for i := range versions {
		actualIDs = append(actualIDs, versions[i].VersionID)
	}
	expected := append([]string(nil), expectedIDs...)
	sort.Strings(actualIDs)
	sort.Strings(expected)
	return slices.Equal(actualIDs, expected)
}

func authoritativeStorageContentForVersion(ctx context.Context, db bun.IDB, version *model.ObjectVersion) (*model.StorageContent, error) {
	if version.ContentID == nil || *version.ContentID <= 0 {
		return nil, nil
	}
	upload := new(model.StorageContent)
	if err := db.NewSelect().Model(upload).Where("id = ?", *version.ContentID).Scan(ctx); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("loading storage upload for permanent delete: %w", err)
	}
	return upload, nil
}

func sortedContentIDs(uploadsByID map[int64]*model.StorageContent) []int64 {
	ids := make([]int64, 0, len(uploadsByID))
	for id := range uploadsByID {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func storageUploadCopyCanBeCancelledForPermanentDelete(copyRow model.StorageCopy) bool {
	switch copyRow.Status {
	case model.StorageCopyStatusPending, model.StorageCopyStatusPieceReady, model.StorageCopyStatusCommitting:
		return true
	default:
		return false
	}
}

func storageUploadCopyHasAttemptedCommit(copyRow model.StorageCopy) bool {
	if copyRow.CommitAttemptID != nil && *copyRow.CommitAttemptID != "" && copyRow.CommitAttemptedAt != nil {
		return true
	}
	// A transaction id outside 'committed' records a submission whose outcome was
	// never resolved, including rows older code left behind when it sent a
	// submitted copy back to 'piece_ready'. Treat those as busy so a permanent
	// delete cannot discard a piece the provider may still be paid to keep.
	return copyRow.Status != model.StorageCopyStatusCommitted &&
		copyRow.CommitTransactionID != nil && *copyRow.CommitTransactionID != ""
}

func reserveStorageCleanupForDeletedVersions(ctx context.Context, db bun.IDB, contentID int64, deletedVersionIDs []string) (*StorageCleanupReservation, error) {
	if contentID == 0 || len(deletedVersionIDs) == 0 {
		return nil, fmt.Errorf("preparing storage cleanup: %w", ErrInvalidInput)
	}
	copies, err := storageCleanupCopySnapshots(ctx, db, contentID)
	if err != nil {
		return nil, err
	}
	if len(copies) == 0 {
		return nil, nil
	}
	now := time.Now()
	for i := range copies {
		copies[i].CreatedAt = now
		copies[i].UpdatedAt = now
	}
	_, err = db.NewInsert().
		Model(&copies).
		On("CONFLICT (content_id, storage_data_set_id, piece_id) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("persisting storage cleanup snapshots: %w", err)
	}
	reservation := new(StorageCleanupReservation)
	err = db.NewRaw(`UPDATE storage_contents
		SET cleanup_generation = cleanup_generation + CASE WHEN cleanup_task_id IS NULL THEN 1 ELSE 0 END,
		    updated_at = ?
		WHERE id = ?
		RETURNING id AS content_id, cleanup_generation AS generation, cleanup_task_id AS task_id`, now, contentID).Scan(ctx, reservation)
	if err != nil {
		return nil, fmt.Errorf("reserving storage cleanup generation: %w", err)
	}
	return reservation, nil
}

func appendUniqueString(values []string, value string) []string {
	if value == "" {
		return values
	}
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}

func storageCleanupCopySnapshots(ctx context.Context, db bun.IDB, contentID int64) ([]model.StorageCleanupCopy, error) {
	var copies []model.StorageCleanupCopy
	err := db.NewRaw(`SELECT
			storage_copy.content_id,
			storage_copy.bucket_id,
			storage_copy.copy_index,
			storage_copy.provider_id,
			storage_copy.storage_data_set_id,
			storage_data_set.data_set_id,
			storage_data_set.client_data_set_id,
			storage_copy.piece_id,
			storage_content.piece_cid,
			storage_copy.retrieval_url,
			? AS status
		FROM storage_copies AS storage_copy
		JOIN storage_contents AS storage_content ON storage_content.id = storage_copy.content_id
		JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id
		WHERE storage_copy.content_id = ? AND storage_copy.status = ?
		ORDER BY storage_copy.copy_index ASC`,
		model.StorageCleanupCopyStatusPending,
		contentID,
		model.StorageCopyStatusCommitted,
	).Scan(ctx, &copies)
	if err != nil {
		return nil, fmt.Errorf("loading storage cleanup copy snapshots: %w", err)
	}
	return copies, nil
}

func createVersionAndSetCurrentIfChanged(ctx context.Context, db bun.IDB, version *model.ObjectVersion) (ObjectVersionWriteResult, error) {
	normalizeObjectVersion(version)
	if err := prepareNewObjectVersionStorageReference(ctx, db, version); err != nil {
		return ObjectVersionWriteResult{}, fmt.Errorf("preparing object version storage reference: %w", err)
	}
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
	normalizeObjectVersion(version)
	if err := prepareNewObjectVersionStorageReference(ctx, db, version); err != nil {
		return 0, fmt.Errorf("preparing object version storage reference: %w", err)
	}
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

func createRestoredVersionAndSetCurrent(ctx context.Context, db bun.IDB, version *model.ObjectVersion, sourceVersionID, expectedCurrentVersionID string) (int64, error) {
	normalizeObjectVersion(version)
	if err := prepareNewObjectVersionStorageReference(ctx, db, version); err != nil {
		return 0, fmt.Errorf("preparing restored version storage reference: %w", err)
	}
	if err := lockCurrentObjectIfExists(ctx, db, version.BucketID, version.Key); err != nil {
		return 0, err
	}

	source, err := selectVersionByBucketKeyAndID(ctx, db, version.BucketID, version.Key, sourceVersionID)
	if err != nil {
		return 0, err
	}
	if source == nil {
		return 0, fmt.Errorf("creating restored object version: %w", ErrNotFound)
	}
	if source.IsDeleteMarker {
		return 0, fmt.Errorf("creating restored object version: %w", ErrConflict)
	}

	current, err := selectCurrentVersionByBucketAndKey(ctx, db, version.BucketID, version.Key)
	if err != nil {
		return 0, err
	}
	if current == nil || current.VersionID != expectedCurrentVersionID || current.ObjectID != source.ObjectID {
		return 0, fmt.Errorf("creating restored object version: %w", ErrConflict)
	}
	if restoreSourceAlreadyCurrent(source, current) {
		return 0, fmt.Errorf("creating restored object version: %w", ErrAlreadyCurrent)
	}

	now := time.Now()
	version.ObjectID = source.ObjectID
	if version.CreatedAt.IsZero() {
		version.CreatedAt = now
	}
	if version.UpdatedAt.IsZero() {
		version.UpdatedAt = now
	}

	if _, err := db.NewInsert().Model(version).Exec(ctx); err != nil {
		return 0, fmt.Errorf("inserting restored object version: %w", err)
	}
	// Moving the pointer is the compare-and-set: it succeeds only while the
	// caller's expected version is still the current one.
	res, err := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("current_version_id = ?", version.VersionID).
		Set("updated_at = ?", now).
		Where("id = ? AND current_version_id = ?", source.ObjectID, expectedCurrentVersionID).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("pointing object at restored version: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return 0, fmt.Errorf("creating restored object version: %w", ErrConflict)
	}
	return source.ObjectID, nil
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
		if version.CreatedAt.IsZero() {
			version.CreatedAt = now
		}
		if version.UpdatedAt.IsZero() {
			version.UpdatedAt = now
		}
		if _, insertErr := db.NewInsert().Model(version).Exec(ctx); insertErr != nil {
			return ObjectVersionWriteResult{}, fmt.Errorf("inserting object version: %w", insertErr)
		}
		// The object is inserted with a null pointer because its version does
		// not exist yet; one update completes the identity.
		if _, updateErr := db.NewUpdate().
			Model((*model.Object)(nil)).
			Set("current_version_id = ?", version.VersionID).
			Set("updated_at = ?", now).
			Where("id = ?", obj.ID).
			Exec(ctx); updateErr != nil {
			return ObjectVersionWriteResult{}, fmt.Errorf("pointing new object at its first version: %w", updateErr)
		}
		version.IsCurrent = true
		return ObjectVersionWriteResult{
			ObjectID:  obj.ID,
			VersionID: version.VersionID,
			ETag:      version.ETag,
			Created:   true,
		}, nil
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
	if _, updateErr := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("current_version_id = ?", version.VersionID).
		Set("updated_at = ?", now).
		Where("id = ?", existing.ID).
		Exec(ctx); updateErr != nil {
		return ObjectVersionWriteResult{}, fmt.Errorf("pointing object at new version: %w", updateErr)
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

	marker.IsDeleteMarker = true
	marker.Size = 0
	marker.ETag = ""
	marker.ContentType = ""
	marker.ContentID = nil
	if marker.CreatedAt.IsZero() {
		marker.CreatedAt = now
	}
	if marker.UpdatedAt.IsZero() {
		marker.UpdatedAt = now
	}

	if _, err := db.NewInsert().
		Model(marker).
		Column("version_id", "object_id", "bucket_id", "key", "content_id", "size", "e_tag", "content_type", "metadata", "is_delete_marker", "created_at", "updated_at").
		Value("content_type", "?", "").
		Exec(ctx); err != nil {
		return fmt.Errorf("inserting delete marker: %w", err)
	}
	if _, err := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("current_version_id = ?", marker.VersionID).
		Set("updated_at = ?", now).
		Where("id = ?", marker.ObjectID).
		Exec(ctx); err != nil {
		return fmt.Errorf("pointing object at delete marker: %w", err)
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

	// The object's pointer has to leave this version before the row can go.
	if version.IsCurrent {
		if err := repointObjectAwayFromVersion(ctx, db, version.ObjectID, versionID); err != nil {
			return err
		}
	}
	if _, err := db.NewDelete().
		Model((*model.ObjectVersion)(nil)).
		Where("bucket_id = ? AND key = ? AND version_id = ?", bucketID, key, versionID).
		Exec(ctx); err != nil {
		return fmt.Errorf("deleting marker version: %w", err)
	}
	if version.IsCurrent {
		return deleteObjectIdentityIfEmpty(ctx, db, version.ObjectID)
	}
	return nil
}

// repointObjectAwayFromVersion moves the object's current pointer to the newest
// version that is not the one about to be deleted, or clears it when that
// version was the last. A version can only be removed once nothing points at it.
func repointObjectAwayFromVersion(ctx context.Context, db bun.IDB, objectID int64, versionID string) error {
	next, err := selectLatestVersionByObjectIDExcluding(ctx, db, objectID, versionID)
	if err != nil {
		return err
	}
	query := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", objectID)
	if next == nil {
		query = query.Set("current_version_id = NULL")
	} else {
		query = query.Set("current_version_id = ?", next.VersionID)
	}
	if _, err := query.Exec(ctx); err != nil {
		return fmt.Errorf("repointing object away from version %s: %w", versionID, err)
	}
	return nil
}

// deleteObjectIdentityIfEmpty removes an object that has no versions left.
func deleteObjectIdentityIfEmpty(ctx context.Context, db bun.IDB, objectID int64) error {
	remaining, err := db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", objectID).
		Count(ctx)
	if err != nil {
		return fmt.Errorf("counting remaining object versions: %w", err)
	}
	if remaining > 0 {
		return nil
	}
	if _, err := db.NewDelete().
		Model((*model.Object)(nil)).
		Where("id = ?", objectID).
		Exec(ctx); err != nil {
		return fmt.Errorf("deleting empty object identity: %w", err)
	}
	return nil
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

	// The pointer moves before the markers go: a version cannot be deleted while
	// the object still points at it.
	if _, err := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("current_version_id = ?", restoreTarget.VersionID).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", current.ObjectID).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("pointing object at restored version: %w", err)
	}
	if _, err := db.NewDelete().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ? AND version_id IN (?)", current.ObjectID, bun.List(markerIDs)).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("deleting marker stack: %w", err)
	}
	restoreTarget.IsCurrent = true
	return restoreTarget, nil
}

func normalizeObjectVersion(version *model.ObjectVersion) {
	if version.Metadata == nil {
		version.Metadata = map[string]string{}
	}
	if version.IsDeleteMarker {
		version.ContentID = nil
		version.Size = 0
		version.ETag = ""
		version.ContentType = ""
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
	err := q.Where("object_version.object_id = ?", objectID).
		Where("current_object.current_version_id = object_version.version_id").Scan(ctx)
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
	err := q.Where("object_version.bucket_id = ? AND object_version.key = ?", bucketID, key).
		Where("current_object.current_version_id = object_version.version_id").Scan(ctx)
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

// selectLatestVersionByObjectIDExcluding finds the version that should take over
// when one version is about to be removed.
func selectLatestVersionByObjectIDExcluding(ctx context.Context, db bun.IDB, objectID int64, excludeVersionID string) (*model.ObjectVersion, error) {
	version := new(model.ObjectVersion)
	q := db.NewSelect().
		Model(version).
		ModelTableExpr("object_versions AS object_version")
	q = withObjectVersionStorageColumns(q, "object_version")
	err := q.Where("object_version.object_id = ?", objectID).
		Where("object_version.version_id <> ?", excludeVersionID).
		OrderExpr("object_version.created_at DESC").
		OrderExpr("object_version.version_id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting replacement object version: %w", err)
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

func lockObjectVersionsByID(ctx context.Context, db bun.IDB, versionIDs []string) error {
	ids := append([]string(nil), versionIDs...)
	sort.Strings(ids)
	previous := ""
	for _, versionID := range ids {
		if versionID == "" || versionID == previous {
			continue
		}
		previous = versionID
		res, err := db.NewUpdate().
			Model((*model.ObjectVersion)(nil)).
			Set("updated_at = updated_at").
			Where("version_id = ?", versionID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("locking object version for permanent delete: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return ErrNotFound
		}
	}
	return nil
}

func objectVersionMatchesVersion(current *model.ObjectVersion, version *model.ObjectVersion) bool {
	if current == nil || current.State == model.ObjectStateFailed || current.IsDeleteMarker {
		return false
	}
	// Content identity carries bucket, checksum and size, so it replaces the
	// old size+checksum pair. The incoming version only ever carries a content
	// id; checksum is a read projection and would compare empty here.
	return current.ContentID != nil && version.ContentID != nil &&
		*current.ContentID == *version.ContentID &&
		current.ETag == version.ETag &&
		current.ContentType == version.ContentType &&
		maps.Equal(current.Metadata, version.Metadata)
}

func restoreSourceAlreadyCurrent(source, current *model.ObjectVersion) bool {
	if source == nil || current == nil {
		return false
	}
	if source.VersionID == current.VersionID {
		return true
	}
	if current.State == model.ObjectStateFailed || current.IsDeleteMarker || (!current.InCache && !current.InFilecoin) {
		return false
	}
	// Identical bytes are identical content rows, so content identity replaces
	// the old size+checksum comparison and no longer depends on a projection.
	return source.ContentID != nil && current.ContentID != nil &&
		*source.ContentID == *current.ContentID &&
		current.ContentType == source.ContentType &&
		maps.Equal(current.Metadata, source.Metadata)
}

func objectIdentityFromVersion(version *model.ObjectVersion) *model.Object {
	return &model.Object{
		BucketID: version.BucketID,
		Key:      version.Key,
	}
}

// setVersionCachePresence records residency against the version's content.
// Several versions can share one cached file, so presence is stored once per
// content rather than once per version.
func setVersionCachePresence(ctx context.Context, db bun.IDB, versionID string, inCache bool) error {
	contentID, err := contentIDForVersion(ctx, db, versionID)
	if err != nil {
		return fmt.Errorf("setting version cache presence: %w", err)
	}
	if contentID == nil {
		return nil
	}
	return upsertContentCachePresence(ctx, db, *contentID, inCache, nil)
}

func contentIDForVersion(ctx context.Context, db bun.IDB, versionID string) (*int64, error) {
	var contentID *int64
	err := db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Column("content_id").
		Where("version_id = ?", versionID).
		Scan(ctx, &contentID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("version %s not found", versionID)
		}
		return nil, err
	}
	return contentID, nil
}

// upsertContentCachePresence writes the residency row, creating it the first
// time a content is cached.
func upsertContentCachePresence(ctx context.Context, db bun.IDB, contentID int64, inCache bool, accessedAt *time.Time) error {
	now := time.Now()
	entry := &model.ObjectCache{
		ContentID:       contentID,
		InCache:         inCache,
		CacheAccessedAt: accessedAt,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	query := db.NewInsert().Model(entry).On("CONFLICT (content_id) DO UPDATE").
		Set("in_cache = EXCLUDED.in_cache").
		Set("updated_at = EXCLUDED.updated_at")
	if accessedAt != nil {
		query = query.Set(`cache_accessed_at = CASE
			WHEN object_cache.cache_accessed_at IS NULL OR object_cache.cache_accessed_at < EXCLUDED.cache_accessed_at
			THEN EXCLUDED.cache_accessed_at ELSE object_cache.cache_accessed_at END`)
	}
	if _, err := query.Exec(ctx); err != nil {
		return fmt.Errorf("recording content cache presence: %w", err)
	}
	return nil
}
