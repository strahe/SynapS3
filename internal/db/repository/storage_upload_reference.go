package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

const objectVersionReferencesStorageUploadIDSQL = "(storage_upload_id = ? OR (version_id = ? AND storage_upload_id IS NULL))"

// lockStorageUploadsByID serializes object-version references with permanent
// deletion. Callers must acquire these locks before object or version locks.
func lockStorageUploadsByID(ctx context.Context, db bun.IDB, uploadIDs []int64) (map[int64]*model.StorageUpload, error) {
	ids := append([]int64(nil), uploadIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	uploads := make(map[int64]*model.StorageUpload, len(ids))
	var previous int64
	for _, uploadID := range ids {
		if uploadID <= 0 || uploadID == previous {
			continue
		}
		previous = uploadID

		res, err := db.NewUpdate().
			Model((*model.StorageUpload)(nil)).
			Set("status = status").
			Where("id = ?", uploadID).
			Exec(ctx)
		if err != nil {
			return nil, fmt.Errorf("locking storage upload %d: %w", uploadID, err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return nil, fmt.Errorf("locking storage upload %d: %w", uploadID, ErrNotFound)
		}

		upload := new(model.StorageUpload)
		if err := db.NewSelect().Model(upload).Where("id = ?", uploadID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return nil, fmt.Errorf("loading locked storage upload %d: %w", uploadID, ErrNotFound)
			}
			return nil, fmt.Errorf("loading locked storage upload %d: %w", uploadID, err)
		}
		uploads[uploadID] = upload
	}
	return uploads, nil
}

func lockStorageUploadForObjectState(
	ctx context.Context,
	db bun.IDB,
	uploadID int64,
	state model.ObjectState,
) (*model.StorageUpload, error) {
	uploads, err := lockStorageUploadsByID(ctx, db, []int64{uploadID})
	if err != nil {
		return nil, err
	}
	upload := uploads[uploadID]
	if upload == nil || !storageUploadSupportsObjectState(upload.Status, state) {
		return nil, fmt.Errorf("storage upload %d cannot back object state %s: %w", uploadID, state, ErrConflict)
	}
	if err := requireReadableCommittedCopy(ctx, db, uploadID); err != nil {
		return nil, fmt.Errorf("storage upload %d cannot back an object version: %w", uploadID, ErrConflict)
	}
	return upload, nil
}

func lockStorageUploadForCopyMutation(ctx context.Context, db bun.IDB, uploadID int64) error {
	uploads, err := lockStorageUploadsByID(ctx, db, []int64{uploadID})
	if err != nil {
		return err
	}
	upload := uploads[uploadID]
	if upload == nil || upload.Status == model.StorageUploadStatusSuperseded {
		return fmt.Errorf("storage upload %d cannot accept copy updates: %w", uploadID, ErrConflict)
	}
	return nil
}

func storageUploadSupportsObjectState(status model.StorageUploadStatus, state model.ObjectState) bool {
	switch state {
	case model.ObjectStateStored, model.ObjectStateCacheEvicted:
		return status == model.StorageUploadStatusComplete
	case model.ObjectStateReplicating:
		return status != model.StorageUploadStatusRejected && status != model.StorageUploadStatusSuperseded
	default:
		return false
	}
}

func prepareNewObjectVersionStorageReference(ctx context.Context, db bun.IDB, version *model.ObjectVersion) error {
	if version == nil || version.StorageUploadID == nil || *version.StorageUploadID <= 0 {
		return nil
	}
	_, err := lockStorageUploadForObjectState(ctx, db, *version.StorageUploadID, version.State)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrNotFound) {
		return err
	}
	if !version.InCache {
		return err
	}

	version.StorageUploadID = nil
	version.State = model.ObjectStateCached
	version.FailedAtState = nil
	version.LastError = nil
	return nil
}

func sameStorageUploadIDs(left, right map[int64]*model.StorageUpload) bool {
	if len(left) != len(right) {
		return false
	}
	for uploadID := range left {
		if _, ok := right[uploadID]; !ok {
			return false
		}
	}
	return true
}

func objectVersionReferencesStorageUpload(version *model.ObjectVersion, upload *model.StorageUpload) bool {
	if version == nil || upload == nil || version.IsDeleteMarker {
		return false
	}
	if version.StorageUploadID != nil {
		return *version.StorageUploadID == upload.ID
	}
	return version.VersionID == upload.SourceVersionID
}

func objectVersionReferencesStorageUploadSQL(versionAlias, uploadAlias string) string {
	return fmt.Sprintf(
		`(%s.storage_upload_id = %s.id OR (%s.version_id = %s.source_version_id AND %s.storage_upload_id IS NULL))`,
		versionAlias,
		uploadAlias,
		versionAlias,
		uploadAlias,
		versionAlias,
	)
}
