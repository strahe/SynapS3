package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// lockStorageContentsByID serializes object-version references with permanent
// deletion. Callers must acquire these locks before object or version locks.
func lockStorageContentsByID(ctx context.Context, db bun.IDB, contentIDs []int64) (map[int64]*model.StorageContent, error) {
	ids := append([]int64(nil), contentIDs...)
	slices.Sort(ids)

	uploads := make(map[int64]*model.StorageContent, len(ids))
	var previous int64
	for _, contentID := range ids {
		if contentID <= 0 || contentID == previous {
			continue
		}
		previous = contentID

		res, err := db.NewUpdate().
			Model((*model.StorageContent)(nil)).
			Set("updated_at = updated_at").
			Where("id = ?", contentID).
			Exec(ctx)
		if err != nil {
			return nil, fmt.Errorf("locking storage upload %d: %w", contentID, err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return nil, fmt.Errorf("locking storage upload %d: %w", contentID, ErrNotFound)
		}

		upload := new(model.StorageContent)
		if err := db.NewSelect().Model(upload).Where("id = ?", contentID).Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return nil, fmt.Errorf("loading locked storage upload %d: %w", contentID, ErrNotFound)
			}
			return nil, fmt.Errorf("loading locked storage upload %d: %w", contentID, err)
		}
		uploads[contentID] = upload
	}
	return uploads, nil
}

func lockStorageContentForObjectState(
	ctx context.Context,
	db bun.IDB,
	contentID int64,
	state model.ObjectState,
) (*model.StorageContent, error) {
	var bucketID int64
	if err := db.NewSelect().
		Model((*model.StorageContent)(nil)).
		Column("bucket_id").
		Where("id = ?", contentID).
		Scan(ctx, &bucketID); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("loading storage upload %d: %w", contentID, ErrNotFound)
		}
		return nil, fmt.Errorf("loading storage upload %d bucket: %w", contentID, err)
	}
	if bucket, err := lockBucketByID(ctx, db, bucketID); err != nil {
		return nil, err
	} else if bucket == nil {
		return nil, fmt.Errorf("locking storage upload %d bucket: %w", contentID, ErrNotFound)
	}
	uploads, err := lockStorageContentsByID(ctx, db, []int64{contentID})
	if err != nil {
		return nil, err
	}
	upload := uploads[contentID]
	if upload == nil {
		return nil, fmt.Errorf("storage content %d cannot back object state %s: %w", contentID, state, ErrConflict)
	}
	// Whether the content can back a version is a fact about its copies, so it
	// is asked of them rather than of a status column that mirrored them.
	if err := requireReadableCommittedCopy(ctx, db, contentID); err != nil {
		return nil, fmt.Errorf("storage content %d cannot back an object version: %w", contentID, ErrConflict)
	}
	return upload, nil
}

func lockStorageContentForCopyMutation(ctx context.Context, db bun.IDB, contentID int64) error {
	uploads, err := lockStorageContentsByID(ctx, db, []int64{contentID})
	if err != nil {
		return err
	}
	upload := uploads[contentID]
	if upload == nil {
		return fmt.Errorf("storage content %d cannot accept copy updates: %w", contentID, ErrConflict)
	}
	return nil
}

// prepareNewObjectVersionStorageReference locks the content a new version is
// about to name.
//
// It no longer demands a readable committed copy first. That requirement came
// from the old model, where a version only gained storage_upload_id once the
// upload was already readable, so the reference doubled as a durability claim.
// content_id is plain identity: it is set the moment the bytes are known, and
// durability is read from the copy rows instead.
func prepareNewObjectVersionStorageReference(ctx context.Context, db bun.IDB, version *model.ObjectVersion) error {
	if version == nil || version.ContentID == nil || *version.ContentID <= 0 {
		return nil
	}
	contents, err := lockStorageContentsByID(ctx, db, []int64{*version.ContentID})
	if errors.Is(err, ErrNotFound) {
		// Content rows are only deleted when their cleanup finishes, so the bytes
		// have to be written again, which creates new content.
		return fmt.Errorf("storage content %d: %w", *version.ContentID, ErrContentCleanupInProgress)
	}
	if err != nil {
		return err
	}
	content := contents[*version.ContentID]
	if content == nil {
		return fmt.Errorf("storage content %d: %w", *version.ContentID, ErrNotFound)
	}
	// Cleanup starts only when the last version is gone, and a new version
	// cannot bring content back while it is being removed.
	if content.CleanupTaskID != nil {
		return fmt.Errorf("storage content %d: %w", *version.ContentID, ErrContentCleanupInProgress)
	}
	// A data version is only created after its bytes are durably in the local
	// cache, so the content is resident. Residency is per content: versions that
	// share bytes share this row.
	if !version.IsDeleteMarker {
		accessedAt := version.CreatedAt
		if accessedAt.IsZero() {
			accessedAt = time.Now()
		}
		if err := upsertContentCachePresence(ctx, db, *version.ContentID, true, &accessedAt); err != nil {
			return err
		}
	}
	return nil
}

func sameContentIDs(left, right map[int64]*model.StorageContent) bool {
	if len(left) != len(right) {
		return false
	}
	for contentID := range left {
		if _, ok := right[contentID]; !ok {
			return false
		}
	}
	return true
}

// objectVersionReferencesStorageContentSQL matches the versions backed by one
// content. A data version always carries content_id, so the reference is that
// column alone.
func objectVersionReferencesStorageContentSQL(versionAlias, uploadAlias string) string {
	return fmt.Sprintf("%s.content_id = %s.id", versionAlias, uploadAlias)
}
