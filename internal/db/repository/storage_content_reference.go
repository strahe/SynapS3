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
	"github.com/uptrace/bun/dialect"
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
// about to name, and freezes the bucket's current copy policy into content no
// version has named before. content_id is plain identity: it is set the moment
// the bytes are known, and durability is read from the copy rows.
func prepareNewObjectVersionStorageReference(ctx context.Context, db bun.IDB, version *model.ObjectVersion) error {
	if version == nil || version.ContentID == nil || *version.ContentID <= 0 {
		return nil
	}
	// The policy is read before the content lock, as other paths that lock both
	// do. Cache eviction locks the content first, but its cache gate keeps it
	// from running beside a write of the same content.
	defaultCopies, err := bucketCopyPolicyForReference(ctx, db, version.BucketID)
	if err != nil {
		return err
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
	if err := freezeRequestedCopiesOnFirstReference(ctx, db, content, defaultCopies); err != nil {
		return err
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

// bucketCopyPolicyForReference reads the policy a first reference freezes into
// its content. PostgreSQL holds a share lock until the reference commits, so a
// policy change cannot commit in between: content named before the change
// keeps the old target and content named after it gets the new one. SQLite
// serializes writers, so it needs no lock.
func bucketCopyPolicyForReference(ctx context.Context, db bun.IDB, bucketID int64) (int, error) {
	var defaultCopies int
	query := db.NewSelect().
		Model((*model.Bucket)(nil)).
		Column("default_copies").
		Where("id = ?", bucketID)
	if db.Dialect().Name() == dialect.PG {
		query = query.For("SHARE")
	}
	if err := query.Scan(ctx, &defaultCopies); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("bucket %d: %w", bucketID, ErrNotFound)
		}
		return 0, fmt.Errorf("reading bucket %d copy policy: %w", bucketID, err)
	}
	return defaultCopies, nil
}

// freezeRequestedCopiesOnFirstReference gives content its copy target when a
// version names it for the first time; content some version has named keeps
// its target. A version's removal raises cleanup_generation, so a row still at
// zero that no version names has never been named.
func freezeRequestedCopiesOnFirstReference(ctx context.Context, db bun.IDB, content *model.StorageContent, defaultCopies int) error {
	requestedCopies := model.ClampStorageCopies(defaultCopies)
	if content.RequestedCopies == requestedCopies || content.CleanupGeneration != 0 {
		return nil
	}
	named, err := db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("content_id = ?", content.ID).
		Exists(ctx)
	if err != nil {
		return fmt.Errorf("checking references to storage content %d: %w", content.ID, err)
	}
	if named {
		return nil
	}
	if _, err := db.NewUpdate().
		Model((*model.StorageContent)(nil)).
		Set("requested_copies = ?", requestedCopies).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", content.ID).
		Exec(ctx); err != nil {
		return fmt.Errorf("freezing requested copies for storage content %d: %w", content.ID, err)
	}
	content.RequestedCopies = requestedCopies
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
