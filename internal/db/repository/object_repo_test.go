package repository_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/migrations"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
)

// seedContent creates the byte identity a data version needs before it exists.
func seedContent(t *testing.T, repos *repository.Repositories, bucketID int64, checksum string, size int64) int64 {
	t.Helper()
	content, err := repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucketID, ContentSize: size, Checksum: testutil.StorageChecksum(checksum), RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("EnsureContent(%s): %v", checksum, err)
	}
	return content.ID
}

// createVersion gives a data version the content identity it now requires and
// then creates it. Tests used to insert versions directly; the bytes are their
// own row now, so content comes first.
func createVersion(t *testing.T, repos *repository.Repositories, version *model.ObjectVersion) (int64, error) {
	t.Helper()
	if !version.IsDeleteMarker && version.ContentID == nil {
		id := seedContent(t, repos, version.BucketID, "checksum-"+version.VersionID, version.Size)
		version.ContentID = &id
	}
	return repos.Objects.CreateVersionAndSetCurrent(t.Context(), version)
}

// withContent attaches the content identity a data version now requires.
func withContent(t *testing.T, repos *repository.Repositories, version *model.ObjectVersion) *model.ObjectVersion {
	t.Helper()
	if !version.IsDeleteMarker && version.ContentID == nil {
		id := seedContent(t, repos, version.BucketID, "checksum-"+version.VersionID, version.Size)
		version.ContentID = &id
	}
	return version
}

func TestObjectRepo_AggregateByStateIncludesTotalSize(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "state-aggregate-bucket")

	cached := newObjectVersion(bucket.ID, "cached.txt", "01J00000000000000000000B01", 10)
	if _, err := createVersion(t, repos, cached); err != nil {
		t.Fatalf("seed cached version: %v", err)
	}
	// A content whose every copy failed reads as failed, so the aggregate has
	// to derive the bucket instead of grouping a stored column.
	failed := newObjectVersion(bucket.ID, "failed.txt", "01J00000000000000000000B02", 20)
	if _, err := createVersion(t, repos, failed); err != nil {
		t.Fatalf("seed failed version: %v", err)
	}
	seedFailedContentCopy(t, db, repos, bucket.ID, *failed.ContentID)
	if _, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "deleted.txt", "01J00000000000000000000B03"); err != nil {
		t.Fatalf("seed delete marker: %v", err)
	}

	rows, err := repos.Objects.AggregateByState(ctx)
	if err != nil {
		t.Fatalf("AggregateByState: %v", err)
	}
	byState := make(map[string]repository.ObjectStateAggregate, len(rows))
	for _, row := range rows {
		byState[row.State] = row
	}
	if got := byState[string(model.ObjectStateCached)]; got.Count != 1 || got.TotalSize != 10 {
		t.Fatalf("cached aggregate = count:%d size:%d, want 1/10", got.Count, got.TotalSize)
	}
	if got := byState[string(model.ObjectStateFailed)]; got.Count != 1 || got.TotalSize != 20 {
		t.Fatalf("failed aggregate = count:%d size:%d, want 1/20", got.Count, got.TotalSize)
	}
}

func TestObjectRepo_CountOverviewAttention(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "overview-attention-bucket")

	healthy := newObjectVersion(bucket.ID, "healthy.txt", "01J00000000000000000000A01", 10)
	if _, err := createVersion(t, repos, healthy); err != nil {
		t.Fatalf("seed healthy version: %v", err)
	}

	// Two distinct ways to need attention: every copy failed, and the content
	// carries an ingest error.
	failedCopies := newObjectVersion(bucket.ID, "failed-copies.txt", "01J00000000000000000000A02", 10)
	if _, err := createVersion(t, repos, failedCopies); err != nil {
		t.Fatalf("seed failed-copies version: %v", err)
	}
	seedFailedContentCopy(t, db, repos, bucket.ID, *failedCopies.ContentID)

	failedContent := newObjectVersion(bucket.ID, "failed-content.txt", "01J00000000000000000000A03", 10)
	if _, err := createVersion(t, repos, failedContent); err != nil {
		t.Fatalf("seed failed-content version: %v", err)
	}
	if _, err := db.NewUpdate().Model((*model.StorageContent)(nil)).
		Set("error_message = ?", "provider failed").
		Where("id = ?", *failedContent.ContentID).
		Exec(ctx); err != nil {
		t.Fatalf("record content failure: %v", err)
	}

	// Unavailable is the absence of both a cached copy and a readable one.
	unavailable := newObjectVersion(bucket.ID, "unavailable.txt", "01J00000000000000000000A04", 10)
	if _, err := createVersion(t, repos, unavailable); err != nil {
		t.Fatalf("seed unavailable version: %v", err)
	}
	if err := repos.Objects.ClearContentCachePresence(ctx, *unavailable.ContentID); err != nil {
		t.Fatalf("clear unavailable cache presence: %v", err)
	}

	if _, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "deleted.txt", "01J00000000000000000000A05"); err != nil {
		t.Fatalf("seed deleted marker: %v", err)
	}

	counts, err := repos.Objects.CountOverviewAttention(ctx)
	if err != nil {
		t.Fatalf("CountOverviewAttention: %v", err)
	}
	if counts.NeedsAttention != 2 {
		t.Fatalf("NeedsAttention = %d, want 2", counts.NeedsAttention)
	}
	if counts.Unavailable != 1 {
		t.Fatalf("Unavailable = %d, want 1", counts.Unavailable)
	}
}

// seedFailedContentCopy binds one copy to a content and fails it, which is how
// an ingest failure is expressed once state is derived from the copies.
func seedFailedContentCopy(t *testing.T, db *bun.DB, repos *repository.Repositories, bucketID, contentID int64) {
	t.Helper()
	ctx := context.Background()
	binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID: bucketID, ProviderID: onChainID(t, "808"), CopyIndex: 0, CreatedByContentID: contentID,
	})
	if err != nil {
		t.Fatalf("seed failed copy binding: %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, contentID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID, CopyIndex: 0,
		TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: binding.ProviderID,
	}}); err != nil {
		t.Fatalf("seed failed copy: %v", err)
	}
	if _, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("status = ?", model.StorageCopyStatusFailed).
		Where("content_id = ?", contentID).
		Exec(ctx); err != nil {
		t.Fatalf("fail seeded copy: %v", err)
	}
}

func TestObjectRepo_CreateDeleteMarkerHidesCurrentObjectButKeepsVersionHistory(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "delete-marker-bucket")

	data := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001001", 10)
	if _, err := createVersion(t, repos, data); err != nil {
		t.Fatalf("create data version: %v", err)
	}
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J00000000000000000001002")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}
	// A marker names no bytes, so it has no content, no cache key and no size.
	if !marker.IsDeleteMarker || marker.Size != 0 || marker.ContentID != nil || marker.CacheKey() != "" || marker.InCache {
		t.Fatalf("marker = %#v, want metadata-only delete marker", marker)
	}
	if marker.Metadata == nil || len(marker.Metadata) != 0 {
		t.Fatalf("delete marker metadata = %#v, want empty map", marker.Metadata)
	}

	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetCurrentVersionByBucketAndKey: %v", err)
	}
	if current == nil || !current.IsDeleteMarker || current.VersionID != marker.VersionID {
		t.Fatalf("current = %#v, want the delete marker", current)
	}

	currentList, err := repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, "", "", 10)
	if err != nil {
		t.Fatalf("ListCurrentVersionsByBucket: %v", err)
	}
	if len(currentList) != 0 {
		t.Fatalf("current list len = %d, want object hidden", len(currentList))
	}

	versions, err := repos.Objects.ListVersionsByBucket(ctx, bucket.ID, "", "", "", 10)
	if err != nil {
		t.Fatalf("ListVersionsByBucket: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("versions len = %d, want marker plus data version", len(versions))
	}
	if !versions[0].IsDeleteMarker || !versions[0].IsCurrent {
		t.Fatalf("first version = %#v, want current delete marker", versions[0].ObjectVersion)
	}
	if versions[1].VersionID != data.VersionID || versions[1].IsDeleteMarker {
		t.Fatalf("second version = %#v, want data version %s", versions[1].ObjectVersion, data.VersionID)
	}
	// The hidden data version keeps its bytes and its derived position.
	if versions[1].State != model.ObjectStateCached {
		t.Fatalf("hidden data version state = %s, want cached", versions[1].State)
	}
}

func TestObjectRepo_CacheAccessAndCommitKeepPresenceSemanticsSeparate(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "cache-access-bucket")
	version := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000CA01", 10)
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	// Residency lives on the content's cache entry, so the version row is not
	// what these writes touch.
	lifecycleUpdatedAt := time.Date(2026, time.July, 1, 12, 0, 0, 0, time.UTC)
	mustExecRaw(t, db, `UPDATE object_versions SET updated_at = ? WHERE version_id = ?`, lifecycleUpdatedAt, version.VersionID)
	if err := repos.Objects.ClearContentCachePresence(ctx, *version.ContentID); err != nil {
		t.Fatalf("clear cache presence: %v", err)
	}
	mustExecRaw(t, db, `UPDATE object_cache SET cache_accessed_at = NULL WHERE content_id = ?`, *version.ContentID)

	accessedAt := lifecycleUpdatedAt.Add(2 * time.Hour)
	if err := repos.Objects.RecordContentCacheAccess(ctx, *version.ContentID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}
	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", got, err)
	}
	if got.InCache {
		t.Fatal("in_cache = true after access-only timestamp update, want false")
	}
	if got.CacheAccessedAt == nil || !got.CacheAccessedAt.Equal(accessedAt) {
		t.Fatalf("cache_accessed_at = %v, want %v", got.CacheAccessedAt, accessedAt)
	}
	if !got.UpdatedAt.Equal(lifecycleUpdatedAt) {
		t.Fatalf("updated_at = %v, want unchanged %v", got.UpdatedAt, lifecycleUpdatedAt)
	}

	committedAt := accessedAt.Add(time.Hour)
	if err := repos.Objects.RecordContentCacheCommit(ctx, *version.ContentID, committedAt); err != nil {
		t.Fatalf("RecordVersionCacheCommit: %v", err)
	}
	got, err = repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID after cache commit: version=%v err=%v", got, err)
	}
	if !got.InCache {
		t.Fatal("in_cache = false after cache commit, want true")
	}
	if got.CacheAccessedAt == nil || !got.CacheAccessedAt.Equal(committedAt) {
		t.Fatalf("cache_accessed_at after commit = %v, want %v", got.CacheAccessedAt, committedAt)
	}
	if !got.UpdatedAt.Equal(lifecycleUpdatedAt) {
		t.Fatalf("updated_at after cache commit = %v, want unchanged %v", got.UpdatedAt, lifecycleUpdatedAt)
	}

	if err := repos.Objects.ClearContentCachePresence(ctx, *version.ContentID); err != nil {
		t.Fatalf("ClearContentCachePresence: %v", err)
	}
	olderAccess := committedAt.Add(-time.Hour)
	if err := repos.Objects.RecordContentCacheAccess(ctx, *version.ContentID, olderAccess); err != nil {
		t.Fatalf("RecordVersionCacheAccess(older): %v", err)
	}
	got, err = repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID after older access: version=%v err=%v", got, err)
	}
	if got.InCache {
		t.Fatal("in_cache = true after access-only update of an absent cache entry")
	}
	if got.CacheAccessedAt == nil || !got.CacheAccessedAt.Equal(committedAt) {
		t.Fatalf("cache_accessed_at after older write = %v, want monotonic %v", got.CacheAccessedAt, committedAt)
	}
}

// mustExecRaw runs one setup statement that has no repository entry point.
func mustExecRaw(t *testing.T, db *bun.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.NewRaw(query, args...).Exec(context.Background()); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func newObjectVersion(bucketID int64, key, versionID string, size int64) *model.ObjectVersion {
	return &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucketID,
		Key:         key,
		Size:        size,
		ETag:        "etag-" + versionID,
		ContentType: "text/plain",
	}
}

func TestObjectRepo_CreateVersionAndSetCurrent_SecondUploadKeepsVersionHistory(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "version-bucket")

	v1 := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000001", 10)
	objectID, err := createVersion(t, repos, v1)
	if err != nil {
		t.Fatalf("first CreateVersionAndSetCurrent: %v", err)
	}
	v2 := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000002", 20)
	objectID2, err := createVersion(t, repos, v2)
	if err != nil {
		t.Fatalf("second CreateVersionAndSetCurrent: %v", err)
	}
	if objectID2 != objectID {
		t.Fatalf("object id changed on second upload: got %d want %d", objectID2, objectID)
	}

	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.VersionID != v2.VersionID {
		t.Fatalf("current version = %s, want %s", current.VersionID, v2.VersionID)
	}
	if current.Size != 20 || current.ETag != v2.ETag || current.CacheKey() != v2.CacheKey() {
		t.Fatalf("current version not refreshed: size=%d etag=%s cache=%s", current.Size, current.ETag, current.CacheKey())
	}

	gotV1, err := repos.Objects.GetVersionByID(ctx, v1.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(v1): %v", err)
	}
	gotV2, err := repos.Objects.GetVersionByID(ctx, v2.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(v2): %v", err)
	}
	if gotV1 == nil || gotV2 == nil {
		t.Fatal("expected both versions to remain queryable")
	}
	if gotV1.Metadata == nil || gotV2.Metadata == nil || len(gotV1.Metadata) != 0 || len(gotV2.Metadata) != 0 {
		t.Fatalf("nil metadata was not normalized: v1=%#v v2=%#v", gotV1.Metadata, gotV2.Metadata)
	}
	if gotV1.ObjectID != objectID || gotV2.ObjectID != objectID {
		t.Fatalf("version object ids = %d/%d, want %d", gotV1.ObjectID, gotV2.ObjectID, objectID)
	}
	if !gotV1.InCache || !gotV2.InCache || !current.InCache {
		t.Fatalf("new versions should be marked in cache: v1=%v v2=%v current=%v", gotV1.InCache, gotV2.InCache, current.InCache)
	}
}

func TestObjectRepo_CreateRestoredVersionAndSetCurrent(t *testing.T) {
	t.Run("creates new current and preserves history", func(t *testing.T) {
		db := testDB(t)
		repos := repository.NewRepositories(db)
		ctx := context.Background()
		bucket := seedBucket(t, db, "restore-version-bucket")

		source := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R001", 10)
		source.Metadata = map[string]string{"source": "old"}
		objectID, err := createVersion(t, repos, source)
		if err != nil {
			t.Fatalf("create source: %v", err)
		}
		current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R002", source.Size)
		current.ETag = source.ETag
		current.Checksum = source.Checksum
		current.Metadata = map[string]string{"source": "new"}
		if _, err := createVersion(t, repos, current); err != nil {
			t.Fatalf("create current: %v", err)
		}

		restored := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R003", source.Size)
		restored.ETag = source.ETag
		restored.Checksum = source.Checksum
		restored.Metadata = map[string]string{"source": "old"}
		gotObjectID, err := repos.Objects.CreateRestoredVersionAndSetCurrent(ctx, withContent(t, repos, restored), source.VersionID, current.VersionID)
		if err != nil {
			t.Fatalf("CreateRestoredVersionAndSetCurrent: %v", err)
		}
		if gotObjectID != objectID {
			t.Fatalf("object ID = %d, want %d", gotObjectID, objectID)
		}

		gotCurrent, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
		if err != nil || gotCurrent == nil {
			t.Fatalf("get current: version=%v err=%v", gotCurrent, err)
		}
		if gotCurrent.VersionID != restored.VersionID {
			t.Fatalf("current version = %s, want %s", gotCurrent.VersionID, restored.VersionID)
		}
		gotSource, err := repos.Objects.GetVersionByID(ctx, source.VersionID)
		if err != nil || gotSource == nil {
			t.Fatalf("get source: version=%v err=%v", gotSource, err)
		}
		if gotSource.IsCurrent || gotSource.Size != source.Size || gotSource.ETag != source.ETag || gotSource.Metadata["source"] != "old" {
			t.Fatalf("source version changed unexpectedly: %#v", gotSource)
		}
		gotPrevious, err := repos.Objects.GetVersionByID(ctx, current.VersionID)
		if err != nil || gotPrevious == nil || gotPrevious.IsCurrent {
			t.Fatalf("previous current after restore: version=%v err=%v", gotPrevious, err)
		}
		count, err := db.NewSelect().Model((*model.ObjectVersion)(nil)).Where("object_id = ?", objectID).Count(ctx)
		if err != nil {
			t.Fatalf("count versions: %v", err)
		}
		if count != 3 {
			t.Fatalf("version count = %d, want 3", count)
		}
	})

	t.Run("rejects missing source", func(t *testing.T) {
		db := testDB(t)
		repos := repository.NewRepositories(db)
		ctx := context.Background()
		bucket := seedBucket(t, db, "restore-missing-source-bucket")
		current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R011", 10)
		if _, err := createVersion(t, repos, current); err != nil {
			t.Fatalf("create current: %v", err)
		}

		_, err := repos.Objects.CreateRestoredVersionAndSetCurrent(
			ctx,
			newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R012", 10),
			"01J0000000000000000000R099",
			current.VersionID,
		)
		if !errors.Is(err, repository.ErrNotFound) {
			t.Fatalf("error = %v, want ErrNotFound", err)
		}
	})

	t.Run("rejects current source", func(t *testing.T) {
		db := testDB(t)
		repos := repository.NewRepositories(db)
		ctx := context.Background()
		bucket := seedBucket(t, db, "restore-current-source-bucket")
		current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R015", 10)
		if _, err := createVersion(t, repos, current); err != nil {
			t.Fatalf("create current: %v", err)
		}
		restored := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R016", 10)

		_, err := repos.Objects.CreateRestoredVersionAndSetCurrent(ctx, withContent(t, repos, restored), current.VersionID, current.VersionID)
		if !errors.Is(err, repository.ErrAlreadyCurrent) {
			t.Fatalf("error = %v, want ErrAlreadyCurrent", err)
		}
		if got, err := repos.Objects.GetVersionByID(ctx, restored.VersionID); err != nil || got != nil {
			t.Fatalf("no-op restore row = %v err=%v, want absent", got, err)
		}
	})

	t.Run("rejects historical source matching readable current", func(t *testing.T) {
		db := testDB(t)
		repos := repository.NewRepositories(db)
		ctx := context.Background()
		bucket := seedBucket(t, db, "restore-matching-source-bucket")
		sharedContentID := seedContent(t, repos, bucket.ID, "shared-restore-checksum", 10)
		source := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R017", 10)
		source.ContentID = &sharedContentID
		source.Metadata = map[string]string{"content": "same"}
		if _, err := createVersion(t, repos, source); err != nil {
			t.Fatalf("create source: %v", err)
		}
		current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R018", source.Size)
		current.ETag = "different-etag-for-the-same-content"
		current.ContentID = &sharedContentID
		current.ContentType = source.ContentType
		current.Metadata = map[string]string{"content": "same"}
		if _, err := createVersion(t, repos, current); err != nil {
			t.Fatalf("create current: %v", err)
		}
		restored := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R019", source.Size)

		_, err := repos.Objects.CreateRestoredVersionAndSetCurrent(ctx, withContent(t, repos, restored), source.VersionID, current.VersionID)
		if !errors.Is(err, repository.ErrAlreadyCurrent) {
			t.Fatalf("error = %v, want ErrAlreadyCurrent", err)
		}
		if got, err := repos.Objects.GetVersionByID(ctx, restored.VersionID); err != nil || got != nil {
			t.Fatalf("matching restore row = %v err=%v, want absent", got, err)
		}
		gotCurrent, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
		if err != nil || gotCurrent == nil || gotCurrent.VersionID != current.VersionID {
			t.Fatalf("current after matching restore: version=%v err=%v", gotCurrent, err)
		}
	})

	t.Run("rejects stale current token", func(t *testing.T) {
		db := testDB(t)
		repos := repository.NewRepositories(db)
		ctx := context.Background()
		bucket := seedBucket(t, db, "restore-stale-token-bucket")
		source := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R021", 10)
		if _, err := createVersion(t, repos, source); err != nil {
			t.Fatalf("create source: %v", err)
		}
		current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R022", 20)
		if _, err := createVersion(t, repos, current); err != nil {
			t.Fatalf("create current: %v", err)
		}
		restored := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R023", 10)

		_, err := repos.Objects.CreateRestoredVersionAndSetCurrent(ctx, withContent(t, repos, restored), source.VersionID, source.VersionID)
		if !errors.Is(err, repository.ErrConflict) {
			t.Fatalf("error = %v, want ErrConflict", err)
		}
		if got, err := repos.Objects.GetVersionByID(ctx, restored.VersionID); err != nil || got != nil {
			t.Fatalf("stale restore row = %v err=%v, want absent", got, err)
		}
		gotCurrent, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
		if err != nil || gotCurrent == nil || gotCurrent.VersionID != current.VersionID {
			t.Fatalf("current after stale restore: version=%v err=%v", gotCurrent, err)
		}
	})
}

func TestObjectRepo_CreateRestoredVersionAndSetCurrent_ConcurrentTokenHasOneWinner(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "restore-objects.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening sqlite db: %v", err)
	}
	sqldb.SetMaxOpenConns(8)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "concurrent-restore-bucket")
	source := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R031", 10)
	if _, err := createVersion(t, repos, source); err != nil {
		t.Fatalf("create source: %v", err)
	}
	current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R032", 20)
	if _, err := createVersion(t, repos, current); err != nil {
		t.Fatalf("create current: %v", err)
	}

	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			versionID := "01J0000000000000000000R04" + string(rune('0'+i))
			_, err := repos.Objects.CreateRestoredVersionAndSetCurrent(
				ctx,
				withContent(t, repos, newObjectVersion(bucket.ID, "file.txt", versionID, source.Size)),
				source.VersionID,
				current.VersionID,
			)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)

	succeeded := 0
	conflicted := 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, repository.ErrConflict):
			conflicted++
		default:
			t.Fatalf("unexpected concurrent restore error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("results = success:%d conflict:%d, want 1/1", succeeded, conflicted)
	}
	count, err := db.NewSelect().Model((*model.ObjectVersion)(nil)).Where("bucket_id = ? AND key = ?", bucket.ID, "file.txt").Count(ctx)
	if err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if count != 3 {
		t.Fatalf("version count = %d, want source + previous current + winner", count)
	}
}

func TestObjectRepo_CreateVersionAndSetCurrent_ConcurrentFirstUpload(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "objects.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening sqlite db: %v", err)
	}
	sqldb.SetMaxOpenConns(8)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "concurrent-version-bucket")

	const writers = 8
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	objectIDs := make(chan int64, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			versionID := "01J00000000000000000000C" + string(rune('A'+i))
			objectID, err := createVersion(t, repos, newObjectVersion(bucket.ID, "same-key.txt", versionID, int64(i+1)))
			if err != nil {
				errs <- err
				return
			}
			objectIDs <- objectID
		}(i)
	}
	wg.Wait()
	close(errs)
	close(objectIDs)

	for err := range errs {
		t.Fatalf("CreateVersionAndSetCurrent concurrent error: %v", err)
	}

	var objectID int64
	for id := range objectIDs {
		if objectID == 0 {
			objectID = id
			continue
		}
		if id != objectID {
			t.Fatalf("concurrent object IDs differ: got %d want %d", id, objectID)
		}
	}

	versionCount, err := db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", objectID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting versions: %v", err)
	}
	if versionCount != writers {
		t.Fatalf("version count = %d, want %d", versionCount, writers)
	}
}

func TestObjectRepo_CreateVersionAndSetCurrentIfChanged_ConcurrentIdenticalWriteReusesCurrent(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "objects.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening sqlite db: %v", err)
	}
	sqldb.SetMaxOpenConns(8)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrations.NewMigrator(db)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "concurrent-dedupe-bucket")

	// Identical bytes are one content row, so every writer points at the same
	// content the way the backend would after a single EnsureContent.
	sharedContentID := seedContent(t, repos, bucket.ID, "same-key-checksum", 100)

	const writers = 8
	var wg sync.WaitGroup
	results := make(chan repository.ObjectVersionWriteResult, writers)
	errs := make(chan error, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			versionID := "01J00000000000000000000D" + string(rune('A'+i))
			version := newObjectVersion(bucket.ID, "same-key.txt", versionID, 100)
			version.ETag = "same-etag"
			version.ContentID = &sharedContentID
			result, err := repos.Objects.CreateVersionAndSetCurrentIfChanged(ctx, version)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(i)
	}
	wg.Wait()
	close(errs)
	close(results)

	for err := range errs {
		t.Fatalf("CreateVersionAndSetCurrentIfChanged concurrent error: %v", err)
	}

	var objectID int64
	created := 0
	for result := range results {
		if objectID == 0 {
			objectID = result.ObjectID
		}
		if result.ObjectID != objectID {
			t.Fatalf("concurrent object IDs differ: got %d want %d", result.ObjectID, objectID)
		}
		if result.Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created result count = %d, want 1", created)
	}

	versionCount, err := db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", objectID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting versions: %v", err)
	}
	if versionCount != 1 {
		t.Fatalf("version count = %d, want 1", versionCount)
	}
}

func TestObjectRepo_ListByBucketReadsCurrentVersionOnly(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "list-bucket")

	for _, tc := range []struct {
		key  string
		size int64
	}{
		{"a.txt", 1},
		{"b.txt", 2},
		{"Dir/c.txt", 4},
		{"dir/c.txt", 3},
	} {
		v := newObjectVersion(bucket.ID, tc.key, "01J0000000000000000000000"+string(rune('3'+tc.size)), tc.size)
		if _, err := createVersion(t, repos, v); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(%s): %v", tc.key, err)
		}
	}
	latestB := newObjectVersion(bucket.ID, "b.txt", "01J00000000000000000000009", 22)
	if _, err := createVersion(t, repos, latestB); err != nil {
		t.Fatalf("second b.txt upload: %v", err)
	}

	all, err := repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, "", "", 0)
	if err != nil {
		t.Fatalf("ListByBucket: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("object count = %d, want 4 current keys", len(all))
	}
	var currentB *model.ObjectVersion
	for i := range all {
		if all[i].Key == "b.txt" {
			currentB = &all[i]
			break
		}
	}
	if currentB == nil || currentB.VersionID != latestB.VersionID || currentB.Size != 22 {
		t.Fatalf("b.txt current version = %#v", currentB)
	}

	prefixed, err := repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, "dir/", "", 0)
	if err != nil {
		t.Fatalf("ListByBucket(prefix): %v", err)
	}
	if len(prefixed) != 1 || prefixed[0].Key != "dir/c.txt" {
		t.Fatalf("prefixed keys = %#v", prefixed)
	}

	afterKey, err := repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, "", "a.txt", 0)
	if err != nil {
		t.Fatalf("ListByBucket(afterKey): %v", err)
	}
	if len(afterKey) != 2 || afterKey[0].Key != "b.txt" {
		t.Fatalf("afterKey result = %#v", afterKey)
	}

	fromKey, err := repos.Objects.ListCurrentVersionsByBucketAtOrAfter(ctx, bucket.ID, "", "b.txt", 0)
	if err != nil {
		t.Fatalf("ListByBucketAtOrAfter(fromKey): %v", err)
	}
	if len(fromKey) != 2 || fromKey[0].Key != "b.txt" {
		t.Fatalf("fromKey result = %#v, want b.txt first", fromKey)
	}

	for _, tc := range []struct {
		key       string
		versionID string
	}{
		{"wild%/literal.txt", "01J00000000000000000002001"},
		{"wildX/literal.txt", "01J00000000000000000002002"},
		{"under_/literal.txt", "01J00000000000000000002003"},
		{"underX/literal.txt", "01J00000000000000000002004"},
	} {
		if _, err := createVersion(t, repos, newObjectVersion(bucket.ID, tc.key, tc.versionID, 10)); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(%s): %v", tc.key, err)
		}
	}

	tests := []struct {
		name   string
		prefix string
		want   []string
	}{
		{name: "percent", prefix: "wild%/", want: []string{"wild%/literal.txt"}},
		{name: "underscore", prefix: "under_/", want: []string{"under_/literal.txt"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, tt.prefix, "", 10)
			if err != nil {
				t.Fatalf("ListCurrentVersionsByBucket(%q): %v", tt.prefix, err)
			}
			requireObjectVersionKeys(t, rows, tt.want)
		})
	}

	page, err := repos.Objects.ListCurrentVersionsByBucket(ctx, bucket.ID, "under", "underX/literal.txt", 10)
	if err != nil {
		t.Fatalf("ListCurrentVersionsByBucket marker: %v", err)
	}
	requireObjectVersionKeys(t, page, []string{"under_/literal.txt"})
}

func requireObjectVersionKeys(t *testing.T, rows []model.ObjectVersion, want []string) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("keys = %#v, want %#v", objectVersionKeys(rows), want)
	}
	for i := range want {
		if rows[i].Key != want[i] {
			t.Fatalf("keys = %#v, want %#v", objectVersionKeys(rows), want)
		}
	}
}

func objectVersionKeys(rows []model.ObjectVersion) []string {
	keys := make([]string, len(rows))
	for i := range rows {
		keys[i] = rows[i].Key
	}
	return keys
}

func TestObjectRepo_GetVersionByBucketKeyAndIDScopesVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "version-scope-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000071", 10)
	if _, err := createVersion(t, repos, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}

	got, err := repos.Objects.GetVersionByBucketKeyAndID(ctx, bucket.ID, "file.txt", version.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByBucketKeyAndID: %v", err)
	}
	if got == nil || got.VersionID != version.VersionID {
		t.Fatalf("scoped version = %#v, want %s", got, version.VersionID)
	}

	mismatch, err := repos.Objects.GetVersionByBucketKeyAndID(ctx, bucket.ID, "other.txt", version.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByBucketKeyAndID mismatch: %v", err)
	}
	if mismatch != nil {
		t.Fatalf("mismatched key returned %#v, want nil", mismatch)
	}
}

func TestObjectRepo_ListVersionsByBucketOrdersAndMarksCurrent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "version-list-bucket")

	oldVersion := newObjectVersion(bucket.ID, "a.txt", "01J00000000000000000000074", 10)
	if _, err := createVersion(t, repos, oldVersion); err != nil {
		t.Fatalf("create old version: %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "a.txt", "01J00000000000000000000075", 20)
	if _, err := createVersion(t, repos, currentVersion); err != nil {
		t.Fatalf("create current version: %v", err)
	}
	if _, err := createVersion(t, repos, newObjectVersion(bucket.ID, "b.txt", "01J00000000000000000000076", 30)); err != nil {
		t.Fatalf("create b version: %v", err)
	}

	rows, err := repos.Objects.ListVersionsByBucket(ctx, bucket.ID, "", "", "", 10)
	if err != nil {
		t.Fatalf("ListVersionsByBucket: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows len = %d, want 3", len(rows))
	}
	if rows[0].Key != "a.txt" || rows[0].VersionID != currentVersion.VersionID {
		t.Fatalf("first row = %s/%s, want current a.txt", rows[0].Key, rows[0].VersionID)
	}
	if rows[0].VersionID != currentVersion.VersionID {
		t.Fatalf("current marker = %s, want %s", rows[0].VersionID, currentVersion.VersionID)
	}

	page, err := repos.Objects.ListVersionsByBucket(ctx, bucket.ID, "", rows[0].Key, rows[0].VersionID, 10)
	if err != nil {
		t.Fatalf("ListVersionsByBucket marker: %v", err)
	}
	if len(page) == 0 || page[0].VersionID != oldVersion.VersionID {
		t.Fatalf("marker page first = %#v, want old version", page)
	}

	if _, err := createVersion(t, repos, newObjectVersion(bucket.ID, "case/a.txt", "01J00000000000000000000077", 40)); err != nil {
		t.Fatalf("create case version: %v", err)
	}
	if _, err := createVersion(t, repos, newObjectVersion(bucket.ID, "Case/a.txt", "01J00000000000000000000078", 50)); err != nil {
		t.Fatalf("create Case version: %v", err)
	}
	prefixed, err := repos.Objects.ListVersionsByBucket(ctx, bucket.ID, "case/", "", "", 10)
	if err != nil {
		t.Fatalf("ListVersionsByBucket prefix: %v", err)
	}
	if len(prefixed) != 1 || prefixed[0].Key != "case/a.txt" {
		t.Fatalf("case-sensitive prefix rows = %#v", prefixed)
	}
}

func TestObjectRepo_ListVersionsByKey(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "version-list-key-bucket")
	otherBucket := seedBucket(t, db, "version-list-key-other-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000002001", 10)
	if _, err := createVersion(t, repos, oldVersion); err != nil {
		t.Fatalf("create old version: %v", err)
	}
	middleVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000002002", 20)
	if _, err := createVersion(t, repos, middleVersion); err != nil {
		t.Fatalf("create middle version: %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000002003", 30)
	if _, err := createVersion(t, repos, currentVersion); err != nil {
		t.Fatalf("create current version: %v", err)
	}
	if _, err := createVersion(t, repos, newObjectVersion(bucket.ID, "other.txt", "01J00000000000000000002004", 40)); err != nil {
		t.Fatalf("create other key version: %v", err)
	}
	if _, err := createVersion(t, repos, newObjectVersion(otherBucket.ID, "file.txt", "01J00000000000000000002005", 50)); err != nil {
		t.Fatalf("create other bucket version: %v", err)
	}

	rows, err := repos.Objects.ListVersionsByKey(ctx, bucket.ID, "file.txt", "", 10)
	if err != nil {
		t.Fatalf("ListVersionsByKey: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows len = %d, want 3", len(rows))
	}
	for i, row := range rows {
		if row.BucketID != bucket.ID || row.Key != "file.txt" {
			t.Fatalf("row %d = bucket:%d key:%q, want target bucket/file.txt", i, row.BucketID, row.Key)
		}
	}
	if rows[0].VersionID != currentVersion.VersionID || !rows[0].IsCurrent {
		t.Fatalf("first row = %#v, want current version %s", rows[0], currentVersion.VersionID)
	}
	if rows[1].VersionID != middleVersion.VersionID || rows[2].VersionID != oldVersion.VersionID {
		t.Fatalf("ordered rows = %s/%s/%s, want current/middle/old", rows[0].VersionID, rows[1].VersionID, rows[2].VersionID)
	}

	page, err := repos.Objects.ListVersionsByKey(ctx, bucket.ID, "file.txt", currentVersion.VersionID, 10)
	if err != nil {
		t.Fatalf("ListVersionsByKey marker: %v", err)
	}
	if len(page) != 2 || page[0].VersionID != middleVersion.VersionID || page[1].VersionID != oldVersion.VersionID {
		t.Fatalf("marker page = %#v, want middle then old", page)
	}

	limited, err := repos.Objects.ListVersionsByKey(ctx, bucket.ID, "file.txt", "", 2)
	if err != nil {
		t.Fatalf("ListVersionsByKey limit: %v", err)
	}
	if len(limited) != 2 || limited[0].VersionID != currentVersion.VersionID || limited[1].VersionID != middleVersion.VersionID {
		t.Fatalf("limited rows = %#v, want current then middle", limited)
	}
}

func TestObjectRepo_DeleteMarkerVersionRestoresPreviousCurrentVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "delete-marker-restore-bucket")

	first := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001011", 10)
	second := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001012", 20)
	if _, err := createVersion(t, repos, first); err != nil {
		t.Fatalf("create first version: %v", err)
	}
	if _, err := createVersion(t, repos, second); err != nil {
		t.Fatalf("create second version: %v", err)
	}
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J00000000000000000001013")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	if err := repos.Objects.DeleteMarkerVersion(ctx, bucket.ID, "file.txt", marker.VersionID); err != nil {
		t.Fatalf("DeleteMarkerVersion: %v", err)
	}

	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetCurrentVersionByBucketAndKey: %v", err)
	}
	if current == nil || current.VersionID != second.VersionID || current.IsDeleteMarker {
		t.Fatalf("current = %#v, want restored data version %s", current, second.VersionID)
	}
	deletedMarker, err := repos.Objects.GetVersionByID(ctx, marker.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(marker): %v", err)
	}
	if deletedMarker != nil {
		t.Fatalf("deleted marker still exists: %#v", deletedMarker)
	}
}

func TestObjectRepo_DeleteOnlyMarkerVersionDeletesObjectIdentity(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "delete-only-marker-bucket")

	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "deleted.txt", "01J00000000000000000001014")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}

	if err := repos.Objects.DeleteMarkerVersion(ctx, bucket.ID, "deleted.txt", marker.VersionID); err != nil {
		t.Fatalf("DeleteMarkerVersion: %v", err)
	}

	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "deleted.txt")
	if err != nil {
		t.Fatalf("GetCurrentVersionByBucketAndKey: %v", err)
	}
	if current != nil {
		t.Fatalf("current = %#v, want none after deleting only marker", current)
	}
	objectCount, err := db.NewSelect().
		Model((*model.Object)(nil)).
		Where("bucket_id = ? AND key = ?", bucket.ID, "deleted.txt").
		Count(ctx)
	if err != nil {
		t.Fatalf("count object identities: %v", err)
	}
	if objectCount != 0 {
		t.Fatalf("object identity count = %d, want 0", objectCount)
	}
}

func TestObjectRepo_RestoreCurrentDeleteMarkerStackRemovesMarkersUntilDataVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "delete-marker-stack-bucket")

	data := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001021", 10)
	if _, err := createVersion(t, repos, data); err != nil {
		t.Fatalf("create data version: %v", err)
	}
	if _, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J00000000000000000001022"); err != nil {
		t.Fatalf("create first marker: %v", err)
	}
	currentMarker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J00000000000000000001023")
	if err != nil {
		t.Fatalf("create second marker: %v", err)
	}

	restored, err := repos.Objects.RestoreCurrentDeleteMarkerStack(ctx, bucket.ID, "file.txt", currentMarker.VersionID)
	if err != nil {
		t.Fatalf("RestoreCurrentDeleteMarkerStack: %v", err)
	}
	if restored.VersionID != data.VersionID || restored.IsDeleteMarker {
		t.Fatalf("restored = %#v, want data version %s", restored, data.VersionID)
	}

	versions, err := repos.Objects.ListVersionsByBucket(ctx, bucket.ID, "", "", "", 10)
	if err != nil {
		t.Fatalf("ListVersionsByBucket: %v", err)
	}
	if len(versions) != 1 || versions[0].VersionID != data.VersionID || !versions[0].IsCurrent {
		t.Fatalf("versions after restore = %#v, want only current data version", versions)
	}
}

func TestObjectRepo_DeleteMarkerStatsAndRecoverableListIgnoreUnrestorableMarkers(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "delete-marker-stats-bucket")

	data := newObjectVersion(bucket.ID, "restorable.txt", "01J00000000000000000001031", 25)
	if _, err := createVersion(t, repos, data); err != nil {
		t.Fatalf("create data version: %v", err)
	}
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "restorable.txt", "01J00000000000000000001032")
	if err != nil {
		t.Fatalf("create restorable marker: %v", err)
	}
	if _, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "missing.txt", "01J00000000000000000001033"); err != nil {
		t.Fatalf("create unrestorable marker: %v", err)
	}

	count, err := repos.Objects.CountByBucket(ctx, bucket.ID)
	if err != nil {
		t.Fatalf("CountByBucket: %v", err)
	}
	if count != 0 {
		t.Fatalf("current count = %d, want delete markers ignored", count)
	}
	total, err := repos.Objects.TotalSizeByBucket(ctx, bucket.ID)
	if err != nil {
		t.Fatalf("TotalSizeByBucket: %v", err)
	}
	if total != 0 {
		t.Fatalf("current size = %d, want delete markers ignored", total)
	}
	bucketStats, err := repos.Objects.BucketStats(ctx, bucket.ID)
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	if bucketStats.Count != 0 || bucketStats.TotalSize != 0 {
		t.Fatalf("bucket stats = count:%d size:%d, want delete markers ignored", bucketStats.Count, bucketStats.TotalSize)
	}

	deleted, err := repos.Objects.ListRecoverableDeleteMarkers(ctx, bucket.ID, "", "", 10)
	if err != nil {
		t.Fatalf("ListRecoverableDeleteMarkers: %v", err)
	}
	if len(deleted) != 1 {
		t.Fatalf("recoverable markers len = %d, want 1", len(deleted))
	}
	if deleted[0].Marker.VersionID != marker.VersionID || deleted[0].RestoreVersion.VersionID != data.VersionID {
		t.Fatalf("recoverable marker = %#v, want marker %s restoring %s", deleted[0], marker.VersionID, data.VersionID)
	}

	lowerData := newObjectVersion(bucket.ID, "trash/lower.txt", "01J00000000000000000001003", 10)
	if _, err := createVersion(t, repos, lowerData); err != nil {
		t.Fatalf("create lower trash data: %v", err)
	}
	lowerMarker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "trash/lower.txt", "01J00000000000000000001004")
	if err != nil {
		t.Fatalf("create lower trash marker: %v", err)
	}
	if _, err := createVersion(t, repos, newObjectVersion(bucket.ID, "Trash/lower.txt", "01J00000000000000000001005", 10)); err != nil {
		t.Fatalf("create upper trash data: %v", err)
	}
	if _, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "Trash/lower.txt", "01J00000000000000000001006"); err != nil {
		t.Fatalf("create upper trash marker: %v", err)
	}
	prefixedDeleted, err := repos.Objects.ListRecoverableDeleteMarkers(ctx, bucket.ID, "trash/", "", 10)
	if err != nil {
		t.Fatalf("ListRecoverableDeleteMarkers prefix: %v", err)
	}
	if len(prefixedDeleted) != 1 || prefixedDeleted[0].Marker.VersionID != lowerMarker.VersionID {
		t.Fatalf("case-sensitive recoverable markers = %#v", prefixedDeleted)
	}
}

func TestObjectRepo_RestoreCurrentDeleteMarkerStackRejectsStaleMarker(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "delete-marker-stale-bucket")

	if _, err := createVersion(t, repos, newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001041", 10)); err != nil {
		t.Fatalf("create data version: %v", err)
	}
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J00000000000000000001042")
	if err != nil {
		t.Fatalf("create marker: %v", err)
	}
	if _, err := createVersion(t, repos, newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001043", 20)); err != nil {
		t.Fatalf("create newer data version: %v", err)
	}

	if _, err := repos.Objects.RestoreCurrentDeleteMarkerStack(ctx, bucket.ID, "file.txt", marker.VersionID); err == nil {
		t.Fatal("RestoreCurrentDeleteMarkerStack returned nil error for stale marker")
	}
}

func TestObjectRepo_SetVersionCachePresenceMirrorsOnlyCurrentVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "cache-location-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000023", 10)
	if _, err := createVersion(t, repos, oldVersion); err != nil {
		t.Fatalf("old version: %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000024", 20)
	if _, err := createVersion(t, repos, currentVersion); err != nil {
		t.Fatalf("current version: %v", err)
	}

	if err := repos.Objects.SetVersionCachePresence(ctx, oldVersion.VersionID, false); err != nil {
		t.Fatalf("old SetVersionCachePresence: %v", err)
	}
	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if !current.InCache {
		t.Fatal("old cache update polluted current in_cache")
	}

	if err := repos.Objects.SetVersionCachePresence(ctx, currentVersion.VersionID, false); err != nil {
		t.Fatalf("current SetVersionCachePresence: %v", err)
	}
	current, err = repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey after current cache update: %v", err)
	}
	if current.InCache {
		t.Fatal("current in_cache = true, want false")
	}
}

func TestObjectRepo_CurrentStatsUseCurrentVersions(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucketA := seedBucket(t, db, "stats-a")
	bucketB := seedBucket(t, db, "stats-b")
	emptyBucket := seedBucket(t, db, "stats-empty")

	emptyStats, err := repos.Objects.BucketStats(ctx, emptyBucket.ID)
	if err != nil {
		t.Fatalf("BucketStats empty: %v", err)
	}
	if emptyStats.Count != 0 || emptyStats.TotalSize != 0 {
		t.Fatalf("empty bucket stats = count:%d size:%d, want 0/0", emptyStats.Count, emptyStats.TotalSize)
	}

	if _, err := createVersion(t, repos, newObjectVersion(bucketA.ID, "a.txt", "01J00000000000000000000041", 100)); err != nil {
		t.Fatalf("create a v1: %v", err)
	}
	if _, err := createVersion(t, repos, newObjectVersion(bucketA.ID, "a.txt", "01J00000000000000000000042", 250)); err != nil {
		t.Fatalf("create a v2: %v", err)
	}
	if _, err := createVersion(t, repos, newObjectVersion(bucketB.ID, "b.txt", "01J00000000000000000000043", 500)); err != nil {
		t.Fatalf("create b: %v", err)
	}

	countA, err := repos.Objects.CountByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("CountByBucket: %v", err)
	}
	if countA != 1 {
		t.Fatalf("bucket A current count = %d, want 1", countA)
	}

	totalA, err := repos.Objects.TotalSizeByBucket(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("TotalSizeByBucket: %v", err)
	}
	if totalA != 250 {
		t.Fatalf("bucket A current size = %d, want 250", totalA)
	}
	statsA, err := repos.Objects.BucketStats(ctx, bucketA.ID)
	if err != nil {
		t.Fatalf("BucketStats: %v", err)
	}
	if statsA.Count != 1 || statsA.TotalSize != 250 {
		t.Fatalf("bucket A stats = count:%d size:%d, want count:1 size:250", statsA.Count, statsA.TotalSize)
	}

	stats, err := repos.Objects.AggregateByBucket(ctx)
	if err != nil {
		t.Fatalf("AggregateByBucket: %v", err)
	}
	if got := stats[bucketA.ID]; got.Count != 1 || got.TotalSize != 250 {
		t.Fatalf("bucket A aggregate = count:%d size:%d", got.Count, got.TotalSize)
	}
	if got := stats[bucketB.ID]; got.Count != 1 || got.TotalSize != 500 {
		t.Fatalf("bucket B aggregate = count:%d size:%d", got.Count, got.TotalSize)
	}
}

func TestRepos_WithTx(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()

	err := repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		b := &model.Bucket{Name: "tx-bucket", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
		if err := txRepos.Buckets.Create(ctx, b); err != nil {
			return err
		}
		return context.Canceled
	})
	if err == nil {
		t.Fatal("expected error from WithTx")
	}

	got, err := repos.Buckets.GetByName(ctx, "tx-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil after rollback")
	}

	err = repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		b := &model.Bucket{Name: "tx-committed", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}
		return txRepos.Buckets.Create(ctx, b)
	})
	if err != nil {
		t.Fatalf("WithTx commit: %v", err)
	}

	got, err = repos.Buckets.GetByName(ctx, "tx-committed")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if got == nil {
		t.Fatal("expected bucket after commit")
	}
}
