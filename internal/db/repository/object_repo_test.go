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
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/migrate"
)

func newObjectVersion(bucketID int64, key, versionID string, size int64) *model.ObjectVersion {
	return &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucketID,
		Key:         key,
		Size:        size,
		ETag:        "etag-" + versionID,
		Checksum:    "checksum-" + versionID,
		ContentType: "text/plain",
		CacheKey:    ".versions/" + versionID,
		State:       model.ObjectStateCached,
	}
}

func TestObjectRepo_CreateVersionAndSetCurrent_SecondUploadKeepsVersionHistory(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "version-bucket")

	v1 := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000001", 10)
	objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, v1)
	if err != nil {
		t.Fatalf("first CreateVersionAndSetCurrent: %v", err)
	}
	v2 := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000002", 20)
	objectID2, err := repos.Objects.CreateVersionAndSetCurrent(ctx, v2)
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
	if current.Size != 20 || current.ETag != v2.ETag || current.CacheKey != v2.CacheKey {
		t.Fatalf("current version not refreshed: size=%d etag=%s cache=%s", current.Size, current.ETag, current.CacheKey)
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
		objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source)
		if err != nil {
			t.Fatalf("create source: %v", err)
		}
		current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R002", source.Size)
		current.ETag = source.ETag
		current.Checksum = source.Checksum
		current.Metadata = map[string]string{"source": "new"}
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, current); err != nil {
			t.Fatalf("create current: %v", err)
		}

		restored := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R003", source.Size)
		restored.ETag = source.ETag
		restored.Checksum = source.Checksum
		restored.Metadata = map[string]string{"source": "old"}
		gotObjectID, err := repos.Objects.CreateRestoredVersionAndSetCurrent(ctx, restored, source.VersionID, current.VersionID)
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
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, current); err != nil {
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
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, current); err != nil {
			t.Fatalf("create current: %v", err)
		}
		restored := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R016", 10)

		_, err := repos.Objects.CreateRestoredVersionAndSetCurrent(ctx, restored, current.VersionID, current.VersionID)
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
		source := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R017", 10)
		source.Metadata = map[string]string{"content": "same"}
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source); err != nil {
			t.Fatalf("create source: %v", err)
		}
		current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R018", source.Size)
		current.ETag = "different-etag-for-the-same-content"
		current.Checksum = source.Checksum
		current.ContentType = source.ContentType
		current.Metadata = map[string]string{"content": "same"}
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, current); err != nil {
			t.Fatalf("create current: %v", err)
		}
		restored := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R019", source.Size)

		_, err := repos.Objects.CreateRestoredVersionAndSetCurrent(ctx, restored, source.VersionID, current.VersionID)
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
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source); err != nil {
			t.Fatalf("create source: %v", err)
		}
		current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R022", 20)
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, current); err != nil {
			t.Fatalf("create current: %v", err)
		}
		restored := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R023", 10)

		_, err := repos.Objects.CreateRestoredVersionAndSetCurrent(ctx, restored, source.VersionID, source.VersionID)
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
	migrator := migrate.NewMigrator(db, migrations.Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "concurrent-restore-bucket")
	source := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R031", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, source); err != nil {
		t.Fatalf("create source: %v", err)
	}
	current := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000R032", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, current); err != nil {
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
				newObjectVersion(bucket.ID, "file.txt", versionID, source.Size),
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

func TestObjectRepo_CountOverviewAttention(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "overview-attention-bucket")

	healthy := newObjectVersion(bucket.ID, "healthy.txt", "01J00000000000000000000A01", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, healthy); err != nil {
		t.Fatalf("seed healthy version: %v", err)
	}

	failedState := newObjectVersion(bucket.ID, "failed-state.txt", "01J00000000000000000000A02", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, failedState); err != nil {
		t.Fatalf("seed failed-state version: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, failedState.VersionID, model.ObjectStateCached, "upload failed"); err != nil {
		t.Fatalf("mark failed-state version failed: %v", err)
	}

	failedUpload := newObjectVersion(bucket.ID, "failed-upload.txt", "01J00000000000000000000A03", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, failedUpload); err != nil {
		t.Fatalf("seed failed-upload version: %v", err)
	}
	mustExec(t, db, `INSERT INTO storage_uploads (bucket_id, source_version_id, content_size, checksum, status, error_message, requested_copies) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		bucket.ID, failedUpload.VersionID, failedUpload.Size, failedUpload.Checksum, model.StorageUploadStatusFailed, "provider failed", 3)

	resolvedUpload := newObjectVersion(bucket.ID, "resolved-upload.txt", "01J00000000000000000000A06", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, resolvedUpload); err != nil {
		t.Fatalf("seed resolved-upload version: %v", err)
	}
	mustExec(t, db, `INSERT INTO storage_uploads (bucket_id, source_version_id, content_size, checksum, status, requested_copies) VALUES (?, ?, ?, ?, ?, ?)`,
		bucket.ID, resolvedUpload.VersionID, resolvedUpload.Size, resolvedUpload.Checksum, model.StorageUploadStatusFailed, 3)
	mustExec(t, db, `INSERT INTO storage_uploads (bucket_id, source_version_id, content_size, checksum, status, requested_copies) VALUES (?, ?, ?, ?, ?, ?)`,
		bucket.ID, resolvedUpload.VersionID, resolvedUpload.Size, resolvedUpload.Checksum, model.StorageUploadStatusComplete, 3)

	rejectedLatestUpload := newObjectVersion(bucket.ID, "rejected-latest-upload.txt", "01J00000000000000000000A07", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, rejectedLatestUpload); err != nil {
		t.Fatalf("seed rejected-latest-upload version: %v", err)
	}
	mustExec(t, db, `INSERT INTO storage_uploads (bucket_id, source_version_id, content_size, checksum, status, requested_copies) VALUES (?, ?, ?, ?, ?, ?)`,
		bucket.ID, rejectedLatestUpload.VersionID, rejectedLatestUpload.Size, rejectedLatestUpload.Checksum, model.StorageUploadStatusComplete, 3)
	mustExec(t, db, `INSERT INTO storage_uploads (bucket_id, source_version_id, content_size, checksum, status, requested_copies) VALUES (?, ?, ?, ?, ?, ?)`,
		bucket.ID, rejectedLatestUpload.VersionID, rejectedLatestUpload.Size, rejectedLatestUpload.Checksum, model.StorageUploadStatusRejected, 3)

	boundUpload := newObjectVersion(bucket.ID, "bound-upload.txt", "01J00000000000000000000A08", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, boundUpload); err != nil {
		t.Fatalf("seed bound-upload version: %v", err)
	}
	mustExec(t, db, `INSERT INTO storage_uploads (bucket_id, content_size, checksum, status, requested_copies) VALUES (?, ?, ?, ?, ?)`,
		bucket.ID, boundUpload.Size, boundUpload.Checksum, model.StorageUploadStatusComplete, 3)
	mustExec(t, db, `UPDATE object_versions SET state = ?, storage_upload_id = (SELECT MAX(id) FROM storage_uploads) WHERE version_id = ?`,
		model.ObjectStateStored, boundUpload.VersionID)
	mustExec(t, db, `INSERT INTO storage_uploads (bucket_id, source_version_id, content_size, checksum, status, requested_copies) VALUES (?, ?, ?, ?, ?, ?)`,
		bucket.ID, boundUpload.VersionID, boundUpload.Size, boundUpload.Checksum, model.StorageUploadStatusFailed, 3)

	unavailable := newObjectVersion(bucket.ID, "unavailable.txt", "01J00000000000000000000A04", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, unavailable); err != nil {
		t.Fatalf("seed unavailable version: %v", err)
	}
	mustExec(t, db, `INSERT INTO storage_uploads (bucket_id, source_version_id, content_size, checksum, status, requested_copies) VALUES (?, ?, ?, ?, ?, ?)`,
		bucket.ID, unavailable.VersionID, unavailable.Size, unavailable.Checksum, model.StorageUploadStatusComplete, 3)
	mustExec(t, db, `UPDATE object_versions SET state = ?, storage_upload_id = (SELECT MAX(id) FROM storage_uploads), in_cache = FALSE WHERE version_id = ?`,
		model.ObjectStateStored, unavailable.VersionID)

	if _, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "deleted.txt", "01J00000000000000000000A05"); err != nil {
		t.Fatalf("seed deleted marker: %v", err)
	}

	counts, err := repos.Objects.CountOverviewAttention(ctx)
	if err != nil {
		t.Fatalf("CountOverviewAttention: %v", err)
	}
	if counts.NeedsAttention != 3 {
		t.Fatalf("NeedsAttention = %d, want 3", counts.NeedsAttention)
	}
	if counts.Unavailable != 1 {
		t.Fatalf("Unavailable = %d, want 1", counts.Unavailable)
	}
}

func TestObjectRepo_AggregateByStateIncludesTotalSize(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "state-aggregate-bucket")

	cached := newObjectVersion(bucket.ID, "cached.txt", "01J00000000000000000000B01", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, cached); err != nil {
		t.Fatalf("seed cached version: %v", err)
	}
	failed := newObjectVersion(bucket.ID, "failed.txt", "01J00000000000000000000B02", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, failed); err != nil {
		t.Fatalf("seed failed version: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, failed.VersionID, model.ObjectStateCached, "upload failed"); err != nil {
		t.Fatalf("mark failed version: %v", err)
	}
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

func TestObjectRepo_CreateVersionAndSetCurrent_ConcurrentFirstUpload(t *testing.T) {
	sqldb, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "objects.db")+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("opening sqlite db: %v", err)
	}
	sqldb.SetMaxOpenConns(8)
	db := bun.NewDB(sqldb, sqlitedialect.New())
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	migrator := migrate.NewMigrator(db, migrations.Migrations)
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
			objectID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, "same-key.txt", versionID, int64(i+1)))
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
	migrator := migrate.NewMigrator(db, migrations.Migrations)
	if err := migrator.Init(ctx); err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	repos := repository.NewRepositories(db)
	bucket := seedBucket(t, db, "concurrent-dedupe-bucket")

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
			version.Checksum = "same-checksum"
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
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, v); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(%s): %v", tc.key, err)
		}
	}
	latestB := newObjectVersion(bucket.ID, "b.txt", "01J00000000000000000000009", 22)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, latestB); err != nil {
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
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, tc.key, tc.versionID, 10)); err != nil {
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
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
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

func TestObjectRepo_FindReusableStoredVersionRequiresStoredChainInfo(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "reuse-version-bucket")

	cached := newObjectVersion(bucket.ID, "cached.txt", "01J00000000000000000000072", 10)
	cached.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, cached); err != nil {
		t.Fatalf("create cached version: %v", err)
	}

	stored := newObjectVersion(bucket.ID, "stored.txt", "01J00000000000000000000073", 10)
	stored.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, stored); err != nil {
		t.Fatalf("create stored version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, stored.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("stored uploading: %v", err)
	}
	acceptTestStorageUploadForVersion(t, repos, bucket.ID, stored, "piece-reuse")

	got, err := repos.Objects.FindReusableStoredVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableStoredVersion: %v", err)
	}
	if got == nil || got.VersionID != stored.VersionID {
		t.Fatalf("reusable version = %#v, want %s", got, stored.VersionID)
	}
}

func TestObjectRepo_SetVersionStorageUploadAndTransitionUsesNewUpload(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "reuse-storage-upload-bucket")

	stored := newObjectVersion(bucket.ID, "stored.txt", "01J0000000000000000000007G", 10)
	stored.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, stored); err != nil {
		t.Fatalf("create stored version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, stored.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("stored uploading: %v", err)
	}
	uploadID := acceptTestStorageUploadForVersion(t, repos, bucket.ID, stored, "piece-reuse")

	follower := newObjectVersion(bucket.ID, "follower.txt", "01J0000000000000000000007H", 10)
	follower.Checksum = "same-checksum"
	follower.State = model.ObjectStateUploading
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("create follower version: %v", err)
	}

	if err := repos.Objects.SetVersionStorageUploadAndTransition(ctx, follower.VersionID, uploadID, model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageUploadAndTransition: %v", err)
	}
	got, err := repos.Objects.GetVersionByID(ctx, follower.VersionID)
	if err != nil || got == nil {
		t.Fatalf("get follower: got=%v err=%v", got, err)
	}
	if got.StorageUploadID == nil || *got.StorageUploadID != uploadID || got.State != model.ObjectStateStored || !got.InFilecoin {
		t.Fatalf("follower storage = state:%s upload:%v filecoin:%v, want stored with upload %d", got.State, got.StorageUploadID, got.InFilecoin, uploadID)
	}
}

func TestObjectRepo_FindReusableActiveUploadVersionRequiresActiveTask(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "active-reuse-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000007A", 10)
	version.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("create version: %v", err)
	}

	got, err := repos.Objects.FindReusableActiveUploadVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableActiveUploadVersion without task: %v", err)
	}
	if got != nil {
		t.Fatalf("active reusable without task = %#v, want nil", got)
	}

	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          version.ObjectID,
		RefVersionID:   version.VersionID,
		IdempotencyKey: "upload:" + version.VersionID,
		Status:         model.TaskStatusQueued,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("create upload task: %v", err)
	}

	got, err = repos.Objects.FindReusableActiveUploadVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableActiveUploadVersion: %v", err)
	}
	if got == nil || got.VersionID != version.VersionID {
		t.Fatalf("active reusable = %#v, want %s", got, version.VersionID)
	}

	if err := repos.Objects.SetVersionCachePresence(ctx, version.VersionID, false); err != nil {
		t.Fatalf("SetVersionCachePresence: %v", err)
	}
	got, err = repos.Objects.FindReusableActiveUploadVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableActiveUploadVersion with missing cache: %v", err)
	}
	if got != nil {
		t.Fatalf("active reusable with missing cache = %#v, want nil", got)
	}
}

func TestObjectRepo_FindReusableActiveUploadVersionUsesDurableUploadLeader(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "durable-active-reuse-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000007B", 10)
	version.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("create version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}

	got, err := repos.Objects.FindReusableActiveUploadVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableActiveUploadVersion(uploading): %v", err)
	}
	if got == nil || got.VersionID != version.VersionID {
		t.Fatalf("uploading durable leader = %#v, want %s", got, version.VersionID)
	}

	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("mark committing: %v", err)
	}
	got, err = repos.Objects.FindReusableActiveUploadVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableActiveUploadVersion(committing without readable copy): %v", err)
	}
	if got != nil {
		t.Fatalf("committing leader without readable copy = %#v, want nil", got)
	}

	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: binding.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacefake",
		RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyPieceReady: %v", err)
	}
	got, err = repos.Objects.FindReusableActiveUploadVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableActiveUploadVersion(committing with uncommitted copy): %v", err)
	}
	if got != nil {
		t.Fatalf("committing leader with uncommitted copy = %#v, want nil", got)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacefake",
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}

	got, err = repos.Objects.FindReusableActiveUploadVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableActiveUploadVersion(committing with readable copy): %v", err)
	}
	if got == nil || got.VersionID != version.VersionID {
		t.Fatalf("committing durable leader = %#v, want %s", got, version.VersionID)
	}
}

func TestObjectRepo_AcceptStorageUploadForContentUpdatesMatchingVersions(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "content-storage-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000007B", 10)
	oldVersion.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("old version: %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000007C", 10)
	currentVersion.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("current version: %v", err)
	}
	for _, versionID := range []string{oldVersion.VersionID, currentVersion.VersionID} {
		if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			t.Fatalf("mark %s uploading: %v", versionID, err)
		}
	}

	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: currentVersion.VersionID,
		ContentSize:     currentVersion.Size,
		Checksum:        currentVersion.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	seedCommittedUploadCopies(t, repos, bucket.ID, upload.ID, "piece-shared", []storageUploadCopySeed{
		{ProviderID: onChainIDPtr(t, "101"), DataSetID: onChainIDPtr(t, "1001001"), PieceID: onChainIDPtr(t, "2001"), TransferMethod: model.StorageCopyTransferMethodIngress, RetrievalURL: strPtr("https://provider.example/shared")},
	})
	refs := bindReadableUploadForContent(t, repos, upload.ID, bucket.ID, currentVersion.Size, currentVersion.Checksum)
	finalizeUploadForTest(t, repos, upload.ID)
	if len(refs) != 2 {
		t.Fatalf("updated refs len = %d, want 2", len(refs))
	}

	for _, versionID := range []string{oldVersion.VersionID, currentVersion.VersionID} {
		got, err := repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || got == nil {
			t.Fatalf("version %s: got=%v err=%v", versionID, got, err)
		}
		if got.State != model.ObjectStateStored {
			t.Fatalf("version %s state = %s, want stored", versionID, got.State)
		}
		if got.PieceCID == nil || *got.PieceCID != "piece-shared" {
			t.Fatalf("version %s piece = %v, want piece-shared", versionID, got.PieceCID)
		}
		if !got.InFilecoin {
			t.Fatalf("version %s in_filecoin = false, want true", versionID)
		}
	}

	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.VersionID != currentVersion.VersionID || current.State != model.ObjectStateStored {
		t.Fatalf("current version = version:%s state:%s, want %s stored", current.VersionID, current.State, currentVersion.VersionID)
	}
	if current.PieceCID == nil || *current.PieceCID != "piece-shared" {
		t.Fatalf("current piece = %v, want piece-shared", current.PieceCID)
	}
	if !current.InFilecoin {
		t.Fatal("current in_filecoin = false, want true")
	}
}

func TestObjectRepo_FailUploadingContentFollowersKeepsIndependentActiveUpload(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "content-failure-bucket")

	leader := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000007D", 10)
	leader.Checksum = "same-checksum"
	objID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, leader)
	if err != nil {
		t.Fatalf("leader version: %v", err)
	}
	follower := newObjectVersion(bucket.ID, "file.txt", "01J0000000000000000000007E", 10)
	follower.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, follower); err != nil {
		t.Fatalf("follower version: %v", err)
	}
	committingFollower := newObjectVersion(bucket.ID, "commit.txt", "01J0000000000000000000007G", 10)
	committingFollower.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, committingFollower); err != nil {
		t.Fatalf("committing follower version: %v", err)
	}
	independent := newObjectVersion(bucket.ID, "other.txt", "01J0000000000000000000007F", 10)
	independent.Checksum = "same-checksum"
	independentObjID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, independent)
	if err != nil {
		t.Fatalf("independent version: %v", err)
	}
	independentUpload := newObjectVersion(bucket.ID, "other-active.txt", "01J0000000000000000000007H", 10)
	independentUpload.Checksum = "same-checksum"
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, independentUpload); err != nil {
		t.Fatalf("independent upload version: %v", err)
	}
	for _, versionID := range []string{leader.VersionID, follower.VersionID, committingFollower.VersionID, independent.VersionID, independentUpload.VersionID} {
		if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			t.Fatalf("mark %s uploading: %v", versionID, err)
		}
	}
	if err := repos.Objects.UpdateVersionState(ctx, committingFollower.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("mark committing follower committing: %v", err)
	}
	if _, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: independentUpload.VersionID,
		ContentSize:     independentUpload.Size,
		Checksum:        independentUpload.Checksum,
		RequestedCopies: 3,
	}); err != nil {
		t.Fatalf("start independent active upload: %v", err)
	}

	for _, task := range []*model.Task{
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          objID,
			RefVersionID:   leader.VersionID,
			IdempotencyKey: "upload:" + leader.VersionID,
			Status:         model.TaskStatusRunning,
			MaxRetries:     1,
			ScheduledAt:    time.Now(),
		},
		{
			Type:           model.TaskTypeUpload,
			RefType:        "object",
			RefID:          independentObjID,
			RefVersionID:   independent.VersionID,
			IdempotencyKey: "upload:" + independent.VersionID,
			Status:         model.TaskStatusQueued,
			MaxRetries:     1,
			ScheduledAt:    time.Now(),
		},
	} {
		if err := repos.Tasks.Create(ctx, task); err != nil {
			t.Fatalf("create upload task for %s: %v", task.RefVersionID, err)
		}
	}

	refs, err := repos.Objects.FailUploadingContentFollowers(ctx, bucket.ID, 10, "same-checksum", leader.VersionID, "upload failed")
	if err != nil {
		t.Fatalf("FailUploadingContentFollowers: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("failed refs len = %d, want 3", len(refs))
	}

	wantFailedAtState := map[string]model.ObjectState{
		leader.VersionID:             model.ObjectStateUploading,
		follower.VersionID:           model.ObjectStateUploading,
		committingFollower.VersionID: model.ObjectStateCommitting,
	}
	for versionID, failedAtState := range wantFailedAtState {
		got, err := repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || got == nil {
			t.Fatalf("version %s: got=%v err=%v", versionID, got, err)
		}
		if got.State != model.ObjectStateFailed {
			t.Fatalf("version %s state = %s, want failed", versionID, got.State)
		}
		if got.FailedAtState == nil || *got.FailedAtState != failedAtState {
			t.Fatalf("version %s failed_at_state = %#v, want %s", versionID, got.FailedAtState, failedAtState)
		}
		if got.LastError == nil || *got.LastError != "upload failed" {
			t.Fatalf("version %s last_error = %#v, want upload failed", versionID, got.LastError)
		}
	}
	gotIndependent, err := repos.Objects.GetVersionByID(ctx, independent.VersionID)
	if err != nil || gotIndependent == nil {
		t.Fatalf("independent version: got=%v err=%v", gotIndependent, err)
	}
	if gotIndependent.State != model.ObjectStateUploading {
		t.Fatalf("independent state = %s, want uploading", gotIndependent.State)
	}
	gotIndependentUpload, err := repos.Objects.GetVersionByID(ctx, independentUpload.VersionID)
	if err != nil || gotIndependentUpload == nil {
		t.Fatalf("independent upload version: got=%v err=%v", gotIndependentUpload, err)
	}
	if gotIndependentUpload.State != model.ObjectStateUploading {
		t.Fatalf("independent upload state = %s, want uploading", gotIndependentUpload.State)
	}
}

func TestObjectRepo_ListVersionsByBucketOrdersAndMarksCurrent(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "version-list-bucket")

	oldVersion := newObjectVersion(bucket.ID, "a.txt", "01J00000000000000000000074", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("create old version: %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "a.txt", "01J00000000000000000000075", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("create current version: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, "b.txt", "01J00000000000000000000076", 30)); err != nil {
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

	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, "case/a.txt", "01J00000000000000000000077", 40)); err != nil {
		t.Fatalf("create case version: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, "Case/a.txt", "01J00000000000000000000078", 50)); err != nil {
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
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("create old version: %v", err)
	}
	middleVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000002002", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, middleVersion); err != nil {
		t.Fatalf("create middle version: %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000002003", 30)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("create current version: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, "other.txt", "01J00000000000000000002004", 40)); err != nil {
		t.Fatalf("create other key version: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(otherBucket.ID, "file.txt", "01J00000000000000000002005", 50)); err != nil {
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

func TestObjectRepo_CreateDeleteMarkerHidesCurrentObjectButKeepsVersionHistory(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "delete-marker-bucket")

	data := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001001", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, data); err != nil {
		t.Fatalf("create data version: %v", err)
	}
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J00000000000000000001002")
	if err != nil {
		t.Fatalf("CreateDeleteMarkerAndSetCurrent: %v", err)
	}
	if !marker.IsDeleteMarker || marker.Size != 0 || marker.CacheKey != "" || marker.InCache {
		t.Fatalf("marker = %#v, want metadata-only delete marker", marker)
	}

	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetCurrentVersionByBucketAndKey: %v", err)
	}
	if current == nil || !current.IsDeleteMarker || current.VersionID != marker.VersionID {
		t.Fatalf("current = %#v, want delete marker %s", current, marker.VersionID)
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

	cached, err := repos.Objects.ListVersionsByState(ctx, model.ObjectStateCached, 10)
	if err != nil {
		t.Fatalf("ListVersionsByState: %v", err)
	}
	if len(cached) != 1 || cached[0].VersionID != data.VersionID {
		t.Fatalf("cached versions = %#v, want only data version", cached)
	}
}

func TestObjectRepo_DeleteMarkerVersionRestoresPreviousCurrentVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "delete-marker-restore-bucket")

	first := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001011", 10)
	second := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001012", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, first); err != nil {
		t.Fatalf("create first version: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, second); err != nil {
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
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, data); err != nil {
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
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, data); err != nil {
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
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, lowerData); err != nil {
		t.Fatalf("create lower trash data: %v", err)
	}
	lowerMarker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "trash/lower.txt", "01J00000000000000000001004")
	if err != nil {
		t.Fatalf("create lower trash marker: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, "Trash/lower.txt", "01J00000000000000000001005", 10)); err != nil {
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

	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001041", 10)); err != nil {
		t.Fatalf("create data version: %v", err)
	}
	marker, err := repos.Objects.CreateDeleteMarkerAndSetCurrent(ctx, bucket.ID, "file.txt", "01J00000000000000000001042")
	if err != nil {
		t.Fatalf("create marker: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000001043", 20)); err != nil {
		t.Fatalf("create newer data version: %v", err)
	}

	if _, err := repos.Objects.RestoreCurrentDeleteMarkerStack(ctx, bucket.ID, "file.txt", marker.VersionID); err == nil {
		t.Fatal("RestoreCurrentDeleteMarkerStack returned nil error for stale marker")
	}
}

func TestObjectRepo_VersionStateUpdatesOnlyMirrorCurrentVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "state-bucket")

	v1 := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000011", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, v1); err != nil {
		t.Fatalf("first version: %v", err)
	}
	v2 := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000012", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, v2); err != nil {
		t.Fatalf("second version: %v", err)
	}

	if err := repos.Objects.UpdateVersionState(ctx, v1.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateVersionState(old): %v", err)
	}
	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.VersionID != v2.VersionID || current.State != model.ObjectStateCached {
		t.Fatalf("old version update polluted current version: version=%s state=%s", current.VersionID, current.State)
	}

	if err := repos.Objects.UpdateVersionState(ctx, v2.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateVersionState(current): %v", err)
	}
	current, err = repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey after current update: %v", err)
	}
	if current.State != model.ObjectStateUploading {
		t.Fatalf("current state = %s, want uploading", current.State)
	}
}

func TestObjectRepo_UpdateVersionStateFromFailedClearsFailureDetails(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "failed-retry-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000013", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, version.VersionID, model.ObjectStateUploading, "upload failed"); err != nil {
		t.Fatalf("failed: %v", err)
	}

	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateFailed, model.ObjectStateUploading); err != nil {
		t.Fatalf("retry uploading: %v", err)
	}

	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("version after retry: got=%v err=%v", got, err)
	}
	if got.FailedAtState != nil {
		t.Fatalf("version failed_at_state = %#v, want nil", got.FailedAtState)
	}
	if got.LastError != nil {
		t.Fatalf("version last_error = %#v, want nil", got.LastError)
	}
	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil || current == nil {
		t.Fatalf("current after retry: got=%v err=%v", current, err)
	}
	if current.FailedAtState != nil {
		t.Fatalf("current failed_at_state = %#v, want nil", current.FailedAtState)
	}
	if current.LastError != nil {
		t.Fatalf("current last_error = %#v, want nil", current.LastError)
	}
}

func TestObjectRepo_UpdateVersionStateToFailedClearsStorageUploadID(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "failed-upload-binding-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000120", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("CreateVersionAndSetCurrent: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:              primary.ID,
		UploadID:        upload.ID,
		DataSetID:       onChainID(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "9001"),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     "bafk2bzacefailedbinding",
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: "https://provider.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}

	if err := repos.Objects.UpdateVersionStateToFailed(ctx, version.VersionID, model.ObjectStateReplicating, "replication failed"); err != nil {
		t.Fatalf("UpdateVersionStateToFailed: %v", err)
	}
	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("GetVersionByID: got=%v err=%v", got, err)
	}
	if got.State != model.ObjectStateFailed || got.StorageUploadID != nil {
		t.Fatalf("failed version = state:%s upload:%#v, want failed without upload binding", got.State, got.StorageUploadID)
	}
	if got.FailedAtState == nil || *got.FailedAtState != model.ObjectStateReplicating {
		t.Fatalf("failed_at_state = %#v, want replicating", got.FailedAtState)
	}
}

func TestObjectRepo_AcceptStorageUploadMirrorsOnlyCurrentVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "storage-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000021", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("old version: %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000022", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("current version: %v", err)
	}

	if err := repos.Objects.UpdateVersionState(ctx, oldVersion.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("old version upload transition: %v", err)
	}
	acceptTestStorageUploadForVersion(t, repos, bucket.ID, oldVersion, "piece-old")
	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.PieceCID != nil || current.State != model.ObjectStateCached {
		t.Fatalf("old storage update polluted current version: piece=%v state=%s", current.PieceCID, current.State)
	}
	if current.InFilecoin {
		t.Fatal("old storage update polluted current in_filecoin")
	}

	if err := repos.Objects.UpdateVersionState(ctx, currentVersion.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("current version upload transition: %v", err)
	}
	acceptTestStorageUploadForVersion(t, repos, bucket.ID, currentVersion, "piece-current")
	current, err = repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey after current storage: %v", err)
	}
	if current.PieceCID == nil || *current.PieceCID != "piece-current" {
		t.Fatalf("current storage info = piece:%v", current.PieceCID)
	}
	if current.State != model.ObjectStateStored {
		t.Fatalf("current state = %s, want stored", current.State)
	}
	if !current.InFilecoin {
		t.Fatal("current in_filecoin = false, want true")
	}
}

func TestObjectRepo_SetVersionCachePresenceMirrorsOnlyCurrentVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "cache-location-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000023", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("old version: %v", err)
	}
	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000024", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
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

func TestObjectRepo_UpdateVersionStateMarksCacheEvictedLocation(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "cache-evicted-location-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000025", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	acceptTestStorageUploadForVersion(t, repos, bucket.ID, version, "piece")

	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateStored, model.ObjectStateCacheEvicted); err != nil {
		t.Fatalf("cache evicted: %v", err)
	}

	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("version: got=%v err=%v", got, err)
	}
	if got.InCache {
		t.Fatal("version in_cache = true, want false")
	}
	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.State != model.ObjectStateCacheEvicted || current.InCache {
		t.Fatalf("current state/cache = %s/%v, want cache_evicted/false", current.State, current.InCache)
	}
}

func TestObjectRepo_ListAndResetVersionsByState(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "version-state-bucket")

	for _, versionID := range []string{"01J00000000000000000000031", "01J00000000000000000000032"} {
		v := newObjectVersion(bucket.ID, "file-"+versionID+".txt", versionID, 1)
		if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, v); err != nil {
			t.Fatalf("CreateVersionAndSetCurrent(%s): %v", versionID, err)
		}
		if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			t.Fatalf("UpdateVersionState(%s): %v", versionID, err)
		}
	}

	versions, err := repos.Objects.ListVersionsByState(ctx, model.ObjectStateUploading, 10)
	if err != nil {
		t.Fatalf("ListVersionsByState: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("uploading version count = %d, want 2", len(versions))
	}

	reset, err := repos.Objects.ResetStaleVersionStates(ctx, model.ObjectStateUploading, model.ObjectStateCached, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ResetStaleVersionStates: %v", err)
	}
	if reset != 2 {
		t.Fatalf("reset count = %d, want 2", reset)
	}
}

func TestObjectRepo_ResetStaleVersionStatesFromFailedClearsFailureDetails(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "failed-stale-reset-bucket")

	version := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000033", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, version); err != nil {
		t.Fatalf("version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, version.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionStateToFailed(ctx, version.VersionID, model.ObjectStateUploading, "upload failed"); err != nil {
		t.Fatalf("failed: %v", err)
	}

	reset, err := repos.Objects.ResetStaleVersionStates(ctx, model.ObjectStateFailed, model.ObjectStateUploading, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ResetStaleVersionStates: %v", err)
	}
	if reset != 1 {
		t.Fatalf("reset count = %d, want 1", reset)
	}

	got, err := repos.Objects.GetVersionByID(ctx, version.VersionID)
	if err != nil || got == nil {
		t.Fatalf("version after reset: got=%v err=%v", got, err)
	}
	if got.FailedAtState != nil || got.LastError != nil {
		t.Fatalf("version failure details = failed_at_state:%#v last_error:%#v, want nil", got.FailedAtState, got.LastError)
	}
	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil || current == nil {
		t.Fatalf("current after reset: got=%v err=%v", current, err)
	}
	if current.FailedAtState != nil || current.LastError != nil {
		t.Fatalf("current failure details = failed_at_state:%#v last_error:%#v, want nil", current.FailedAtState, current.LastError)
	}
}

func TestObjectRepo_ResetStaleVersionStatesDoesNotMirrorOldVersion(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "stale-current-bucket")

	oldVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000051", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, oldVersion); err != nil {
		t.Fatalf("old version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, oldVersion.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("old uploading: %v", err)
	}

	currentVersion := newObjectVersion(bucket.ID, "file.txt", "01J00000000000000000000052", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("current version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, currentVersion.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("current uploading: %v", err)
	}
	acceptTestStorageUploadForVersion(t, repos, bucket.ID, currentVersion, "piece-current")

	reset, err := repos.Objects.ResetStaleVersionStates(ctx, model.ObjectStateUploading, model.ObjectStateCached, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ResetStaleVersionStates: %v", err)
	}
	if reset != 1 {
		t.Fatalf("reset count = %d, want 1", reset)
	}

	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.VersionID != currentVersion.VersionID || current.State != model.ObjectStateStored {
		t.Fatalf("current version polluted: version=%s state=%s", current.VersionID, current.State)
	}
}

func TestObjectRepo_ResetStaleVersionStatesLeavesSkippedVersionUnchanged(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucket := seedBucket(t, db, "stale-skip-bucket")

	currentVersion := newObjectVersion(bucket.ID, "current.txt", "01J00000000000000000000061", 10)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, currentVersion); err != nil {
		t.Fatalf("current version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, currentVersion.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("current uploading: %v", err)
	}

	otherVersion := newObjectVersion(bucket.ID, "other.txt", "01J00000000000000000000062", 20)
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, otherVersion); err != nil {
		t.Fatalf("other version: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, otherVersion.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("other uploading: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE TRIGGER skip_current_stale_reset
		BEFORE UPDATE OF state ON object_versions
		WHEN OLD.version_id = '01J00000000000000000000061'
		BEGIN
			SELECT RAISE(IGNORE);
		END;
	`); err != nil {
		t.Fatalf("creating skip trigger: %v", err)
	}

	reset, err := repos.Objects.ResetStaleVersionStates(ctx, model.ObjectStateUploading, model.ObjectStateCached, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ResetStaleVersionStates: %v", err)
	}
	if reset != 1 {
		t.Fatalf("reset count = %d, want 1", reset)
	}

	current, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "current.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey(current): %v", err)
	}
	if current.State != model.ObjectStateUploading {
		t.Fatalf("skipped current version state = %s, want uploading", current.State)
	}

	other, err := repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "other.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey(other): %v", err)
	}
	if other.State != model.ObjectStateCached {
		t.Fatalf("updated current version state = %s, want cached", other.State)
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

	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucketA.ID, "a.txt", "01J00000000000000000000041", 100)); err != nil {
		t.Fatalf("create a v1: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucketA.ID, "a.txt", "01J00000000000000000000042", 250)); err != nil {
		t.Fatalf("create a v2: %v", err)
	}
	if _, err := repos.Objects.CreateVersionAndSetCurrent(ctx, newObjectVersion(bucketB.ID, "b.txt", "01J00000000000000000000043", 500)); err != nil {
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
		b := &model.Bucket{Name: "tx-bucket", Status: model.BucketStatusActive}
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
		b := &model.Bucket{Name: "tx-committed", Status: model.BucketStatusActive}
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
