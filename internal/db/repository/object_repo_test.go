package repository_test

import (
	"context"
	"database/sql"
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

	current, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.CurrentVersionID != v2.VersionID {
		t.Fatalf("current version = %s, want %s", current.CurrentVersionID, v2.VersionID)
	}
	if current.Size != 20 || current.ETag != v2.ETag || current.CacheKey != v2.CacheKey {
		t.Fatalf("current snapshot not refreshed: size=%d etag=%s cache=%s", current.Size, current.ETag, current.CacheKey)
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

func TestObjectRepo_ListByBucketReadsCurrentSnapshotOnly(t *testing.T) {
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

	all, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "", 0)
	if err != nil {
		t.Fatalf("ListByBucket: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("object count = %d, want 3 current keys", len(all))
	}
	if all[1].Key != "b.txt" || all[1].CurrentVersionID != latestB.VersionID || all[1].Size != 22 {
		t.Fatalf("b.txt current snapshot = key:%s version:%s size:%d", all[1].Key, all[1].CurrentVersionID, all[1].Size)
	}

	prefixed, err := repos.Objects.ListByBucket(ctx, bucket.ID, "dir/", "", 0)
	if err != nil {
		t.Fatalf("ListByBucket(prefix): %v", err)
	}
	if len(prefixed) != 1 || prefixed[0].Key != "dir/c.txt" {
		t.Fatalf("prefixed keys = %#v", prefixed)
	}

	afterKey, err := repos.Objects.ListByBucket(ctx, bucket.ID, "", "a.txt", 0)
	if err != nil {
		t.Fatalf("ListByBucket(afterKey): %v", err)
	}
	if len(afterKey) != 2 || afterKey[0].Key != "b.txt" {
		t.Fatalf("afterKey result = %#v", afterKey)
	}
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
	if err := repos.Objects.SetVersionStorageInfoAndTransition(ctx, stored.VersionID, "piece-reuse", "https://provider.example/reuse", model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("stored chain info: %v", err)
	}

	got, err := repos.Objects.FindReusableStoredVersion(ctx, bucket.ID, 10, "same-checksum")
	if err != nil {
		t.Fatalf("FindReusableStoredVersion: %v", err)
	}
	if got == nil || got.VersionID != stored.VersionID {
		t.Fatalf("reusable version = %#v, want %s", got, stored.VersionID)
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
		Status:         model.TaskStatusPending,
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
}

func TestObjectRepo_SetStorageInfoForUploadingContentUpdatesMatchingVersions(t *testing.T) {
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

	refs, err := repos.Objects.SetStorageInfoForUploadingContent(ctx, bucket.ID, 10, "same-checksum", "piece-shared", "https://provider.example/shared")
	if err != nil {
		t.Fatalf("SetStorageInfoForUploadingContent: %v", err)
	}
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
	}

	current, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.CurrentVersionID != currentVersion.VersionID || current.State != model.ObjectStateStored {
		t.Fatalf("current snapshot = version:%s state:%s, want %s stored", current.CurrentVersionID, current.State, currentVersion.VersionID)
	}
	if current.PieceCID == nil || *current.PieceCID != "piece-shared" {
		t.Fatalf("current piece = %v, want piece-shared", current.PieceCID)
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
	independent := newObjectVersion(bucket.ID, "other.txt", "01J0000000000000000000007F", 10)
	independent.Checksum = "same-checksum"
	independentObjID, err := repos.Objects.CreateVersionAndSetCurrent(ctx, independent)
	if err != nil {
		t.Fatalf("independent version: %v", err)
	}
	for _, versionID := range []string{leader.VersionID, follower.VersionID, independent.VersionID} {
		if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			t.Fatalf("mark %s uploading: %v", versionID, err)
		}
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
			Status:         model.TaskStatusPending,
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
	if len(refs) != 2 {
		t.Fatalf("failed refs len = %d, want 2", len(refs))
	}

	for _, versionID := range []string{leader.VersionID, follower.VersionID} {
		got, err := repos.Objects.GetVersionByID(ctx, versionID)
		if err != nil || got == nil {
			t.Fatalf("version %s: got=%v err=%v", versionID, got, err)
		}
		if got.State != model.ObjectStateFailed {
			t.Fatalf("version %s state = %s, want failed", versionID, got.State)
		}
	}
	gotIndependent, err := repos.Objects.GetVersionByID(ctx, independent.VersionID)
	if err != nil || gotIndependent == nil {
		t.Fatalf("independent version: got=%v err=%v", gotIndependent, err)
	}
	if gotIndependent.State != model.ObjectStateUploading {
		t.Fatalf("independent state = %s, want uploading", gotIndependent.State)
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
	if rows[0].CurrentVersionID != currentVersion.VersionID {
		t.Fatalf("current marker = %s, want %s", rows[0].CurrentVersionID, currentVersion.VersionID)
	}

	page, err := repos.Objects.ListVersionsByBucket(ctx, bucket.ID, "", rows[0].Key, rows[0].VersionID, 10)
	if err != nil {
		t.Fatalf("ListVersionsByBucket marker: %v", err)
	}
	if len(page) == 0 || page[0].VersionID != oldVersion.VersionID {
		t.Fatalf("marker page first = %#v, want old version", page)
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
	current, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.CurrentVersionID != v2.VersionID || current.State != model.ObjectStateCached {
		t.Fatalf("old version update polluted current snapshot: version=%s state=%s", current.CurrentVersionID, current.State)
	}

	if err := repos.Objects.UpdateVersionState(ctx, v2.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("UpdateVersionState(current): %v", err)
	}
	current, err = repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey after current update: %v", err)
	}
	if current.State != model.ObjectStateUploading {
		t.Fatalf("current state = %s, want uploading", current.State)
	}
}

func TestObjectRepo_SetVersionStorageInfoAndTransitionMirrorsOnlyCurrentVersion(t *testing.T) {
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
	if err := repos.Objects.SetVersionStorageInfoAndTransition(ctx, oldVersion.VersionID, "piece-old", "https://old.example", model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("old SetVersionStorageInfoAndTransition: %v", err)
	}
	current, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.PieceCID != nil || current.RetrievalURL != nil || current.State != model.ObjectStateCached {
		t.Fatalf("old storage update polluted current snapshot: piece=%v url=%v state=%s", current.PieceCID, current.RetrievalURL, current.State)
	}

	if err := repos.Objects.UpdateVersionState(ctx, currentVersion.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("current version upload transition: %v", err)
	}
	if err := repos.Objects.SetVersionStorageInfoAndTransition(ctx, currentVersion.VersionID, "piece-current", "https://current.example", model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("current SetVersionStorageInfoAndTransition: %v", err)
	}
	current, err = repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey after current storage: %v", err)
	}
	if current.PieceCID == nil || *current.PieceCID != "piece-current" || current.RetrievalURL == nil || *current.RetrievalURL != "https://current.example" {
		t.Fatalf("current storage info = piece:%v url:%v", current.PieceCID, current.RetrievalURL)
	}
	if current.State != model.ObjectStateStored {
		t.Fatalf("current state = %s, want stored", current.State)
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
	if err := repos.Objects.SetVersionStorageInfoAndTransition(ctx, currentVersion.VersionID, "piece-current", "https://current.example", model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("current stored: %v", err)
	}

	reset, err := repos.Objects.ResetStaleVersionStates(ctx, model.ObjectStateUploading, model.ObjectStateCached, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("ResetStaleVersionStates: %v", err)
	}
	if reset != 1 {
		t.Fatalf("reset count = %d, want 1", reset)
	}

	current, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "file.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if current.CurrentVersionID != currentVersion.VersionID || current.State != model.ObjectStateStored {
		t.Fatalf("current snapshot polluted: version=%s state=%s", current.CurrentVersionID, current.State)
	}
}

func TestObjectRepo_ResetStaleVersionStatesDoesNotMirrorSkippedRows(t *testing.T) {
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
	if _, err := db.NewUpdate().
		Model((*model.Object)(nil)).
		Set("state = ?", model.ObjectStateStored).
		Where("current_version_id = ?", currentVersion.VersionID).
		Exec(ctx); err != nil {
		t.Fatalf("seeding current snapshot state: %v", err)
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

	current, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "current.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey(current): %v", err)
	}
	if current.State != model.ObjectStateStored {
		t.Fatalf("skipped current snapshot state = %s, want stored", current.State)
	}

	other, err := repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "other.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey(other): %v", err)
	}
	if other.State != model.ObjectStateCached {
		t.Fatalf("updated current snapshot state = %s, want cached", other.State)
	}
}

func TestObjectRepo_CurrentStatsUseObjectsSnapshot(t *testing.T) {
	db := testDB(t)
	repos := repository.NewRepositories(db)
	ctx := context.Background()
	bucketA := seedBucket(t, db, "stats-a")
	bucketB := seedBucket(t, db, "stats-b")

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
