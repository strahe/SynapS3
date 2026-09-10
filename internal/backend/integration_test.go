package backend_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ipfs/go-cid"
	multihash "github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/s3response"
)

// integrationBackend extends testBackend with direct DB access for task verification.
type integrationBackend struct {
	backend *backend.SynapseBackend
	repos   *repository.Repositories
	cache   cache.Cache
	storage *testutil.MockStorageClient
	db      *bun.DB
}

func newIntegrationBackend(t *testing.T) *integrationBackend {
	t.Helper()
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	dir := t.TempDir()
	fsCache, err := cache.NewFilesystem(dir, 1<<30)
	if err != nil {
		t.Fatalf("creating test cache: %v", err)
	}
	sc := &testutil.MockStorageClient{}
	logger := slog.Default()
	cacheGate, accessTracker := newBackendCacheAccess(repos)

	b := backend.New(repos, fsCache, sc, cacheGate, accessTracker, logger, backend.WithTaskService(newBackendTaskService(t, repos)))
	return &integrationBackend{
		backend: b,
		repos:   repos,
		cache:   fsCache,
		storage: sc,
		db:      db,
	}
}

// findTasks queries the storage tasks reachable from one object. Ingest is
// scheduled per content now, so the object is reached through the contents its
// versions point at.
func findTasks(t *testing.T, db *bun.DB, refType string, refID int64) []model.Task {
	t.Helper()
	if refType != "object" {
		t.Fatalf("unsupported test task subject %q", refType)
	}
	var tasks []model.Task
	err := db.NewSelect().Model(&tasks).
		Where("task.subject_type = ?", "storage_content").
		Where(`EXISTS (
			SELECT 1 FROM object_versions AS task_version
			WHERE CAST(task_version.content_id AS TEXT) = task.subject_key
			  AND task_version.object_id = ?
		)`, refID).
		OrderExpr("id ASC").
		Scan(context.Background())
	if err != nil {
		t.Fatalf("querying tasks: %v", err)
	}
	return tasks
}

// contentSubject is the task subject key for the content a version points at.
func contentSubject(t *testing.T, db *bun.DB, versionID string) string {
	t.Helper()
	var contentID int64
	if err := db.NewSelect().
		Table("object_versions").
		Column("content_id").
		Where("version_id = ?", versionID).
		Scan(context.Background(), &contentID); err != nil {
		t.Fatalf("reading content for version %s: %v", versionID, err)
	}
	return strconv.FormatInt(contentID, 10)
}

// putObject is a helper that calls PutObject with the given string body.
func putObject(t *testing.T, b *backend.SynapseBackend, bucket, key, body string) s3response.PutObjectOutput {
	t.Helper()
	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   strings.NewReader(validTestObjectBody(body)),
	})
	if err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
	return out
}

// getObjectBody calls GetObject and reads the full body as a string.
func getObjectBody(t *testing.T, b *backend.SynapseBackend, bucket, key string) string {
	t.Helper()
	out, err := b.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("GetObject(%s/%s): %v", bucket, key, err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("reading body: %v", err)
	}
	return string(data)
}

func TestIntegration_FullWritePath(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	// Seed an active bucket.
	bucket := testutil.SeedBucket(t, ib.db, "test-bucket")

	// 1. PutObject
	putObject(t, ib.backend, "test-bucket", "test-key", "hello world")

	obj, err := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "test-key")
	if err != nil || obj == nil {
		t.Fatalf("expected object in DB, got err=%v obj=%v", err, obj)
	}
	// Nothing has reached a provider yet, and pipeline position is read from
	// the content's copies, so the object is cached and no further.
	if obj.State != model.ObjectStateCached {
		t.Fatalf("expected state=cached, got %s", obj.State)
	}
	if obj.VersionID == "" || obj.ContentID == nil {
		t.Fatalf("expected a current version bound to content, got version=%q content=%v", obj.VersionID, obj.ContentID)
	}
	if !obj.InCache {
		t.Fatal("expected the written bytes to be cached")
	}

	tasks := findTasks(t, ib.db, "object", obj.ObjectID)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 upload task, got %d", len(tasks))
	}
	if tasks[0].Type != model.TaskTypeUploadPlan {
		t.Fatalf("expected task type upload_plan, got %s", tasks[0].Type)
	}
	// Ingest is planned for the bytes, so the task names the content.
	if want := contentSubject(t, ib.db, obj.VersionID); tasks[0].SubjectKey == nil || *tasks[0].SubjectKey != want {
		t.Fatalf("expected task content=%s, got %v", want, tasks[0].SubjectKey)
	}

	// 2. Simulate the uploader placing the content with a provider.
	acceptBackendVersionUpload(t, ib.db, ib.repos, obj.VersionID, "bafk2test123", "https://provider.example/pieces/test")

	obj, _ = ib.repos.Objects.GetCurrentVersionByObjectID(ctx, obj.ObjectID)
	if obj.State != model.ObjectStateStored {
		t.Fatalf("expected state=stored, got %s", obj.State)
	}
	if obj.ContentID == nil || obj.PieceCID == nil || *obj.PieceCID != "bafk2test123" {
		t.Fatalf("expected accepted content with PieceCID=bafk2test123, got content=%v piece=%v", obj.ContentID, obj.PieceCID)
	}
	if !obj.InFilecoin {
		t.Fatal("expected a readable committed copy after acceptance")
	}

	// 3. Simulate the evictor: durability is unchanged, only the cached copy goes.
	cacheKey := obj.CacheKey()
	if err := ib.repos.Objects.ClearContentCachePresence(ctx, *obj.ContentID); err != nil {
		t.Fatalf("mark cache absent: %v", err)
	}
	if err := ib.cache.Delete(ctx, "test-bucket", cacheKey); err != nil {
		t.Fatalf("cache delete: %v", err)
	}

	obj, _ = ib.repos.Objects.GetCurrentVersionByObjectID(ctx, obj.ObjectID)
	if obj.State != model.ObjectStateStored || obj.InCache {
		t.Fatalf("expected stored remote-only object, got state=%s in_cache=%v", obj.State, obj.InCache)
	}
	if ib.cache.Exists(ctx, "test-bucket", cacheKey) {
		t.Fatal("expected cache file to be gone")
	}
}

func TestIntegration_ColdReadAfterEviction(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "test-bucket")
	content := "hello world"

	// PutObject
	putObject(t, ib.backend, "test-bucket", "test-key", content)

	obj, _ := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "test-key")

	// Create a valid CID for PieceCID
	mh, err := multihash.Sum([]byte("test"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	testPieceCID := cid.NewCidV1(cid.Raw, mh)

	// Simulate a stored object whose local cache has been removed. Residency is
	// content-addressed, so both the record and the file are keyed on content.
	acceptBackendVersionUpload(t, ib.db, ib.repos, obj.VersionID, testPieceCID.String(), "https://provider.example/pieces/test")
	cacheKey := obj.CacheKey()
	if err := ib.repos.Objects.ClearContentCachePresence(ctx, *obj.ContentID); err != nil {
		t.Fatal(err)
	}
	if err := ib.cache.Delete(ctx, "test-bucket", cacheKey); err != nil {
		t.Fatal(err)
	}
	if ib.cache.Exists(ctx, "test-bucket", cacheKey) {
		t.Fatal("cache should be empty after eviction")
	}

	// Configure mock SP download to return the original content
	ib.storage.DownloadFunc = func(_ context.Context, pc cid.Cid, _ *storage.DownloadOptions) (io.ReadCloser, error) {
		if pc.Equals(testPieceCID) {
			return io.NopCloser(bytes.NewReader([]byte(content))), nil
		}
		return nil, fmt.Errorf("unexpected CID: %s", pc)
	}

	// GetObject should succeed via SP download
	body := getObjectBody(t, ib.backend, "test-bucket", "test-key")
	if body != content {
		t.Fatalf("expected body=%q, got %q", content, body)
	}

	// Cache rehydration is async (TeeReader goroutine writes while body is consumed).
	// Poll with a timeout to avoid flakiness.
	rehydrated := false
	for range 200 {
		if ib.cache.Exists(ctx, "test-bucket", cacheKey) {
			rehydrated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !rehydrated {
		t.Fatal("expected cache to be rehydrated after cold read (timed out)")
	}
}

func TestIntegration_MultipartUpload_Abort(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	testutil.SeedBucket(t, ib.db, "bucket")

	// 1. CreateMultipartUpload
	createOut, err := ib.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: new("bucket"),
		Key:    new("file"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := createOut.UploadId

	// 2. UploadPart
	part1Num := int32(1)
	_, err = ib.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     new("bucket"),
		Key:        new("file"),
		UploadId:   &uploadID,
		PartNumber: &part1Num,
		Body:       strings.NewReader("data"),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	// 3. AbortMultipartUpload
	err = ib.backend.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   new("bucket"),
		Key:      new("file"),
		UploadId: &uploadID,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	// 4. Verify: upload should be in aborted status (getActiveUpload won't find it)
	upload, err := ib.repos.Multiparts.GetByUploadID(ctx, uploadID)
	if err != nil {
		t.Fatalf("querying upload: %v", err)
	}
	if upload == nil {
		t.Fatal("expected upload record to still exist (with aborted status)")
	}
	if upload.Status != model.MultipartStatusAborted {
		t.Fatalf("expected status=aborted, got %s", upload.Status)
	}

	// Parts should be cleaned up
	parts, err := ib.repos.Multiparts.GetParts(ctx, uploadID, 0, 100)
	if err != nil {
		t.Fatalf("querying parts: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("expected 0 parts after abort, got %d", len(parts))
	}

	// No object should be created
	var objects []model.Object
	err = ib.db.NewSelect().Model(&objects).
		Where("key = ?", "file").
		Scan(ctx)
	if err != nil {
		t.Fatalf("querying objects: %v", err)
	}
	if len(objects) != 0 {
		t.Fatalf("expected 0 objects after abort, got %d", len(objects))
	}
}

func TestIntegration_OverwritePath(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")

	// First write
	putObject(t, ib.backend, "bucket", "key", "v1")

	obj, _ := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "key")
	firstVersionID := obj.VersionID
	if firstVersionID == "" {
		t.Fatal("expected current version id after first put")
	}

	tasks := findTasks(t, ib.db, "object", obj.ObjectID)
	firstSubject := contentSubject(t, ib.db, firstVersionID)
	if len(tasks) != 1 || tasks[0].SubjectKey == nil || *tasks[0].SubjectKey != firstSubject {
		t.Fatalf("expected 1 task with first content, got %d tasks", len(tasks))
	}

	// Overwrite
	putObject(t, ib.backend, "bucket", "key", "v2")

	obj, _ = ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "key")
	secondVersionID := obj.VersionID
	if secondVersionID == "" || secondVersionID == firstVersionID {
		t.Fatalf("expected new current version after overwrite, first=%s second=%s", firstVersionID, secondVersionID)
	}

	tasks = findTasks(t, ib.db, "object", obj.ObjectID)
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}
	secondSubject := contentSubject(t, ib.db, secondVersionID)
	if tasks[1].SubjectKey == nil || *tasks[1].SubjectKey != secondSubject {
		t.Fatalf("expected second task content=%s, got %v", secondSubject, tasks[1].SubjectKey)
	}

	if tasks[0].SubjectKey == nil || *tasks[0].SubjectKey != firstSubject {
		t.Fatalf("expected first task content=%s, got %v", firstSubject, tasks[0].SubjectKey)
	}

	// GetObject should return the current version.
	body := getObjectBody(t, ib.backend, "bucket", "key")
	if want := validTestObjectBody("v2"); body != want {
		t.Fatalf("expected body=%q, got %q", want, body)
	}
}

func TestIntegration_CopyObjectPath(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")

	// Put source object
	putOut := putObject(t, ib.backend, "bucket", "src-key", "data")

	// Copy source → dest
	srcCopy := "bucket/src-key"
	dstBucket := "bucket"
	dstKey := "dst-key"
	_, err := ib.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:     &dstBucket,
		Key:        &dstKey,
		CopySource: &srcCopy,
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}

	// Verify destination object exists
	dstObj, err := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "dst-key")
	if err != nil || dstObj == nil {
		t.Fatalf("expected dst object, got err=%v", err)
	}

	// ETags should match (same data)
	srcETag := strings.Trim(putOut.ETag, `"`)
	if dstObj.ETag != srcETag {
		t.Fatalf("expected dst etag=%s, got %s", srcETag, dstObj.ETag)
	}

	// Pipeline position is derived from the content's copies, and no copy exists
	// until the ingest plan runs, so a fresh copy reads as cached.
	if dstObj.State != model.ObjectStateCached {
		t.Fatalf("expected dst state=cached, got %s", dstObj.State)
	}

	// Same-bucket copies of identical bytes resolve to one content, so they
	// share its single ingest plan instead of scheduling a second.
	dstTasks := findTasks(t, ib.db, "object", dstObj.ObjectID)
	if len(dstTasks) != 1 {
		t.Fatalf("expected the destination to share one upload task, got %d", len(dstTasks))
	}
	totalPlans, err := ib.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("type = ?", model.TaskTypeUploadPlan).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting upload tasks: %v", err)
	}
	if totalPlans != 1 {
		t.Fatalf("upload task count = %d, want 1", totalPlans)
	}

	// GetObject on dest should return the same data
	body := getObjectBody(t, ib.backend, "bucket", "dst-key")
	if want := validTestObjectBody("data"); body != want {
		t.Fatalf("expected body=%q, got %q", want, body)
	}
}

func TestIntegration_DeletePath_CreatesDeleteMarker(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")
	putOut := putObject(t, ib.backend, "bucket", "key", "data")

	deleteOut, err := ib.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: new("bucket"),
		Key:    new("key"),
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if deleteOut.DeleteMarker == nil || !*deleteOut.DeleteMarker || deleteOut.VersionId == nil || *deleteOut.VersionId == "" {
		t.Fatalf("DeleteObject output = %#v, want delete marker version", deleteOut)
	}

	current, err := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "key")
	if err != nil {
		t.Fatalf("GetCurrentVersionByBucketAndKey: %v", err)
	}
	if current == nil || !current.IsDeleteMarker {
		t.Fatalf("current version = %#v, want delete marker", current)
	}

	if _, err := ib.backend.GetObject(ctx, &s3.GetObjectInput{Bucket: new("bucket"), Key: new("key")}); err == nil {
		t.Fatal("GetObject after delete returned nil error")
	}
	listOut, err := ib.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: new("bucket")})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(listOut.Contents) != 0 {
		t.Fatalf("ListObjectsV2 contents = %#v, want deleted object hidden", listOut.Contents)
	}
	versionsOut, err := ib.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: new("bucket")})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(versionsOut.DeleteMarkers) != 1 || versionsOut.DeleteMarkers[0].VersionId == nil || *versionsOut.DeleteMarkers[0].VersionId != *deleteOut.VersionId {
		t.Fatalf("delete markers = %#v, want created marker", versionsOut.DeleteMarkers)
	}
	if len(versionsOut.Versions) != 1 || versionsOut.Versions[0].VersionId == nil || *versionsOut.Versions[0].VersionId != putOut.VersionID {
		t.Fatalf("versions = %#v, want original data version", versionsOut.Versions)
	}

	if _, err := ib.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    new("bucket"),
		Key:       new("key"),
		VersionId: deleteOut.VersionId,
	}); err != nil {
		t.Fatalf("DeleteObject marker version: %v", err)
	}
	if body := getObjectBody(t, ib.backend, "bucket", "key"); body != validTestObjectBody("data") {
		t.Fatalf("restored body = %q, want data", body)
	}
}

func TestIntegration_BucketLifecycle(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	// 1. CreateBucket creates the namespace and schedules provider storage.
	err := ib.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: new("my-bucket"),
	}, nil)
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Provider storage is not ready until the provisioning task finishes.
	bkt, err := ib.repos.Buckets.GetByName(ctx, "my-bucket")
	if err != nil || bkt == nil {
		t.Fatalf("expected bucket, got err=%v", err)
	}
	if bkt.Status != model.BucketStatusProvisioning {
		t.Fatalf("expected status=provisioning, got %s", bkt.Status)
	}

	// 2. HeadBucket should succeed
	_, err = ib.backend.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: new("my-bucket"),
	})
	if err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}

	// 3. Bucket should appear in ListBuckets
	listOut, err := ib.backend.ListBuckets(ctx, s3response.ListBucketsInput{IsAdmin: true})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	found := false
	for _, b := range listOut.Buckets.Bucket {
		if b.Name == "my-bucket" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected my-bucket in ListBuckets")
	}

	if err := ib.repos.Buckets.UpdateStatus(ctx, bkt.ID, model.BucketStatusProvisioning, model.BucketStatusReady); err != nil {
		t.Fatalf("mark test bucket ready: %v", err)
	}

	// 4. PutObject should succeed after provisioning completes.
	putObject(t, ib.backend, "my-bucket", "temp-key", "temp")

	// 5. DeleteBucket should return error (not supported)
	err = ib.backend.DeleteBucket(ctx, "my-bucket")
	if err == nil {
		t.Fatal("expected DeleteBucket to return error (not supported)")
	}
}

func TestIntegration_MultipartUpload_HappyPath(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")

	// 1. CreateMultipartUpload
	createOut, err := ib.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: new("bucket"),
		Key:    new("big-file"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	contentID := createOut.UploadId

	// 2. UploadPart 1
	part1Num := int32(1)
	part1Body := validTestObjectBody("part1-data")
	part1Out, err := ib.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     new("bucket"),
		Key:        new("big-file"),
		UploadId:   &contentID,
		PartNumber: &part1Num,
		Body:       strings.NewReader(part1Body),
	})
	if err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}

	// 3. UploadPart 2
	part2Num := int32(2)
	part2Body := "part2-data"
	part2Out, err := ib.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     new("bucket"),
		Key:        new("big-file"),
		UploadId:   &contentID,
		PartNumber: &part2Num,
		Body:       strings.NewReader(part2Body),
	})
	if err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}

	// 4. CompleteMultipartUpload
	_, _, err = ib.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   new("bucket"),
		Key:      new("big-file"),
		UploadId: &contentID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: &part1Num, ETag: part1Out.ETag},
				{PartNumber: &part2Num, ETag: part2Out.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	// 5. Verify object exists with correct size
	obj, err := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "big-file")
	if err != nil || obj == nil {
		t.Fatalf("expected object after complete, err=%v", err)
	}
	expectedBody := part1Body + part2Body
	expectedSize := int64(len(expectedBody))
	if obj.Size != expectedSize {
		t.Fatalf("expected size=%d, got %d", expectedSize, obj.Size)
	}

	// Verify upload task created
	tasks := findTasks(t, ib.db, "object", obj.ObjectID)
	if len(tasks) == 0 {
		t.Fatal("expected upload task for completed multipart object")
	}

	// 6. GetObject → verify assembled body
	body := getObjectBody(t, ib.backend, "bucket", "big-file")
	if body != expectedBody {
		t.Fatalf("expected body=%q, got %q", expectedBody, body)
	}
}

func TestIntegration_StringAndShutdown(t *testing.T) {
	ib := newIntegrationBackend(t)

	s := ib.backend.String()
	if s == "" {
		t.Fatal("expected non-empty String()")
	}

	// Shutdown should not panic.
	ib.backend.Shutdown()
}

func TestIntegration_ListMultipartUploads(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	testutil.SeedBucket(t, ib.db, "bucket")

	// Create 3 multipart uploads
	for i := range 3 {
		key := fmt.Sprintf("multi-key-%d", i)
		_, err := ib.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
			Bucket: new("bucket"),
			Key:    &key,
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload %d: %v", i, err)
		}
	}

	// ListMultipartUploads
	listOut, err := ib.backend.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: new("bucket"),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(listOut.Uploads) != 3 {
		t.Fatalf("expected 3 uploads, got %d", len(listOut.Uploads))
	}
	if listOut.IsTruncated {
		t.Fatal("expected IsTruncated=false")
	}

	// Verify MaxUploads pagination
	maxUploads := int32(1)
	listOut2, err := ib.backend.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket:     new("bucket"),
		MaxUploads: &maxUploads,
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads paginated: %v", err)
	}
	if len(listOut2.Uploads) != 1 {
		t.Fatalf("expected 1 upload in paginated result, got %d", len(listOut2.Uploads))
	}
	if !listOut2.IsTruncated {
		t.Fatal("expected IsTruncated=true for paginated result")
	}
}

func TestIntegration_ListParts(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	testutil.SeedBucket(t, ib.db, "bucket")

	// Create multipart upload
	createOut, err := ib.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: new("bucket"),
		Key:    new("parts-file"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	contentID := createOut.UploadId

	// Upload 3 parts
	for i := int32(1); i <= 3; i++ {
		partNum := i
		body := fmt.Sprintf("part-%d-data", i)
		_, err := ib.backend.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     new("bucket"),
			Key:        new("parts-file"),
			UploadId:   &contentID,
			PartNumber: &partNum,
			Body:       strings.NewReader(body),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i, err)
		}
	}

	// ListParts
	listOut, err := ib.backend.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   new("bucket"),
		Key:      new("parts-file"),
		UploadId: &contentID,
	})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(listOut.Parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(listOut.Parts))
	}
	for i, p := range listOut.Parts {
		if p.PartNumber != i+1 {
			t.Fatalf("part %d: expected PartNumber=%d, got %d", i, i+1, p.PartNumber)
		}
		if p.ETag == "" {
			t.Fatalf("part %d: expected non-empty ETag", i)
		}
		if p.Size <= 0 {
			t.Fatalf("part %d: expected Size>0, got %d", i, p.Size)
		}
	}

	// ListParts with MaxParts pagination
	maxParts := int32(1)
	listOut2, err := ib.backend.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   new("bucket"),
		Key:      new("parts-file"),
		UploadId: &contentID,
		MaxParts: &maxParts,
	})
	if err != nil {
		t.Fatalf("ListParts paginated: %v", err)
	}
	if len(listOut2.Parts) != 1 {
		t.Fatalf("expected 1 part in paginated result, got %d", len(listOut2.Parts))
	}
	if !listOut2.IsTruncated {
		t.Fatal("expected IsTruncated=true for paginated ListParts")
	}
}

func TestIntegration_UploadPartCopy(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	testutil.SeedBucket(t, ib.db, "bucket")

	// Put a source object
	putObject(t, ib.backend, "bucket", "copy-src", "source-data-for-part-copy")

	// Create a multipart upload
	createOut, err := ib.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: new("bucket"),
		Key:    new("copy-dst"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	contentID := createOut.UploadId

	// UploadPartCopy: copy source into part 1
	partNum := int32(1)
	copySource := "bucket/copy-src"
	partCopyOut, err := ib.backend.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:     new("bucket"),
		Key:        new("copy-dst"),
		UploadId:   &contentID,
		PartNumber: &partNum,
		CopySource: &copySource,
	})
	if err != nil {
		t.Fatalf("UploadPartCopy: %v", err)
	}
	if partCopyOut.ETag == nil || *partCopyOut.ETag == "" {
		t.Fatal("expected non-empty ETag from UploadPartCopy")
	}

	// Complete the multipart upload with the copied part
	_, _, err = ib.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   new("bucket"),
		Key:      new("copy-dst"),
		UploadId: &contentID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: &partNum, ETag: partCopyOut.ETag},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	// Verify the assembled object has the same data as the source
	body := getObjectBody(t, ib.backend, "bucket", "copy-dst")
	if want := validTestObjectBody("source-data-for-part-copy"); body != want {
		t.Fatalf("expected body=%q, got %q", want, body)
	}
}

func TestIntegration_CopyObject_MetadataMatch(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")

	// Put source with specific content type
	srcBucket := "bucket"
	srcKey := "original.txt"
	_, err := ib.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket:      &srcBucket,
		Key:         &srcKey,
		Body:        strings.NewReader(validTestObjectBody("copy-me")),
		ContentType: new("text/plain"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	srcObj, err := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "original.txt")
	if err != nil || srcObj == nil {
		t.Fatalf("expected src object, got err=%v", err)
	}

	// Copy to destination
	copySource := "bucket/original.txt"
	dstBucket := "bucket"
	dstKey := "copy.txt"
	copyOut, err := ib.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:     &dstBucket,
		Key:        &dstKey,
		CopySource: &copySource,
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}

	// Verify CopyObjectResult output
	if copyOut.CopyObjectResult == nil {
		t.Fatal("expected CopyObjectResult to be non-nil")
	}
	if copyOut.CopyObjectResult.ETag == nil || *copyOut.CopyObjectResult.ETag == "" {
		t.Fatal("expected CopyObjectResult.ETag to be non-empty")
	}

	// Verify destination metadata matches source
	dstObj, err := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "copy.txt")
	if err != nil || dstObj == nil {
		t.Fatalf("expected dst object, got err=%v", err)
	}

	if dstObj.Size != srcObj.Size {
		t.Fatalf("size mismatch: src=%d dst=%d", srcObj.Size, dstObj.Size)
	}
	if dstObj.ETag != srcObj.ETag {
		t.Fatalf("etag mismatch: src=%s dst=%s", srcObj.ETag, dstObj.ETag)
	}
	if dstObj.ContentType != srcObj.ContentType {
		t.Fatalf("content-type mismatch: src=%s dst=%s", srcObj.ContentType, dstObj.ContentType)
	}
	// Pipeline position is derived from the content's copies, and no copy exists
	// until the ingest plan runs, so a fresh copy reads as cached.
	if dstObj.State != model.ObjectStateCached {
		t.Fatalf("expected dst state=cached, got %s", dstObj.State)
	}
	if dstObj.VersionID == "" {
		t.Fatal("expected destination current version id")
	}

	// Verify body matches
	body := getObjectBody(t, ib.backend, "bucket", "copy.txt")
	if want := validTestObjectBody("copy-me"); body != want {
		t.Fatalf("expected body=%q, got %q", want, body)
	}
}

func TestIntegration_DeleteObjects_BatchCreatesDeleteMarkers(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	testutil.SeedBucket(t, ib.db, "bucket")
	putObject(t, ib.backend, "bucket", "file-a", "aaa")
	putObject(t, ib.backend, "bucket", "file-b", "bbb")

	out, err := ib.backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: new("bucket"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: new("file-a")},
				{Key: new("file-b")},
			},
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if len(out.Error) != 0 {
		t.Fatalf("DeleteObjects errors = %#v, want none", out.Error)
	}
	if len(out.Deleted) != 2 {
		t.Fatalf("DeleteObjects deleted = %#v, want two entries", out.Deleted)
	}
	for _, deleted := range out.Deleted {
		if deleted.Key == nil || *deleted.Key == "" {
			t.Fatalf("deleted entry missing key: %#v", deleted)
		}
		if deleted.DeleteMarker == nil || !*deleted.DeleteMarker {
			t.Fatalf("deleted entry DeleteMarker = %v, want true", deleted.DeleteMarker)
		}
		if deleted.DeleteMarkerVersionId == nil || *deleted.DeleteMarkerVersionId == "" {
			t.Fatalf("deleted entry missing DeleteMarkerVersionId: %#v", deleted)
		}
	}

	listOut, err := ib.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: new("bucket")})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(listOut.Contents) != 0 {
		t.Fatalf("ListObjectsV2 contents = %#v, want hidden objects", listOut.Contents)
	}

	versionsOut, err := ib.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: new("bucket")})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(versionsOut.Versions) != 2 || len(versionsOut.DeleteMarkers) != 2 {
		t.Fatalf("versions=%#v deleteMarkers=%#v, want two data versions and two delete markers", versionsOut.Versions, versionsOut.DeleteMarkers)
	}
}

func TestIntegration_ListObjectsV2_Pagination(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	testutil.SeedBucket(t, ib.db, "bucket")

	// Put 5 objects with lexicographic keys
	keys := []string{"obj-a", "obj-b", "obj-c", "obj-d", "obj-e"}
	for _, k := range keys {
		putObject(t, ib.backend, "bucket", k, "data-"+k)
	}

	// Page 1: MaxKeys=2
	maxKeys := int32(2)
	out1, err := ib.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  new("bucket"),
		MaxKeys: &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page 1: %v", err)
	}
	if *out1.KeyCount != 2 {
		t.Fatalf("page 1: expected KeyCount=2, got %d", *out1.KeyCount)
	}
	if !*out1.IsTruncated {
		t.Fatal("page 1: expected IsTruncated=true")
	}
	if out1.NextContinuationToken == nil || *out1.NextContinuationToken == "" {
		t.Fatal("page 1: expected NextContinuationToken")
	}

	// Collect all keys through pagination
	var allKeys []string
	for _, obj := range out1.Contents {
		allKeys = append(allKeys, *obj.Key)
	}

	// Page 2
	out2, err := ib.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            new("bucket"),
		MaxKeys:           &maxKeys,
		ContinuationToken: out1.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page 2: %v", err)
	}
	if *out2.KeyCount != 2 {
		t.Fatalf("page 2: expected KeyCount=2, got %d", *out2.KeyCount)
	}
	if !*out2.IsTruncated {
		t.Fatal("page 2: expected IsTruncated=true")
	}
	for _, obj := range out2.Contents {
		allKeys = append(allKeys, *obj.Key)
	}

	// Page 3 (last page, should have 1 object)
	out3, err := ib.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            new("bucket"),
		MaxKeys:           &maxKeys,
		ContinuationToken: out2.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page 3: %v", err)
	}
	if *out3.KeyCount != 1 {
		t.Fatalf("page 3: expected KeyCount=1, got %d", *out3.KeyCount)
	}
	if *out3.IsTruncated {
		t.Fatal("page 3: expected IsTruncated=false")
	}
	for _, obj := range out3.Contents {
		allKeys = append(allKeys, *obj.Key)
	}

	// Verify we got all 5 keys in order
	if len(allKeys) != 5 {
		t.Fatalf("expected 5 total keys, got %d: %v", len(allKeys), allKeys)
	}
	for i, k := range keys {
		if allKeys[i] != k {
			t.Fatalf("key[%d]: expected %s, got %s", i, k, allKeys[i])
		}
	}
}

func TestIntegration_HeadObject(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	testutil.SeedBucket(t, ib.db, "bucket")

	content := validTestObjectBody("hello-head-object")
	bucketName := "bucket"
	keyName := "head-key"
	_, err := ib.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket:      &bucketName,
		Key:         &keyName,
		Body:        strings.NewReader(content),
		ContentType: new("application/json"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// HeadObject
	headOut, err := ib.backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: new("bucket"),
		Key:    new("head-key"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}

	// Verify ContentLength
	if headOut.ContentLength == nil || *headOut.ContentLength != int64(len(content)) {
		t.Fatalf("expected ContentLength=%d, got %v", len(content), headOut.ContentLength)
	}

	// Verify ETag is non-empty and quoted
	if headOut.ETag == nil || *headOut.ETag == "" {
		t.Fatal("expected ETag to be non-empty")
	}
	if !strings.HasPrefix(*headOut.ETag, `"`) || !strings.HasSuffix(*headOut.ETag, `"`) {
		t.Fatalf("expected ETag to be quoted, got %s", *headOut.ETag)
	}

	// Verify ContentType
	if headOut.ContentType == nil || *headOut.ContentType != "application/json" {
		t.Fatalf("expected ContentType=application/json, got %v", headOut.ContentType)
	}

	// Verify LastModified is set
	if headOut.LastModified == nil || headOut.LastModified.IsZero() {
		t.Fatal("expected LastModified to be set")
	}
}

func TestIntegration_GetObject_CacheMiss_NoPieceCID(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")

	// Put object so it's in DB and cache
	putObject(t, ib.backend, "bucket", "evicted-key", "some data")

	obj, err := ib.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "evicted-key")
	if err != nil || obj == nil {
		t.Fatalf("expected object in DB, got err=%v", err)
	}
	// Confirm PieceCID is nil (hasn't been uploaded to SP yet)
	if obj.PieceCID != nil {
		t.Fatalf("expected PieceCID=nil before upload, got %v", obj.PieceCID)
	}

	// Manually delete from cache to simulate a cache miss
	if err := ib.cache.Delete(ctx, "bucket", obj.CacheKey()); err != nil {
		t.Fatalf("cache delete: %v", err)
	}
	if ib.cache.Exists(ctx, "bucket", obj.CacheKey()) {
		t.Fatal("expected cache file to be gone")
	}

	// GetObject should fail — object is in DB but no cache and no PieceCID for SP download
	_, err = ib.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: new("bucket"),
		Key:    new("evicted-key"),
	})
	if err == nil {
		t.Fatal("expected GetObject to fail on cache miss with no PieceCID")
	}
}
