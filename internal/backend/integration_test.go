package backend_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/data-preservation-programs/go-synapse/storage"
	cid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/s3response"
)

// integrationBackend extends testBackend with direct DB access for task verification.
type integrationBackend struct {
	backend *backend.SynapseBackend
	repos   *repository.Repositories
	cache   cache.Cache
	storage *testutil.MockStorageClient
	proof   *testutil.MockProofSetClient
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
	sm := state.NewObjectStateMachine()
	sc := &testutil.MockStorageClient{}
	pc := &testutil.MockProofSetClient{}
	logger := slog.Default()

	b := backend.New(repos, fsCache, sm, sc, pc, logger)
	return &integrationBackend{
		backend: b,
		repos:   repos,
		cache:   fsCache,
		storage: sc,
		proof:   pc,
		db:      db,
	}
}

// activateBucket transitions a bucket from creating → active by setting proof set ID and status.
func activateBucket(t *testing.T, repos *repository.Repositories, bucketID int64) {
	t.Helper()
	ctx := context.Background()
	if err := repos.Buckets.SetProofSetID(ctx, bucketID, "42"); err != nil {
		t.Fatalf("setting proof set ID: %v", err)
	}
	if err := repos.Buckets.UpdateStatus(ctx, bucketID, model.BucketStatusCreating, model.BucketStatusActive); err != nil {
		t.Fatalf("activating bucket: %v", err)
	}
}

// findTasks queries all tasks matching the given ref type and ref ID.
func findTasks(t *testing.T, db *bun.DB, refType string, refID int64) []model.Task {
	t.Helper()
	var tasks []model.Task
	err := db.NewSelect().Model(&tasks).
		Where("ref_type = ? AND ref_id = ?", refType, refID).
		OrderExpr("id ASC").
		Scan(context.Background())
	if err != nil {
		t.Fatalf("querying tasks: %v", err)
	}
	return tasks
}

// findTasksByType queries all tasks matching the given type.
func findTasksByType(t *testing.T, db *bun.DB, taskType model.TaskType) []model.Task {
	t.Helper()
	var tasks []model.Task
	err := db.NewSelect().Model(&tasks).
		Where("type = ?", taskType).
		OrderExpr("id ASC").
		Scan(context.Background())
	if err != nil {
		t.Fatalf("querying tasks by type: %v", err)
	}
	return tasks
}

// putObject is a helper that calls PutObject with the given string body.
func putObject(t *testing.T, b *backend.SynapseBackend, bucket, key, body string) s3response.PutObjectOutput {
	t.Helper()
	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   strings.NewReader(body),
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

	obj, err := ib.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "test-key")
	if err != nil || obj == nil {
		t.Fatalf("expected object in DB, got err=%v obj=%v", err, obj)
	}
	if obj.State != model.ObjectStateCached {
		t.Fatalf("expected state=cached, got %s", obj.State)
	}
	if obj.Generation != 1 {
		t.Fatalf("expected generation=1, got %d", obj.Generation)
	}

	tasks := findTasks(t, ib.db, "object", obj.ID)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 upload task, got %d", len(tasks))
	}
	if tasks[0].Type != model.TaskTypeUploadToSP {
		t.Fatalf("expected task type upload_to_sp, got %s", tasks[0].Type)
	}
	if tasks[0].RefGeneration != 1 {
		t.Fatalf("expected task gen=1, got %d", tasks[0].RefGeneration)
	}

	// 2. Simulate uploader: cached → uploading
	if err := ib.repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("cached→uploading: %v", err)
	}

	// Simulate uploader sets PieceCID: uploading → uploaded
	if err := ib.repos.Objects.SetPieceCIDAndTransition(ctx, obj.ID, 1, "bafk2test123", model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatalf("uploading→uploaded: %v", err)
	}

	obj, _ = ib.repos.Objects.GetByID(ctx, obj.ID)
	if obj.State != model.ObjectStateUploaded {
		t.Fatalf("expected state=uploaded, got %s", obj.State)
	}
	if obj.PieceCID == nil || *obj.PieceCID != "bafk2test123" {
		t.Fatalf("expected PieceCID=bafk2test123, got %v", obj.PieceCID)
	}

	// 3. Simulate onchain: uploaded → onchaining → onchained
	if err := ib.repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateUploaded, model.ObjectStateOnChaining); err != nil {
		t.Fatalf("uploaded→onchaining: %v", err)
	}
	if err := ib.repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateOnChaining, model.ObjectStateOnChained); err != nil {
		t.Fatalf("onchaining→onchained: %v", err)
	}

	obj, _ = ib.repos.Objects.GetByID(ctx, obj.ID)
	if obj.State != model.ObjectStateOnChained {
		t.Fatalf("expected state=onchained, got %s", obj.State)
	}

	// 4. Simulate evictor: onchained → cache_evicted, remove cache file
	if err := ib.repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateOnChained, model.ObjectStateCacheEvicted); err != nil {
		t.Fatalf("onchained→cache_evicted: %v", err)
	}
	if err := ib.cache.Delete(ctx, "test-bucket", "test-key"); err != nil {
		t.Fatalf("cache delete: %v", err)
	}

	obj, _ = ib.repos.Objects.GetByID(ctx, obj.ID)
	if obj.State != model.ObjectStateCacheEvicted {
		t.Fatalf("expected state=cache_evicted, got %s", obj.State)
	}
	if ib.cache.Exists(ctx, "test-bucket", "test-key") {
		t.Fatal("expected cache file to be gone")
	}
}

func TestIntegration_OverwritePath(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")

	// First write
	putObject(t, ib.backend, "bucket", "key", "v1")

	obj, _ := ib.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "key")
	if obj.Generation != 1 {
		t.Fatalf("expected gen=1 after first put, got %d", obj.Generation)
	}

	tasks := findTasks(t, ib.db, "object", obj.ID)
	if len(tasks) != 1 || tasks[0].RefGeneration != 1 {
		t.Fatalf("expected 1 task with gen=1, got %d tasks", len(tasks))
	}

	// Overwrite
	putObject(t, ib.backend, "bucket", "key", "v2")

	obj, _ = ib.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "key")
	if obj.Generation != 2 {
		t.Fatalf("expected gen=2 after overwrite, got %d", obj.Generation)
	}

	tasks = findTasks(t, ib.db, "object", obj.ID)
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}
	if tasks[1].RefGeneration != 2 {
		t.Fatalf("expected second task gen=2, got %d", tasks[1].RefGeneration)
	}

	// Old task (gen=1) is stale: object generation is now 2
	if tasks[0].RefGeneration != 1 {
		t.Fatalf("expected first task gen=1, got %d", tasks[0].RefGeneration)
	}
	// The object's current generation (2) differs from old task's gen (1) → stale

	// GetObject should return v2
	body := getObjectBody(t, ib.backend, "bucket", "key")
	if body != "v2" {
		t.Fatalf("expected body=v2, got %q", body)
	}
}

func TestIntegration_ColdReadAfterEviction(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "test-bucket")
	content := "hello world"

	// PutObject
	putObject(t, ib.backend, "test-bucket", "test-key", content)

	obj, _ := ib.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "test-key")

	// Simulate full pipeline to cache_evicted
	if err := ib.repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatal(err)
	}

	// Create a valid CID for PieceCID
	mh, err := multihash.Sum([]byte("test"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	testPieceCID := cid.NewCidV1(cid.Raw, mh)

	if err := ib.repos.Objects.SetPieceCIDAndTransition(ctx, obj.ID, 1, testPieceCID.String(), model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatal(err)
	}
	if err := ib.repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateUploaded, model.ObjectStateOnChaining); err != nil {
		t.Fatal(err)
	}
	if err := ib.repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateOnChaining, model.ObjectStateOnChained); err != nil {
		t.Fatal(err)
	}
	if err := ib.repos.Objects.UpdateState(ctx, obj.ID, 1, model.ObjectStateOnChained, model.ObjectStateCacheEvicted); err != nil {
		t.Fatal(err)
	}

	// Remove cache file
	if err := ib.cache.Delete(ctx, "test-bucket", "test-key"); err != nil {
		t.Fatal(err)
	}
	if ib.cache.Exists(ctx, "test-bucket", "test-key") {
		t.Fatal("cache should be empty after eviction")
	}

	// Configure mock SP download to return the original content
	ib.storage.DownloadFunc = func(_ context.Context, pc cid.Cid, _ *storage.DownloadOptions) ([]byte, error) {
		if pc.Equals(testPieceCID) {
			return []byte(content), nil
		}
		return nil, fmt.Errorf("unexpected CID: %s", pc)
	}

	// GetObject should succeed via SP download
	body := getObjectBody(t, ib.backend, "test-bucket", "test-key")
	if body != content {
		t.Fatalf("expected body=%q, got %q", content, body)
	}

	// Cache rehydration is synchronous (happens inside GetObject), so the file
	// should already be present after the call returns.
	if !ib.cache.Exists(ctx, "test-bucket", "test-key") {
		t.Fatal("expected cache to be rehydrated after cold read (synchronous rehydration)")
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
	dstObj, err := ib.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "dst-key")
	if err != nil || dstObj == nil {
		t.Fatalf("expected dst object, got err=%v", err)
	}

	// ETags should match (same data)
	srcETag := strings.Trim(putOut.ETag, `"`)
	if dstObj.ETag != srcETag {
		t.Fatalf("expected dst etag=%s, got %s", srcETag, dstObj.ETag)
	}

	// Verify upload task created for destination
	dstTasks := findTasks(t, ib.db, "object", dstObj.ID)
	if len(dstTasks) == 0 {
		t.Fatal("expected upload task for dst object")
	}
	if dstTasks[0].Type != model.TaskTypeUploadToSP {
		t.Fatalf("expected upload_to_sp task, got %s", dstTasks[0].Type)
	}

	// GetObject on dest should return the same data
	body := getObjectBody(t, ib.backend, "bucket", "dst-key")
	if body != "data" {
		t.Fatalf("expected body=data, got %q", body)
	}
}

func TestIntegration_DeletePath(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")

	// Put object
	putObject(t, ib.backend, "bucket", "key", "data")

	// Verify visible via ListObjects
	listOut, err := ib.backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: strPtr("bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(listOut.Contents) != 1 {
		t.Fatalf("expected 1 object in list, got %d", len(listOut.Contents))
	}

	// Delete
	_, err = ib.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: strPtr("bucket"),
		Key:    strPtr("key"),
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	// Verify not visible via ListObjects
	listOut, err = ib.backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: strPtr("bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjects after delete: %v", err)
	}
	if len(listOut.Contents) != 0 {
		t.Fatalf("expected 0 objects after delete, got %d", len(listOut.Contents))
	}

	// Verify cache file removed
	if ib.cache.Exists(ctx, "bucket", "key") {
		t.Fatal("expected cache file to be removed after delete")
	}

	// Verify DB object is soft-deleted (GetByBucketAndKey should return nil due to soft delete)
	obj, _ := ib.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "key")
	if obj != nil {
		t.Fatal("expected soft-deleted object to be invisible")
	}
}

func TestIntegration_BucketLifecycle(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	// 1. CreateBucket
	err := ib.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: strPtr("my-bucket"),
	}, nil)
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Verify bucket in creating status
	bkt, err := ib.repos.Buckets.GetByName(ctx, "my-bucket")
	if err != nil || bkt == nil {
		t.Fatalf("expected bucket, got err=%v", err)
	}
	if bkt.Status != model.BucketStatusCreating {
		t.Fatalf("expected status=creating, got %s", bkt.Status)
	}

	// Verify create_proof_set task exists
	cpsTasks := findTasksByType(t, ib.db, model.TaskTypeCreateProofSet)
	if len(cpsTasks) == 0 {
		t.Fatal("expected create_proof_set task")
	}

	// 2. Simulate proofset worker: set proof set ID + activate
	activateBucket(t, ib.repos, bkt.ID)

	// HeadBucket should succeed
	_, err = ib.backend.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: strPtr("my-bucket"),
	})
	if err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}

	// Bucket should appear in ListBuckets
	listOut, err := ib.backend.ListBuckets(ctx, s3response.ListBucketsInput{})
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

	// 3. PutObject + DeleteObject to make bucket empty for deletion
	putObject(t, ib.backend, "my-bucket", "temp-key", "temp")
	_, err = ib.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: strPtr("my-bucket"),
		Key:    strPtr("temp-key"),
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	// 4. DeleteBucket
	err = ib.backend.DeleteBucket(ctx, "my-bucket")
	if err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	// Verify status=deleting
	bkt, _ = ib.repos.Buckets.GetByName(ctx, "my-bucket")
	if bkt.Status != model.BucketStatusDeleting {
		t.Fatalf("expected status=deleting, got %s", bkt.Status)
	}

	// Verify delete_proof_set task exists
	dpsTasks := findTasksByType(t, ib.db, model.TaskTypeDeleteProofSet)
	if len(dpsTasks) == 0 {
		t.Fatal("expected delete_proof_set task")
	}

	// 5. Simulate proofset worker: hard delete bucket
	if err := ib.repos.Buckets.HardDelete(ctx, bkt.ID); err != nil {
		t.Fatalf("HardDelete: %v", err)
	}

	// HeadBucket should return NoSuchBucket
	_, err = ib.backend.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: strPtr("my-bucket"),
	})
	if err == nil {
		t.Fatal("expected HeadBucket to fail after hard delete")
	}
}

func TestIntegration_MultipartUpload_HappyPath(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	bucket := testutil.SeedBucket(t, ib.db, "bucket")

	// 1. CreateMultipartUpload
	createOut, err := ib.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: strPtr("bucket"),
		Key:    strPtr("big-file"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := createOut.UploadId

	// 2. UploadPart 1
	part1Num := int32(1)
	part1Out, err := ib.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     strPtr("bucket"),
		Key:        strPtr("big-file"),
		UploadId:   &uploadID,
		PartNumber: &part1Num,
		Body:       strings.NewReader("part1-data"),
	})
	if err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}

	// 3. UploadPart 2
	part2Num := int32(2)
	part2Out, err := ib.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     strPtr("bucket"),
		Key:        strPtr("big-file"),
		UploadId:   &uploadID,
		PartNumber: &part2Num,
		Body:       strings.NewReader("part2-data"),
	})
	if err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}

	// 4. CompleteMultipartUpload
	_, _, err = ib.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   strPtr("bucket"),
		Key:      strPtr("big-file"),
		UploadId: &uploadID,
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
	obj, err := ib.repos.Objects.GetByBucketAndKey(ctx, bucket.ID, "big-file")
	if err != nil || obj == nil {
		t.Fatalf("expected object after complete, err=%v", err)
	}
	expectedSize := int64(len("part1-data") + len("part2-data"))
	if obj.Size != expectedSize {
		t.Fatalf("expected size=%d, got %d", expectedSize, obj.Size)
	}

	// Verify upload task created
	tasks := findTasks(t, ib.db, "object", obj.ID)
	if len(tasks) == 0 {
		t.Fatal("expected upload task for completed multipart object")
	}

	// 6. GetObject → verify body = "part1-datapart2-data"
	body := getObjectBody(t, ib.backend, "bucket", "big-file")
	if body != "part1-datapart2-data" {
		t.Fatalf("expected body=part1-datapart2-data, got %q", body)
	}
}

func TestIntegration_MultipartUpload_Abort(t *testing.T) {
	ib := newIntegrationBackend(t)
	ctx := context.Background()

	testutil.SeedBucket(t, ib.db, "bucket")

	// 1. CreateMultipartUpload
	createOut, err := ib.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: strPtr("bucket"),
		Key:    strPtr("file"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := createOut.UploadId

	// 2. UploadPart
	part1Num := int32(1)
	_, err = ib.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     strPtr("bucket"),
		Key:        strPtr("file"),
		UploadId:   &uploadID,
		PartNumber: &part1Num,
		Body:       strings.NewReader("data"),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	// 3. AbortMultipartUpload
	err = ib.backend.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   strPtr("bucket"),
		Key:      strPtr("file"),
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

func strPtr(s string) *string {
	return &s
}
