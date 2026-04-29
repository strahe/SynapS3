package backend_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	synaps3backend "github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	synaps3testutil "github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/storage"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// ---------- helpers ----------

// seedActiveBucket creates an active bucket via the repository layer.
func seedActiveBucket(t *testing.T, tb *testBackend, name string) *model.Bucket {
	t.Helper()
	ctx := context.Background()
	bkt := &model.Bucket{Name: name, Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket %q: %v", name, err)
	}
	return bkt
}

// putTestObject is a shorthand that creates a bucket, puts an object, and returns the etag.
func putTestObject(t *testing.T, tb *testBackend, bucket, key, body string) string {
	t.Helper()
	return putTestObjectOutput(t, tb, bucket, key, body).ETag
}

func putTestObjectOutput(t *testing.T, tb *testBackend, bucket, key, body string) s3response.PutObjectOutput {
	t.Helper()
	ctx := context.Background()
	ct := "text/plain"
	out, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        strings.NewReader(body),
		ContentType: &ct,
	})
	if err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
	return out
}

func touchVersionLifecycle(t *testing.T, tb *testBackend, ctx context.Context, versionID string) *model.ObjectVersion {
	t.Helper()
	time.Sleep(5 * time.Millisecond)
	if err := tb.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("touch version lifecycle: %v", err)
	}
	version, err := tb.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get touched version: version=%v err=%v", version, err)
	}
	if !version.UpdatedAt.After(version.CreatedAt) {
		t.Fatalf("test setup did not make UpdatedAt mutable: created=%s updated=%s", version.CreatedAt, version.UpdatedAt)
	}
	return version
}

type getByBucketAndKeyAfterReadRepo struct {
	repository.ObjectRepository
	calls          int
	afterFirstRead func()
}

func (r *getByBucketAndKeyAfterReadRepo) GetByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.Object, error) {
	r.calls++
	obj, err := r.ObjectRepository.GetByBucketAndKey(ctx, bucketID, key)
	if r.calls == 1 && r.afterFirstRead != nil {
		r.afterFirstRead()
		r.afterFirstRead = nil
	}
	return obj, err
}

func seedBackendObjectVersion(t *testing.T, tb *testBackend, bucket *model.Bucket, key string, size int64, etag, checksum, contentType string, state model.ObjectState, pieceCID, retrievalURL *string) (int64, string) {
	t.Helper()
	versionID := model.NewVersionID()
	version := &model.ObjectVersion{
		VersionID:    versionID,
		BucketID:     bucket.ID,
		Key:          key,
		Size:         size,
		ETag:         etag,
		Checksum:     checksum,
		ContentType:  contentType,
		CacheKey:     ".versions/" + versionID,
		PieceCID:     pieceCID,
		RetrievalURL: retrievalURL,
		State:        state,
	}
	objID, err := tb.repos.Objects.CreateVersionAndSetCurrent(context.Background(), version)
	if err != nil {
		t.Fatalf("seeding object version: %v", err)
	}
	return objID, versionID
}

func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range metricFamilies {
		if family.GetName() != name {
			continue
		}
		var total float64
		for _, metric := range family.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
		return total
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

// ---------- PutObject ----------

func TestPutObject_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "put-bucket")

	body := "hello world"
	ct := "text/plain"
	out, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket:      aws.String("put-bucket"),
		Key:         aws.String("greeting.txt"),
		Body:        strings.NewReader(body),
		ContentType: &ct,
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if out.ETag == "" {
		t.Error("expected non-empty ETag")
	}
	if out.VersionID == "" {
		t.Error("expected non-empty VersionID")
	}

	// Verify DB record.
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "put-bucket")
	obj, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "greeting.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if obj == nil {
		t.Fatal("object not found in DB")
	}
	if obj.State != model.ObjectStateCached {
		t.Errorf("object state = %q, want %q", obj.State, model.ObjectStateCached)
	}
	if obj.Size != int64(len(body)) {
		t.Errorf("object size = %d, want %d", obj.Size, len(body))
	}
	if obj.CurrentVersionID == "" {
		t.Fatal("expected current version id")
	}

	// Verify cache file exists.
	if !tb.cache.Exists(ctx, "put-bucket", obj.CacheKey) {
		t.Error("cache file does not exist")
	}
}

func TestPutObjectUsesConfiguredUploadMaxRetries(t *testing.T) {
	tb := newTestBackendWithOptions(t, synaps3backend.WithUploadMaxRetries(11))
	ctx := context.Background()
	seedActiveBucket(t, tb, "put-retries-bucket")

	_, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket: aws.String("put-retries-bucket"),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader("data"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	task, err := tb.repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if task == nil {
		t.Fatal("expected upload task")
	}
	if task.MaxRetries != 11 {
		t.Fatalf("task MaxRetries = %d, want 11", task.MaxRetries)
	}
}

func TestPutObject_CacheFull(t *testing.T) {
	mc := &synaps3testutil.MockCache{
		PutStagedFunc: func(_ context.Context, _, _ string, _ io.Reader) (*cache.StagedObject, error) {
			return nil, cache.ErrCacheFull
		},
		CreateBucketDirFunc: func(_ context.Context, _ string) error { return nil },
	}
	tb := newTestBackendWithMockCache(t, mc)
	ctx := context.Background()
	seedActiveBucket(t, tb, "full-bucket")

	_, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket: aws.String("full-bucket"),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader("data"),
	})
	if err == nil {
		t.Fatal("expected error when cache is full")
	}
	if !errors.Is(err, cache.ErrCacheFull) && !strings.Contains(err.Error(), "cache") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPutObject_Overwrite(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "ow-bucket")

	putTestObject(t, tb, "ow-bucket", "file.txt", "version-1")

	bkt, _ := tb.repos.Buckets.GetByName(ctx, "ow-bucket")
	obj1, _ := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if obj1.CurrentVersionID == "" {
		t.Fatal("first current version id is empty")
	}
	firstVersionID := obj1.CurrentVersionID

	putTestObject(t, tb, "ow-bucket", "file.txt", "version-2")

	obj2, _ := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if obj2.CurrentVersionID == "" || obj2.CurrentVersionID == firstVersionID {
		t.Fatalf("second current version id = %q, first = %q", obj2.CurrentVersionID, firstVersionID)
	}
	firstVersion, err := tb.repos.Objects.GetVersionByID(ctx, firstVersionID)
	if err != nil || firstVersion == nil {
		t.Fatalf("first version missing: version=%v err=%v", firstVersion, err)
	}
	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, obj2.CurrentVersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("second version missing: version=%v err=%v", secondVersion, err)
	}
	if firstVersion.ObjectID != obj2.ID || secondVersion.ObjectID != obj2.ID {
		t.Fatalf("versions should reference object %d, got %d/%d", obj2.ID, firstVersion.ObjectID, secondVersion.ObjectID)
	}
}

func TestPutObjectIdenticalCurrentObjectCreatesNewVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "dedupe-bucket")

	firstOut := putTestObjectOutput(t, tb, "dedupe-bucket", "file.txt", "same data")

	bkt, _ := tb.repos.Buckets.GetByName(ctx, "dedupe-bucket")
	obj1, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || obj1 == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", obj1, err)
	}

	secondOut := putTestObjectOutput(t, tb, "dedupe-bucket", "file.txt", "same data")

	obj2, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || obj2 == nil {
		t.Fatalf("current object after second put: obj=%v err=%v", obj2, err)
	}
	if secondOut.ETag != firstOut.ETag {
		t.Fatalf("second ETag = %s, want %s", secondOut.ETag, firstOut.ETag)
	}
	if secondOut.VersionID == "" || secondOut.VersionID == firstOut.VersionID {
		t.Fatalf("second VersionID = %q, first = %q", secondOut.VersionID, firstOut.VersionID)
	}
	if obj2.CurrentVersionID == obj1.CurrentVersionID {
		t.Fatalf("current version did not change for identical put: %s", obj2.CurrentVersionID)
	}

	versionCount, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", obj1.ID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting object versions: %v", err)
	}
	if versionCount != 2 {
		t.Fatalf("object version count = %d, want 2", versionCount)
	}

	taskCount, err := tb.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("ref_type = ? AND ref_id = ?", "object", obj1.ID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting upload tasks: %v", err)
	}
	if taskCount != 1 {
		t.Fatalf("task count = %d, want 1", taskCount)
	}

	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, secondOut.VersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("second version: version=%v err=%v", secondVersion, err)
	}
	if secondVersion.State != model.ObjectStateUploading {
		t.Fatalf("second version state = %s, want uploading", secondVersion.State)
	}
}

func TestPutObjectIdenticalUploadingContentFollowsActiveUploadTask(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "uploading-reuse-bucket")

	firstOut := putTestObjectOutput(t, tb, "uploading-reuse-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "uploading-reuse-bucket")
	firstObj, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}

	claimed, err := tb.repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("claim upload task: %v", err)
	}
	if claimed == nil || claimed.RefVersionID != firstOut.VersionID {
		t.Fatalf("claimed task = %#v, want version %s", claimed, firstOut.VersionID)
	}
	if err := tb.repos.Objects.UpdateVersionState(ctx, firstOut.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark first version uploading: %v", err)
	}

	tasksBefore, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", 10, 0)
	if err != nil {
		t.Fatalf("list tasks before second put: %v", err)
	}

	secondOut := putTestObjectOutput(t, tb, "uploading-reuse-bucket", "file.txt", "same data")
	if secondOut.VersionID == "" || secondOut.VersionID == firstOut.VersionID {
		t.Fatalf("second VersionID = %q, first = %q", secondOut.VersionID, firstOut.VersionID)
	}

	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, secondOut.VersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("second version: version=%v err=%v", secondVersion, err)
	}
	if secondVersion.State != model.ObjectStateUploading {
		t.Fatalf("second version state = %s, want uploading", secondVersion.State)
	}
	if secondVersion.PieceCID != nil || secondVersion.RetrievalURL != nil {
		t.Fatalf("second version storage = piece:%v url:%v, want unset while upload runs", secondVersion.PieceCID, secondVersion.RetrievalURL)
	}

	tasksAfter, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", 10, 0)
	if err != nil {
		t.Fatalf("list tasks after second put: %v", err)
	}
	if len(tasksAfter) != len(tasksBefore) {
		t.Fatalf("upload task count changed from %d to %d", len(tasksBefore), len(tasksAfter))
	}
}

func TestPutObjectIdenticalStoredContentReusesChainStorage(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "stored-reuse-bucket")

	firstOut := putTestObjectOutput(t, tb, "stored-reuse-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "stored-reuse-bucket")
	firstObj, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}
	if err := tb.repos.Objects.UpdateVersionState(ctx, firstObj.CurrentVersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark first version uploading: %v", err)
	}
	if err := tb.repos.Objects.SetVersionStorageInfoAndTransition(ctx, firstObj.CurrentVersionID, "piece-shared", "https://provider.example/shared", model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("mark first version stored: %v", err)
	}

	tasksBefore, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", 10, 0)
	if err != nil {
		t.Fatalf("list tasks before second put: %v", err)
	}
	secondOut := putTestObjectOutput(t, tb, "stored-reuse-bucket", "file.txt", "same data")
	if secondOut.VersionID == "" || secondOut.VersionID == firstOut.VersionID {
		t.Fatalf("second VersionID = %q, first = %q", secondOut.VersionID, firstOut.VersionID)
	}

	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, secondOut.VersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("second version: version=%v err=%v", secondVersion, err)
	}
	if secondVersion.State != model.ObjectStateStored {
		t.Fatalf("second version state = %s, want stored", secondVersion.State)
	}
	if secondVersion.PieceCID == nil || *secondVersion.PieceCID != "piece-shared" {
		t.Fatalf("second version piece cid = %v, want piece-shared", secondVersion.PieceCID)
	}
	if secondVersion.RetrievalURL == nil || *secondVersion.RetrievalURL != "https://provider.example/shared" {
		t.Fatalf("second version retrieval url = %v", secondVersion.RetrievalURL)
	}

	tasksAfter, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", 10, 0)
	if err != nil {
		t.Fatalf("list tasks after second put: %v", err)
	}
	if len(tasksAfter) != len(tasksBefore) {
		t.Fatalf("upload task count changed from %d to %d", len(tasksBefore), len(tasksAfter))
	}
}

func TestPutObjectIdenticalStoredContentQueuesEvictWhenAutoEvictEnabled(t *testing.T) {
	tb := newTestBackendWithOptions(t, synaps3backend.WithAutoEvict(true), synaps3backend.WithEvictMaxRetries(9))
	ctx := context.Background()
	seedActiveBucket(t, tb, "stored-reuse-evict-bucket")

	putTestObject(t, tb, "stored-reuse-evict-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "stored-reuse-evict-bucket")
	firstObj, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}
	if err := tb.repos.Objects.UpdateVersionState(ctx, firstObj.CurrentVersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark first version uploading: %v", err)
	}
	if err := tb.repos.Objects.SetVersionStorageInfoAndTransition(ctx, firstObj.CurrentVersionID, "piece-shared", "https://provider.example/shared", model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("mark first version stored: %v", err)
	}

	secondOut := putTestObjectOutput(t, tb, "stored-reuse-evict-bucket", "file.txt", "same data")
	task, err := tb.repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claim evict task: %v", err)
	}
	if task == nil {
		t.Fatal("expected evict task for reused stored version")
	}
	if task.RefVersionID != secondOut.VersionID {
		t.Fatalf("evict task version = %s, want %s", task.RefVersionID, secondOut.VersionID)
	}
	if task.MaxRetries != 9 {
		t.Fatalf("evict task MaxRetries = %d, want 9", task.MaxRetries)
	}
}

// ---------- GetObject ----------

func TestGetObject_FromCache(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "get-bucket")
	putTestObject(t, tb, "get-bucket", "hello.txt", "hello")

	out, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("get-bucket"),
		Key:    aws.String("hello.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() { _ = out.Body.Close() }()

	data, _ := io.ReadAll(out.Body)
	if string(data) != "hello" {
		t.Errorf("body = %q, want %q", string(data), "hello")
	}
}

func TestGetObject_WithVersionIDReadsSpecifiedVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "get-version-bucket")

	firstOut := putTestObjectOutput(t, tb, "get-version-bucket", "hello.txt", "old")
	putTestObject(t, tb, "get-version-bucket", "hello.txt", "new")

	out, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String("get-version-bucket"),
		Key:       aws.String("hello.txt"),
		VersionId: aws.String(firstOut.VersionID),
	})
	if err != nil {
		t.Fatalf("GetObject(version): %v", err)
	}
	defer func() { _ = out.Body.Close() }()

	data, _ := io.ReadAll(out.Body)
	if string(data) != "old" {
		t.Fatalf("body = %q, want old version content", string(data))
	}
	if out.VersionId == nil || *out.VersionId != firstOut.VersionID {
		t.Fatalf("VersionId = %v, want %s", out.VersionId, firstOut.VersionID)
	}
}

func TestGetObject_WithMismatchedVersionIDReturnsNoSuchVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "get-mismatch-bucket")
	other := putTestObjectOutput(t, tb, "get-mismatch-bucket", "other.txt", "data")

	_, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String("get-mismatch-bucket"),
		Key:       aws.String("target.txt"),
		VersionId: aws.String(other.VersionID),
	})
	if err == nil {
		t.Fatal("expected NoSuchVersion")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrNoSuchVersion); apiErr.Code != want.Code {
		t.Fatalf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestGetObject_NotFound(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "nf-bucket")

	_, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("nf-bucket"),
		Key:    aws.String("no-such-key"),
	})
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrNoSuchKey)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestGetObject_SPFallback(t *testing.T) {
	// Use mock cache: first Get returns os.ErrNotExist (cache miss), Put succeeds
	// and stashes the data, second Get returns the rehydrated data.
	var rehydrated []byte
	mc := &synaps3testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			if rehydrated == nil {
				return nil, nil, os.ErrNotExist
			}
			return io.NopCloser(bytes.NewReader(rehydrated)), &cache.ObjectInfo{
				Path: "/fake/path", Size: int64(len(rehydrated)), ETag: "fakemd5", Checksum: "fakesha256",
			}, nil
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			data, _ := io.ReadAll(r)
			rehydrated = data
			return &cache.ObjectInfo{
				Path:     "/fake/path",
				Size:     int64(len(data)),
				ETag:     "fakemd5",
				Checksum: "fakesha256",
			}, nil
		},
		DeleteFunc:          func(_ context.Context, _, _ string) error { return nil },
		ExistsFunc:          func(_ context.Context, _, _ string) bool { return false },
		CreateBucketDirFunc: func(_ context.Context, _ string) error { return nil },
		DeleteUploadFunc:    func(_ context.Context, _ string) error { return nil },
	}
	tb := newTestBackendWithMockCache(t, mc)
	ctx := context.Background()
	bkt := seedActiveBucket(t, tb, "sp-bucket")

	// Build a valid CID for the PieceCID field.
	pieceCIDStr := buildDummyCID(t)

	retrievalURL := "https://provider.example/pieces/1"
	seedBackendObjectVersion(t, tb, bkt, "remote-file.txt", 5, "abc123", "sha256hex", "text/plain", model.ObjectStateCached, &pieceCIDStr, &retrievalURL)

	tb.storage.DownloadFunc = func(_ context.Context, _ cid.Cid, opts *storage.DownloadOptions) (io.ReadCloser, error) {
		if opts == nil || opts.URL != "https://provider.example/pieces/1" {
			return nil, fmt.Errorf("expected retrieval URL opts, got %#v", opts)
		}
		return io.NopCloser(bytes.NewReader([]byte("hello"))), nil
	}

	out, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("sp-bucket"),
		Key:    aws.String("remote-file.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject SP fallback: %v", err)
	}
	defer func() { _ = out.Body.Close() }()

	data, _ := io.ReadAll(out.Body)
	if string(data) != "hello" {
		t.Errorf("body = %q, want %q", string(data), "hello")
	}
}

func TestGetObject_SPFallback_CurrentVersionChangeDoesNotServeStaleData(t *testing.T) {
	var latestReady bool
	mc := &synaps3testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			if !latestReady {
				return nil, nil, os.ErrNotExist
			}
			data := []byte("new")
			return io.NopCloser(bytes.NewReader(data)), &cache.ObjectInfo{
				Path: "/fake/new", Size: int64(len(data)), ETag: "new", Checksum: "new",
			}, nil
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			data, _ := io.ReadAll(r)
			return &cache.ObjectInfo{Path: "/fake/path", Size: int64(len(data)), ETag: "fakemd5", Checksum: "fakesha256"}, nil
		},
		DeleteFunc:          func(_ context.Context, _, _ string) error { return nil },
		ExistsFunc:          func(_ context.Context, _, _ string) bool { return latestReady },
		CreateBucketDirFunc: func(_ context.Context, _ string) error { return nil },
		DeleteUploadFunc:    func(_ context.Context, _ string) error { return nil },
	}
	tb := newTestBackendWithMockCache(t, mc)
	ctx := context.Background()
	bkt := seedActiveBucket(t, tb, "sp-race-bucket")

	pieceCIDStr := buildDummyCID(t)
	oldRetrievalURL := "https://provider.example/pieces/old"
	seedBackendObjectVersion(t, tb, bkt, "remote-file.txt", 3, "abc123", "sha256hex", "text/plain", model.ObjectStateCached, &pieceCIDStr, &oldRetrievalURL)

	tb.storage.DownloadFunc = func(_ context.Context, _ cid.Cid, _ *storage.DownloadOptions) (io.ReadCloser, error) {
		latestReady = true
		seedBackendObjectVersion(t, tb, bkt, "remote-file.txt", 3, "new", "new", "text/plain", model.ObjectStateCached, nil, nil)
		return io.NopCloser(bytes.NewReader([]byte("old"))), nil
	}

	out, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("sp-race-bucket"),
		Key:    aws.String("remote-file.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() { _ = out.Body.Close() }()

	data, _ := io.ReadAll(out.Body)
	if string(data) != "new" {
		t.Fatalf("body = %q, want latest content", string(data))
	}
}

func TestGetObject_SPFallback_DownloadFailure(t *testing.T) {
	mc := &synaps3testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			data, _ := io.ReadAll(r)
			return &cache.ObjectInfo{Path: "/fake", Size: int64(len(data)), ETag: "e", Checksum: "c"}, nil
		},
		CreateBucketDirFunc: func(_ context.Context, _ string) error { return nil },
	}
	tb := newTestBackendWithMockCache(t, mc)
	ctx := context.Background()
	bkt := seedActiveBucket(t, tb, "sp-fail-bucket")

	pieceCIDStr := buildDummyCID(t)
	retrievalURL := "https://provider.example/pieces/fail"
	seedBackendObjectVersion(t, tb, bkt, "fail-dl.txt", 5, "e", "c", "text/plain", model.ObjectStateCached, &pieceCIDStr, &retrievalURL)

	tb.storage.DownloadFunc = func(_ context.Context, _ cid.Cid, _ *storage.DownloadOptions) (io.ReadCloser, error) {
		return nil, errors.New("provider unreachable")
	}

	missesBefore := counterValue(t, "synaps3_cache_misses_total")
	_, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("sp-fail-bucket"),
		Key:    aws.String("fail-dl.txt"),
	})
	if err == nil {
		t.Fatal("expected error on SP download failure")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrInternalError)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
	missesAfter := counterValue(t, "synaps3_cache_misses_total")
	if missesAfter != missesBefore+1 {
		t.Fatalf("cache misses = %v, want %v", missesAfter, missesBefore+1)
	}
}

func TestGetObject_NilStorage(t *testing.T) {
	mc := &synaps3testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, os.ErrNotExist
		},
		PutFunc: func(_ context.Context, _, _ string, r io.Reader) (*cache.ObjectInfo, error) {
			data, _ := io.ReadAll(r)
			return &cache.ObjectInfo{Path: "/fake", Size: int64(len(data)), ETag: "e", Checksum: "c"}, nil
		},
		CreateBucketDirFunc: func(_ context.Context, _ string) error { return nil },
	}
	// Use newTestBackendWithSDK with nil StorageClient.
	tb := newTestBackendWithSDK(t, nil)
	// But we need the mock cache. Rebuild the backend with mock cache and nil storage.
	_ = tb
	tb2 := newTestBackendWithMockCache(t, mc)
	ctx := context.Background()
	bkt := seedActiveBucket(t, tb2, "nil-sc-bucket")

	// Object exists in DB but no PieceCID and cache miss.
	seedBackendObjectVersion(t, tb2, bkt, "orphan.txt", 5, "e", "c", "text/plain", model.ObjectStateCached, nil, nil)

	_, err := tb2.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("nil-sc-bucket"),
		Key:    aws.String("orphan.txt"),
	})
	if err == nil {
		t.Fatal("expected error on cache miss with nil storage")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrNoSuchKey)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

// ---------- HeadObject ----------

func TestHeadObject_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "head-obj-bucket")
	putTestObject(t, tb, "head-obj-bucket", "meta.txt", "content")

	out, err := tb.backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("head-obj-bucket"),
		Key:    aws.String("meta.txt"),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if out.ContentType == nil || *out.ContentType != "text/plain" {
		t.Errorf("content-type = %v, want text/plain", out.ContentType)
	}
	if out.ContentLength == nil || *out.ContentLength != int64(len("content")) {
		t.Errorf("content-length = %v, want %d", out.ContentLength, len("content"))
	}
	if out.ETag == nil || *out.ETag == "" {
		t.Error("expected non-empty ETag")
	}
	if out.VersionId == nil || *out.VersionId == "" {
		t.Error("expected non-empty VersionId")
	}
}

func TestHeadObject_WithVersionIDReadsSpecifiedVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "head-version-bucket")

	firstOut := putTestObjectOutput(t, tb, "head-version-bucket", "meta.txt", "old")
	firstVersion := touchVersionLifecycle(t, tb, ctx, firstOut.VersionID)
	putTestObject(t, tb, "head-version-bucket", "meta.txt", "newer-body")

	out, err := tb.backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String("head-version-bucket"),
		Key:       aws.String("meta.txt"),
		VersionId: aws.String(firstOut.VersionID),
	})
	if err != nil {
		t.Fatalf("HeadObject(version): %v", err)
	}
	if out.ContentLength == nil || *out.ContentLength != int64(len("old")) {
		t.Fatalf("ContentLength = %v, want %d", out.ContentLength, len("old"))
	}
	if out.VersionId == nil || *out.VersionId != firstOut.VersionID {
		t.Fatalf("VersionId = %v, want %s", out.VersionId, firstOut.VersionID)
	}
	if out.LastModified == nil || !out.LastModified.Equal(firstVersion.CreatedAt) {
		t.Fatalf("LastModified = %v, want version CreatedAt %s", out.LastModified, firstVersion.CreatedAt)
	}
}

func TestHeadObject_NotFound(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "head-nf-bucket")

	_, err := tb.backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("head-nf-bucket"),
		Key:    aws.String("missing"),
	})
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrNoSuchKey)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestGetObjectAttributes_WithVersionID(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "attrs-version-bucket")

	firstOut := putTestObjectOutput(t, tb, "attrs-version-bucket", "attrs.txt", "old")
	firstVersion := touchVersionLifecycle(t, tb, ctx, firstOut.VersionID)
	putTestObject(t, tb, "attrs-version-bucket", "attrs.txt", "newer")

	out, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String("attrs-version-bucket"),
		Key:              aws.String("attrs.txt"),
		VersionId:        aws.String(firstOut.VersionID),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag, types.ObjectAttributesObjectSize, types.ObjectAttributesChecksum, types.ObjectAttributesStorageClass},
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes(version): %v", err)
	}
	if out.VersionId == nil || *out.VersionId != firstOut.VersionID {
		t.Fatalf("VersionId = %v, want %s", out.VersionId, firstOut.VersionID)
	}
	if out.ObjectSize == nil || *out.ObjectSize != int64(len("old")) {
		t.Fatalf("ObjectSize = %v, want %d", out.ObjectSize, len("old"))
	}
	if out.ETag == nil || *out.ETag != firstOut.ETag {
		t.Fatalf("ETag = %v, want %s", out.ETag, firstOut.ETag)
	}
	if out.LastModified == nil || !out.LastModified.Equal(firstVersion.CreatedAt) {
		t.Fatalf("LastModified = %v, want version CreatedAt %s", out.LastModified, firstVersion.CreatedAt)
	}
}

// ---------- ListObjects ----------

func TestListObjects_Pagination(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "list-bucket")

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("obj-%02d", i)
		putTestObject(t, tb, "list-bucket", key, "data")
	}

	maxKeys := int32(3)
	result, err := tb.backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:  aws.String("list-bucket"),
		MaxKeys: &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Contents) != 3 {
		t.Errorf("contents length = %d, want 3", len(result.Contents))
	}
	if result.IsTruncated == nil || !*result.IsTruncated {
		t.Error("expected IsTruncated=true")
	}
	if result.NextMarker != nil {
		t.Errorf("NextMarker = %q, want nil without delimiter", *result.NextMarker)
	}
}

func TestListObjects_DelimiterPaginationSkipsDuplicateCommonPrefix(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "list-delimiter-page-bucket")

	putTestObject(t, tb, "list-delimiter-page-bucket", "a/1.txt", "a1")
	putTestObject(t, tb, "list-delimiter-page-bucket", "a/2.txt", "a2")
	putTestObject(t, tb, "list-delimiter-page-bucket", "b/1.txt", "b1")

	maxKeys := int32(1)
	page1, err := tb.backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:    aws.String("list-delimiter-page-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjects page1: %v", err)
	}
	if len(page1.CommonPrefixes) != 1 || page1.CommonPrefixes[0].Prefix == nil || *page1.CommonPrefixes[0].Prefix != "a/" {
		t.Fatalf("page1 common prefixes = %#v, want a/", page1.CommonPrefixes)
	}
	if page1.NextMarker == nil || *page1.NextMarker == "" {
		t.Fatalf("page1 NextMarker = %v, want non-empty marker", page1.NextMarker)
	}

	page2, err := tb.backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:    aws.String("list-delimiter-page-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   &maxKeys,
		Marker:    aws.String("a/1.txt"),
	})
	if err != nil {
		t.Fatalf("ListObjects page2: %v", err)
	}
	if len(page2.CommonPrefixes) != 1 || page2.CommonPrefixes[0].Prefix == nil || *page2.CommonPrefixes[0].Prefix != "b/" {
		t.Fatalf("page2 common prefixes = %#v, want b/", page2.CommonPrefixes)
	}
}

func TestListObjects_MaxKeysZero(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "zero-bucket")
	putTestObject(t, tb, "zero-bucket", "file.txt", "data")

	maxKeys := int32(0)
	result, err := tb.backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:  aws.String("zero-bucket"),
		MaxKeys: &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Contents) != 0 {
		t.Errorf("contents length = %d, want 0", len(result.Contents))
	}
	if result.IsTruncated == nil || *result.IsTruncated {
		t.Error("expected IsTruncated=false for MaxKeys=0")
	}
}

func TestListObjects_Prefix(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "prefix-bucket")

	putTestObject(t, tb, "prefix-bucket", "photos/a.jpg", "a")
	putTestObject(t, tb, "prefix-bucket", "photos/b.jpg", "b")
	putTestObject(t, tb, "prefix-bucket", "docs/readme.md", "r")

	result, err := tb.backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket: aws.String("prefix-bucket"),
		Prefix: aws.String("photos/"),
	})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(result.Contents) != 2 {
		t.Errorf("contents length = %d, want 2 (photos/ prefix)", len(result.Contents))
	}
}

// ---------- ListObjectsV2 ----------

func TestListObjectsV2_ContinuationToken(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "v2-bucket")

	for i := 0; i < 5; i++ {
		putTestObject(t, tb, "v2-bucket", fmt.Sprintf("key-%02d", i), "data")
	}

	maxKeys := int32(2)
	r1, err := tb.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String("v2-bucket"),
		MaxKeys: &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page1: %v", err)
	}
	if len(r1.Contents) != 2 {
		t.Fatalf("page1 contents = %d, want 2", len(r1.Contents))
	}
	if r1.NextContinuationToken == nil {
		t.Fatal("expected NextContinuationToken for page1")
	}

	r2, err := tb.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String("v2-bucket"),
		MaxKeys:           &maxKeys,
		ContinuationToken: r1.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 page2: %v", err)
	}
	if len(r2.Contents) != 2 {
		t.Fatalf("page2 contents = %d, want 2", len(r2.Contents))
	}

	// Verify page2 starts after page1's last key.
	if *r2.Contents[0].Key <= *r1.Contents[1].Key {
		t.Errorf("page2 first key %q should be > page1 last key %q",
			*r2.Contents[0].Key, *r1.Contents[1].Key)
	}
}

func TestListObjectsV2_StartAfter(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "sa-bucket")

	putTestObject(t, tb, "sa-bucket", "aaa", "a")
	putTestObject(t, tb, "sa-bucket", "bbb", "b")
	putTestObject(t, tb, "sa-bucket", "ccc", "c")

	result, err := tb.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String("sa-bucket"),
		StartAfter: aws.String("aaa"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(result.Contents) != 2 {
		t.Errorf("contents = %d, want 2 (bbb, ccc)", len(result.Contents))
	}
	if len(result.Contents) > 0 && *result.Contents[0].Key != "bbb" {
		t.Errorf("first key = %q, want bbb", *result.Contents[0].Key)
	}
}

func TestListObjectsV2_DelimiterStartAfterSkipsCommonPrefix(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "v2-delimiter-start-after-bucket")

	putTestObject(t, tb, "v2-delimiter-start-after-bucket", "a/1.txt", "a1")
	putTestObject(t, tb, "v2-delimiter-start-after-bucket", "a/2.txt", "a2")
	putTestObject(t, tb, "v2-delimiter-start-after-bucket", "b/1.txt", "b1")

	maxKeys := int32(1)
	result, err := tb.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String("v2-delimiter-start-after-bucket"),
		Delimiter:  aws.String("/"),
		StartAfter: aws.String("a/1.txt"),
		MaxKeys:    &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(result.CommonPrefixes) != 1 || result.CommonPrefixes[0].Prefix == nil || *result.CommonPrefixes[0].Prefix != "b/" {
		t.Fatalf("common prefixes = %#v, want b/", result.CommonPrefixes)
	}
}

func TestListObjectVersions_PrefixDelimiterAndIsLatest(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "versions-list-bucket")

	first := putTestObjectOutput(t, tb, "versions-list-bucket", "photos/a.txt", "v1")
	second := putTestObjectOutput(t, tb, "versions-list-bucket", "photos/a.txt", "v2")
	putTestObject(t, tb, "versions-list-bucket", "photos/nested/b.txt", "nested")
	putTestObject(t, tb, "versions-list-bucket", "docs/readme.md", "docs")

	maxKeys := int32(10)
	result, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:    aws.String("versions-list-bucket"),
		Prefix:    aws.String("photos/"),
		Delimiter: aws.String("/"),
		MaxKeys:   &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(result.Versions) != 2 {
		t.Fatalf("versions len = %d, want 2", len(result.Versions))
	}
	if got := *result.Versions[0].VersionId; got != second.VersionID {
		t.Fatalf("first version = %s, want latest %s", got, second.VersionID)
	}
	if result.Versions[0].IsLatest == nil || !*result.Versions[0].IsLatest {
		t.Fatalf("latest version IsLatest = %v, want true", result.Versions[0].IsLatest)
	}
	if got := *result.Versions[1].VersionId; got != first.VersionID {
		t.Fatalf("second version = %s, want older %s", got, first.VersionID)
	}
	if result.Versions[1].IsLatest == nil || *result.Versions[1].IsLatest {
		t.Fatalf("older version IsLatest = %v, want false", result.Versions[1].IsLatest)
	}
	if len(result.CommonPrefixes) != 1 || result.CommonPrefixes[0].Prefix == nil || *result.CommonPrefixes[0].Prefix != "photos/nested/" {
		t.Fatalf("common prefixes = %#v, want photos/nested/", result.CommonPrefixes)
	}
	if len(result.DeleteMarkers) != 0 {
		t.Fatalf("delete markers = %#v, want empty", result.DeleteMarkers)
	}
}

func TestListObjectVersions_MarkerPagination(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "versions-marker-bucket")

	putTestObject(t, tb, "versions-marker-bucket", "a.txt", "v1")
	putTestObject(t, tb, "versions-marker-bucket", "a.txt", "v2")
	putTestObject(t, tb, "versions-marker-bucket", "b.txt", "v1")

	maxKeys := int32(1)
	page1, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:  aws.String("versions-marker-bucket"),
		MaxKeys: &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjectVersions page1: %v", err)
	}
	if len(page1.Versions) != 1 || page1.IsTruncated == nil || !*page1.IsTruncated {
		t.Fatalf("page1 versions=%d truncated=%v, want one truncated result", len(page1.Versions), page1.IsTruncated)
	}
	if page1.NextKeyMarker == nil || page1.NextVersionIdMarker == nil {
		t.Fatalf("page1 next markers missing: key=%v version=%v", page1.NextKeyMarker, page1.NextVersionIdMarker)
	}

	page2, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:          aws.String("versions-marker-bucket"),
		MaxKeys:         &maxKeys,
		KeyMarker:       page1.NextKeyMarker,
		VersionIdMarker: page1.NextVersionIdMarker,
	})
	if err != nil {
		t.Fatalf("ListObjectVersions page2: %v", err)
	}
	if len(page2.Versions) != 1 {
		t.Fatalf("page2 versions len = %d, want 1", len(page2.Versions))
	}
	if *page2.Versions[0].VersionId == *page1.Versions[0].VersionId {
		t.Fatalf("page2 repeated page1 version %s", *page2.Versions[0].VersionId)
	}
}

func TestListObjectVersions_DelimiterPaginationSkipsDuplicateCommonPrefix(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "versions-delimiter-page-bucket")

	putTestObject(t, tb, "versions-delimiter-page-bucket", "a/1.txt", "a1")
	putTestObject(t, tb, "versions-delimiter-page-bucket", "a/2.txt", "a2")
	putTestObject(t, tb, "versions-delimiter-page-bucket", "b/1.txt", "b1")

	maxKeys := int32(1)
	page1, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:    aws.String("versions-delimiter-page-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   &maxKeys,
	})
	if err != nil {
		t.Fatalf("ListObjectVersions page1: %v", err)
	}
	if len(page1.CommonPrefixes) != 1 || page1.CommonPrefixes[0].Prefix == nil || *page1.CommonPrefixes[0].Prefix != "a/" {
		t.Fatalf("page1 common prefixes = %#v, want a/", page1.CommonPrefixes)
	}
	if page1.NextKeyMarker == nil || page1.NextVersionIdMarker == nil {
		t.Fatalf("page1 next markers missing: key=%v version=%v", page1.NextKeyMarker, page1.NextVersionIdMarker)
	}

	page2, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket:    aws.String("versions-delimiter-page-bucket"),
		Delimiter: aws.String("/"),
		MaxKeys:   &maxKeys,
		KeyMarker: aws.String("a/1.txt"),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions page2: %v", err)
	}
	if len(page2.CommonPrefixes) != 1 || page2.CommonPrefixes[0].Prefix == nil || *page2.CommonPrefixes[0].Prefix != "b/" {
		t.Fatalf("page2 common prefixes = %#v, want b/", page2.CommonPrefixes)
	}
}

// ---------- CopyObject ----------

func TestCopyObject_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "src-bucket")
	seedActiveBucket(t, tb, "dst-bucket")
	putTestObject(t, tb, "src-bucket", "original.txt", "copy me")

	out, err := tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:     aws.String("dst-bucket"),
		Key:        aws.String("copied.txt"),
		CopySource: aws.String("/src-bucket/original.txt"),
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if out.CopyObjectResult == nil || out.CopyObjectResult.ETag == nil {
		t.Error("expected ETag in copy result")
	}
	if out.VersionId == nil || *out.VersionId == "" {
		t.Error("expected destination VersionId")
	}

	// Verify destination object in DB.
	dstBkt, _ := tb.repos.Buckets.GetByName(ctx, "dst-bucket")
	dstObj, _ := tb.repos.Objects.GetByBucketAndKey(ctx, dstBkt.ID, "copied.txt")
	if dstObj == nil {
		t.Fatal("destination object not found in DB")
	}
	if dstObj.State != model.ObjectStateCached {
		t.Errorf("dst state = %q, want %q", dstObj.State, model.ObjectStateCached)
	}
}

func TestCopyObjectIdenticalCurrentObjectCreatesNewVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "copy-dedupe-src")
	seedActiveBucket(t, tb, "copy-dedupe-dst")
	putTestObject(t, tb, "copy-dedupe-src", "original.txt", "copy me")

	copyInput := s3response.CopyObjectInput{
		Bucket:     aws.String("copy-dedupe-dst"),
		Key:        aws.String("copied.txt"),
		CopySource: aws.String("/copy-dedupe-src/original.txt"),
	}
	if _, err := tb.backend.CopyObject(ctx, copyInput); err != nil {
		t.Fatalf("first CopyObject: %v", err)
	}

	dstBkt, _ := tb.repos.Buckets.GetByName(ctx, "copy-dedupe-dst")
	obj1, err := tb.repos.Objects.GetByBucketAndKey(ctx, dstBkt.ID, "copied.txt")
	if err != nil || obj1 == nil {
		t.Fatalf("current object after first copy: obj=%v err=%v", obj1, err)
	}

	if _, err := tb.backend.CopyObject(ctx, copyInput); err != nil {
		t.Fatalf("second CopyObject: %v", err)
	}

	obj2, err := tb.repos.Objects.GetByBucketAndKey(ctx, dstBkt.ID, "copied.txt")
	if err != nil || obj2 == nil {
		t.Fatalf("current object after second copy: obj=%v err=%v", obj2, err)
	}
	if obj2.CurrentVersionID == obj1.CurrentVersionID {
		t.Fatalf("current version did not change for identical copy: %s", obj2.CurrentVersionID)
	}

	versionCount, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", obj1.ID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting object versions: %v", err)
	}
	if versionCount != 2 {
		t.Fatalf("object version count = %d, want 2", versionCount)
	}

	taskCount, err := tb.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("ref_type = ? AND ref_id = ?", "object", obj1.ID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting upload tasks: %v", err)
	}
	if taskCount != 2 {
		t.Fatalf("task count = %d, want 2", taskCount)
	}
}

func TestCopyObjectUsesConfiguredUploadMaxRetries(t *testing.T) {
	tb := newTestBackendWithOptions(t, synaps3backend.WithUploadMaxRetries(13))
	ctx := context.Background()
	seedActiveBucket(t, tb, "copy-retry-src")
	seedActiveBucket(t, tb, "copy-retry-dst")
	putTestObject(t, tb, "copy-retry-src", "original.txt", "copy me")

	_, err := tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:     aws.String("copy-retry-dst"),
		Key:        aws.String("copied.txt"),
		CopySource: aws.String("/copy-retry-src/original.txt"),
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}

	dstBkt, err := tb.repos.Buckets.GetByName(ctx, "copy-retry-dst")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	dstObj, err := tb.repos.Objects.GetByBucketAndKey(ctx, dstBkt.ID, "copied.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}

	tasks, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), string(model.TaskStatusPending), 10, 0)
	if err != nil {
		t.Fatalf("List tasks: %v", err)
	}
	for _, task := range tasks {
		if task.RefType == "object" && task.RefID == dstObj.ID {
			if task.MaxRetries != 13 {
				t.Fatalf("copy upload task MaxRetries = %d, want 13", task.MaxRetries)
			}
			return
		}
	}
	t.Fatalf("copy upload task for object %d not found in %#v", dstObj.ID, tasks)
}

func TestCopyObject_MetadataReplace(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "mr-src")
	seedActiveBucket(t, tb, "mr-dst")
	putTestObject(t, tb, "mr-src", "file.txt", "data")

	newCT := "application/json"
	out, err := tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:            aws.String("mr-dst"),
		Key:               aws.String("file.json"),
		CopySource:        aws.String("mr-src/file.txt"),
		MetadataDirective: types.MetadataDirectiveReplace,
		ContentType:       &newCT,
		Metadata:          map[string]string{"custom": "value"},
	})
	if err != nil {
		t.Fatalf("CopyObject replace: %v", err)
	}
	if out.CopyObjectResult == nil {
		t.Fatal("expected copy result")
	}

	dstBkt, _ := tb.repos.Buckets.GetByName(ctx, "mr-dst")
	dstObj, _ := tb.repos.Objects.GetByBucketAndKey(ctx, dstBkt.ID, "file.json")
	if dstObj == nil {
		t.Fatal("destination object not found")
	}
	if dstObj.ContentType != "application/json" {
		t.Errorf("content-type = %q, want application/json", dstObj.ContentType)
	}
	if dstObj.Metadata["custom"] != "value" {
		t.Errorf("metadata[custom] = %q, want value", dstObj.Metadata["custom"])
	}
}

func TestCopyObject_SameBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "same-bucket")
	putTestObject(t, tb, "same-bucket", "src.txt", "data")

	_, err := tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:     aws.String("same-bucket"),
		Key:        aws.String("dst.txt"),
		CopySource: aws.String("same-bucket/src.txt"),
	})
	if err != nil {
		t.Fatalf("CopyObject same bucket: %v", err)
	}

	// Verify both objects exist.
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "same-bucket")
	src, _ := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "src.txt")
	dst, _ := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "dst.txt")
	if src == nil || dst == nil {
		t.Error("both source and destination objects should exist")
	}
}

func TestCopyObject_CopySourceVersionIDCopiesSpecifiedVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "copy-version-src")
	seedActiveBucket(t, tb, "copy-version-dst")

	firstOut := putTestObjectOutput(t, tb, "copy-version-src", "original.txt", "old")
	putTestObject(t, tb, "copy-version-src", "original.txt", "new")

	out, err := tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:     aws.String("copy-version-dst"),
		Key:        aws.String("copied.txt"),
		CopySource: aws.String("/copy-version-src/original.txt?versionId=" + firstOut.VersionID),
	})
	if err != nil {
		t.Fatalf("CopyObject(version): %v", err)
	}
	if out.CopySourceVersionId == nil || *out.CopySourceVersionId != firstOut.VersionID {
		t.Fatalf("CopySourceVersionId = %v, want %s", out.CopySourceVersionId, firstOut.VersionID)
	}

	got, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("copy-version-dst"),
		Key:    aws.String("copied.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject copied: %v", err)
	}
	defer func() { _ = got.Body.Close() }()
	data, _ := io.ReadAll(got.Body)
	if string(data) != "old" {
		t.Fatalf("copied body = %q, want old source version", string(data))
	}
}

func TestCopyObjectBindsImplicitCurrentReadToResolvedVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	srcBkt := seedActiveBucket(t, tb, "copy-implicit-race-src")
	seedActiveBucket(t, tb, "copy-implicit-race-dst")

	firstOut := putTestObjectOutput(t, tb, "copy-implicit-race-src", "original.txt", "old")
	baseObjects := tb.repos.Objects
	var hookErr error
	tb.repos.Objects = &getByBucketAndKeyAfterReadRepo{
		ObjectRepository: baseObjects,
		afterFirstRead: func() {
			newVersionID := model.NewVersionID()
			cacheKey := path.Join(".versions", newVersionID)
			info, err := tb.cache.Put(ctx, "copy-implicit-race-src", cacheKey, strings.NewReader("new"))
			if err != nil {
				hookErr = err
				return
			}
			_, hookErr = baseObjects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
				VersionID:   newVersionID,
				BucketID:    srcBkt.ID,
				Key:         "original.txt",
				Size:        info.Size,
				ETag:        info.ETag,
				Checksum:    info.Checksum,
				ContentType: "text/plain",
				CacheKey:    cacheKey,
				State:       model.ObjectStateCached,
			})
		},
	}

	out, err := tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:     aws.String("copy-implicit-race-dst"),
		Key:        aws.String("copied.txt"),
		CopySource: aws.String("/copy-implicit-race-src/original.txt"),
	})
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if hookErr != nil {
		t.Fatalf("source overwrite hook: %v", hookErr)
	}
	if out.CopySourceVersionId == nil || *out.CopySourceVersionId != firstOut.VersionID {
		t.Fatalf("CopySourceVersionId = %v, want %s", out.CopySourceVersionId, firstOut.VersionID)
	}

	got, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("copy-implicit-race-dst"),
		Key:    aws.String("copied.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject copied: %v", err)
	}
	defer func() { _ = got.Body.Close() }()
	data, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("read copied body: %v", err)
	}
	if string(data) != "old" {
		t.Fatalf("copied body = %q, want resolved source version body", string(data))
	}
}

func TestParseCopySource_Formats(t *testing.T) {
	// parseCopySource is unexported. We test it indirectly through CopyObject
	// by providing different source formats.
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "fmt-bucket")
	putTestObject(t, tb, "fmt-bucket", "key with spaces.txt", "data")

	tests := []struct {
		name   string
		source string
	}{
		{"slash-prefix", "/fmt-bucket/key with spaces.txt"},
		{"no-slash", "fmt-bucket/key with spaces.txt"},
		{"url-encoded", "/fmt-bucket/key%20with%20spaces.txt"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seedActiveBucket(t, tb, "fmt-dst-"+tc.name)
			_, err := tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
				Bucket:     aws.String("fmt-dst-" + tc.name),
				Key:        aws.String("dest.txt"),
				CopySource: aws.String(tc.source),
			})
			if err != nil {
				t.Fatalf("CopyObject with source %q: %v", tc.source, err)
			}
		})
	}
}

// ---------- helpers for CID construction ----------

func buildDummyCID(t *testing.T) string {
	t.Helper()
	data := []byte("dummy-piece-data")
	hash, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("building multihash: %v", err)
	}
	c := cid.NewCidV1(cid.Raw, hash)
	return c.String()
}
