package backend_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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
	return out.ETag
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

func TestPutObjectIdenticalCurrentObjectDoesNotCreateVersionOrTask(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "dedupe-bucket")

	firstETag := putTestObject(t, tb, "dedupe-bucket", "file.txt", "same data")

	bkt, _ := tb.repos.Buckets.GetByName(ctx, "dedupe-bucket")
	obj1, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || obj1 == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", obj1, err)
	}

	secondETag := putTestObject(t, tb, "dedupe-bucket", "file.txt", "same data")

	obj2, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || obj2 == nil {
		t.Fatalf("current object after second put: obj=%v err=%v", obj2, err)
	}
	if secondETag != firstETag {
		t.Fatalf("second ETag = %s, want %s", secondETag, firstETag)
	}
	if obj2.CurrentVersionID != obj1.CurrentVersionID {
		t.Fatalf("current version changed for identical put: got %s want %s", obj2.CurrentVersionID, obj1.CurrentVersionID)
	}

	versionCount, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", obj1.ID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting object versions: %v", err)
	}
	if versionCount != 1 {
		t.Fatalf("object version count = %d, want 1", versionCount)
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
	if result.NextMarker == nil || *result.NextMarker == "" {
		t.Error("expected non-empty NextMarker")
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

func TestCopyObjectIdenticalCurrentObjectDoesNotCreateVersionOrTask(t *testing.T) {
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
	if obj2.CurrentVersionID != obj1.CurrentVersionID {
		t.Fatalf("current version changed for identical copy: got %s want %s", obj2.CurrentVersionID, obj1.CurrentVersionID)
	}

	versionCount, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", obj1.ID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting object versions: %v", err)
	}
	if versionCount != 1 {
		t.Fatalf("object version count = %d, want 1", versionCount)
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
