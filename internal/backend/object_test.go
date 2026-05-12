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
	"github.com/strahe/synapse-go/chain"
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

// putTestObject seeds an exact cached object and returns the ETag.
func putTestObject(t *testing.T, tb *testBackend, bucket, key, body string) string {
	t.Helper()
	return putTestObjectOutput(t, tb, bucket, key, body).ETag
}

func putTestObjectOutput(t *testing.T, tb *testBackend, bucket, key, body string) s3response.PutObjectOutput {
	t.Helper()
	ctx := context.Background()
	bkt, err := tb.repos.Buckets.GetByName(ctx, bucket)
	if err != nil || bkt == nil {
		t.Fatalf("getting seeded bucket %q: bucket=%v err=%v", bucket, bkt, err)
	}
	versionID := model.NewVersionID()
	cacheKey := path.Join(".versions", versionID)
	info, err := tb.cache.Put(ctx, bucket, cacheKey, strings.NewReader(body))
	if err != nil {
		t.Fatalf("seeding cache object %s/%s: %v", bucket, key, err)
	}
	if _, err := tb.repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bkt.ID,
		Key:         key,
		Size:        info.Size,
		ETag:        info.ETag,
		Checksum:    info.Checksum,
		ContentType: "text/plain",
		CacheKey:    cacheKey,
		State:       model.ObjectStateCached,
	}); err != nil {
		t.Fatalf("seeding object version %s/%s: %v", bucket, key, err)
	}
	etag := fmt.Sprintf(`"%s"`, info.ETag)
	return s3response.PutObjectOutput{ETag: etag, VersionID: versionID, Size: &info.Size}
}

func putValidTestObject(t *testing.T, tb *testBackend, bucket, key, body string) string {
	t.Helper()
	return putValidTestObjectOutput(t, tb, bucket, key, body).ETag
}

// putValidTestObjectOutput uses the production PutObject path with FOC-valid content.
func putValidTestObjectOutput(t *testing.T, tb *testBackend, bucket, key, body string) s3response.PutObjectOutput {
	t.Helper()
	ctx := context.Background()
	ct := "text/plain"
	validBody := validTestObjectBody(body)
	out, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        strings.NewReader(validBody),
		ContentType: &ct,
	})
	if err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
	return out
}

func ptrInt64(v int64) *int64 {
	return &v
}

func assertS3ErrorCode(t *testing.T, err error, wantCode s3err.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatal("expected APIError")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if want := s3err.GetAPIError(wantCode); apiErr.Code != want.Code {
		t.Fatalf("error code = %s, want %s", apiErr.Code, want.Code)
	}
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

type getCurrentVersionByBucketAndKeyAfterReadRepo struct {
	repository.ObjectRepository
	calls          int
	afterFirstRead func()
}

func (r *getCurrentVersionByBucketAndKeyAfterReadRepo) GetCurrentVersionByBucketAndKey(ctx context.Context, bucketID int64, key string) (*model.ObjectVersion, error) {
	r.calls++
	version, err := r.ObjectRepository.GetCurrentVersionByBucketAndKey(ctx, bucketID, key)
	if r.calls == 1 && r.afterFirstRead != nil {
		r.afterFirstRead()
		r.afterFirstRead = nil
	}
	return version, err
}

func seedBackendObjectVersion(t *testing.T, tb *testBackend, bucket *model.Bucket, key string, size int64, etag, checksum, contentType string, state model.ObjectState, pieceCID, retrievalURL *string) (int64, string) {
	t.Helper()
	versionID := model.NewVersionID()
	createState := state
	if state == model.ObjectStateStored || state == model.ObjectStateCacheEvicted {
		createState = model.ObjectStateUploading
	}
	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         key,
		Size:        size,
		ETag:        etag,
		Checksum:    checksum,
		ContentType: contentType,
		CacheKey:    ".versions/" + versionID,
		State:       createState,
	}
	objID, err := tb.repos.Objects.CreateVersionAndSetCurrent(context.Background(), version)
	if err != nil {
		t.Fatalf("seeding object version: %v", err)
	}
	if (state == model.ObjectStateStored || state == model.ObjectStateCacheEvicted) && pieceCID != nil && retrievalURL != nil {
		acceptBackendVersionUpload(t, tb.repos, versionID, *pieceCID, *retrievalURL)
		if state == model.ObjectStateCacheEvicted {
			if err := tb.repos.Objects.UpdateVersionState(context.Background(), versionID, model.ObjectStateStored, model.ObjectStateCacheEvicted); err != nil {
				t.Fatalf("restore seeded cache evicted version: %v", err)
			}
		}
	}
	return objID, versionID
}

func acceptBackendVersionUpload(t *testing.T, repos *repository.Repositories, versionID string, pieceCID string, retrievalURL string) *model.StorageUpload {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for upload accept: version=%v err=%v", version, err)
	}
	if version.State == model.ObjectStateUploading {
		if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
			t.Fatalf("mark committing: %v", err)
		}
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	providerID := onChainID(t, "101")
	binding, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          version.BucketID,
		ProviderID:        providerID,
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("ensure dataset binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("mark dataset ready: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       providerID,
	}}); err != nil {
		t.Fatalf("create upload copy: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      onChainIDPtr(t, "1"),
		RetrievalURL: retrievalURL,
	}); err != nil {
		t.Fatalf("mark copy committed: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("bind readable upload: %v", err)
	}
	if finalized, _, err := repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: upload.ID}); err != nil {
		t.Fatalf("finalize upload: %v", err)
	} else if !finalized {
		t.Fatal("finalize upload = false, want true")
	}
	return upload
}

func bindBackendPrimaryCommittedUpload(t *testing.T, repos *repository.Repositories, versionID string, pieceCID string, retrievalURL string) *model.StorageUpload {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for primary bind: version=%v err=%v", version, err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark uploading: %v", err)
	}
	if err := repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("mark committing: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        version.BucketID,
		SourceVersionID: version.VersionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
	})
	if err != nil {
		t.Fatalf("start upload attempt: %v", err)
	}
	primary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          version.BucketID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("ensure primary dataset binding: %v", err)
	}
	if err := repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        primary.ID,
		UploadID:  upload.ID,
		DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("mark primary dataset ready: %v", err)
	}
	secondary, err := repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          version.BucketID,
		ProviderID:        onChainID(t, "202"),
		CopyIndex:         1,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("ensure secondary dataset binding: %v", err)
	}
	if err := repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("create upload copy rows: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		RetrievalURL: retrievalURL,
	}); err != nil {
		t.Fatalf("mark primary piece ready: %v", err)
	}
	if err := repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: retrievalURL,
	}); err != nil {
		t.Fatalf("mark primary committed: %v", err)
	}
	if _, err := repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    version.BucketID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("bind primary committed upload: %v", err)
	}
	return upload
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

	body := validTestObjectBody("hello world")
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
	obj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "greeting.txt")
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
	if obj.VersionID == "" {
		t.Fatal("expected current version id")
	}

	// Verify cache file exists.
	if !tb.cache.Exists(ctx, "put-bucket", obj.CacheKey) {
		t.Error("cache file does not exist")
	}
}

func TestPutObjectRejectsFOCUploadSizeLimits(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		contentLength *int64
		wantCode      s3err.ErrorCode
	}{
		{
			name:     "below minimum",
			body:     strings.Repeat("a", chain.MinUploadSize-1),
			wantCode: s3err.ErrEntityTooSmall,
		},
		{
			name:          "known length above maximum",
			body:          "short body",
			contentLength: ptrInt64(chain.MaxUploadSize + 1),
			wantCode:      s3err.ErrEntityTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := newTestBackend(t)
			ctx := context.Background()
			seedActiveBucket(t, tb, "put-size-limit-bucket")

			_, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
				Bucket:        aws.String("put-size-limit-bucket"),
				Key:           aws.String("file.txt"),
				Body:          strings.NewReader(tt.body),
				ContentLength: tt.contentLength,
			})
			if err == nil {
				t.Fatal("expected size limit error")
			}
			apiErr, ok := err.(s3err.APIError)
			if !ok {
				t.Fatalf("expected APIError, got %T: %v", err, err)
			}
			if want := s3err.GetAPIError(tt.wantCode); apiErr.Code != want.Code {
				t.Fatalf("error code = %s, want %s", apiErr.Code, want.Code)
			}
		})
	}
}

func TestPutObjectUsesConfiguredUploadMaxRetries(t *testing.T) {
	tb := newTestBackendWithOptions(t, synaps3backend.WithUploadMaxRetries(11))
	ctx := context.Background()
	seedActiveBucket(t, tb, "put-retries-bucket")

	_, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket: aws.String("put-retries-bucket"),
		Key:    aws.String("file.txt"),
		Body:   strings.NewReader(validTestObjectBody("data")),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	task, err := tb.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimReady: %v", err)
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
		Body:   strings.NewReader(validTestObjectBody("data")),
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

	putValidTestObject(t, tb, "ow-bucket", "file.txt", "version-1")

	bkt, _ := tb.repos.Buckets.GetByName(ctx, "ow-bucket")
	obj1, _ := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if obj1.VersionID == "" {
		t.Fatal("first current version id is empty")
	}
	firstVersionID := obj1.VersionID

	putValidTestObject(t, tb, "ow-bucket", "file.txt", "version-2")

	obj2, _ := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if obj2.VersionID == "" || obj2.VersionID == firstVersionID {
		t.Fatalf("second current version id = %q, first = %q", obj2.VersionID, firstVersionID)
	}
	firstVersion, err := tb.repos.Objects.GetVersionByID(ctx, firstVersionID)
	if err != nil || firstVersion == nil {
		t.Fatalf("first version missing: version=%v err=%v", firstVersion, err)
	}
	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, obj2.VersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("second version missing: version=%v err=%v", secondVersion, err)
	}
	if firstVersion.ObjectID != obj2.ObjectID || secondVersion.ObjectID != obj2.ObjectID {
		t.Fatalf("versions should reference object %d, got %d/%d", obj2.ObjectID, firstVersion.ObjectID, secondVersion.ObjectID)
	}
}

func TestPutObjectIdenticalCurrentObjectCreatesNewVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "dedupe-bucket")

	firstOut := putValidTestObjectOutput(t, tb, "dedupe-bucket", "file.txt", "same data")

	bkt, _ := tb.repos.Buckets.GetByName(ctx, "dedupe-bucket")
	obj1, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || obj1 == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", obj1, err)
	}

	secondOut := putValidTestObjectOutput(t, tb, "dedupe-bucket", "file.txt", "same data")

	obj2, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || obj2 == nil {
		t.Fatalf("current object after second put: obj=%v err=%v", obj2, err)
	}
	if secondOut.ETag != firstOut.ETag {
		t.Fatalf("second ETag = %s, want %s", secondOut.ETag, firstOut.ETag)
	}
	if secondOut.VersionID == "" || secondOut.VersionID == firstOut.VersionID {
		t.Fatalf("second VersionID = %q, first = %q", secondOut.VersionID, firstOut.VersionID)
	}
	if obj2.VersionID == obj1.VersionID {
		t.Fatalf("current version did not change for identical put: %s", obj2.VersionID)
	}

	versionCount, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", obj1.ObjectID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting object versions: %v", err)
	}
	if versionCount != 2 {
		t.Fatalf("object version count = %d, want 2", versionCount)
	}

	taskCount, err := tb.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("ref_type = ? AND ref_id = ?", "object", obj1.ObjectID).
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

	firstOut := putValidTestObjectOutput(t, tb, "uploading-reuse-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "uploading-reuse-bucket")
	firstObj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}

	claimed, err := tb.repos.Tasks.ClaimReady(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("claim upload task: %v", err)
	}
	if claimed == nil || claimed.RefVersionID != firstOut.VersionID {
		t.Fatalf("claimed task = %#v, want version %s", claimed, firstOut.VersionID)
	}
	if err := tb.repos.Objects.UpdateVersionState(ctx, firstOut.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark first version uploading: %v", err)
	}

	tasksBefore, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", "", 10, 0)
	if err != nil {
		t.Fatalf("list tasks before second put: %v", err)
	}

	secondOut := putValidTestObjectOutput(t, tb, "uploading-reuse-bucket", "file.txt", "same data")
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

	tasksAfter, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", "", 10, 0)
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

	firstOut := putValidTestObjectOutput(t, tb, "stored-reuse-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "stored-reuse-bucket")
	firstObj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}
	if err := tb.repos.Objects.UpdateVersionState(ctx, firstObj.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark first version uploading: %v", err)
	}
	acceptBackendVersionUpload(t, tb.repos, firstObj.VersionID, "piece-shared", "https://provider.example/shared")

	tasksBefore, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", "", 10, 0)
	if err != nil {
		t.Fatalf("list tasks before second put: %v", err)
	}
	secondOut := putValidTestObjectOutput(t, tb, "stored-reuse-bucket", "file.txt", "same data")
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
	if secondVersion.StorageUploadID == nil {
		t.Fatal("second version storage_upload_id is nil, want reused upload")
	}

	tasksAfter, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", "", 10, 0)
	if err != nil {
		t.Fatalf("list tasks after second put: %v", err)
	}
	if len(tasksAfter) != len(tasksBefore) {
		t.Fatalf("upload task count changed from %d to %d", len(tasksBefore), len(tasksAfter))
	}
}

func TestPutObjectIdenticalReplicatingContentReusesPrimaryCommittedUpload(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "replicating-reuse-bucket")

	firstOut := putValidTestObjectOutput(t, tb, "replicating-reuse-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "replicating-reuse-bucket")
	firstObj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}
	upload := bindBackendPrimaryCommittedUpload(t, tb.repos, firstObj.VersionID, buildDummyCID(t), "https://provider.example/primary")

	tasksBefore, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", "", 10, 0)
	if err != nil {
		t.Fatalf("list tasks before second put: %v", err)
	}
	secondOut := putValidTestObjectOutput(t, tb, "replicating-reuse-bucket", "file.txt", "same data")
	if secondOut.VersionID == "" || secondOut.VersionID == firstOut.VersionID {
		t.Fatalf("second VersionID = %q, first = %q", secondOut.VersionID, firstOut.VersionID)
	}

	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, secondOut.VersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("second version: version=%v err=%v", secondVersion, err)
	}
	if secondVersion.State != model.ObjectStateReplicating {
		t.Fatalf("second version state = %s, want replicating", secondVersion.State)
	}
	if secondVersion.StorageUploadID == nil || *secondVersion.StorageUploadID != upload.ID || !secondVersion.InFilecoin {
		t.Fatalf("second version storage = upload:%v in_filecoin:%v, want upload %d readable", secondVersion.StorageUploadID, secondVersion.InFilecoin, upload.ID)
	}

	tasksAfter, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", "", 10, 0)
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

	putValidTestObject(t, tb, "stored-reuse-evict-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "stored-reuse-evict-bucket")
	firstObj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}
	if err := tb.repos.Objects.UpdateVersionState(ctx, firstObj.VersionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("mark first version uploading: %v", err)
	}
	acceptBackendVersionUpload(t, tb.repos, firstObj.VersionID, "piece-shared", "https://provider.example/shared")

	secondOut := putValidTestObjectOutput(t, tb, "stored-reuse-evict-bucket", "file.txt", "same data")
	task, err := tb.repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, time.Minute)
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
	seedBackendObjectVersion(t, tb, bkt, "remote-file.txt", 5, "abc123", "sha256hex", "text/plain", model.ObjectStateStored, &pieceCIDStr, &retrievalURL)

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
	seedBackendObjectVersion(t, tb, bkt, "remote-file.txt", 3, "abc123", "sha256hex", "text/plain", model.ObjectStateStored, &pieceCIDStr, &oldRetrievalURL)

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
	seedBackendObjectVersion(t, tb, bkt, "fail-dl.txt", 5, "e", "c", "text/plain", model.ObjectStateStored, &pieceCIDStr, &retrievalURL)

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

func TestHeadObject_DeleteMarkerCurrentAndVersionID(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "head-delete-marker-bucket")

	putTestObject(t, tb, "head-delete-marker-bucket", "file.txt", "data")
	marker, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("head-delete-marker-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if marker.VersionId == nil || *marker.VersionId == "" {
		t.Fatalf("DeleteObject marker = %#v, want version id", marker)
	}

	_, err = tb.backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("head-delete-marker-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err == nil {
		t.Fatal("HeadObject current delete marker returned nil error")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("HeadObject current delete marker error = %T %v, want APIError", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrNoSuchKey); apiErr.Code != want.Code {
		t.Fatalf("HeadObject current delete marker code = %q, want %q", apiErr.Code, want.Code)
	}

	_, err = tb.backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String("head-delete-marker-bucket"),
		Key:       aws.String("file.txt"),
		VersionId: marker.VersionId,
	})
	if err == nil {
		t.Fatal("HeadObject delete marker version returned nil error")
	}
	apiErr, ok = err.(s3err.APIError)
	if !ok {
		t.Fatalf("HeadObject delete marker version error = %T %v, want APIError", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrMethodNotAllowed); apiErr.Code != want.Code {
		t.Fatalf("HeadObject delete marker version code = %q, want %q", apiErr.Code, want.Code)
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

// ---------- DeleteObject ----------

func TestDeleteObject_CreatesDeleteMarkerAndHidesCurrentObject(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-marker-bucket")

	putOut := putTestObjectOutput(t, tb, "delete-marker-bucket", "file.txt", "old")
	delOut, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("delete-marker-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if delOut.DeleteMarker == nil || !*delOut.DeleteMarker || delOut.VersionId == nil || *delOut.VersionId == "" {
		t.Fatalf("delete output = %#v, want delete marker and version id", delOut)
	}
	if *delOut.VersionId == putOut.VersionID {
		t.Fatalf("delete marker version id reused data version id %s", putOut.VersionID)
	}

	_, err = tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("delete-marker-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err == nil {
		t.Fatal("GetObject returned nil error for delete-marker current object")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("GetObject error = %T %v, want APIError", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrNoSuchKey); apiErr.Code != want.Code {
		t.Fatalf("GetObject code = %q, want %q", apiErr.Code, want.Code)
	}

	listOut, err := tb.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("delete-marker-bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(listOut.Contents) != 0 {
		t.Fatalf("ListObjectsV2 contents = %#v, want hidden object", listOut.Contents)
	}

	versionsOut, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String("delete-marker-bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(versionsOut.Versions) != 1 || *versionsOut.Versions[0].VersionId != putOut.VersionID {
		t.Fatalf("versions = %#v, want data version %s", versionsOut.Versions, putOut.VersionID)
	}
	if len(versionsOut.DeleteMarkers) != 1 || versionsOut.DeleteMarkers[0].VersionId == nil || *versionsOut.DeleteMarkers[0].VersionId != *delOut.VersionId {
		t.Fatalf("delete markers = %#v, want marker %s", versionsOut.DeleteMarkers, *delOut.VersionId)
	}
	if versionsOut.DeleteMarkers[0].IsLatest == nil || !*versionsOut.DeleteMarkers[0].IsLatest {
		t.Fatalf("delete marker IsLatest = %v, want true", versionsOut.DeleteMarkers[0].IsLatest)
	}

	versioned, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String("delete-marker-bucket"),
		Key:       aws.String("file.txt"),
		VersionId: aws.String(putOut.VersionID),
	})
	if err != nil {
		t.Fatalf("GetObject(data version): %v", err)
	}
	defer func() { _ = versioned.Body.Close() }()
	body, _ := io.ReadAll(versioned.Body)
	if string(body) != "old" {
		t.Fatalf("versioned body = %q, want old", string(body))
	}

	_, err = tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String("delete-marker-bucket"),
		Key:       aws.String("file.txt"),
		VersionId: delOut.VersionId,
	})
	if err == nil {
		t.Fatal("GetObject(delete marker version) returned nil error")
	}
	apiErr, ok = err.(s3err.APIError)
	if !ok {
		t.Fatalf("GetObject(delete marker) error = %T %v, want APIError", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrMethodNotAllowed); apiErr.Code != want.Code {
		t.Fatalf("GetObject(delete marker) code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestDeleteObject_DeleteMarkerVersionRestoresObject(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-marker-restore-bucket")

	putTestObject(t, tb, "delete-marker-restore-bucket", "file.txt", "restored")
	marker, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("delete-marker-restore-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject marker: %v", err)
	}

	out, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String("delete-marker-restore-bucket"),
		Key:       aws.String("file.txt"),
		VersionId: marker.VersionId,
	})
	if err != nil {
		t.Fatalf("DeleteObject(marker version): %v", err)
	}
	if out.DeleteMarker == nil || !*out.DeleteMarker || out.VersionId == nil || *out.VersionId != *marker.VersionId {
		t.Fatalf("delete marker version output = %#v, want marker version %s", out, *marker.VersionId)
	}

	got, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("delete-marker-restore-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject restored: %v", err)
	}
	defer func() { _ = got.Body.Close() }()
	body, _ := io.ReadAll(got.Body)
	if string(body) != "restored" {
		t.Fatalf("restored body = %q, want restored", string(body))
	}
}

func TestDeleteObject_MissingKeyCreatesDeleteMarker(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-missing-bucket")

	out, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("delete-missing-bucket"),
		Key:    aws.String("missing.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject missing key: %v", err)
	}
	if out.DeleteMarker == nil || !*out.DeleteMarker || out.VersionId == nil || *out.VersionId == "" {
		t.Fatalf("delete missing output = %#v, want delete marker", out)
	}

	versionsOut, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String("delete-missing-bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(versionsOut.Versions) != 0 || len(versionsOut.DeleteMarkers) != 1 {
		t.Fatalf("versions=%#v markers=%#v, want one marker and no data versions", versionsOut.Versions, versionsOut.DeleteMarkers)
	}
}

func TestDeleteObject_DataVersionPermanentDeleteRemovesHiddenVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-data-version-bucket")

	putOut := putTestObjectOutput(t, tb, "delete-data-version-bucket", "file.txt", "data")
	if _, err := tb.db.NewRaw(`UPDATE tasks SET status = ? WHERE ref_type = ? AND ref_version_id = ?`, model.TaskStatusCompleted, "object", putOut.VersionID).Exec(ctx); err != nil {
		t.Fatalf("complete upload task: %v", err)
	}
	marker, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("delete-data-version-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject(marker): %v", err)
	}
	if marker.DeleteMarker == nil || !*marker.DeleteMarker {
		t.Fatalf("marker output = %#v, want delete marker", marker)
	}

	versionBeforeDelete, err := tb.repos.Objects.GetVersionByID(ctx, putOut.VersionID)
	if err != nil || versionBeforeDelete == nil {
		t.Fatalf("GetVersionByID(before delete): version=%v err=%v", versionBeforeDelete, err)
	}
	if !tb.cache.Exists(ctx, "delete-data-version-bucket", versionBeforeDelete.CacheKey) {
		t.Fatal("expected cache file before permanent delete")
	}

	out, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String("delete-data-version-bucket"),
		Key:       aws.String("file.txt"),
		VersionId: aws.String(putOut.VersionID),
	})
	if err != nil {
		t.Fatalf("DeleteObject(data version): %v", err)
	}
	if out.VersionId == nil || *out.VersionId != putOut.VersionID {
		t.Fatalf("DeleteObject(data version) VersionId = %v, want %s", out.VersionId, putOut.VersionID)
	}
	if out.DeleteMarker != nil && *out.DeleteMarker {
		t.Fatalf("DeleteObject(data version) DeleteMarker = true, want false/nil")
	}

	deleted, err := tb.repos.Objects.GetVersionByID(ctx, putOut.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(after delete): %v", err)
	}
	if deleted != nil {
		t.Fatalf("deleted data version still exists: %#v", deleted)
	}
	if tb.cache.Exists(ctx, "delete-data-version-bucket", versionBeforeDelete.CacheKey) {
		t.Fatal("cache file still exists after permanent delete")
	}

	var cacheStatus string
	if err := tb.db.NewRaw(`SELECT cache_cleanup_status FROM object_deletions WHERE version_id = ?`, putOut.VersionID).Scan(ctx, &cacheStatus); err != nil {
		t.Fatalf("object deletion cache status: %v", err)
	}
	if cacheStatus != string(model.CacheCleanupStatusDeleted) {
		t.Fatalf("cache cleanup status = %q, want %q", cacheStatus, model.CacheCleanupStatusDeleted)
	}

	versionsOut, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String("delete-data-version-bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(versionsOut.Versions) != 0 || len(versionsOut.DeleteMarkers) != 1 {
		t.Fatalf("versions=%#v markers=%#v, want only delete marker", versionsOut.Versions, versionsOut.DeleteMarkers)
	}
}

func TestDeleteObject_DataVersionPermanentDeleteRemovesCurrentVisibleVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-current-data-version-bucket")

	putOut := putTestObjectOutput(t, tb, "delete-current-data-version-bucket", "file.txt", "data")
	if _, err := tb.db.NewRaw(`UPDATE tasks SET status = ? WHERE ref_type = ? AND ref_version_id = ?`, model.TaskStatusCompleted, "object", putOut.VersionID).Exec(ctx); err != nil {
		t.Fatalf("complete upload task: %v", err)
	}
	out, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String("delete-current-data-version-bucket"),
		Key:       aws.String("file.txt"),
		VersionId: aws.String(putOut.VersionID),
	})
	if err != nil {
		t.Fatalf("DeleteObject(current data version): %v", err)
	}
	if out.VersionId == nil || *out.VersionId != putOut.VersionID {
		t.Fatalf("DeleteObject(current data version) VersionId = %v, want %s", out.VersionId, putOut.VersionID)
	}
	if out.DeleteMarker != nil && *out.DeleteMarker {
		t.Fatalf("DeleteObject(current data version) DeleteMarker = true, want false/nil")
	}

	deleted, err := tb.repos.Objects.GetVersionByID(ctx, putOut.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(after delete): %v", err)
	}
	if deleted != nil {
		t.Fatalf("deleted current data version still exists: %#v", deleted)
	}
	_, err = tb.backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("delete-current-data-version-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err == nil {
		t.Fatal("HeadObject after deleting only current version returned nil error")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("HeadObject after deleting only current version error = %T %v, want APIError", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrNoSuchKey); apiErr.Code != want.Code {
		t.Fatalf("HeadObject after deleting only current version code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestDeleteObjects_EmptyListReturnsEmptyResult(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-objects-empty-bucket")

	out, err := tb.backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String("delete-objects-empty-bucket"),
		Delete: &types.Delete{},
	})
	if err != nil {
		t.Fatalf("DeleteObjects(empty): %v", err)
	}
	if len(out.Deleted) != 0 || len(out.Error) != 0 {
		t.Fatalf("DeleteObjects(empty) = %#v, want empty result", out)
	}
}

func TestDeleteObjects_TooManyObjectsReturnsRequestError(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-objects-too-many-bucket")
	putTestObject(t, tb, "delete-objects-too-many-bucket", "file-0000", "data")

	objects := make([]types.ObjectIdentifier, 1001)
	for i := range objects {
		objects[i] = types.ObjectIdentifier{Key: aws.String(fmt.Sprintf("file-%04d", i))}
	}

	_, err := tb.backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String("delete-objects-too-many-bucket"),
		Delete: &types.Delete{
			Objects: objects,
		},
	})
	if err == nil {
		t.Fatal("DeleteObjects returned nil error for more than 1000 objects")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("DeleteObjects error = %T %v, want APIError", err, err)
	}
	if want := s3err.GetAPIError(s3err.ErrMalformedXML); apiErr.Code != want.Code {
		t.Fatalf("DeleteObjects code = %q, want %q", apiErr.Code, want.Code)
	}

	got, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("delete-objects-too-many-bucket"),
		Key:    aws.String("file-0000"),
	})
	if err != nil {
		t.Fatalf("GetObject(file-0000): %v", err)
	}
	defer func() { _ = got.Body.Close() }()
}

func TestDeleteObjects_MixedSuccessAndMissingVersionReturnsEntryResults(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-objects-mixed-bucket")
	putTestObject(t, tb, "delete-objects-mixed-bucket", "file.txt", "data")

	missingVersionID := "missing-version"
	out, err := tb.backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String("delete-objects-mixed-bucket"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("file.txt")},
				{Key: aws.String("file.txt"), VersionId: aws.String(missingVersionID)},
			},
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects(mixed): %v", err)
	}
	if len(out.Deleted) != 1 {
		t.Fatalf("Deleted = %#v, want one entry", out.Deleted)
	}
	deleted := out.Deleted[0]
	if deleted.Key == nil || *deleted.Key != "file.txt" {
		t.Fatalf("deleted key = %v, want file.txt", deleted.Key)
	}
	if deleted.DeleteMarker == nil || !*deleted.DeleteMarker {
		t.Fatalf("deleted DeleteMarker = %v, want true", deleted.DeleteMarker)
	}
	if deleted.DeleteMarkerVersionId == nil || *deleted.DeleteMarkerVersionId == "" {
		t.Fatalf("deleted DeleteMarkerVersionId = %v, want marker version", deleted.DeleteMarkerVersionId)
	}
	if len(out.Error) != 1 {
		t.Fatalf("Error = %#v, want one entry", out.Error)
	}
	entryErr := out.Error[0]
	wantErr := s3err.GetAPIError(s3err.ErrNoSuchVersion)
	if entryErr.Code == nil || *entryErr.Code != wantErr.Code {
		t.Fatalf("error code = %v, want %q", entryErr.Code, wantErr.Code)
	}
	if entryErr.VersionId == nil || *entryErr.VersionId != missingVersionID {
		t.Fatalf("error version id = %v, want %s", entryErr.VersionId, missingVersionID)
	}
}

func TestDeleteObjects_DeleteMarkerVersionRestoresObject(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-objects-marker-bucket")
	putTestObject(t, tb, "delete-objects-marker-bucket", "file.txt", "restored")
	marker, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("delete-objects-marker-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject(marker): %v", err)
	}

	out, err := tb.backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String("delete-objects-marker-bucket"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("file.txt"), VersionId: marker.VersionId},
			},
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects(marker version): %v", err)
	}
	if len(out.Error) != 0 {
		t.Fatalf("Error = %#v, want none", out.Error)
	}
	if len(out.Deleted) != 1 {
		t.Fatalf("Deleted = %#v, want one entry", out.Deleted)
	}
	deleted := out.Deleted[0]
	if deleted.DeleteMarker == nil || !*deleted.DeleteMarker {
		t.Fatalf("deleted DeleteMarker = %v, want true", deleted.DeleteMarker)
	}
	if deleted.VersionId == nil || *deleted.VersionId != *marker.VersionId {
		t.Fatalf("deleted VersionId = %v, want %s", deleted.VersionId, *marker.VersionId)
	}
	if deleted.DeleteMarkerVersionId == nil || *deleted.DeleteMarkerVersionId != *marker.VersionId {
		t.Fatalf("deleted DeleteMarkerVersionId = %v, want %s", deleted.DeleteMarkerVersionId, *marker.VersionId)
	}

	got, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("delete-objects-marker-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject(restored): %v", err)
	}
	defer func() { _ = got.Body.Close() }()
	body, _ := io.ReadAll(got.Body)
	if string(body) != "restored" {
		t.Fatalf("restored body = %q, want restored", string(body))
	}
}

func TestDeleteObjects_DataVersionPermanentDeleteRemovesHiddenVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-objects-data-version-bucket")

	putOut := putTestObjectOutput(t, tb, "delete-objects-data-version-bucket", "file.txt", "data")
	if _, err := tb.db.NewRaw(`UPDATE tasks SET status = ? WHERE ref_type = ? AND ref_version_id = ?`, model.TaskStatusCompleted, "object", putOut.VersionID).Exec(ctx); err != nil {
		t.Fatalf("complete upload task: %v", err)
	}
	marker, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("delete-objects-data-version-bucket"),
		Key:    aws.String("file.txt"),
	})
	if err != nil {
		t.Fatalf("DeleteObject(marker): %v", err)
	}
	versionBeforeDelete, err := tb.repos.Objects.GetVersionByID(ctx, putOut.VersionID)
	if err != nil || versionBeforeDelete == nil {
		t.Fatalf("GetVersionByID(before delete): version=%v err=%v", versionBeforeDelete, err)
	}
	if !tb.cache.Exists(ctx, "delete-objects-data-version-bucket", versionBeforeDelete.CacheKey) {
		t.Fatal("expected cache file before permanent delete")
	}

	out, err := tb.backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String("delete-objects-data-version-bucket"),
		Delete: &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("file.txt"), VersionId: aws.String(putOut.VersionID)},
			},
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects(data version): %v", err)
	}
	if len(out.Error) != 0 {
		t.Fatalf("Error = %#v, want none", out.Error)
	}
	if len(out.Deleted) != 1 {
		t.Fatalf("Deleted = %#v, want one entry", out.Deleted)
	}
	deleted := out.Deleted[0]
	if deleted.VersionId == nil || *deleted.VersionId != putOut.VersionID {
		t.Fatalf("deleted VersionId = %v, want %s", deleted.VersionId, putOut.VersionID)
	}
	if deleted.DeleteMarker == nil || *deleted.DeleteMarker {
		t.Fatalf("deleted DeleteMarker = %v, want false", deleted.DeleteMarker)
	}

	removed, err := tb.repos.Objects.GetVersionByID(ctx, putOut.VersionID)
	if err != nil {
		t.Fatalf("GetVersionByID(after delete): %v", err)
	}
	if removed != nil {
		t.Fatalf("deleted data version still exists: %#v", removed)
	}
	if tb.cache.Exists(ctx, "delete-objects-data-version-bucket", versionBeforeDelete.CacheKey) {
		t.Fatal("cache file still exists after permanent delete")
	}
	var cacheStatus string
	if err := tb.db.NewRaw(`SELECT cache_cleanup_status FROM object_deletions WHERE version_id = ?`, putOut.VersionID).Scan(ctx, &cacheStatus); err != nil {
		t.Fatalf("object deletion cache status: %v", err)
	}
	if cacheStatus != string(model.CacheCleanupStatusDeleted) {
		t.Fatalf("cache cleanup status = %q, want %q", cacheStatus, model.CacheCleanupStatusDeleted)
	}

	versionsOut, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String("delete-objects-data-version-bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(versionsOut.Versions) != 0 || len(versionsOut.DeleteMarkers) != 1 || *versionsOut.DeleteMarkers[0].VersionId != *marker.VersionId {
		t.Fatalf("versions=%#v markers=%#v, want only marker %s", versionsOut.Versions, versionsOut.DeleteMarkers, *marker.VersionId)
	}
}

// ---------- CopyObject ----------

func TestCopyObject_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "src-bucket")
	seedActiveBucket(t, tb, "dst-bucket")
	putValidTestObject(t, tb, "src-bucket", "original.txt", "copy me")

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
	dstObj, _ := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, dstBkt.ID, "copied.txt")
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
	putValidTestObject(t, tb, "copy-dedupe-src", "original.txt", "copy me")

	copyInput := s3response.CopyObjectInput{
		Bucket:     aws.String("copy-dedupe-dst"),
		Key:        aws.String("copied.txt"),
		CopySource: aws.String("/copy-dedupe-src/original.txt"),
	}
	if _, err := tb.backend.CopyObject(ctx, copyInput); err != nil {
		t.Fatalf("first CopyObject: %v", err)
	}

	dstBkt, _ := tb.repos.Buckets.GetByName(ctx, "copy-dedupe-dst")
	obj1, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, dstBkt.ID, "copied.txt")
	if err != nil || obj1 == nil {
		t.Fatalf("current object after first copy: obj=%v err=%v", obj1, err)
	}

	if _, err := tb.backend.CopyObject(ctx, copyInput); err != nil {
		t.Fatalf("second CopyObject: %v", err)
	}

	obj2, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, dstBkt.ID, "copied.txt")
	if err != nil || obj2 == nil {
		t.Fatalf("current object after second copy: obj=%v err=%v", obj2, err)
	}
	if obj2.VersionID == obj1.VersionID {
		t.Fatalf("current version did not change for identical copy: %s", obj2.VersionID)
	}

	versionCount, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("object_id = ?", obj1.ObjectID).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting object versions: %v", err)
	}
	if versionCount != 2 {
		t.Fatalf("object version count = %d, want 2", versionCount)
	}

	taskCount, err := tb.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("ref_type = ? AND ref_id = ?", "object", obj1.ObjectID).
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
	putValidTestObject(t, tb, "copy-retry-src", "original.txt", "copy me")

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
	dstObj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, dstBkt.ID, "copied.txt")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}

	tasks, _, err := tb.repos.Tasks.List(ctx, string(model.TaskTypeUpload), "", string(model.TaskStatusQueued), 10, 0)
	if err != nil {
		t.Fatalf("List tasks: %v", err)
	}
	for _, task := range tasks {
		if task.RefType == "object" && task.RefID == dstObj.ObjectID {
			if task.MaxRetries != 13 {
				t.Fatalf("copy upload task MaxRetries = %d, want 13", task.MaxRetries)
			}
			if task.Stage == nil || *task.Stage != "prepare_upload" {
				t.Fatalf("copy upload task Stage = %#v, want prepare_upload", task.Stage)
			}
			return
		}
	}
	t.Fatalf("copy upload task for object %d not found in %#v", dstObj.ObjectID, tasks)
}

func TestCopyObject_MetadataReplace(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "mr-src")
	seedActiveBucket(t, tb, "mr-dst")
	putValidTestObject(t, tb, "mr-src", "file.txt", "data")

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
	dstObj, _ := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, dstBkt.ID, "file.json")
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
	putValidTestObject(t, tb, "same-bucket", "src.txt", "data")

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
	src, _ := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "src.txt")
	dst, _ := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "dst.txt")
	if src == nil || dst == nil {
		t.Error("both source and destination objects should exist")
	}
}

func TestCopyObject_CopySourceVersionIDCopiesSpecifiedVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "copy-version-src")
	seedActiveBucket(t, tb, "copy-version-dst")

	firstOut := putValidTestObjectOutput(t, tb, "copy-version-src", "original.txt", "old")
	putValidTestObject(t, tb, "copy-version-src", "original.txt", "new")

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
	if want := validTestObjectBody("old"); string(data) != want {
		t.Fatalf("copied body = %q, want %q", string(data), want)
	}
}

func TestCopyObjectBindsImplicitCurrentReadToResolvedVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	srcBkt := seedActiveBucket(t, tb, "copy-implicit-race-src")
	seedActiveBucket(t, tb, "copy-implicit-race-dst")

	firstOut := putValidTestObjectOutput(t, tb, "copy-implicit-race-src", "original.txt", "old")
	baseObjects := tb.repos.Objects
	var hookErr error
	tb.repos.Objects = &getCurrentVersionByBucketAndKeyAfterReadRepo{
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
	if want := validTestObjectBody("old"); string(data) != want {
		t.Fatalf("copied body = %q, want resolved source version body", string(data))
	}
}

func TestParseCopySource_Formats(t *testing.T) {
	// parseCopySource is unexported. We test it indirectly through CopyObject
	// by providing different source formats.
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "fmt-bucket")
	putValidTestObject(t, tb, "fmt-bucket", "key with spaces.txt", "data")

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
