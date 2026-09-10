package backend_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/storagepipeline"
	synaps3testutil "github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/storage"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// ---------- helpers ----------

// seedActiveBucket creates an active bucket via the repository layer.
func seedActiveBucket(t *testing.T, tb *testBackend, name string) *model.Bucket {
	t.Helper()
	ctx := context.Background()
	bkt := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket %q: %v", name, err)
	}
	return bkt
}

// seedActiveBucketWithCopies seeds a bucket whose durability policy is the given
// copy count. Content freezes requested_copies from the policy at first ingest,
// so a test that wants partially replicated content sets it on the bucket.
func seedActiveBucketWithCopies(t *testing.T, tb *testBackend, name string, copies int) *model.Bucket {
	t.Helper()
	ctx := context.Background()
	bkt := &model.Bucket{Name: name, Status: model.BucketStatusActive, DefaultCopies: copies, MinimumDurableCopies: copies}
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
	sum := sha256.Sum256([]byte(body))
	content, err := tb.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
		BucketID:        bkt.ID,
		ContentSize:     int64(len(body)),
		Checksum:        hex.EncodeToString(sum[:]),
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("seeding content %s/%s: %v", bucket, key, err)
	}
	info, err := tb.cache.Put(ctx, bucket, model.ContentCacheKey(content.ID), strings.NewReader(body))
	if err != nil {
		t.Fatalf("seeding cache object %s/%s: %v", bucket, key, err)
	}
	if _, err := tb.repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bkt.ID,
		Key:         key,
		ContentID:   &content.ID,
		Size:        info.Size,
		ETag:        info.ETag,
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("seeding object version %s/%s: %v", bucket, key, err)
	}
	etag := fmt.Sprintf(`"%s"`, info.ETag)
	return s3response.PutObjectOutput{ETag: etag, VersionID: versionID, Size: &info.Size}
}

func TestObjectOperationsRejectNilInputsWithInvalidArgument(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "GetObject",
			run: func() error {
				_, err := tb.backend.GetObject(ctx, nil)
				return err
			},
		},
		{
			name: "HeadObject",
			run: func() error {
				_, err := tb.backend.HeadObject(ctx, nil)
				return err
			},
		},
		{
			name: "GetObjectAttributes",
			run: func() error {
				_, err := tb.backend.GetObjectAttributes(ctx, nil)
				return err
			},
		},
		{
			name: "DeleteObject",
			run: func() error {
				_, err := tb.backend.DeleteObject(ctx, nil)
				return err
			},
		},
		{
			name: "DeleteObjects",
			run: func() error {
				_, err := tb.backend.DeleteObjects(ctx, nil)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireInvalidArgumentError(t, tt.run(), "Bucket")
		})
	}
}

func TestObjectListOperationsRejectMissingBucketInput(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "ListObjects nil input",
			run: func() error {
				_, err := tb.backend.ListObjects(ctx, nil)
				return err
			},
		},
		{
			name: "ListObjects nil bucket",
			run: func() error {
				_, err := tb.backend.ListObjects(ctx, &s3.ListObjectsInput{})
				return err
			},
		},
		{
			name: "ListObjectVersions nil input",
			run: func() error {
				_, err := tb.backend.ListObjectVersions(ctx, nil)
				return err
			},
		},
		{
			name: "ListObjectVersions nil bucket",
			run: func() error {
				_, err := tb.backend.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{})
				return err
			},
		},
		{
			name: "ListObjectsV2 nil input",
			run: func() error {
				_, err := tb.backend.ListObjectsV2(ctx, nil)
				return err
			},
		},
		{
			name: "ListObjectsV2 nil bucket",
			run: func() error {
				_, err := tb.backend.ListObjectsV2(ctx, &s3.ListObjectsV2Input{})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requireInvalidArgumentError(t, tt.run(), "Bucket")
		})
	}
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
	return new(v)
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

// touchVersionLifecycle forces updated_at to diverge from created_at so a reader
// that reported the wrong one would be caught. A version is immutable once
// written now that its mutable state lives on the content and the cache entry,
// so the divergence has to be staged by the test itself.
func touchVersionLifecycle(t *testing.T, tb *testBackend, ctx context.Context, versionID string) *model.ObjectVersion {
	t.Helper()
	if _, err := tb.db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("updated_at = ?", time.Now().Add(time.Second)).
		Where("version_id = ?", versionID).
		Exec(ctx); err != nil {
		t.Fatalf("touching version %s: %v", versionID, err)
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
	if checksum == "" {
		checksum = "checksum-" + versionID
	}
	content, err := tb.repos.Contents.EnsureContent(context.Background(), repository.EnsureContentInput{
		BucketID:        bucket.ID,
		ContentSize:     size,
		Checksum:        synaps3testutil.StorageChecksum(checksum),
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("seeding content: %v", err)
	}
	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         key,
		ContentID:   &content.ID,
		Size:        size,
		ETag:        etag,
		ContentType: contentType,
	}
	objID, err := tb.repos.Objects.CreateVersionAndSetCurrent(context.Background(), version)
	if err != nil {
		t.Fatalf("seeding object version: %v", err)
	}
	if state == model.ObjectStateStored && pieceCID != nil && retrievalURL != nil {
		acceptBackendVersionUpload(t, tb.db, tb.repos, versionID, *pieceCID, *retrievalURL)
	}
	return objID, versionID
}

// contentSubjectForVersion is the task subject key for the content backing a
// version. Ingest tasks are keyed on the bytes, not on one version of them.
func contentSubjectForVersion(t *testing.T, tb *testBackend, versionID string) string {
	t.Helper()
	version, err := tb.repos.Objects.GetVersionByID(t.Context(), versionID)
	if err != nil || version == nil || version.ContentID == nil {
		t.Fatalf("content for version %s: version=%v err=%v", versionID, version, err)
	}
	return strconv.FormatInt(*version.ContentID, 10)
}

// contentForVersion returns the content a seeded version already points at. The
// bytes are their own row now, so upload seeding attaches copies to that row
// rather than minting a second identity for the same version.
func contentForVersion(t *testing.T, repos *repository.Repositories, version *model.ObjectVersion) *model.StorageContent {
	t.Helper()
	if version.ContentID == nil {
		t.Fatalf("version %s has no content", version.VersionID)
	}
	content, err := repos.Contents.GetByID(t.Context(), *version.ContentID)
	if err != nil || content == nil {
		t.Fatalf("get content %d: content=%v err=%v", *version.ContentID, content, err)
	}
	return content
}

func acceptBackendVersionUpload(t *testing.T, db *bun.DB, repos *repository.Repositories, versionID string, pieceCID string, retrievalURL string) *model.StorageContent {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for upload accept: version=%v err=%v", version, err)
	}
	upload := contentForVersion(t, repos, version)
	providerID := onChainID(t, "101")
	binding, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:           version.BucketID,
		ProviderID:         providerID,
		CopyIndex:          0,
		CreatedByContentID: upload.ID,
	})
	if err != nil {
		t.Fatalf("ensure dataset binding: %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: binding.ID, ContentID: upload.ID, DataSetID: onChainID(t, "1001")}); err != nil {
		t.Fatalf("mark dataset ready: %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       providerID,
	}}); err != nil {
		t.Fatalf("create upload copy: %v", err)
	}
	synaps3testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
		ContentID:    upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      onChainIDPtr(t, "1"),
		RetrievalURL: retrievalURL,
	})
	if _, err := repos.Contents.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		ContentID: upload.ID,
		BucketID:  version.BucketID,
	}); err != nil {
		t.Fatalf("bind readable upload: %v", err)
	}
	if finalized, _, err := repos.Contents.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{ContentID: upload.ID}); err != nil {
		t.Fatalf("finalize upload: %v", err)
	} else if !finalized {
		t.Fatal("finalize upload = false, want true")
	}
	return upload
}

func bindBackendPrimaryCommittedUpload(t *testing.T, db *bun.DB, repos *repository.Repositories, versionID string, pieceCID string, retrievalURL string) *model.StorageContent {
	t.Helper()
	ctx := context.Background()
	version, err := repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("get version for primary bind: version=%v err=%v", version, err)
	}
	upload := contentForVersion(t, repos, version)
	primary, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:           version.BucketID,
		ProviderID:         onChainID(t, "101"),
		CopyIndex:          0,
		CreatedByContentID: upload.ID,
	})
	if err != nil {
		t.Fatalf("ensure primary dataset binding: %v", err)
	}
	if err := repos.Contents.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        primary.ID,
		ContentID: upload.ID,
		DataSetID: onChainID(t, "1001"),
	}); err != nil {
		t.Fatalf("mark primary dataset ready: %v", err)
	}
	secondary, err := repos.Contents.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:           version.BucketID,
		ProviderID:         onChainID(t, "202"),
		CopyIndex:          1,
		CreatedByContentID: upload.ID,
	})
	if err != nil {
		t.Fatalf("ensure secondary dataset binding: %v", err)
	}
	if err := repos.Contents.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("create upload copy rows: %v", err)
	}
	if err := repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
		ContentID:    upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		RetrievalURL: retrievalURL,
	}); err != nil {
		t.Fatalf("mark primary piece ready: %v", err)
	}
	synaps3testutil.CommitStorageCopy(t, db, repos, repository.MarkUploadCopyCommittedInput{
		ContentID:    upload.ID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      onChainIDPtr(t, "2001"),
		RetrievalURL: retrievalURL,
	})
	if _, err := repos.Contents.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		ContentID: upload.ID,
		BucketID:  version.BucketID,
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
	if !tb.cache.Exists(ctx, "put-bucket", obj.CacheKey()) {
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

func TestObjectKeyValidationRejectsCreatingNewKeys(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	bucket := seedActiveBucket(t, tb, "key-validation-bucket")
	tooLongKey := strings.Repeat("你", 342)

	_, err := tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket: aws.String("key-validation-bucket"),
		Key:    aws.String(tooLongKey),
		Body:   strings.NewReader(validTestObjectBody("body")),
	})
	requireObjectKeyInvalidArgument(t, err)

	_, err = tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket:     aws.String("key-validation-bucket"),
		Key:        aws.String(tooLongKey),
		CopySource: aws.String("/key-validation-bucket/source.txt"),
	})
	requireObjectKeyInvalidArgument(t, err)

	_, err = tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: aws.String("key-validation-bucket"),
		Key:    aws.String(tooLongKey),
	})
	requireObjectKeyInvalidArgument(t, err)

	_, err = tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("key-validation-bucket"),
		Key:    aws.String(tooLongKey),
	})
	requireObjectKeyInvalidArgument(t, err)

	_, err = tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket: aws.String("key-validation-bucket"),
		Key:    aws.String("folder/\x00/file.txt"),
		Body:   strings.NewReader(validTestObjectBody("body")),
	})
	requireObjectKeyInvalidArgument(t, err)

	legacyUpload := &model.MultipartUpload{
		BucketID: bucket.ID,
		Key:      tooLongKey,
		UploadID: "legacy-invalid-key-upload",
	}
	if err := tb.repos.Multiparts.Create(ctx, legacyUpload); err != nil {
		t.Fatalf("seed legacy multipart upload: %v", err)
	}
	partNumber := int32(1)
	_, _, err = tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("key-validation-bucket"),
		Key:      aws.String(tooLongKey),
		UploadId: aws.String(legacyUpload.UploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{PartNumber: &partNumber}},
		},
	})
	requireObjectKeyInvalidArgument(t, err)
	gotUpload, err := tb.repos.Multiparts.GetByUploadID(ctx, legacyUpload.UploadID)
	if err != nil {
		t.Fatalf("get legacy multipart upload: %v", err)
	}
	if gotUpload == nil || gotUpload.Status != model.MultipartStatusInitiated {
		t.Fatalf("legacy multipart status = %v, want initiated", gotUpload)
	}
}

func TestObjectKeyValidationAllowsExactVersionDelete(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	bucket := seedActiveBucket(t, tb, "key-validation-delete-bucket")
	key := strings.Repeat("你", 342)
	_, versionID := seedBackendObjectVersion(t, tb, bucket, key, 10, "etag-key-validation", testSHA256Hex("key-validation"), "text/plain", model.ObjectStateCached, nil, nil)

	out, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String("key-validation-delete-bucket"),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		t.Fatalf("DeleteObject exact version: %v", err)
	}
	if out.VersionId == nil || *out.VersionId != versionID {
		t.Fatalf("VersionId = %v, want %s", out.VersionId, versionID)
	}
}

func requireObjectKeyInvalidArgument(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want InvalidArgument")
	}
	var invalidArg s3err.InvalidArgumentError
	if !errors.As(err, &invalidArg) {
		t.Fatalf("error = %T %v, want InvalidArgumentError", err, err)
	}
	if invalidArg.BaseError().Code != "InvalidArgument" || invalidArg.ArgumentName != "Key" {
		t.Fatalf("invalid argument = %#v, want Key InvalidArgument", invalidArg)
	}
}

func TestPutObjectEnqueuesRegisteredUploadPlan(t *testing.T) {
	tb := newTestBackend(t)
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

	task, err := tb.repos.Tasks.ClaimNext(ctx, time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if task == nil {
		t.Fatal("expected upload task")
	}
	if task.Type != model.TaskTypeUploadPlan || task.RetryLimit == nil || *task.RetryLimit != 5 {
		t.Fatalf("task = %#v, want upload_plan with retry limit 5", task)
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
		Where("type = ?", model.TaskTypeUploadPlan).
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
	// Both versions share one content whose ingest plan has not produced a copy
	// yet, so the derived position is still cached.
	if secondVersion.State != model.ObjectStateCached {
		t.Fatalf("second version state = %s, want cached", secondVersion.State)
	}
}

func TestPutObjectFreezesRequestedCopiesPerContent(t *testing.T) {
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "requested-copies-freeze")

	first := putValidTestObjectOutput(t, tb, bucket.Name, "first.txt", "first content")
	firstVersion, err := tb.repos.Objects.GetVersionByID(ctx, first.VersionID)
	if err != nil || firstVersion == nil {
		t.Fatalf("GetVersionByID(first): version=%v err=%v", firstVersion, err)
	}
	firstContent := contentForVersion(t, tb.repos, firstVersion)
	if firstContent.RequestedCopies != 1 {
		t.Fatalf("first requested_copies = %d, want 1", firstContent.RequestedCopies)
	}

	twoCopies := 2
	if _, err := tb.repos.Buckets.UpdateCopyPolicy(ctx, repository.UpdateBucketCopyPolicyInput{
		Name:             bucket.Name,
		SetDefaultCopies: true,
		DefaultCopies:    &twoCopies,
	}); err != nil {
		t.Fatalf("UpdateCopyPolicy: %v", err)
	}

	second := putValidTestObjectOutput(t, tb, bucket.Name, "second.txt", "second content")
	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, second.VersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("GetVersionByID(second): version=%v err=%v", secondVersion, err)
	}
	secondContent := contentForVersion(t, tb.repos, secondVersion)
	if secondContent.RequestedCopies != 2 {
		t.Fatalf("second requested_copies = %d, want 2", secondContent.RequestedCopies)
	}

	deduplicated := putValidTestObjectOutput(t, tb, bucket.Name, "deduplicated.txt", "first content")
	deduplicatedVersion, err := tb.repos.Objects.GetVersionByID(ctx, deduplicated.VersionID)
	if err != nil || deduplicatedVersion == nil {
		t.Fatalf("GetVersionByID(deduplicated): version=%v err=%v", deduplicatedVersion, err)
	}
	deduplicatedContent := contentForVersion(t, tb.repos, deduplicatedVersion)
	if deduplicatedContent.ID != firstContent.ID {
		t.Fatalf("deduplicated content id = %d, want original %d", deduplicatedContent.ID, firstContent.ID)
	}
	if deduplicatedContent.RequestedCopies != 1 {
		t.Fatalf("deduplicated requested_copies = %d, want frozen value 1", deduplicatedContent.RequestedCopies)
	}
}

func TestPutObjectReactivatesTerminalUploadPlan(t *testing.T) {
	for _, terminalStatus := range []model.TaskStatus{model.TaskStatusFailed, model.TaskStatusCancelled} {
		t.Run(string(terminalStatus), func(t *testing.T) {
			tb := newTestBackend(t)
			ctx := t.Context()
			bucket := seedActiveBucket(t, tb, "reactivate-plan-"+string(terminalStatus))
			first := putValidTestObjectOutput(t, tb, bucket.Name, "first.bin", "reactivated content")
			version, err := tb.repos.Objects.GetVersionByID(ctx, first.VersionID)
			if err != nil || version == nil || version.ContentID == nil {
				t.Fatalf("first version = %#v, err=%v", version, err)
			}
			taskRow, err := tb.repos.Tasks.GetByIdentity(ctx, model.TaskTypeUploadPlan, storagepipeline.UploadPlanKey(*version.ContentID))
			if err != nil || taskRow == nil {
				t.Fatalf("upload plan = %#v, err=%v", taskRow, err)
			}
			claimed, err := tb.repos.Tasks.ClaimNext(ctx, time.Minute)
			if err != nil || claimed == nil || claimed.ID != taskRow.ID {
				t.Fatalf("claimed upload plan = %#v, err=%v", claimed, err)
			}
			if err := tb.repos.Tasks.WriteCheckpoint(ctx, claimed.ID, claimed.ClaimGeneration, []byte(`{"old":true}`)); err != nil {
				t.Fatalf("write old checkpoint: %v", err)
			}
			if err := tb.repos.Tasks.RequestCancellation(ctx, claimed.ID, "old owner stopped"); err != nil {
				t.Fatalf("request old cancellation: %v", err)
			}
			transition := repository.TaskTransition{
				Status: terminalStatus, ResumeMode: model.TaskResumeModeRecover,
				FailureReason: new("old_plan_failed"), LastError: new("old upload plan failed"), IncrementRetry: true,
			}
			if terminalStatus == model.TaskStatusCancelled {
				transition.FailureReason = nil
				transition.LastError = nil
				transition.RetentionUntil = new(time.Now().Add(time.Hour))
			}
			if err := tb.repos.Tasks.Settle(ctx, claimed.ID, claimed.ClaimGeneration, transition); err != nil {
				t.Fatalf("settle old upload plan: %v", err)
			}
			if terminalStatus == model.TaskStatusFailed {
				if err := tb.repos.Tasks.AcknowledgeFailed(ctx, claimed.ID, time.Hour); err != nil {
					t.Fatalf("acknowledge old upload plan: %v", err)
				}
			}

			second := putValidTestObjectOutput(t, tb, bucket.Name, "second.bin", "reactivated content")
			secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, second.VersionID)
			if err != nil || secondVersion == nil || secondVersion.ContentID == nil || *secondVersion.ContentID != *version.ContentID {
				t.Fatalf("second version = %#v, err=%v", secondVersion, err)
			}
			reactivated, err := tb.repos.Tasks.GetByID(ctx, taskRow.ID)
			if err != nil || reactivated == nil {
				t.Fatalf("reactivated task = %#v, err=%v", reactivated, err)
			}
			if reactivated.Status != model.TaskStatusPending || reactivated.ResumeMode != model.TaskResumeModeExecute ||
				reactivated.RetryCount != 0 || len(reactivated.Checkpoint) != 0 || reactivated.FailureReason != nil ||
				reactivated.CancellationRequestedAt != nil || reactivated.CancellationReason != nil || reactivated.AcknowledgedAt != nil || reactivated.RetentionUntil != nil {
				t.Fatalf("reactivated task retained terminal state: %#v", reactivated)
			}
		})
	}
}

func TestPutObjectRejectsCompletedUploadPlanForCachedContent(t *testing.T) {
	tb := newTestBackend(t)
	ctx := t.Context()
	bucket := seedActiveBucket(t, tb, "completed-cached-plan")
	first := putValidTestObjectOutput(t, tb, bucket.Name, "first.bin", "completed cached content")
	version, err := tb.repos.Objects.GetVersionByID(ctx, first.VersionID)
	if err != nil || version == nil || version.ContentID == nil {
		t.Fatalf("first version = %#v, err=%v", version, err)
	}
	taskRow, err := tb.repos.Tasks.GetByIdentity(ctx, model.TaskTypeUploadPlan, storagepipeline.UploadPlanKey(*version.ContentID))
	if err != nil || taskRow == nil {
		t.Fatalf("upload plan = %#v, err=%v", taskRow, err)
	}
	claimed, err := tb.repos.Tasks.ClaimNext(ctx, time.Minute)
	if err != nil || claimed == nil || claimed.ID != taskRow.ID {
		t.Fatalf("claimed upload plan = %#v, err=%v", claimed, err)
	}
	if err := tb.repos.Tasks.Settle(ctx, claimed.ID, claimed.ClaimGeneration, repository.TaskTransition{
		Status: model.TaskStatusCompleted, ResumeMode: model.TaskResumeModeRecover, RetentionUntil: new(time.Now().Add(time.Hour)),
	}); err != nil {
		t.Fatalf("complete inconsistent upload plan: %v", err)
	}
	contentType := "text/plain"
	validBody := validTestObjectBody("completed cached content")
	_, err = tb.backend.PutObject(ctx, s3response.PutObjectInput{
		Bucket: &bucket.Name, Key: new("second.bin"), Body: strings.NewReader(validBody), ContentType: &contentType,
	})
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("PutObject completed/cached error = %v, want conflict", err)
	}
}

func TestPutObjectIdenticalStoredContentQueuesAfterUploadEviction(t *testing.T) {
	tb := newTestBackendWithOptions(t, backend.WithEvictionPolicy(cache.EvictionPolicyAfterUpload))
	ctx := context.Background()
	seedActiveBucket(t, tb, "stored-reuse-evict-bucket")

	putValidTestObject(t, tb, "stored-reuse-evict-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "stored-reuse-evict-bucket")
	firstObj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}
	acceptBackendVersionUpload(t, tb.db, tb.repos, firstObj.VersionID, "piece-shared", "https://provider.example/shared")

	putValidTestObject(t, tb, "stored-reuse-evict-bucket", "file.txt", "same data")
	page, err := tb.repos.Tasks.List(ctx, repository.TaskListFilter{Type: model.TaskTypeCacheEvict, Limit: 10})
	if err != nil {
		t.Fatalf("claim evict task: %v", err)
	}
	if len(page.Tasks) != 1 {
		t.Fatal("expected evict task for reused stored content")
	}
	// Eviction frees one cache file, and that file belongs to the content, so
	// the task names the content rather than either version of it.
	task := &page.Tasks[0]
	contentID := *firstObj.ContentID
	if task.SubjectKey == nil || *task.SubjectKey != strconv.FormatInt(contentID, 10) {
		t.Fatalf("evict task content = %v, want %d", task.SubjectKey, contentID)
	}
	input, err := cacheeviction.ParseEvictInput(task)
	if err != nil || input.ContentID != contentID || input.AccessedAt != nil {
		t.Fatalf("evict input = %#v, err=%v", input, err)
	}
	if task.IdempotencyKey != cacheeviction.EvictTaskKey(contentID, input.Generation) {
		t.Fatalf("evict task key = %q", task.IdempotencyKey)
	}
	if task.RetryLimit == nil || *task.RetryLimit != 5 {
		t.Fatalf("evict task retry limit = %v, want 5", task.RetryLimit)
	}
}

func TestPutObjectIdenticalStoredContentDoesNotQueueImmediateEvictionOutsideAfterUpload(t *testing.T) {
	for _, policy := range []cache.EvictionPolicy{cache.EvictionPolicyLRU, cache.EvictionPolicyNone} {
		t.Run(string(policy), func(t *testing.T) {
			tb := newTestBackendWithOptions(t, backend.WithEvictionPolicy(policy))
			ctx := context.Background()
			bucketName := "stored-reuse-" + string(policy) + "-bucket"
			seedActiveBucket(t, tb, bucketName)

			putValidTestObject(t, tb, bucketName, "file.txt", "same data")
			bucket, _ := tb.repos.Buckets.GetByName(ctx, bucketName)
			first, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
			if err != nil || first == nil {
				t.Fatalf("current object after first put: object=%v err=%v", first, err)
			}
			acceptBackendVersionUpload(t, tb.db, tb.repos, first.VersionID, "piece-"+string(policy), "https://provider.example/"+string(policy))

			second := putValidTestObjectOutput(t, tb, bucketName, "file.txt", "same data")
			stored, err := tb.repos.Objects.GetVersionByID(ctx, second.VersionID)
			if err != nil || stored == nil || stored.State != model.ObjectStateStored {
				t.Fatalf("reused version = %#v err=%v, want stored", stored, err)
			}
			page, err := tb.repos.Tasks.List(ctx, repository.TaskListFilter{Type: model.TaskTypeCacheEvict, Limit: 10})
			if err != nil {
				t.Fatalf("List eviction tasks: %v", err)
			}
			if len(page.Tasks) != 0 {
				t.Fatalf("policy %s immediate eviction tasks=%#v, want none", policy, page.Tasks)
			}
		})
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
	acceptBackendVersionUpload(t, tb.db, tb.repos, firstObj.VersionID, "piece-shared", "https://provider.example/shared")

	before, err := tb.repos.Tasks.List(ctx, repository.TaskListFilter{Type: model.TaskTypeUploadPlan, Limit: 10})
	if err != nil {
		t.Fatalf("list tasks before second put: %v", err)
	}
	secondOut := putValidTestObjectOutput(t, tb, "stored-reuse-bucket", "file.txt", "same data")
	if secondOut.VersionID == "" || secondOut.VersionID == firstOut.VersionID {
		t.Fatalf("second VersionID = %q, first = %q", secondOut.VersionID, firstOut.VersionID)
	}

	// The rewritten bytes resolve to the content that is already stored, so the
	// new version inherits its durability instead of re-uploading.
	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, secondOut.VersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("second version: version=%v err=%v", secondVersion, err)
	}
	if secondVersion.State != model.ObjectStateStored {
		t.Fatalf("second version state = %s, want stored", secondVersion.State)
	}
	if secondVersion.ContentID == nil || firstObj.ContentID == nil || *secondVersion.ContentID != *firstObj.ContentID {
		t.Fatalf("second version content = %v, want the first version's %v", secondVersion.ContentID, firstObj.ContentID)
	}

	after, err := tb.repos.Tasks.List(ctx, repository.TaskListFilter{Type: model.TaskTypeUploadPlan, Limit: 10})
	if err != nil {
		t.Fatalf("list tasks after second put: %v", err)
	}
	if len(after.Tasks) != len(before.Tasks) {
		t.Fatalf("upload task count changed from %d to %d", len(before.Tasks), len(after.Tasks))
	}
}

func TestPutObjectIdenticalReplicatingContentReusesPrimaryCommittedUpload(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucketWithCopies(t, tb, "replicating-reuse-bucket", 3)

	firstOut := putValidTestObjectOutput(t, tb, "replicating-reuse-bucket", "file.txt", "same data")
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "replicating-reuse-bucket")
	firstObj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "file.txt")
	if err != nil || firstObj == nil {
		t.Fatalf("current object after first put: obj=%v err=%v", firstObj, err)
	}
	upload := bindBackendPrimaryCommittedUpload(t, tb.db, tb.repos, firstObj.VersionID, buildDummyCID(t), "https://provider.example/primary")

	before, err := tb.repos.Tasks.List(ctx, repository.TaskListFilter{Type: model.TaskTypeUploadPlan, Limit: 10})
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
	if secondVersion.ContentID == nil || *secondVersion.ContentID != upload.ID || !secondVersion.InFilecoin {
		t.Fatalf("second version storage = upload:%v in_filecoin:%v, want upload %d readable", secondVersion.ContentID, secondVersion.InFilecoin, upload.ID)
	}

	after, err := tb.repos.Tasks.List(ctx, repository.TaskListFilter{Type: model.TaskTypeUploadPlan, Limit: 10})
	if err != nil {
		t.Fatalf("list tasks after second put: %v", err)
	}
	if len(after.Tasks) != len(before.Tasks) {
		t.Fatalf("upload task count changed from %d to %d", len(before.Tasks), len(after.Tasks))
	}
}

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

type completedMultipartTestObject struct {
	VersionID string
	PartRows  []model.MultipartPart
}

func completeMultipartTestObject(t *testing.T, tb *testBackend, bucket, key string, partBodies []string) completedMultipartTestObject {
	t.Helper()
	ctx := context.Background()

	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload(%s): %v", key, err)
	}

	completedParts := make([]types.CompletedPart, 0, len(partBodies))
	for i, body := range partBodies {
		partNumber := int32(i + 1)
		partOut, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(initResult.UploadId),
			PartNumber: &partNumber,
			Body:       strings.NewReader(body),
		})
		if err != nil {
			t.Fatalf("UploadPart(%s part %d): %v", key, partNumber, err)
		}
		completedParts = append(completedParts, types.CompletedPart{PartNumber: &partNumber, ETag: partOut.ETag})
	}

	_, versionID, err := tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(initResult.UploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload(%s): %v", key, err)
	}

	partRows, err := tb.repos.Multiparts.GetParts(ctx, initResult.UploadId, 0, len(partBodies)+1)
	if err != nil {
		t.Fatalf("GetParts(%s): %v", key, err)
	}
	if len(partRows) != len(partBodies) {
		t.Fatalf("part rows = %d, want %d", len(partRows), len(partBodies))
	}

	return completedMultipartTestObject{
		VersionID: versionID,
		PartRows:  partRows,
	}
}

func assertObjectPartMatchesMultipartPart(t *testing.T, got types.ObjectPart, want model.MultipartPart) {
	t.Helper()
	if got.PartNumber == nil || *got.PartNumber != int32(want.PartNumber) {
		t.Fatalf("PartNumber = %v, want %d", got.PartNumber, want.PartNumber)
	}
	if got.Size == nil || *got.Size != want.Size {
		t.Fatalf("Size = %v, want %d", got.Size, want.Size)
	}
	if want.Checksum == nil {
		t.Fatal("expected test multipart part checksum")
	}
	if got.ChecksumSHA256 == nil || *got.ChecksumSHA256 != *want.Checksum {
		t.Fatalf("ChecksumSHA256 = %v, want %s", got.ChecksumSHA256, *want.Checksum)
	}
}

func TestGetObjectAttributes_ObjectParts(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "attrs-parts-bucket")

	current := completeMultipartTestObject(t, tb, "attrs-parts-bucket", "current.bin", []string{
		validTestObjectBody("part-one"),
		"part-two",
		"part-three",
	})

	maxParts := int32(2)
	out, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String("attrs-parts-bucket"),
		Key:              aws.String("current.bin"),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
		MaxParts:         &maxParts,
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes(ObjectParts): %v", err)
	}
	if out.ObjectParts == nil {
		t.Fatal("ObjectParts = nil, want page")
	}
	if out.ObjectParts.MaxParts != 2 || out.ObjectParts.PartNumberMarker != 0 {
		t.Fatalf("ObjectParts markers = max:%d marker:%d, want max:2 marker:0", out.ObjectParts.MaxParts, out.ObjectParts.PartNumberMarker)
	}
	if !out.ObjectParts.IsTruncated || out.ObjectParts.NextPartNumberMarker != 2 {
		t.Fatalf("ObjectParts pagination = truncated:%t next:%d, want truncated:true next:2", out.ObjectParts.IsTruncated, out.ObjectParts.NextPartNumberMarker)
	}
	if len(out.ObjectParts.Parts) != 2 {
		t.Fatalf("ObjectParts parts = %d, want 2", len(out.ObjectParts.Parts))
	}
	assertObjectPartMatchesMultipartPart(t, out.ObjectParts.Parts[0], current.PartRows[0])
	assertObjectPartMatchesMultipartPart(t, out.ObjectParts.Parts[1], current.PartRows[1])

	partMarker := "2"
	page2, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String("attrs-parts-bucket"),
		Key:              aws.String("current.bin"),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
		MaxParts:         &maxParts,
		PartNumberMarker: &partMarker,
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes(ObjectParts page 2): %v", err)
	}
	if page2.ObjectParts == nil {
		t.Fatal("page 2 ObjectParts = nil, want page")
	}
	if page2.ObjectParts.PartNumberMarker != 2 || page2.ObjectParts.IsTruncated {
		t.Fatalf("page 2 markers = marker:%d truncated:%t, want marker:2 truncated:false", page2.ObjectParts.PartNumberMarker, page2.ObjectParts.IsTruncated)
	}
	if len(page2.ObjectParts.Parts) != 1 {
		t.Fatalf("page 2 parts = %d, want 1", len(page2.ObjectParts.Parts))
	}
	assertObjectPartMatchesMultipartPart(t, page2.ObjectParts.Parts[0], current.PartRows[2])

	controllerPath, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:   aws.String("attrs-parts-bucket"),
		Key:      aws.String("current.bin"),
		MaxParts: &maxParts,
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes(controller path): %v", err)
	}
	if controllerPath.ObjectParts == nil || len(controllerPath.ObjectParts.Parts) != 2 {
		t.Fatalf("controller path ObjectParts = %#v, want 2 parts", controllerPath.ObjectParts)
	}

	metadataOnly, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String("attrs-parts-bucket"),
		Key:              aws.String("current.bin"),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag, types.ObjectAttributesObjectSize, types.ObjectAttributesChecksum, types.ObjectAttributesStorageClass},
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes(metadata only): %v", err)
	}
	if metadataOnly.ObjectParts != nil {
		t.Fatalf("metadata-only ObjectParts = %#v, want nil", metadataOnly.ObjectParts)
	}

	putTestObject(t, tb, "attrs-parts-bucket", "plain.txt", "plain")
	plain, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String("attrs-parts-bucket"),
		Key:              aws.String("plain.txt"),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes(non-multipart): %v", err)
	}
	if plain.ObjectParts == nil {
		t.Fatal("non-multipart ObjectParts = nil, want empty page")
	}
	if plain.ObjectParts.Parts == nil {
		t.Fatal("non-multipart ObjectParts.Parts = nil, want empty slice")
	}
	if len(plain.ObjectParts.Parts) != 0 || plain.ObjectParts.IsTruncated {
		t.Fatalf("non-multipart ObjectParts = %#v, want empty untruncated page", plain.ObjectParts)
	}

	versioned := completeMultipartTestObject(t, tb, "attrs-parts-bucket", "versioned.bin", []string{
		validTestObjectBody("old-version-part"),
	})
	putTestObject(t, tb, "attrs-parts-bucket", "versioned.bin", "new-current")
	versionOut, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String("attrs-parts-bucket"),
		Key:              aws.String("versioned.bin"),
		VersionId:        aws.String(versioned.VersionID),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes(ObjectParts version): %v", err)
	}
	if versionOut.VersionId == nil || *versionOut.VersionId != versioned.VersionID {
		t.Fatalf("VersionId = %v, want %s", versionOut.VersionId, versioned.VersionID)
	}
	if versionOut.ObjectParts == nil || len(versionOut.ObjectParts.Parts) != 1 {
		t.Fatalf("version ObjectParts = %#v, want 1 part", versionOut.ObjectParts)
	}
	assertObjectPartMatchesMultipartPart(t, versionOut.ObjectParts.Parts[0], versioned.PartRows[0])
}

func TestGetObjectAttributes_ObjectPartsRejectsInvalidPartNumberMarker(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "attrs-invalid-marker-bucket")

	completeMultipartTestObject(t, tb, "attrs-invalid-marker-bucket", "current.bin", []string{
		validTestObjectBody("part-one"),
	})

	for _, tc := range []struct {
		name    string
		marker  string
		wantErr s3err.S3Error
	}{
		{
			name:    "non-numeric",
			marker:  "not-an-int",
			wantErr: s3err.GetInvalidArgMaxLimiter("part-number-marker", "not-an-int"),
		},
		{
			name:    "negative",
			marker:  "-1",
			wantErr: s3err.GetInvalidArgNegativeMaxLimiter("part-number-marker", "-1"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
				Bucket:           aws.String("attrs-invalid-marker-bucket"),
				Key:              aws.String("current.bin"),
				ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
				PartNumberMarker: &tc.marker,
			})
			if err == nil {
				t.Fatal("GetObjectAttributes invalid part marker returned nil error")
			}
			s3Err, ok := err.(s3err.S3Error)
			if !ok {
				t.Fatalf("GetObjectAttributes invalid part marker error = %T %v, want S3Error", err, err)
			}
			apiErr := s3Err.BaseError()
			wantErr := tc.wantErr.BaseError()
			if apiErr.Code != wantErr.Code {
				t.Fatalf("GetObjectAttributes invalid part marker code = %q, want %q", apiErr.Code, wantErr.Code)
			}
		})
	}
}

// ---------- ListObjects ----------

func TestListObjects_Pagination(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "list-bucket")

	for i := range 5 {
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

	for i := range 5 {
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

func TestDeleteObject_DataVersionPermanentDeleteReportsActiveStorageWork(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-active-storage-work-bucket")
	// The put leaves an ingest plan scheduled against the content, which is the
	// storage work that must block the delete.
	putOut := putValidTestObjectOutput(t, tb, "delete-active-storage-work-bucket", "file.txt", "data")
	_, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String("delete-active-storage-work-bucket"),
		Key:       aws.String("file.txt"),
		VersionId: aws.String(putOut.VersionID),
	})
	var apiErr s3err.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("DeleteObject error = %T %v, want s3 API error", err, err)
	}
	if apiErr.Code != "InvalidRequest" || apiErr.Description != "The object version cannot be deleted while storage is still in progress or a Filecoin transaction is awaiting confirmation. Try again later." {
		t.Fatalf("DeleteObject API error = %#v, want actionable storage-work conflict", apiErr)
	}
}

func TestDeleteObjects_DataVersionReportsActiveStorageWorkPerEntry(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-objects-active-storage-work-bucket")
	putOut := putValidTestObjectOutput(t, tb, "delete-objects-active-storage-work-bucket", "file.txt", "data")
	out, err := tb.backend.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String("delete-objects-active-storage-work-bucket"),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String("file.txt"), VersionId: aws.String(putOut.VersionID)},
		}},
	})
	if err != nil {
		t.Fatalf("DeleteObjects(active storage work): %v", err)
	}
	if len(out.Deleted) != 0 || len(out.Error) != 1 {
		t.Fatalf("DeleteObjects(active storage work) = %#v, want one entry error", out)
	}
	entryErr := out.Error[0]
	if entryErr.Key == nil || *entryErr.Key != "file.txt" || entryErr.VersionId == nil || *entryErr.VersionId != putOut.VersionID {
		t.Fatalf("entry identity = key:%v version:%v, want file.txt/%s", entryErr.Key, entryErr.VersionId, putOut.VersionID)
	}
	if entryErr.Code == nil || *entryErr.Code != "InvalidRequest" {
		t.Fatalf("entry code = %v, want InvalidRequest", entryErr.Code)
	}
	wantMessage := "The object version cannot be deleted while storage is still in progress or a Filecoin transaction is awaiting confirmation. Try again later."
	if entryErr.Message == nil || *entryErr.Message != wantMessage {
		t.Fatalf("entry message = %v, want %q", entryErr.Message, wantMessage)
	}
	got, err := tb.repos.Objects.GetVersionByID(ctx, putOut.VersionID)
	if err != nil || got == nil {
		t.Fatalf("version after rejected DeleteObjects = %#v err=%v, want retained", got, err)
	}
}

func TestDeleteObjects_DataVersionPermanentDeleteRemovesHiddenVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-objects-data-version-bucket")

	putOut := putTestObjectOutput(t, tb, "delete-objects-data-version-bucket", "file.txt", "data")
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
	cacheKey := versionBeforeDelete.CacheKey()
	if !tb.cache.Exists(ctx, "delete-objects-data-version-bucket", cacheKey) {
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
	if tb.cache.Exists(ctx, "delete-objects-data-version-bucket", cacheKey) {
		t.Fatal("cache file still exists after permanent delete")
	}
	var tombstones int
	if err := tb.db.NewRaw(`SELECT COUNT(*) FROM object_deletions WHERE version_id = ?`, putOut.VersionID).Scan(ctx, &tombstones); err != nil {
		t.Fatalf("count object deletions: %v", err)
	}
	if tombstones != 1 {
		t.Fatalf("object deletion tombstones = %d, want 1", tombstones)
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

func TestDeleteObject_DataVersionPermanentDeleteRemovesHiddenVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "delete-data-version-bucket")

	putOut := putTestObjectOutput(t, tb, "delete-data-version-bucket", "file.txt", "data")
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
	cacheKey := versionBeforeDelete.CacheKey()
	if !tb.cache.Exists(ctx, "delete-data-version-bucket", cacheKey) {
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
	// That version held the last reference to those bytes, so the shared cache
	// file goes with it.
	if tb.cache.Exists(ctx, "delete-data-version-bucket", cacheKey) {
		t.Fatal("cache file still exists after permanent delete")
	}

	// object_deletions is an append-only tombstone now; the record of the
	// deletion is the row itself, not a cleanup status on it.
	var tombstones int
	if err := tb.db.NewRaw(`SELECT COUNT(*) FROM object_deletions WHERE version_id = ?`, putOut.VersionID).Scan(ctx, &tombstones); err != nil {
		t.Fatalf("count object deletions: %v", err)
	}
	if tombstones != 1 {
		t.Fatalf("object deletion tombstones = %d, want 1", tombstones)
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
	if _, err := tb.db.NewRaw(`DELETE FROM tasks WHERE subject_type = ? AND subject_key = ?`, "object_version", putOut.VersionID).Exec(ctx); err != nil {
		t.Fatalf("remove upload task: %v", err)
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
				{Key: aws.String("")},
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
	if len(out.Error) != 2 {
		t.Fatalf("Error = %#v, want two entries", out.Error)
	}
	emptyKeyErr := out.Error[0]
	invalidArgument := s3err.InvalidArgumentError{Description: testInvalidArgumentDescription}.BaseError()
	if emptyKeyErr.Code == nil || *emptyKeyErr.Code != invalidArgument.Code {
		t.Fatalf("empty key error code = %v, want %q", emptyKeyErr.Code, invalidArgument.Code)
	}
	if emptyKeyErr.Message == nil || *emptyKeyErr.Message != invalidArgument.Description {
		t.Fatalf("empty key error message = %v, want %q", emptyKeyErr.Message, invalidArgument.Description)
	}

	entryErr := out.Error[1]
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

func TestCopyObjectMissingCopySourceUsesHeaderArgumentName(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.CopyObject(ctx, s3response.CopyObjectInput{
		Bucket: aws.String("dst-bucket"),
		Key:    aws.String("copied.txt"),
	})
	requireInvalidArgumentError(t, err, "x-amz-copy-source")
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

	// Ingest is scheduled per content and both copies carry the same bytes into
	// the same bucket, so the two versions share one plan.
	if first, second := contentSubjectForVersion(t, tb, obj1.VersionID), contentSubjectForVersion(t, tb, obj2.VersionID); first != second {
		t.Fatalf("identical copies resolved to contents %s and %s, want one", first, second)
	}
	taskCount, err := tb.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("type = ?", model.TaskTypeUploadPlan).
		Where("subject_type = ?", "storage_content").
		Where("subject_key = ?", contentSubjectForVersion(t, tb, obj2.VersionID)).
		Count(ctx)
	if err != nil {
		t.Fatalf("counting upload tasks: %v", err)
	}
	if taskCount != 1 {
		t.Fatalf("task count = %d, want 1", taskCount)
	}
}

func TestCopyObjectEnqueuesRegisteredUploadPlan(t *testing.T) {
	tb := newTestBackend(t)
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

	page, err := tb.repos.Tasks.List(ctx, repository.TaskListFilter{Type: model.TaskTypeUploadPlan, Status: model.TaskStatusPending, Limit: 10})
	if err != nil {
		t.Fatalf("List tasks: %v", err)
	}
	for _, task := range page.Tasks {
		if task.SubjectType != nil && task.SubjectKey != nil && *task.SubjectType == "storage_content" && *task.SubjectKey == contentSubjectForVersion(t, tb, dstObj.VersionID) {
			if task.RetryLimit == nil || *task.RetryLimit != 5 {
				t.Fatalf("copy upload task retry limit = %v, want 5", task.RetryLimit)
			}
			return
		}
	}
	t.Fatalf("copy upload task for object %d not found in %#v", dstObj.ObjectID, page.Tasks)
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

func TestRestoreObjectVersionCreatesNewCurrentAndPreservesHistory(t *testing.T) {
	type setupResult struct {
		sourceVersionID          string
		expectedCurrentVersionID string
		wantBody                 string
		historyVersionIDs        []string
	}
	tests := []struct {
		name  string
		setup func(*testing.T, *testBackend, string) setupResult
	}{
		{
			name: "non-current data version",
			setup: func(t *testing.T, tb *testBackend, bucketName string) setupResult {
				source := putValidTestObjectOutput(t, tb, bucketName, "file.txt", "old")
				current := putValidTestObjectOutput(t, tb, bucketName, "file.txt", "new")
				return setupResult{
					sourceVersionID:          source.VersionID,
					expectedCurrentVersionID: current.VersionID,
					wantBody:                 validTestObjectBody("old"),
					historyVersionIDs:        []string{source.VersionID, current.VersionID},
				}
			},
		},
		{
			name: "same data with different metadata",
			setup: func(t *testing.T, tb *testBackend, bucketName string) setupResult {
				put := func(metadata map[string]string) s3response.PutObjectOutput {
					body := validTestObjectBody("same-data")
					contentType := "text/plain"
					out, err := tb.backend.PutObject(context.Background(), s3response.PutObjectInput{
						Bucket:      aws.String(bucketName),
						Key:         aws.String("file.txt"),
						Body:        strings.NewReader(body),
						ContentType: &contentType,
						Metadata:    metadata,
					})
					if err != nil {
						t.Fatalf("PutObject: %v", err)
					}
					return out
				}
				source := put(map[string]string{"revision": "source"})
				current := put(map[string]string{"revision": "current"})
				return setupResult{
					sourceVersionID:          source.VersionID,
					expectedCurrentVersionID: current.VersionID,
					wantBody:                 validTestObjectBody("same-data"),
					historyVersionIDs:        []string{source.VersionID, current.VersionID},
				}
			},
		},
		{
			name: "data version hidden by delete marker",
			setup: func(t *testing.T, tb *testBackend, bucketName string) setupResult {
				source := putValidTestObjectOutput(t, tb, bucketName, "file.txt", "hidden")
				deleted, err := tb.backend.DeleteObject(context.Background(), &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String("file.txt"),
				})
				if err != nil {
					t.Fatalf("DeleteObject: %v", err)
				}
				if deleted.VersionId == nil || *deleted.VersionId == "" {
					t.Fatal("delete marker version ID is empty")
				}
				return setupResult{
					sourceVersionID:          source.VersionID,
					expectedCurrentVersionID: *deleted.VersionId,
					wantBody:                 validTestObjectBody("hidden"),
					historyVersionIDs:        []string{source.VersionID, *deleted.VersionId},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := newTestBackend(t)
			ctx := context.Background()
			bucketName := "restore-" + strings.ReplaceAll(tt.name, " ", "-")
			bucket := seedActiveBucket(t, tb, bucketName)
			setup := tt.setup(t, tb, bucketName)
			sourceBefore, err := tb.repos.Objects.GetVersionByID(ctx, setup.sourceVersionID)
			if err != nil || sourceBefore == nil {
				t.Fatalf("source before restore: version=%v err=%v", sourceBefore, err)
			}
			beforeCount, err := tb.db.NewSelect().
				Model((*model.ObjectVersion)(nil)).
				Where("bucket_id = ? AND key = ?", bucket.ID, "file.txt").
				Count(ctx)
			if err != nil {
				t.Fatalf("count before restore: %v", err)
			}

			versionID, err := tb.backend.RestoreObjectVersion(
				ctx,
				bucketName,
				"file.txt",
				setup.sourceVersionID,
				setup.expectedCurrentVersionID,
			)
			if err != nil {
				t.Fatalf("RestoreObjectVersion: %v", err)
			}
			if versionID == "" || versionID == setup.sourceVersionID {
				t.Fatalf("restored version ID = %q, want a new ID", versionID)
			}

			current, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bucket.ID, "file.txt")
			if err != nil || current == nil {
				t.Fatalf("current after restore: version=%v err=%v", current, err)
			}
			if current.VersionID != versionID {
				t.Fatalf("current version = %s, want %s", current.VersionID, versionID)
			}
			// A restore rewrites the same bytes, which resolve to the source's
			// content, so the two versions share one cache file rather than
			// each keeping a private copy.
			if current.CacheKey() != sourceBefore.CacheKey() || !tb.cache.Exists(ctx, bucketName, current.CacheKey()) {
				t.Fatalf("restored cache key = %q, source = %q, exists=%v", current.CacheKey(), sourceBefore.CacheKey(), tb.cache.Exists(ctx, bucketName, current.CacheKey()))
			}
			if current.ContentType != sourceBefore.ContentType {
				t.Fatalf("content type = %q, want %q", current.ContentType, sourceBefore.ContentType)
			}
			if !maps.Equal(current.Metadata, sourceBefore.Metadata) {
				t.Fatalf("metadata = %#v, want %#v", current.Metadata, sourceBefore.Metadata)
			}

			got, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucketName), Key: aws.String("file.txt")})
			if err != nil {
				t.Fatalf("GetObject restored: %v", err)
			}
			body, readErr := io.ReadAll(got.Body)
			closeErr := got.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("read restored body: read=%v close=%v", readErr, closeErr)
			}
			if string(body) != setup.wantBody {
				t.Fatalf("restored body = %q, want %q", string(body), setup.wantBody)
			}

			for _, historicalVersionID := range setup.historyVersionIDs {
				historical, err := tb.repos.Objects.GetVersionByID(ctx, historicalVersionID)
				if err != nil || historical == nil {
					t.Fatalf("historical version %s: version=%v err=%v", historicalVersionID, historical, err)
				}
				if historical.IsCurrent {
					t.Fatalf("historical version %s is still current", historicalVersionID)
				}
			}
			afterCount, err := tb.db.NewSelect().
				Model((*model.ObjectVersion)(nil)).
				Where("bucket_id = ? AND key = ?", bucket.ID, "file.txt").
				Count(ctx)
			if err != nil {
				t.Fatalf("count after restore: %v", err)
			}
			if afterCount != beforeCount+1 {
				t.Fatalf("version count = %d, want %d", afterCount, beforeCount+1)
			}
		})
	}
}

func TestRestoreObjectVersionRejectsAlreadyCurrent(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *testBackend, string) (sourceVersionID, currentVersionID string)
	}{
		{
			name: "current source",
			setup: func(t *testing.T, tb *testBackend, bucketName string) (string, string) {
				current := putValidTestObjectOutput(t, tb, bucketName, "file.txt", "current")
				return current.VersionID, current.VersionID
			},
		},
		{
			name: "historical source matching current",
			setup: func(t *testing.T, tb *testBackend, bucketName string) (string, string) {
				source := putValidTestObjectOutput(t, tb, bucketName, "file.txt", "same")
				current := putValidTestObjectOutput(t, tb, bucketName, "file.txt", "same")
				return source.VersionID, current.VersionID
			},
		},
		{
			name: "multipart source matching restored current",
			setup: func(t *testing.T, tb *testBackend, bucketName string) (string, string) {
				source := completeMultipartTestObject(t, tb, bucketName, "file.txt", []string{
					validTestObjectBody("multipart-source"),
				})
				current := putValidTestObjectOutput(t, tb, bucketName, "file.txt", "different")
				restoredID, err := tb.backend.RestoreObjectVersion(
					context.Background(),
					bucketName,
					"file.txt",
					source.VersionID,
					current.VersionID,
				)
				if err != nil {
					t.Fatalf("first RestoreObjectVersion: %v", err)
				}
				sourceVersion, err := tb.repos.Objects.GetVersionByID(context.Background(), source.VersionID)
				if err != nil || sourceVersion == nil {
					t.Fatalf("source version: version=%v err=%v", sourceVersion, err)
				}
				restoredVersion, err := tb.repos.Objects.GetVersionByID(context.Background(), restoredID)
				if err != nil || restoredVersion == nil {
					t.Fatalf("restored version: version=%v err=%v", restoredVersion, err)
				}
				if sourceVersion.ETag == restoredVersion.ETag || sourceVersion.Checksum != restoredVersion.Checksum {
					t.Fatalf(
						"source/restored identity = etag:%q/%q checksum:%q/%q, want different ETags and matching checksums",
						sourceVersion.ETag,
						restoredVersion.ETag,
						sourceVersion.Checksum,
						restoredVersion.Checksum,
					)
				}
				return source.VersionID, restoredID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := newTestBackend(t)
			ctx := context.Background()
			bucketName := "restore-already-current-" + strings.ReplaceAll(tt.name, " ", "-")
			bucket := seedActiveBucket(t, tb, bucketName)
			sourceVersionID, currentVersionID := tt.setup(t, tb, bucketName)
			usedBeforeRestore := tb.cache.UsedBytes()
			versionsBeforeRestore, err := tb.db.NewSelect().
				Model((*model.ObjectVersion)(nil)).
				Where("bucket_id = ? AND key = ?", bucket.ID, "file.txt").
				Count(ctx)
			if err != nil {
				t.Fatalf("count versions before restore: %v", err)
			}
			tasksBeforeRestore, err := tb.db.NewSelect().Model((*model.Task)(nil)).Count(ctx)
			if err != nil {
				t.Fatalf("count tasks before restore: %v", err)
			}

			_, err = tb.backend.RestoreObjectVersion(ctx, bucketName, "file.txt", sourceVersionID, currentVersionID)
			if !errors.Is(err, repository.ErrAlreadyCurrent) {
				t.Fatalf("error = %v, want ErrAlreadyCurrent", err)
			}
			if got := tb.cache.UsedBytes(); got != usedBeforeRestore {
				t.Fatalf("cache used bytes = %d, want %d after no-op restore", got, usedBeforeRestore)
			}
			versionsAfterRestore, err := tb.db.NewSelect().
				Model((*model.ObjectVersion)(nil)).
				Where("bucket_id = ? AND key = ?", bucket.ID, "file.txt").
				Count(ctx)
			if err != nil {
				t.Fatalf("count versions after restore: %v", err)
			}
			if versionsAfterRestore != versionsBeforeRestore {
				t.Fatalf("version count = %d, want %d after no-op restore", versionsAfterRestore, versionsBeforeRestore)
			}
			tasksAfterRestore, err := tb.db.NewSelect().Model((*model.Task)(nil)).Count(ctx)
			if err != nil {
				t.Fatalf("count tasks after restore: %v", err)
			}
			if tasksAfterRestore != tasksBeforeRestore {
				t.Fatalf("task count = %d, want %d after no-op restore", tasksAfterRestore, tasksBeforeRestore)
			}
		})
	}
}

func TestRestoreObjectVersionRejectsUnavailableSources(t *testing.T) {
	t.Run("existing but unreadable data version", func(t *testing.T) {
		tb := newTestBackend(t)
		ctx := context.Background()
		bucket := seedActiveBucket(t, tb, "restore-unreadable-source")
		versionID := "01J0000000000000000000BR00"
		unreadableSize := int64(len(validTestObjectBody("unreadable")))
		unreadableContent, err := tb.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
			BucketID:        bucket.ID,
			ContentSize:     unreadableSize,
			Checksum:        synaps3testutil.StorageChecksum("unreadable-checksum"),
			RequestedCopies: 1,
		})
		if err != nil {
			t.Fatalf("create unreadable content: %v", err)
		}
		if _, err := tb.repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
			VersionID:   versionID,
			BucketID:    bucket.ID,
			Key:         "file.txt",
			ContentID:   &unreadableContent.ID,
			Size:        unreadableSize,
			ETag:        "unreadable-etag",
			ContentType: "text/plain",
		}); err != nil {
			t.Fatalf("create unreadable version: %v", err)
		}
		current := putValidTestObjectOutput(t, tb, bucket.Name, "file.txt", "current")

		_, err = tb.backend.RestoreObjectVersion(ctx, bucket.Name, "file.txt", versionID, current.VersionID)
		if err == nil {
			t.Fatal("RestoreObjectVersion succeeded for unreadable source")
		}
		if errors.Is(err, repository.ErrNotFound) {
			t.Fatalf("error = %v, unreadable existing source must not map to ErrNotFound", err)
		}
		if !errors.Is(err, objectreader.ErrNoSuchVersion) {
			t.Fatalf("error = %v, want underlying source read error", err)
		}
	})

	t.Run("permanently deleted data version", func(t *testing.T) {
		tb := newTestBackend(t)
		ctx := context.Background()
		bucket := seedActiveBucket(t, tb, "restore-permanently-deleted")
		seed := func(versionID, body string) {
			payload := validTestObjectBody(body)
			sum := sha256.Sum256([]byte(payload))
			content, err := tb.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
				BucketID:        bucket.ID,
				ContentSize:     int64(len(payload)),
				Checksum:        hex.EncodeToString(sum[:]),
				RequestedCopies: 1,
			})
			if err != nil {
				t.Fatalf("ensure content: %v", err)
			}
			info, err := tb.cache.Put(ctx, bucket.Name, model.ContentCacheKey(content.ID), strings.NewReader(payload))
			if err != nil {
				t.Fatalf("cache Put: %v", err)
			}
			if _, err := tb.repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
				VersionID:   versionID,
				BucketID:    bucket.ID,
				Key:         "file.txt",
				ContentID:   &content.ID,
				Size:        info.Size,
				ETag:        info.ETag,
				ContentType: "text/plain",
			}); err != nil {
				t.Fatalf("create version: %v", err)
			}
		}
		sourceVersionID := "01J0000000000000000000BR01"
		currentVersionID := "01J0000000000000000000BR02"
		seed(sourceVersionID, "source")
		seed(currentVersionID, "current")
		if _, err := tb.repos.Objects.DeleteObjectVersionPermanently(ctx, repository.DeleteObjectVersionInput{
			BucketID:  bucket.ID,
			Key:       "file.txt",
			VersionID: sourceVersionID,
		}); err != nil {
			t.Fatalf("DeleteObjectVersionPermanently: %v", err)
		}

		_, err := tb.backend.RestoreObjectVersion(ctx, bucket.Name, "file.txt", sourceVersionID, currentVersionID)
		if !errors.Is(err, repository.ErrNotFound) {
			t.Fatalf("error = %v, want ErrNotFound", err)
		}
	})

	t.Run("delete marker", func(t *testing.T) {
		tb := newTestBackend(t)
		ctx := context.Background()
		bucket := seedActiveBucket(t, tb, "restore-delete-marker")
		putValidTestObject(t, tb, bucket.Name, "file.txt", "data")
		deleted, err := tb.backend.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket.Name), Key: aws.String("file.txt")})
		if err != nil {
			t.Fatalf("DeleteObject: %v", err)
		}
		if deleted.VersionId == nil {
			t.Fatal("delete marker version ID is nil")
		}

		_, err = tb.backend.RestoreObjectVersion(ctx, bucket.Name, "file.txt", *deleted.VersionId, *deleted.VersionId)
		if !errors.Is(err, repository.ErrConflict) {
			t.Fatalf("error = %v, want ErrConflict", err)
		}
	})
}

type currentChangingCache struct {
	cache.Cache
	once  sync.Once
	onGet func()
}

func (c *currentChangingCache) Get(ctx context.Context, bucket, key string) (io.ReadCloser, *cache.ObjectInfo, error) {
	body, info, err := c.Cache.Get(ctx, bucket, key)
	if err == nil {
		c.once.Do(c.onGet)
	}
	return body, info, err
}

type cancelingPutStagedCache struct {
	cache.Cache
	cancel context.CancelFunc
}

func (c *cancelingPutStagedCache) PutStaged(ctx context.Context, bucket, key string, body io.Reader) (*cache.StagedObject, error) {
	staged, err := c.Cache.PutStaged(ctx, bucket, key, body)
	if err == nil && c.cancel != nil {
		c.cancel()
	}
	return staged, err
}

func TestRestoreObjectVersionCancellationCleansCommittedCache(t *testing.T) {
	baseCache := newTestCache(t, 1<<30)
	cancelingCache := &cancelingPutStagedCache{Cache: baseCache}
	tb := newTestBackendWithCache(t, cancelingCache)
	ctx := context.Background()
	bucket := seedActiveBucket(t, tb, "restore-cancel-cleanup")
	source := putValidTestObjectOutput(t, tb, bucket.Name, "file.txt", "source")
	current := putValidTestObjectOutput(t, tb, bucket.Name, "file.txt", "current")

	usedBeforeRestore := baseCache.UsedBytes()
	versionsBeforeRestore, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("bucket_id = ? AND key = ?", bucket.ID, "file.txt").
		Count(ctx)
	if err != nil {
		t.Fatalf("count versions before restore: %v", err)
	}
	tasksBeforeRestore, err := tb.db.NewSelect().Model((*model.Task)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("count tasks before restore: %v", err)
	}

	restoreCtx, cancel := context.WithCancel(ctx)
	cancelingCache.cancel = cancel
	_, err = tb.backend.RestoreObjectVersion(restoreCtx, bucket.Name, "file.txt", source.VersionID, current.VersionID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := baseCache.UsedBytes(); got != usedBeforeRestore {
		t.Fatalf("cache used bytes = %d, want %d after canceled restore cleanup", got, usedBeforeRestore)
	}
	versionsAfterRestore, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("bucket_id = ? AND key = ?", bucket.ID, "file.txt").
		Count(ctx)
	if err != nil {
		t.Fatalf("count versions after restore: %v", err)
	}
	if versionsAfterRestore != versionsBeforeRestore {
		t.Fatalf("version count = %d, want %d after canceled restore", versionsAfterRestore, versionsBeforeRestore)
	}
	tasksAfterRestore, err := tb.db.NewSelect().Model((*model.Task)(nil)).Count(ctx)
	if err != nil {
		t.Fatalf("count tasks after restore: %v", err)
	}
	if tasksAfterRestore != tasksBeforeRestore {
		t.Fatalf("task count = %d, want %d after canceled restore", tasksAfterRestore, tasksBeforeRestore)
	}
}

func TestRestoreObjectVersionCASConflictCleansCommittedCache(t *testing.T) {
	baseCache := newTestCache(t, 1<<30)
	changingCache := &currentChangingCache{Cache: baseCache}
	tb := newTestBackendWithCache(t, changingCache)
	ctx := context.Background()
	bucket := seedActiveBucket(t, tb, "restore-cas-cleanup")
	source := putValidTestObjectOutput(t, tb, bucket.Name, "file.txt", "source")
	current := putValidTestObjectOutput(t, tb, bucket.Name, "file.txt", "current")

	var usedAfterConcurrentWrite int64
	changingCache.onGet = func() {
		versionID := model.NewVersionID()
		payload := validTestObjectBody("concurrent")
		sum := sha256.Sum256([]byte(payload))
		content, err := tb.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
			BucketID:        bucket.ID,
			ContentSize:     int64(len(payload)),
			Checksum:        hex.EncodeToString(sum[:]),
			RequestedCopies: 1,
		})
		if err != nil {
			t.Fatalf("ensure concurrent content: %v", err)
		}
		info, err := baseCache.Put(ctx, bucket.Name, model.ContentCacheKey(content.ID), strings.NewReader(payload))
		if err != nil {
			t.Fatalf("cache concurrent version: %v", err)
		}
		if _, err := tb.repos.Objects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
			VersionID:   versionID,
			BucketID:    bucket.ID,
			Key:         "file.txt",
			ContentID:   &content.ID,
			Size:        info.Size,
			ETag:        info.ETag,
			ContentType: "text/plain",
		}); err != nil {
			t.Fatalf("create concurrent version: %v", err)
		}
		usedAfterConcurrentWrite = baseCache.UsedBytes()
	}

	_, err := tb.backend.RestoreObjectVersion(ctx, bucket.Name, "file.txt", source.VersionID, current.VersionID)
	if !errors.Is(err, repository.ErrConflict) {
		t.Fatalf("error = %v, want ErrConflict", err)
	}
	if got := baseCache.UsedBytes(); got != usedAfterConcurrentWrite {
		t.Fatalf("cache used bytes = %d, want %d after failed restore cleanup", got, usedAfterConcurrentWrite)
	}
	versionCount, err := tb.db.NewSelect().
		Model((*model.ObjectVersion)(nil)).
		Where("bucket_id = ? AND key = ?", bucket.ID, "file.txt").
		Count(ctx)
	if err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if versionCount != 3 {
		t.Fatalf("version count = %d, want source + old current + concurrent current", versionCount)
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
			sum := sha256.Sum256([]byte("new"))
			content, err := tb.repos.Contents.EnsureContent(ctx, repository.EnsureContentInput{
				BucketID:        srcBkt.ID,
				ContentSize:     int64(len("new")),
				Checksum:        hex.EncodeToString(sum[:]),
				RequestedCopies: 1,
			})
			if err != nil {
				hookErr = err
				return
			}
			info, err := tb.cache.Put(ctx, "copy-implicit-race-src", model.ContentCacheKey(content.ID), strings.NewReader("new"))
			if err != nil {
				hookErr = err
				return
			}
			_, hookErr = baseObjects.CreateVersionAndSetCurrent(ctx, &model.ObjectVersion{
				VersionID:   newVersionID,
				BucketID:    srcBkt.ID,
				Key:         "original.txt",
				ContentID:   &content.ID,
				Size:        info.Size,
				ETag:        info.ETag,
				ContentType: "text/plain",
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
