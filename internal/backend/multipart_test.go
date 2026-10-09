package backend_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	synaps3testutil "github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/chain"
	"github.com/uptrace/bun"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// ---------- CreateMultipartUpload ----------

func TestCreateMultipartUpload_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "mp-bucket")

	ct := "application/octet-stream"
	result, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket:      aws.String("mp-bucket"),
		Key:         aws.String("big-file.bin"),
		ContentType: &ct,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if result.UploadId == "" {
		t.Error("expected non-empty UploadId")
	}
	if result.Bucket != "mp-bucket" {
		t.Errorf("bucket = %q, want mp-bucket", result.Bucket)
	}
	if result.Key != "big-file.bin" {
		t.Errorf("key = %q, want big-file.bin", result.Key)
	}
}

// ---------- UploadPart ----------

func TestMultipartOperationsRejectNilInputsWithInvalidArgument(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "UploadPart",
			run: func() error {
				_, err := tb.backend.UploadPart(ctx, nil)
				return err
			},
		},
		{
			name: "UploadPartCopy",
			run: func() error {
				_, err := tb.backend.UploadPartCopy(ctx, nil)
				return err
			},
		},
		{
			name: "CompleteMultipartUpload",
			run: func() error {
				_, _, err := tb.backend.CompleteMultipartUpload(ctx, nil)
				return err
			},
		},
		{
			name: "AbortMultipartUpload",
			run: func() error {
				return tb.backend.AbortMultipartUpload(ctx, nil)
			},
		},
		{
			name: "ListParts",
			run: func() error {
				_, err := tb.backend.ListParts(ctx, nil)
				return err
			},
		},
		{
			name: "ListMultipartUploads nil input",
			run: func() error {
				_, err := tb.backend.ListMultipartUploads(ctx, nil)
				return err
			},
		},
		{
			name: "ListMultipartUploads nil bucket",
			run: func() error {
				_, err := tb.backend.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{})
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

func TestUploadPart_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "up-bucket")

	ct := "application/octet-stream"
	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket:      aws.String("up-bucket"),
		Key:         aws.String("parts.bin"),
		ContentType: &ct,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	partNum := int32(1)
	partBody := "part-1-data"
	partOut, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("up-bucket"),
		Key:        aws.String("parts.bin"),
		UploadId:   aws.String(initResult.UploadId),
		PartNumber: &partNum,
		Body:       strings.NewReader(partBody),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	if partOut.ETag == nil || *partOut.ETag == "" {
		t.Error("expected non-empty part ETag")
	}

	// Verify part recorded in DB.
	parts, err := tb.repos.Multiparts.GetParts(ctx, initResult.UploadId, 0, 100)
	if err != nil {
		t.Fatalf("GetParts: %v", err)
	}
	if len(parts) != 1 {
		t.Errorf("parts count = %d, want 1", len(parts))
	}
	if len(parts) > 0 && parts[0].PartNumber != 1 {
		t.Errorf("part number = %d, want 1", parts[0].PartNumber)
	}
	if len(parts) > 0 && (parts[0].Checksum == nil || *parts[0].Checksum != testSHA256Hex(partBody)) {
		t.Fatalf("part checksum = %v, want %s", parts[0].Checksum, testSHA256Hex(partBody))
	}
}

func TestUploadPartRejectsDeclaredLengthAboveObjectLimit(t *testing.T) {
	// The cache cannot hold the declared length, yet the part is reported as too
	// large rather than as a full cache a retry could clear.
	tb := newTestBackendWithCache(t, newTestCache(t, 400))
	ctx := context.Background()
	seedActiveBucket(t, tb, "big-part-bucket")
	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: aws.String("big-part-bucket"),
		Key:    aws.String("parts.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	partNum := int32(1)
	_, err = tb.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String("big-part-bucket"),
		Key:           aws.String("parts.bin"),
		UploadId:      aws.String(initResult.UploadId),
		PartNumber:    &partNum,
		Body:          unreadObjectBody{t},
		ContentLength: ptrInt64(chain.MaxUploadSize + 1),
	})
	requireAPIErrorCode(t, err, s3err.GetAPIError(s3err.ErrEntityTooLarge))
}

func TestUploadPartCopyMissingCopySourceUsesHeaderArgumentName(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	partNum := int32(1)

	_, err := tb.backend.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:     aws.String("bucket"),
		Key:        aws.String("key"),
		UploadId:   aws.String("upload-id"),
		PartNumber: &partNum,
	})
	requireInvalidArgumentError(t, err, "x-amz-copy-source")
}

func TestUploadPartCopy_CopySourceVersionIDCopiesSpecifiedVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "up-copy-version-bucket")

	firstOut := putValidTestObjectOutput(t, tb, "up-copy-version-bucket", "source.txt", "old")
	putValidTestObject(t, tb, "up-copy-version-bucket", "source.txt", "new")

	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: aws.String("up-copy-version-bucket"),
		Key:    aws.String("copied.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	partNum := int32(1)
	partOut, err := tb.backend.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:     aws.String("up-copy-version-bucket"),
		Key:        aws.String("copied.bin"),
		UploadId:   aws.String(initResult.UploadId),
		PartNumber: &partNum,
		CopySource: aws.String("/up-copy-version-bucket/source.txt?versionId=" + firstOut.VersionID),
	})
	if err != nil {
		t.Fatalf("UploadPartCopy: %v", err)
	}
	if partOut.CopySourceVersionId != firstOut.VersionID {
		t.Fatalf("CopySourceVersionId = %q, want %s", partOut.CopySourceVersionId, firstOut.VersionID)
	}
	parts, err := tb.repos.Multiparts.GetParts(ctx, initResult.UploadId, 0, 100)
	if err != nil {
		t.Fatalf("GetParts after UploadPartCopy: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("copied parts = %d, want 1", len(parts))
	}
	if parts[0].Checksum == nil || *parts[0].Checksum != testSHA256Hex(validTestObjectBody("old")) {
		t.Fatalf("copied part checksum = %v, want %s", parts[0].Checksum, testSHA256Hex(validTestObjectBody("old")))
	}

	_, versionID, err := tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("up-copy-version-bucket"),
		Key:      aws.String("copied.bin"),
		UploadId: aws.String(initResult.UploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{PartNumber: &partNum, ETag: partOut.ETag}},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if versionID == "" {
		t.Fatal("expected complete multipart version ID")
	}

	out, err := tb.backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("up-copy-version-bucket"),
		Key:    aws.String("copied.bin"),
	})
	if err != nil {
		t.Fatalf("GetObject copied part: %v", err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read copied body: %v", err)
	}
	if want := validTestObjectBody("old"); string(body) != want {
		t.Fatalf("copied body = %q, want %q", string(body), want)
	}
}

// ---------- CompleteMultipartUpload ----------

func TestCompleteMultipartUpload_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "cmp-bucket")

	ct := "application/octet-stream"
	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket:      aws.String("cmp-bucket"),
		Key:         aws.String("assembled.bin"),
		ContentType: &ct,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	uploadID := initResult.UploadId

	// Upload 2 parts.
	var partETags [2]string
	for i := int32(1); i <= 2; i++ {
		partBody := fmt.Sprintf("part-%d-data", i)
		if i == 1 {
			partBody = validTestObjectBody(partBody)
		}
		partOut, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String("cmp-bucket"),
			Key:        aws.String("assembled.bin"),
			UploadId:   aws.String(uploadID),
			PartNumber: &i,
			Body:       strings.NewReader(partBody),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i, err)
		}
		partETags[i-1] = *partOut.ETag
	}

	// Complete.
	pn1, pn2 := int32(1), int32(2)
	completeResult, versionID, err := tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("cmp-bucket"),
		Key:      aws.String("assembled.bin"),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{PartNumber: &pn1, ETag: &partETags[0]},
				{PartNumber: &pn2, ETag: &partETags[1]},
			},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if completeResult.ETag == nil || *completeResult.ETag == "" {
		t.Error("expected non-empty ETag in complete result")
	}
	if versionID == "" {
		t.Error("expected non-empty version ID")
	}

	// Verify object created in DB.
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "cmp-bucket")
	obj, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "assembled.bin")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if obj == nil {
		t.Fatal("assembled object not found in DB")
	}
	if obj.State != model.ObjectStateCached {
		t.Errorf("object state = %q, want %q", obj.State, model.ObjectStateCached)
	}
	if obj.MultipartUploadID == nil || *obj.MultipartUploadID != uploadID {
		t.Fatalf("object multipart_upload_id = %v, want %s", obj.MultipartUploadID, uploadID)
	}
	task, err := tb.repos.Tasks.ClaimNext(ctx, time.Minute, repository.TaskClaimFilter{})
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if task == nil {
		t.Fatal("expected upload task")
	}
	policy, policyErr := worker.DecodePolicy(task)
	if task.Type != model.TaskTypeUploadPlan || policyErr != nil || policy.MaxAttempts != 6 {
		t.Fatalf("task = %#v, policy = %#v, err = %v; want upload_plan with six attempts", task, policy, policyErr)
	}
}

func TestCompleteMultipartUploadRejectsFOCSizeBelowMinimum(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "cmp-small-bucket")

	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: aws.String("cmp-small-bucket"),
		Key:    aws.String("small.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	partNum := int32(1)
	partOut, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("cmp-small-bucket"),
		Key:        aws.String("small.bin"),
		UploadId:   aws.String(initResult.UploadId),
		PartNumber: &partNum,
		Body:       strings.NewReader(strings.Repeat("a", chain.MinUploadSize-1)),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	_, _, err = tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String("cmp-small-bucket"),
		Key:      aws.String("small.bin"),
		UploadId: aws.String(initResult.UploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{PartNumber: &partNum, ETag: partOut.ETag}},
		},
	})
	assertS3ErrorCode(t, err, s3err.ErrEntityTooSmall)
}

func TestCompleteMultipartUploadRejectsFOCSizeAboveMaximum(t *testing.T) {
	assembleCalled := false
	mc := &synaps3testutil.MockCache{
		AssemblePartsFunc: func(_ context.Context, _, _, _ string, _ []int) (*cache.StagedObject, []string, error) {
			assembleCalled = true
			return nil, nil, errors.New("assemble must not run for an oversize object")
		},
	}
	tb := newTestBackendWithMockCache(t, mc)
	ctx := context.Background()
	bucket := seedActiveBucket(t, tb, "cmp-large-bucket")
	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: aws.String(bucket.Name),
		Key:    aws.String("large.bin"),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	partNum := int32(1)
	etag := strings.Repeat("b", 32)
	if err := tb.repos.Multiparts.CreatePart(ctx, &model.MultipartPart{
		UploadID:   initResult.UploadId,
		PartNumber: 1,
		Size:       chain.MaxUploadSize + 1,
		ETag:       etag,
	}); err != nil {
		t.Fatalf("CreatePart: %v", err)
	}

	_, _, err = tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket.Name),
		Key:      aws.String("large.bin"),
		UploadId: aws.String(initResult.UploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{PartNumber: &partNum, ETag: &etag}},
		},
	})
	assertS3ErrorCode(t, err, s3err.ErrEntityTooLarge)
	if assembleCalled {
		t.Fatal("AssembleParts was called for object above FOC maximum")
	}
}

func TestCompleteMultipartUploadIdenticalCurrentObjectCreatesNewVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "cmp-dedupe-bucket")

	completeOnePart := func() string {
		t.Helper()
		initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
			Bucket: aws.String("cmp-dedupe-bucket"),
			Key:    aws.String("assembled.bin"),
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}

		partNum := int32(1)
		partOut, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String("cmp-dedupe-bucket"),
			Key:        aws.String("assembled.bin"),
			UploadId:   aws.String(initResult.UploadId),
			PartNumber: &partNum,
			Body:       strings.NewReader(validTestObjectBody("same multipart data")),
		})
		if err != nil {
			t.Fatalf("UploadPart: %v", err)
		}

		_, versionID, err := tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String("cmp-dedupe-bucket"),
			Key:      aws.String("assembled.bin"),
			UploadId: aws.String(initResult.UploadId),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{{PartNumber: &partNum, ETag: partOut.ETag}},
			},
		})
		if err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
		if versionID == "" {
			t.Fatal("expected complete multipart version ID")
		}
		return versionID
	}

	firstVersionID := completeOnePart()

	bkt, _ := tb.repos.Buckets.GetByName(ctx, "cmp-dedupe-bucket")
	obj1, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "assembled.bin")
	if err != nil || obj1 == nil {
		t.Fatalf("current object after first complete: obj=%v err=%v", obj1, err)
	}

	secondVersionID := completeOnePart()
	if secondVersionID == firstVersionID {
		t.Fatalf("second version id = %s, want different from first", secondVersionID)
	}

	obj2, err := tb.repos.Objects.GetCurrentVersionByBucketAndKey(ctx, bkt.ID, "assembled.bin")
	if err != nil || obj2 == nil {
		t.Fatalf("current object after second complete: obj=%v err=%v", obj2, err)
	}
	if obj2.VersionID == obj1.VersionID {
		t.Fatalf("current version did not change for identical multipart complete: %s", obj2.VersionID)
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

	secondVersion, err := tb.repos.Objects.GetVersionByID(ctx, secondVersionID)
	if err != nil || secondVersion == nil {
		t.Fatalf("second version: version=%v err=%v", secondVersion, err)
	}
	// Both versions share one content whose ingest plan has not produced a copy
	// yet, so the derived position is still cached.
	if secondVersion.State != model.ObjectStateCached {
		t.Fatalf("second version state = %s, want cached", secondVersion.State)
	}
}

// uploadMultipartTestParts starts an upload and uploads one part per body,
// returning the upload ID and the parts as a completion would name them.
func uploadMultipartTestParts(t *testing.T, tb *testBackend, bucket, key string, bodies []string) (string, []types.CompletedPart) {
	t.Helper()
	ctx := context.Background()
	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload(%s): %v", key, err)
	}
	parts := make([]types.CompletedPart, 0, len(bodies))
	for i, body := range bodies {
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
		parts = append(parts, types.CompletedPart{PartNumber: &partNumber, ETag: partOut.ETag})
	}
	return initResult.UploadId, parts
}

func completeMultipartTestParts(ctx context.Context, tb *testBackend, bucket, key, uploadID string, parts []types.CompletedPart) error {
	_, _, err := tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	return err
}

// Parts a completion leaves out are not part of the object, so its attributes
// must not list them.
func TestCompleteMultipartUploadDropsPartsItLeavesOut(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "partial-parts-bucket")
	uploadID, parts := uploadMultipartTestParts(t, tb, "partial-parts-bucket", "partial.bin", []string{
		validTestObjectBody("part-one"), "part-two", "part-three",
	})
	if err := completeMultipartTestParts(ctx, tb, "partial-parts-bucket", "partial.bin", uploadID, []types.CompletedPart{parts[0], parts[2]}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	out, err := tb.backend.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket:           aws.String("partial-parts-bucket"),
		Key:              aws.String("partial.bin"),
		ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesObjectParts},
	})
	if err != nil {
		t.Fatalf("GetObjectAttributes: %v", err)
	}
	if out.ObjectParts == nil {
		t.Fatal("ObjectParts = nil, want the assembled parts")
	}
	if len(out.ObjectParts.Parts) != 2 {
		t.Fatalf("ObjectParts lists %d parts, want only the 2 assembled", len(out.ObjectParts.Parts))
	}
	var total int64
	for i, want := range []int32{1, 3} {
		part := out.ObjectParts.Parts[i]
		if part.PartNumber == nil || *part.PartNumber != want || part.Size == nil {
			t.Fatalf("part %d = %#v, want part number %d", i, part, want)
		}
		total += *part.Size
	}
	if out.ObjectSize == nil || total != *out.ObjectSize {
		t.Fatalf("part sizes sum to %d, want object size %v", total, out.ObjectSize)
	}
}

// A process that stops during a completion leaves the upload completing. The
// next start releases it, so the client can complete the same upload.
func TestInterruptedMultipartCompletionCanFinishAfterRestart(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "interrupted-bucket")
	uploadID, parts := uploadMultipartTestParts(t, tb, "interrupted-bucket", "interrupted.bin", []string{
		validTestObjectBody("part-one"), "part-two",
	})
	if err := tb.repos.Multiparts.SetStatus(ctx, uploadID, model.MultipartStatusInitiated, model.MultipartStatusCompleting); err != nil {
		t.Fatalf("leave upload completing: %v", err)
	}

	released, err := tb.backend.ReleaseInterruptedMultipartCompletions(ctx)
	if err != nil || released != 1 {
		t.Fatalf("ReleaseInterruptedMultipartCompletions = %d, %v, want 1 upload", released, err)
	}
	if err := completeMultipartTestParts(ctx, tb, "interrupted-bucket", "interrupted.bin", uploadID, parts); err != nil {
		t.Fatalf("CompleteMultipartUpload after restart: %v", err)
	}
}

// A completion whose request is canceled, as happens when the server shuts
// down, still releases the upload instead of leaving it completing.
func TestCanceledMultipartCompletionReleasesTheUpload(t *testing.T) {
	tb := newTestBackend(t)
	seedActiveBucket(t, tb, "canceled-complete-bucket")
	uploadID, parts := uploadMultipartTestParts(t, tb, "canceled-complete-bucket", "canceled.bin", []string{
		validTestObjectBody("part-one"), "part-two",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb.db.AddQueryHook(cancelAfterQuery{match: "'completing'", cancel: cancel})

	if err := completeMultipartTestParts(ctx, tb, "canceled-complete-bucket", "canceled.bin", uploadID, parts); err == nil {
		t.Fatal("CompleteMultipartUpload succeeded after its request was canceled")
	}
	upload, err := tb.repos.Multiparts.GetByUploadID(context.Background(), uploadID)
	if err != nil || upload == nil {
		t.Fatalf("GetByUploadID = %#v, %v", upload, err)
	}
	if upload.Status != model.MultipartStatusInitiated {
		t.Fatalf("upload status = %s, want %s", upload.Status, model.MultipartStatusInitiated)
	}
}

// cancelAfterQuery cancels a request once a query containing match has run.
type cancelAfterQuery struct {
	match  string
	cancel context.CancelFunc
}

func (cancelAfterQuery) BeforeQuery(ctx context.Context, _ *bun.QueryEvent) context.Context {
	return ctx
}

func (h cancelAfterQuery) AfterQuery(_ context.Context, event *bun.QueryEvent) {
	if strings.Contains(event.Query, h.match) {
		h.cancel()
	}
}

// ---------- AbortMultipartUpload ----------

func TestAbortMultipartUpload_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "abort-bucket")

	ct := "application/octet-stream"
	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket:      aws.String("abort-bucket"),
		Key:         aws.String("aborted.bin"),
		ContentType: &ct,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	// Upload a part.
	pn := int32(1)
	_, err = tb.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("abort-bucket"),
		Key:        aws.String("aborted.bin"),
		UploadId:   aws.String(initResult.UploadId),
		PartNumber: &pn,
		Body:       strings.NewReader("part data"),
	})
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	// Abort.
	err = tb.backend.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String("abort-bucket"),
		Key:      aws.String("aborted.bin"),
		UploadId: aws.String(initResult.UploadId),
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	// Verify upload status is "aborted" — trying to use the upload should fail.
	pn2 := int32(2)
	_, err = tb.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("abort-bucket"),
		Key:        aws.String("aborted.bin"),
		UploadId:   aws.String(initResult.UploadId),
		PartNumber: &pn2,
		Body:       strings.NewReader("more data"),
	})
	if err == nil {
		t.Error("expected error uploading part to aborted upload")
	}
}

// ---------- ListMultipartUploads ----------

func TestListMultipartUploads_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "lmu-bucket")

	ct := "application/octet-stream"
	for i := range 3 {
		_, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
			Bucket:      aws.String("lmu-bucket"),
			Key:         aws.String(fmt.Sprintf("file-%d.bin", i)),
			ContentType: &ct,
		})
		if err != nil {
			t.Fatalf("CreateMultipartUpload %d: %v", i, err)
		}
	}

	result, err := tb.backend.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String("lmu-bucket"),
	})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(result.Uploads) != 3 {
		t.Errorf("uploads count = %d, want 3", len(result.Uploads))
	}
}

// ---------- ListParts ----------

func TestListParts_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "lp-bucket")

	ct := "application/octet-stream"
	initResult, err := tb.backend.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket:      aws.String("lp-bucket"),
		Key:         aws.String("parts-file.bin"),
		ContentType: &ct,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	for i := int32(1); i <= 3; i++ {
		_, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String("lp-bucket"),
			Key:        aws.String("parts-file.bin"),
			UploadId:   aws.String(initResult.UploadId),
			PartNumber: &i,
			Body:       strings.NewReader(fmt.Sprintf("part-%d", i)),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i, err)
		}
	}

	result, err := tb.backend.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String("lp-bucket"),
		Key:      aws.String("parts-file.bin"),
		UploadId: aws.String(initResult.UploadId),
	})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(result.Parts) != 3 {
		t.Errorf("parts count = %d, want 3", len(result.Parts))
	}
	// Verify ascending order.
	for i, p := range result.Parts {
		if p.PartNumber != i+1 {
			t.Errorf("part[%d].PartNumber = %d, want %d", i, p.PartNumber, i+1)
		}
	}

	maxParts := int32(0)
	zeroMaxResult, err := tb.backend.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   aws.String("lp-bucket"),
		Key:      aws.String("parts-file.bin"),
		UploadId: aws.String(initResult.UploadId),
		MaxParts: &maxParts,
	})
	if err != nil {
		t.Fatalf("ListParts MaxParts=0: %v", err)
	}
	if len(zeroMaxResult.Parts) != 0 {
		t.Fatalf("ListParts MaxParts=0 parts count = %d, want 0", len(zeroMaxResult.Parts))
	}
	if !zeroMaxResult.IsTruncated {
		t.Fatal("ListParts MaxParts=0 IsTruncated = false, want true")
	}
	if zeroMaxResult.NextPartNumberMarker != 0 {
		t.Fatalf("ListParts MaxParts=0 NextPartNumberMarker = %d, want 0", zeroMaxResult.NextPartNumberMarker)
	}
}
