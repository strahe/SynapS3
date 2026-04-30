package backend_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	synaps3backend "github.com/strahe/synaps3/internal/backend"
	"github.com/strahe/synaps3/internal/model"
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
	partOut, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String("up-bucket"),
		Key:        aws.String("parts.bin"),
		UploadId:   aws.String(initResult.UploadId),
		PartNumber: &partNum,
		Body:       strings.NewReader("part-1-data"),
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
}

func TestUploadPartCopy_CopySourceVersionIDCopiesSpecifiedVersion(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedActiveBucket(t, tb, "up-copy-version-bucket")

	firstOut := putTestObjectOutput(t, tb, "up-copy-version-bucket", "source.txt", "old")
	putTestObject(t, tb, "up-copy-version-bucket", "source.txt", "new")

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
	if string(body) != "old" {
		t.Fatalf("copied body = %q, want old", string(body))
	}
}

// ---------- CompleteMultipartUpload ----------

func TestCompleteMultipartUpload_HappyPath(t *testing.T) {
	tb := newTestBackendWithOptions(t, synaps3backend.WithUploadMaxRetries(12))
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
		partOut, err := tb.backend.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String("cmp-bucket"),
			Key:        aws.String("assembled.bin"),
			UploadId:   aws.String(uploadID),
			PartNumber: &i,
			Body:       strings.NewReader(fmt.Sprintf("part-%d-data", i)),
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
	task, err := tb.repos.Tasks.ClaimPending(ctx, model.TaskTypeUpload, time.Minute)
	if err != nil {
		t.Fatalf("ClaimPending: %v", err)
	}
	if task == nil {
		t.Fatal("expected upload task")
	}
	if task.MaxRetries != 12 {
		t.Fatalf("task MaxRetries = %d, want 12", task.MaxRetries)
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
			Body:       strings.NewReader("same multipart data"),
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
		Where("ref_type = ? AND ref_id = ?", "object", obj1.ObjectID).
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
	if secondVersion.State != model.ObjectStateUploading {
		t.Fatalf("second version state = %s, want uploading", secondVersion.State)
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
	for i := 0; i < 3; i++ {
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
}
