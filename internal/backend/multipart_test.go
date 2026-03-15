package backend_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
	completeResult, _, err := tb.backend.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
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

	// Verify object created in DB.
	bkt, _ := tb.repos.Buckets.GetByName(ctx, "cmp-bucket")
	obj, err := tb.repos.Objects.GetByBucketAndKey(ctx, bkt.ID, "assembled.bin")
	if err != nil {
		t.Fatalf("GetByBucketAndKey: %v", err)
	}
	if obj == nil {
		t.Fatal("assembled object not found in DB")
	}
	if obj.State != model.ObjectStateCached {
		t.Errorf("object state = %q, want %q", obj.State, model.ObjectStateCached)
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
