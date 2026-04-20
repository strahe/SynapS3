package backend_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func TestCreateBucket_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	err := tb.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("my-bucket"),
	}, nil)
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Verify bucket in DB with status=creating.
	bucket, err := tb.repos.Buckets.GetByName(ctx, "my-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if bucket == nil {
		t.Fatal("bucket not found in DB")
	}
	if bucket.Status != model.BucketStatusActive {
		t.Errorf("bucket status = %q, want %q", bucket.Status, model.BucketStatusActive)
	}
}

func TestCreateBucket_NilInput(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	err := tb.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: nil,
	}, nil)
	if err == nil {
		t.Fatal("expected error for nil bucket name")
	}

	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrInvalidBucketName)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestCreateBucket_DuplicateKey(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	err := tb.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("dup-bucket"),
	}, nil)
	if err != nil {
		t.Fatalf("first CreateBucket: %v", err)
	}

	err = tb.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("dup-bucket"),
	}, nil)
	if err == nil {
		t.Fatal("expected error for duplicate bucket")
	}

	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrBucketAlreadyExists)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestHeadBucket_Exists(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	// Seed an active bucket directly via repos.
	bkt := &model.Bucket{Name: "head-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	out, err := tb.backend.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("head-bucket"),
	})
	if err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

func TestHeadBucket_NotFound(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("no-such-bucket"),
	})
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}

	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrNoSuchBucket)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestDeleteBucket_NotSupported(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "del-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.DeleteBucket(ctx, "del-bucket")
	if err == nil {
		t.Fatal("expected error from DeleteBucket (not supported)")
	}
}

func TestListBuckets_Empty(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	result, err := tb.backend.ListBuckets(ctx, s3response.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(result.Buckets.Bucket) != 0 {
		t.Errorf("expected 0 buckets, got %d", len(result.Buckets.Bucket))
	}
}

func TestListBuckets_OnlyActive(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	// Seed active buckets.
	for _, name := range []string{"active-1", "active-2", "active-3"} {
		b := &model.Bucket{Name: name, Status: model.BucketStatusActive}
		if err := tb.repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("seeding bucket %q: %v", name, err)
		}
	}

	result, err := tb.backend.ListBuckets(ctx, s3response.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}

	names := make(map[string]bool)
	for _, entry := range result.Buckets.Bucket {
		names[entry.Name] = true
	}

	for _, want := range []string{"active-1", "active-2", "active-3"} {
		if !names[want] {
			t.Errorf("expected bucket %q in list", want)
		}
	}
}
