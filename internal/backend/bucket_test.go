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
	if bucket.Status != model.BucketStatusCreating {
		t.Errorf("bucket status = %q, want %q", bucket.Status, model.BucketStatusCreating)
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

func TestHeadBucket_InvisibleStatus(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	// Seed a bucket with "deleted" status — invisible to S3 clients.
	bkt := &model.Bucket{Name: "deleted-bucket", Status: model.BucketStatusDeleted}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	_, err := tb.backend.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String("deleted-bucket"),
	})
	if err == nil {
		t.Fatal("expected error for invisible bucket")
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

func TestDeleteBucket_HappyPath(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "del-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.DeleteBucket(ctx, "del-bucket")
	if err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	// Verify status transitioned to deleting.
	updated, err := tb.repos.Buckets.GetByName(ctx, "del-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("bucket not found after deletion")
	}
	if updated.Status != model.BucketStatusDeleting {
		t.Errorf("bucket status = %q, want %q", updated.Status, model.BucketStatusDeleting)
	}
}

func TestDeleteBucket_NotActive(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "deleting-bucket", Status: model.BucketStatusDeleting}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.DeleteBucket(ctx, "deleting-bucket")
	if err == nil {
		t.Fatal("expected error deleting non-writable bucket")
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

func TestDeleteBucket_CreatingBucketRejected(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "creating-bucket", Status: model.BucketStatusCreating}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.DeleteBucket(ctx, "creating-bucket")
	if err == nil {
		t.Fatal("expected error deleting creating bucket")
	}

	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrNoSuchBucket)
	if apiErr.Code != want.Code {
		t.Fatalf("error code = %q, want %q", apiErr.Code, want.Code)
	}

	updated, err := tb.repos.Buckets.GetByName(ctx, "creating-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if updated == nil {
		t.Fatal("bucket should remain present")
	}
	if updated.Status != model.BucketStatusCreating {
		t.Errorf("bucket status = %q, want %q", updated.Status, model.BucketStatusCreating)
	}
}

func TestDeleteBucket_NotEmpty(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "notempty-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	// Insert an object so the bucket is not empty.
	obj := &model.Object{
		BucketID:    bkt.ID,
		Key:         "some-file.txt",
		Size:        10,
		ETag:        "abc123",
		Checksum:    "sha256hex",
		ContentType: "text/plain",
		CachePath:   "/fake/path",
		State:       model.ObjectStateCached,
	}
	if _, _, err := tb.repos.Objects.UpsertAndBumpGeneration(ctx, obj); err != nil {
		t.Fatalf("seeding object: %v", err)
	}

	err := tb.backend.DeleteBucket(ctx, "notempty-bucket")
	if err == nil {
		t.Fatal("expected error deleting non-empty bucket")
	}

	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrBucketNotEmpty)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
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

	// Seed buckets with various statuses.
	for _, tc := range []struct {
		name   string
		status model.BucketStatus
	}{
		{"active-1", model.BucketStatusActive},
		{"active-2", model.BucketStatusActive},
		{"creating-1", model.BucketStatusCreating},
		{"deleting-1", model.BucketStatusDeleting},
		{"deleted-1", model.BucketStatusDeleted},
		{"cfailed-1", model.BucketStatusCreateFailed},
	} {
		b := &model.Bucket{Name: tc.name, Status: tc.status}
		if err := tb.repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("seeding bucket %q: %v", tc.name, err)
		}
	}

	result, err := tb.backend.ListBuckets(ctx, s3response.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}

	// ListActive returns active + creating + deleting (visible statuses).
	// deleted and create_failed should be excluded.
	names := make(map[string]bool)
	for _, entry := range result.Buckets.Bucket {
		names[entry.Name] = true
	}

	for _, want := range []string{"active-1", "active-2", "creating-1", "deleting-1"} {
		if !names[want] {
			t.Errorf("expected bucket %q in list", want)
		}
	}
	for _, notWant := range []string{"deleted-1", "cfailed-1"} {
		if names[notWant] {
			t.Errorf("bucket %q should not appear in list", notWant)
		}
	}
}
