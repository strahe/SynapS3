package backend_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/s3err"
)

// --- GetBucketAcl ---

func TestGetBucketAcl_ExistingBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "acl-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	data, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{
		Bucket: aws.String("acl-bucket"),
	})
	if err != nil {
		t.Fatalf("GetBucketAcl: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil ACL data, got %v", data)
	}
}

func TestGetBucketAcl_NonExistentBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{
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

func TestGetBucketAcl_DeletingBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "deleting-acl", Status: model.BucketStatusDeleting}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	// Deleting bucket is visible, so GetBucketAcl should succeed.
	data, err := tb.backend.GetBucketAcl(ctx, &s3.GetBucketAclInput{
		Bucket: aws.String("deleting-acl"),
	})
	if err != nil {
		t.Fatalf("GetBucketAcl on deleting bucket: %v", err)
	}
	if data != nil {
		t.Errorf("expected nil ACL data, got %v", data)
	}
}

// --- PutBucketAcl ---

func TestPutBucketAcl_WritableBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "put-acl-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	if err := tb.backend.PutBucketAcl(ctx, "put-acl-bucket", nil); err != nil {
		t.Fatalf("PutBucketAcl: %v", err)
	}
}

func TestPutBucketAcl_DeletingBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "put-acl-deleting", Status: model.BucketStatusDeleting}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.PutBucketAcl(ctx, "put-acl-deleting", nil)
	if err == nil {
		t.Fatal("expected error writing ACL on non-writable bucket")
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

// --- GetBucketTagging ---

func TestGetBucketTagging_ReturnsAPIError(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "tag-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	_, err := tb.backend.GetBucketTagging(ctx, "tag-bucket")
	if err == nil {
		t.Fatal("expected error from GetBucketTagging")
	}

	// Must be a raw APIError (not wrapped), so VersityGW type assertion works.
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError (unwrapped), got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrBucketTaggingNotFound)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

func TestGetBucketTagging_NonExistentBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.GetBucketTagging(ctx, "no-such-tag-bucket")
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
}

// --- GetBucketVersioning ---

func TestGetBucketVersioning_ExistingBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "ver-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	out, err := tb.backend.GetBucketVersioning(ctx, "ver-bucket")
	if err != nil {
		t.Fatalf("GetBucketVersioning: %v", err)
	}
	// SynapS3 does not support versioning; expect nil fields.
	if out.Status != nil {
		t.Errorf("Status = %v, want nil", out.Status)
	}
	if out.MFADelete != nil {
		t.Errorf("MFADelete = %v, want nil", out.MFADelete)
	}
}

func TestGetBucketVersioning_NonExistentBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	_, err := tb.backend.GetBucketVersioning(ctx, "ghost-bucket")
	if err == nil {
		t.Fatal("expected error for non-existent bucket")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	if apiErr.Code != "NoSuchBucket" {
		t.Errorf("error code = %q, want NoSuchBucket", apiErr.Code)
	}
}

// --- GetObjectLockConfiguration ---

func TestGetObjectLockConfiguration_ReturnsNotFound(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "lock-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	_, err := tb.backend.GetObjectLockConfiguration(ctx, "lock-bucket")
	if err == nil {
		t.Fatal("expected error from GetObjectLockConfiguration")
	}

	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected s3err.APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
}

// --- GetBucketOwnershipControls ---

func TestGetBucketOwnershipControls_ReturnsEnforced(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "own-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	ownership, err := tb.backend.GetBucketOwnershipControls(ctx, "own-bucket")
	if err != nil {
		t.Fatalf("GetBucketOwnershipControls: %v", err)
	}
	if ownership != types.ObjectOwnershipBucketOwnerEnforced {
		t.Errorf("ownership = %q, want %q", ownership, types.ObjectOwnershipBucketOwnerEnforced)
	}
}

// --- GetObjectAcl ---

func TestGetObjectAcl_VisibleBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "obj-acl-bucket", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	out, err := tb.backend.GetObjectAcl(ctx, &s3.GetObjectAclInput{
		Bucket: aws.String("obj-acl-bucket"),
	})
	if err != nil {
		t.Fatalf("GetObjectAcl: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

func TestGetObjectAcl_DeletingBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "obj-acl-deleting", Status: model.BucketStatusDeleting}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	// Deleting bucket is visible; read-only GetObjectAcl should succeed.
	out, err := tb.backend.GetObjectAcl(ctx, &s3.GetObjectAclInput{
		Bucket: aws.String("obj-acl-deleting"),
	})
	if err != nil {
		t.Fatalf("GetObjectAcl on deleting bucket: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
}

// --- PutObjectAcl ---

func TestPutObjectAcl_WritableBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "put-obj-acl", Status: model.BucketStatusActive}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.PutObjectAcl(ctx, &s3.PutObjectAclInput{
		Bucket: aws.String("put-obj-acl"),
	})
	if err != nil {
		t.Fatalf("PutObjectAcl: %v", err)
	}
}

func TestPutObjectAcl_DeletingBucket(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	bkt := &model.Bucket{Name: "put-obj-acl-del", Status: model.BucketStatusDeleting}
	if err := tb.repos.Buckets.Create(ctx, bkt); err != nil {
		t.Fatalf("seeding bucket: %v", err)
	}

	err := tb.backend.PutObjectAcl(ctx, &s3.PutObjectAclInput{
		Bucket: aws.String("put-obj-acl-del"),
	})
	if err == nil {
		t.Fatal("expected error writing object ACL on non-writable bucket")
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
