package backend_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/strahe/synaps3/internal/model"
	"github.com/versity/versitygw/auth"
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

func TestCreateBucketRejectsMissingBucketInput(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	want := s3err.GetAPIError(s3err.ErrInvalidBucketName)
	for _, tc := range []struct {
		name  string
		input *s3.CreateBucketInput
	}{
		{name: "nil input", input: nil},
		{name: "nil bucket", input: &s3.CreateBucketInput{Bucket: nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tb.backend.CreateBucket(ctx, tc.input, nil)
			requireAPIErrorCode(t, err, want)
		})
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

func TestCreateBucket_DuplicateOwnedBucketReturnsAlreadyExists(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "same-owner")
	firstACL, err := json.Marshal(auth.ACL{Owner: "same-owner"})
	if err != nil {
		t.Fatalf("Marshal first ACL: %v", err)
	}
	secondACL, err := json.Marshal(auth.ACL{Owner: "same-owner"})
	if err != nil {
		t.Fatalf("Marshal second ACL: %v", err)
	}

	err = tb.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("same-owner-bucket"),
	}, firstACL)
	if err != nil {
		t.Fatalf("first CreateBucket: %v", err)
	}

	err = tb.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("same-owner-bucket"),
	}, secondACL)
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

func TestCreateBucketRejectsUnknownOwnerFromStaleAuth(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	acl, err := json.Marshal(auth.ACL{Owner: "missing-owner"})
	if err != nil {
		t.Fatalf("Marshal ACL: %v", err)
	}

	err = tb.backend.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("stale-auth-bucket"),
	}, acl)
	if err == nil {
		t.Fatal("expected error for deleted owner")
	}
	apiErr, ok := err.(s3err.APIError)
	if !ok {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	want := s3err.GetAPIError(s3err.ErrAccessDenied)
	if apiErr.Code != want.Code {
		t.Errorf("error code = %q, want %q", apiErr.Code, want.Code)
	}
	bucket, err := tb.repos.Buckets.GetByName(ctx, "stale-auth-bucket")
	if err != nil {
		t.Fatalf("GetByName: %v", err)
	}
	if bucket != nil {
		t.Fatalf("bucket was created for deleted owner: %#v", bucket)
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

func TestHeadBucketRejectsMissingBucketInput(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()

	want := s3err.GetAPIError(s3err.ErrInvalidBucketName)
	for _, tc := range []struct {
		name  string
		input *s3.HeadBucketInput
	}{
		{name: "nil input", input: nil},
		{name: "nil bucket", input: &s3.HeadBucketInput{Bucket: nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tb.backend.HeadBucket(ctx, tc.input)
			requireAPIErrorCode(t, err, want)
		})
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

	result, err := tb.backend.ListBuckets(ctx, s3response.ListBucketsInput{IsAdmin: true})
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

	result, err := tb.backend.ListBuckets(ctx, s3response.ListBucketsInput{IsAdmin: true})
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

func TestListBucketsFiltersNonAdminByOwnerAccessKey(t *testing.T) {
	tb := newTestBackend(t)
	ctx := context.Background()
	seedS3Account(t, tb, "owner-a")
	seedS3Account(t, tb, "owner-b")

	for _, seed := range []struct {
		name  string
		owner string
		acl   []byte
	}{
		{name: "owner-a-bucket", owner: "owner-a"},
		{name: "owner-b-bucket", owner: "owner-b"},
		{name: "legacy-root-bucket"},
		{name: "malformed-acl-bucket", acl: []byte("{")},
	} {
		b := &model.Bucket{Name: seed.name, Status: model.BucketStatusActive}
		switch {
		case seed.acl != nil:
			b.ACL = seed.acl
		case seed.owner != "":
			data, err := json.Marshal(auth.ACL{Owner: seed.owner})
			if err != nil {
				t.Fatalf("Marshal ACL: %v", err)
			}
			b.ACL = data
			b.OwnerAccessKey = &seed.owner
		}
		if err := tb.repos.Buckets.Create(ctx, b); err != nil {
			t.Fatalf("seeding bucket %q: %v", seed.name, err)
		}
	}

	result, err := tb.backend.ListBuckets(ctx, s3response.ListBucketsInput{Owner: "owner-a"})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(result.Buckets.Bucket) != 1 || result.Buckets.Bucket[0].Name != "owner-a-bucket" {
		t.Fatalf("filtered buckets = %#v, want only owner-a-bucket", result.Buckets.Bucket)
	}

	emptyOwnerResult, err := tb.backend.ListBuckets(ctx, s3response.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets empty owner: %v", err)
	}
	if len(emptyOwnerResult.Buckets.Bucket) != 0 {
		t.Fatalf("empty owner buckets = %#v, want none", emptyOwnerResult.Buckets.Bucket)
	}

	adminResult, err := tb.backend.ListBuckets(ctx, s3response.ListBucketsInput{IsAdmin: true})
	if err != nil {
		t.Fatalf("ListBuckets admin: %v", err)
	}
	if len(adminResult.Buckets.Bucket) != 4 {
		t.Fatalf("admin buckets = %#v, want all four buckets", adminResult.Buckets.Bucket)
	}
}
