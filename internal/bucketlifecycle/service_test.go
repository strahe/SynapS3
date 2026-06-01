package bucketlifecycle

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func TestServiceCreatePrecreatesCacheBucketDir(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	ctx := context.Background()
	var cacheCalled bool
	mockCache := &testutil.MockCache{
		CreateBucketDirFunc: func(_ context.Context, bucket string) error {
			cacheCalled = true
			if bucket != "test-bucket" {
				t.Fatalf("cache bucket = %q, want %q", bucket, "test-bucket")
			}
			return nil
		},
	}
	s := New(repos, mockCache, slog.Default())

	bucket, err := s.Create(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if bucket.Name != "test-bucket" {
		t.Fatalf("bucket name = %q, want %q", bucket.Name, "test-bucket")
	}
	if bucket.Status != model.BucketStatusActive {
		t.Fatalf("bucket status = %s, want %s", bucket.Status, model.BucketStatusActive)
	}
	if !cacheCalled {
		t.Fatal("cache directory was not pre-created")
	}
	persisted, err := repos.Buckets.GetByName(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("Buckets.GetByName: %v", err)
	}
	if persisted == nil {
		t.Fatal("persisted bucket is nil")
	}
	if persisted.Status != model.BucketStatusActive {
		t.Fatalf("persisted bucket status = %s, want %s", persisted.Status, model.BucketStatusActive)
	}
}

func TestServiceCreateKeepsBucketWhenCacheDirPrecreateFails(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	ctx := context.Background()
	mockCache := &testutil.MockCache{
		CreateBucketDirFunc: func(context.Context, string) error {
			return errors.New("cache error")
		},
	}
	var logBuf bytes.Buffer
	s := New(repos, mockCache, slog.New(slog.NewTextHandler(&logBuf, nil)))

	bucket, err := s.Create(ctx, "test-bucket-cache-error")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if bucket.Name != "test-bucket-cache-error" {
		t.Fatalf("bucket name = %q, want %q", bucket.Name, "test-bucket-cache-error")
	}
	if !bytes.Contains(logBuf.Bytes(), []byte("pre-creating cache dir failed (non-fatal)")) {
		t.Fatalf("warning log = %q, want cache dir precreate warning", logBuf.String())
	}
	persisted, err := repos.Buckets.GetByName(ctx, "test-bucket-cache-error")
	if err != nil {
		t.Fatalf("Buckets.GetByName: %v", err)
	}
	if persisted == nil {
		t.Fatal("persisted bucket is nil")
	}
	if persisted.Status != model.BucketStatusActive {
		t.Fatalf("persisted bucket status = %s, want %s", persisted.Status, model.BucketStatusActive)
	}
}

func TestServiceCreateWithACLPersistsACL(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	ctx := context.Background()
	s := New(repos, &testutil.MockCache{}, slog.Default())
	acl := []byte(`{"Owner":"owner-access"}`)

	bucket, err := s.CreateWithACL(ctx, "test-bucket-acl", acl)
	if err != nil {
		t.Fatalf("CreateWithACL: %v", err)
	}

	if !bytes.Equal(bucket.ACL, acl) {
		t.Fatalf("bucket ACL = %q, want %q", bucket.ACL, acl)
	}
	persisted, err := repos.Buckets.GetByName(ctx, "test-bucket-acl")
	if err != nil {
		t.Fatalf("Buckets.GetByName: %v", err)
	}
	if persisted == nil {
		t.Fatal("persisted bucket is nil")
	}
	if !bytes.Equal(persisted.ACL, acl) {
		t.Fatalf("persisted ACL = %q, want %q", persisted.ACL, acl)
	}
}

func TestServiceCreateReturnsErrorWhenBucketCreateFails(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	ctx := context.Background()
	if err := repos.Buckets.Create(ctx, &model.Bucket{Name: "test-bucket-duplicate", Status: model.BucketStatusActive}); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	var cacheCalled bool
	mockCache := &testutil.MockCache{
		CreateBucketDirFunc: func(context.Context, string) error {
			cacheCalled = true
			return nil
		},
	}
	s := New(repos, mockCache, slog.Default())

	_, err := s.Create(ctx, "test-bucket-duplicate")
	if !errors.Is(err, repository.ErrAlreadyExists) {
		t.Fatalf("Create error = %v, want %v", err, repository.ErrAlreadyExists)
	}
	if cacheCalled {
		t.Fatal("cache directory was pre-created after bucket create failed")
	}
}

func TestServiceEnsureCacheBucketDirCallsCache(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	var cacheCalled bool
	mockCache := &testutil.MockCache{
		CreateBucketDirFunc: func(_ context.Context, bucket string) error {
			cacheCalled = true
			if bucket != "test-bucket" {
				t.Fatalf("cache bucket = %q, want %q", bucket, "test-bucket")
			}
			return nil
		},
	}
	s := New(repos, mockCache, slog.Default())

	s.EnsureCacheBucketDir(context.Background(), "test-bucket")

	if !cacheCalled {
		t.Fatal("cache directory was not ensured")
	}
}

func TestServiceDeleteReturnsUnsupported(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	s := New(repos, &testutil.MockCache{}, slog.Default())

	_, err := s.Delete(context.Background(), "test-bucket", DeleteOptions{})
	if !errors.Is(err, ErrDeleteNotSupported) {
		t.Fatalf("Delete error = %v, want %v", err, ErrDeleteNotSupported)
	}
}
