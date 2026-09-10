package bucketlifecycle

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
)

type provisionTestHandler struct{ definition taskengine.Definition }

func (h provisionTestHandler) Definition() taskengine.Definition { return h.definition }
func (provisionTestHandler) Execute(context.Context, taskengine.Execution) taskengine.Result {
	return taskengine.Complete("", nil)
}

func (provisionTestHandler) Recover(context.Context, taskengine.Execution) taskengine.Result {
	return taskengine.Complete("", nil)
}

func newLifecycleTestService(t *testing.T, repos *repository.Repositories, c *testutil.MockCache, logger *slog.Logger) *Service {
	t.Helper()
	registry := taskengine.NewRegistry()
	retryLimit := 5
	err := registry.Register(provisionTestHandler{definition: taskengine.Definition{
		Type: model.TaskTypeBucketProvision, InputVersion: 1,
		Codec: taskengine.StrictJSONCodec(func(input *ProvisionInput) error {
			return ValidateProvisionInput(*input)
		}),
		RetryLimit: &retryLimit, AllowRetry: true,
	}})
	if err != nil {
		t.Fatalf("register bucket provision handler: %v", err)
	}
	tasks, err := taskengine.NewService(registry, repos, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("create task service: %v", err)
	}
	service := New(repos, c, 2, logger)
	service.SetTaskService(tasks)
	return service
}

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
	s := newLifecycleTestService(t, repos, mockCache, slog.Default())

	bucket, err := s.Create(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if bucket.Name != "test-bucket" {
		t.Fatalf("bucket name = %q, want %q", bucket.Name, "test-bucket")
	}
	if bucket.Status != model.BucketStatusProvisioning {
		t.Fatalf("bucket status = %s, want %s", bucket.Status, model.BucketStatusProvisioning)
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
	if persisted.Status != model.BucketStatusProvisioning {
		t.Fatalf("persisted bucket status = %s, want %s", persisted.Status, model.BucketStatusProvisioning)
	}
	taskRow, err := repos.Tasks.GetByIdentity(ctx, model.TaskTypeBucketProvision, ProvisionKey(bucket.ID, bucket.DefaultCopies))
	if err != nil || taskRow == nil || taskRow.Status != model.TaskStatusPending {
		t.Fatalf("bucket provision task = %#v, err=%v", taskRow, err)
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
	s := newLifecycleTestService(t, repos, mockCache, slog.New(slog.NewTextHandler(&logBuf, nil)))

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
	if persisted.Status != model.BucketStatusProvisioning {
		t.Fatalf("persisted bucket status = %s, want %s", persisted.Status, model.BucketStatusProvisioning)
	}
}

func TestServiceCreateWithACLPersistsACL(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	ctx := context.Background()
	s := newLifecycleTestService(t, repos, &testutil.MockCache{}, slog.Default())
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
	if err := repos.Buckets.Create(ctx, &model.Bucket{Name: "test-bucket-duplicate", Status: model.BucketStatusActive, DefaultCopies: 8, MinimumDurableCopies: 8}); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	var cacheCalled bool
	mockCache := &testutil.MockCache{
		CreateBucketDirFunc: func(context.Context, string) error {
			cacheCalled = true
			return nil
		},
	}
	s := newLifecycleTestService(t, repos, mockCache, slog.Default())

	_, err := s.Create(ctx, "test-bucket-duplicate")
	if !errors.Is(err, repository.ErrAlreadyExists) {
		t.Fatalf("Create error = %v, want %v", err, repository.ErrAlreadyExists)
	}
	if cacheCalled {
		t.Fatal("cache directory was pre-created after bucket create failed")
	}
}

func TestServiceCreateRollsBackBucketWhenProvisionTaskCannotBeEnqueued(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	registry := taskengine.NewRegistry()
	tasks, err := taskengine.NewService(registry, repos, time.Hour)
	if err != nil {
		t.Fatalf("create task service: %v", err)
	}
	var cacheCalled bool
	mockCache := &testutil.MockCache{CreateBucketDirFunc: func(context.Context, string) error {
		cacheCalled = true
		return nil
	}}
	service := New(repos, mockCache, 2, slog.Default())
	service.SetTaskService(tasks)

	_, err = service.Create(t.Context(), "atomic-create")
	if !errors.Is(err, taskengine.ErrUnknownType) {
		t.Fatalf("Create error = %v, want unknown task type", err)
	}
	stored, getErr := repos.Buckets.GetByName(t.Context(), "atomic-create")
	if getErr != nil || stored != nil {
		t.Fatalf("rolled-back bucket = %#v, err=%v", stored, getErr)
	}
	if cacheCalled {
		t.Fatal("cache directory was created after transaction rollback")
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
	s := newLifecycleTestService(t, repos, mockCache, slog.Default())

	s.EnsureCacheBucketDir(context.Background(), "test-bucket")

	if !cacheCalled {
		t.Fatal("cache directory was not ensured")
	}
}

func TestServiceDeleteReturnsUnsupported(t *testing.T) {
	repos := testutil.NewTestRepos(t)
	s := newLifecycleTestService(t, repos, &testutil.MockCache{}, slog.Default())

	_, err := s.Delete(context.Background(), "test-bucket", DeleteOptions{})
	if !errors.Is(err, ErrDeleteNotSupported) {
		t.Fatalf("Delete error = %v, want %v", err, ErrDeleteNotSupported)
	}
}
