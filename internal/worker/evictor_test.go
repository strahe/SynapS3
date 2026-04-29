package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

// seedStoredObject creates a bucket+object version in stored state with PieceCID.
func seedStoredObject(t *testing.T, env *testWorkerEnv) (*model.Bucket, int64, string) {
	t.Helper()
	ctx := context.Background()

	bucket, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)

	// Transition through the pipeline: cached → uploading → stored (with PieceCID)
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition to uploading: %v", err)
	}
	pieceCID := testCID(t).String()
	if err := env.repos.Objects.SetVersionStorageInfoAndTransition(ctx, versionID, pieceCID, "https://provider.example/pieces/1", model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("SetVersionStorageInfoAndTransition: %v", err)
	}
	return bucket, objID, versionID
}

func TestEvictor_HappyPath(t *testing.T) {
	mc := &testutil.MockCache{
		DeleteFunc: func(_ context.Context, _, _ string) error {
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, versionID := seedStoredObject(t, env)

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	ctx := context.Background()

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetByID(ctx, objID)
	if obj.State != model.ObjectStateCacheEvicted {
		t.Errorf("expected object state cache_evicted, got %s", obj.State)
	}
}

func TestEvictor_MissingVersion(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, _ := seedStoredObject(t, env)

	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   "01J000000000000000MISSING1",
		IdempotencyKey: "evict_cache:missing",
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("creating task: %v", err)
	}

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "object not found") {
		t.Errorf("expected object not found error, got %v", got.LastError)
	}
}

func TestEvictor_WrongState(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	// Object in "uploading" state, not "stored"
	_, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
	ctx := context.Background()

	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "not stored") {
		t.Errorf("expected not stored error, got %v", got.LastError)
	}
}

func TestEvictor_NoPieceCID(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	ctx := context.Background()

	// Create object in stored state without PieceCID
	_, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition: %v", err)
	}
	// Skip PieceCID setting, go directly to stored
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		t.Fatalf("transition: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "no PieceCID") {
		t.Errorf("expected no PieceCID error, got %v", got.LastError)
	}
}

func TestEvictor_BucketNotFound(t *testing.T) {
	mc := &testutil.MockCache{
		DeleteFunc: func(_ context.Context, _, _ string) error {
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	bucket, objID, versionID := seedStoredObject(t, env)

	// Hard-delete the bucket so evictor can't find it
	ctx := context.Background()
	if err := env.repos.Buckets.HardDelete(ctx, bucket.ID); err != nil {
		t.Fatalf("hard-deleting bucket: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "bucket not found") {
		t.Errorf("expected bucket not found error, got %v", got.LastError)
	}
}

func TestEvictor_CacheDeleteFailure(t *testing.T) {
	mc := &testutil.MockCache{
		DeleteFunc: func(_ context.Context, _, _ string) error {
			return errors.New("permission denied")
		},
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, errors.New("not needed")
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, versionID := seedStoredObject(t, env)

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	ctx := context.Background()

	// Cache delete failure is non-fatal: task still completes
	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed despite cache delete failure, got %s", got.Status)
	}

	// Object should still be in cache_evicted state
	obj, _ := env.repos.Objects.GetByID(ctx, objID)
	if obj.State != model.ObjectStateCacheEvicted {
		t.Errorf("expected object state cache_evicted, got %s", obj.State)
	}
}
