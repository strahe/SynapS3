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

// seedOnChainedObject creates a bucket+object in "onchained" state with PieceCID.
func seedOnChainedObject(t *testing.T, env *testWorkerEnv) (*model.Bucket, int64, int64) {
	t.Helper()
	ctx := context.Background()

	bucket, objID, gen := seedObjectInDB(t, env, model.BucketStatusActive)

	// Transition through the pipeline: cached → uploading → uploaded → onchaining → onchained
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition to uploading: %v", err)
	}
	pieceCID := testCID(t).String()
	if err := env.repos.Objects.SetPieceCIDAndTransition(ctx, objID, gen, pieceCID, model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatalf("SetPieceCIDAndTransition: %v", err)
	}
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateUploaded, model.ObjectStateOnChaining); err != nil {
		t.Fatalf("transition to onchaining: %v", err)
	}
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateOnChaining, model.ObjectStateOnChained); err != nil {
		t.Fatalf("transition to onchained: %v", err)
	}
	return bucket, objID, gen
}

func TestEvictor_HappyPath(t *testing.T) {
	mc := &testutil.MockCache{
		DeleteFunc: func(_ context.Context, _, _ string) error {
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, gen := seedOnChainedObject(t, env)

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, gen, 5, 0)

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

func TestEvictor_StaleGeneration(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, _ := seedOnChainedObject(t, env)

	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objID,
		RefGeneration:  0, // stale
		IdempotencyKey: "evict_cache:stale:0",
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
	if got.LastError == nil || !strings.Contains(*got.LastError, "stale generation") {
		t.Errorf("expected stale generation error, got %v", got.LastError)
	}
}

func TestEvictor_WrongState(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	// Object in "uploaded" state, not "onchained"
	_, objID, gen := seedObjectInDB(t, env, model.BucketStatusActive)
	ctx := context.Background()

	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition: %v", err)
	}
	pieceCID := testCID(t).String()
	if err := env.repos.Objects.SetPieceCIDAndTransition(ctx, objID, gen, pieceCID, model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatalf("transition: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, gen, 5, 0)

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "not onchained") {
		t.Errorf("expected not onchained error, got %v", got.LastError)
	}
}

func TestEvictor_NoPieceCID(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	ctx := context.Background()

	// Create object in onchained state without PieceCID
	_, objID, gen := seedObjectInDB(t, env, model.BucketStatusActive)
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition: %v", err)
	}
	// Skip PieceCID setting, go straight through
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateUploaded, model.ObjectStateOnChaining); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if err := env.repos.Objects.UpdateState(ctx, objID, gen, model.ObjectStateOnChaining, model.ObjectStateOnChained); err != nil {
		t.Fatalf("transition: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, gen, 5, 0)

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
	bucket, objID, gen := seedOnChainedObject(t, env)

	// Hard-delete the bucket so evictor can't find it
	ctx := context.Background()
	if err := env.repos.Buckets.HardDelete(ctx, bucket.ID); err != nil {
		t.Fatalf("hard-deleting bucket: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, gen, 5, 0)

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
	_, objID, gen := seedOnChainedObject(t, env)

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, gen, 5, 0)

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
