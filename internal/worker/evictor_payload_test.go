package worker_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

func TestEvictor_LRUMalformedAccessSnapshotCancelsWithoutDeleting(t *testing.T) {
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		UsedBytesFunc: func() int64 { return 11 },
		DeleteFunc: func(context.Context, string, string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objectID, versionID := seedStoredObject(t, env)
	stage := cacheeviction.StageLRU
	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: "evict_cache:lru:" + versionID,
		Payload:        map[string]any{"cache_accessed_at": float64(1)},
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Disable planning so the cancelled fixture task is not immediately
	// reactivated with a valid payload.
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 0, 90, 50, 3),
	)
	got := runWorkerUntilTask(t, env, evictor, task.ID, 3*time.Second)
	if got.Status != model.TaskStatusCancelled {
		t.Fatalf("task status = %s, want cancelled", got.Status)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache delete calls = %d, want 0", deleteCalls.Load())
	}
}
