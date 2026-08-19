package worker_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

type blockingDeletionRecordRepo struct {
	repository.CacheEvictionRepository
	versionID string
	entered   chan struct{}
	release   <-chan struct{}
	once      sync.Once
}

func (r *blockingDeletionRecordRepo) RecordAuthorizedDeletion(
	ctx context.Context,
	task *model.Task,
) error {
	if task.RefVersionID == r.versionID {
		r.once.Do(func() { close(r.entered) })
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.CacheEvictionRepository.RecordAuthorizedDeletion(ctx, task)
}

func TestEvictor_LRUCapacityReservationEndsAtPhysicalDelete(t *testing.T) {
	var used atomic.Int64
	used.Store(33)
	var secondDeleteOnce sync.Once
	secondDeleteEntered := make(chan struct{})

	var firstCacheKey, secondCacheKey string
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, key string) error {
			switch key {
			case firstCacheKey:
				used.Add(-11)
			case secondCacheKey:
				used.Add(-11)
				secondDeleteOnce.Do(func() { close(secondDeleteEntered) })
			}
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, firstObjectID, firstVersionID := seedStoredObject(t, env)
	_, secondObjectID, secondVersionID := seedStoredObject(t, env)
	firstCacheKey = ".versions/" + firstVersionID
	secondCacheKey = ".versions/" + secondVersionID

	accessedAt := futureLRUAccessTime()
	for _, versionID := range []string{firstVersionID, secondVersionID} {
		if err := env.repos.Objects.RecordVersionCacheAccess(
			context.Background(),
			versionID,
			accessedAt,
		); err != nil {
			t.Fatalf("RecordVersionCacheAccess(%s): %v", versionID, err)
		}
	}
	firstTask := seedLRUEvictionTask(t, env, firstObjectID, firstVersionID, accessedAt)
	secondTask := seedLRUEvictionTask(t, env, secondObjectID, secondVersionID, accessedAt)

	stateUpdateEntered := make(chan struct{})
	releaseStateUpdate := make(chan struct{})
	var releaseStateOnce sync.Once
	env.repos.CacheEvictions = &blockingDeletionRecordRepo{
		CacheEvictionRepository: env.repos.CacheEvictions,
		versionID:               firstVersionID,
		entered:                 stateUpdateEntered,
		release:                 releaseStateUpdate,
	}

	heldSecond, err := env.cacheGate.Open(
		secondVersionID,
		func() (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 11}, nil
		},
	)
	if err != nil {
		t.Fatalf("hold second cache entry open: %v", err)
	}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		2,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 30, 90, 60, 3),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(ctx)
		close(done)
	}()
	defer func() {
		_ = heldSecond.Body.Close()
		releaseStateOnce.Do(func() { close(releaseStateUpdate) })
		cancel()
		waitForSignal(t, done, time.Second, "LRU capacity reservation test shutdown")
	}()

	waitForSignal(t, stateUpdateEntered, time.Second, "first cache file deletion")
	if err := heldSecond.Body.Close(); err != nil {
		t.Fatalf("release second cache entry: %v", err)
	}
	waitForSignal(
		t,
		secondDeleteEntered,
		time.Second,
		"second cache file deletion while first database transition is pending",
	)

	releaseStateOnce.Do(func() { close(releaseStateUpdate) })
	waitForTaskStatus(t, env, firstTask.ID, model.TaskStatusCompleted, time.Second)
	waitForTaskStatus(t, env, secondTask.ID, model.TaskStatusCompleted, time.Second)
	if got := used.Load(); got != 11 {
		t.Fatalf("cache used bytes after concurrent LRU deletions = %d, want 11", got)
	}
}
