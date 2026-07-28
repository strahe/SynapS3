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

type blockingVersionStateRepo struct {
	repository.ObjectRepository
	versionID string
	entered   chan struct{}
	release   <-chan struct{}
	once      sync.Once
}

func (r *blockingVersionStateRepo) UpdateVersionState(
	ctx context.Context,
	versionID string,
	from model.ObjectState,
	to model.ObjectState,
) error {
	if versionID == r.versionID &&
		from == model.ObjectStateStored &&
		to == model.ObjectStateCacheEvicted {
		r.once.Do(func() { close(r.entered) })
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.ObjectRepository.UpdateVersionState(ctx, versionID, from, to)
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
	env.repos.Objects = &blockingVersionStateRepo{
		ObjectRepository: env.repos.Objects,
		versionID:        firstVersionID,
		entered:          stateUpdateEntered,
		release:          releaseStateUpdate,
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
