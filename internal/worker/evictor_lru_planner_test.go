package worker_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

func TestEvictor_LRUBelowHighWatermarkDoesNotPlanEviction(t *testing.T) {
	var used atomic.Int64
	used.Store(26)
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	seedStoredObject(t, env)
	pollInterval := 15 * time.Millisecond
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		pollInterval,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 30, 90, 60, 3),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(ctx)
		close(done)
	}()
	time.Sleep(4 * pollInterval)
	cancel()
	waitForSignal(t, done, time.Second, "LRU evictor shutdown")

	tasks, total, err := env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		cacheeviction.StageLRU,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List LRU tasks: %v", err)
	}
	if total != 0 || len(tasks) != 0 {
		t.Fatalf("LRU tasks below high watermark total=%d tasks=%#v, want none", total, tasks)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls below high watermark = %d, want 0", deleteCalls.Load())
	}
}

func TestEvictor_LRUEvictsLeastRecentlyUsedUntilLowWatermark(t *testing.T) {
	var used atomic.Int64
	used.Store(33)
	var deletedMu sync.Mutex
	var deleted []string
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, key string) error {
			deletedMu.Lock()
			deleted = append(deleted, key)
			deletedMu.Unlock()
			used.Add(-11)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	type seeded struct {
		objectID  int64
		versionID string
	}
	var versions []seeded
	for range 3 {
		_, objectID, versionID := seedStoredObject(t, env)
		versions = append(versions, seeded{objectID: objectID, versionID: versionID})
	}
	base := futureLRUAccessTime()
	for index, version := range versions {
		if err := env.repos.Objects.RecordVersionCacheAccess(
			context.Background(),
			version.versionID,
			base.Add(time.Duration(index)*time.Hour),
		); err != nil {
			t.Fatalf("RecordVersionCacheAccess(%s): %v", version.versionID, err)
		}
	}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
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
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, completed, err := env.repos.Tasks.List(
			context.Background(),
			string(model.TaskTypeEvictCache),
			cacheeviction.StageLRU,
			string(model.TaskStatusCompleted),
			10,
			0,
		)
		if err == nil && completed == 2 {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("timed out waiting for LRU eviction; used=%d completed=%d err=%v", used.Load(), completed, err)
		case <-ticker.C:
		}
	}
	cancel()
	waitForSignal(t, done, time.Second, "LRU evictor shutdown")

	deletedMu.Lock()
	gotDeleted := append([]string(nil), deleted...)
	deletedMu.Unlock()
	wantDeleted := []string{
		".versions/" + versions[0].versionID,
		".versions/" + versions[1].versionID,
	}
	if len(gotDeleted) != len(wantDeleted) {
		t.Fatalf("deleted cache keys = %#v, want %#v", gotDeleted, wantDeleted)
	}
	for index := range wantDeleted {
		if gotDeleted[index] != wantDeleted[index] {
			t.Fatalf("deleted cache keys = %#v, want LRU order %#v", gotDeleted, wantDeleted)
		}
	}
	for index, version := range versions {
		got, err := env.repos.Objects.GetVersionByID(context.Background(), version.versionID)
		if err != nil || got == nil {
			t.Fatalf("GetVersionByID(%s): version=%v err=%v", version.versionID, got, err)
		}
		if index < 2 {
			if got.State != model.ObjectStateCacheEvicted || got.InCache {
				t.Fatalf("evicted version %d state/cache = %s/%v", index, got.State, got.InCache)
			}
		} else if got.State != model.ObjectStateStored || !got.InCache {
			t.Fatalf("retained version state/cache = %s/%v, want stored/true", got.State, got.InCache)
		}
	}
	completed, total, err := env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		cacheeviction.StageLRU,
		string(model.TaskStatusCompleted),
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List completed LRU tasks: %v", err)
	}
	if total != 2 || len(completed) != 2 {
		t.Fatalf("completed LRU tasks total=%d tasks=%#v, want 2", total, completed)
	}
	for _, task := range completed {
		if _, ok := task.Payload["cache_accessed_at"].(string); !ok {
			t.Fatalf("LRU task payload = %#v, want RFC3339Nano string snapshot", task.Payload)
		}
	}
}

func TestEvictor_LRUContinuesActiveCycleBelowHighWatermark(t *testing.T) {
	var used atomic.Int64
	used.Store(95)
	var deleteCalls atomic.Int64
	var touchOnce sync.Once
	touchResult := make(chan error, 1)

	var env *testWorkerEnv
	var versionToTouch string
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			used.Add(-11)
			touchOnce.Do(func() {
				touchResult <- env.repos.Objects.RecordVersionCacheAccess(
					context.Background(),
					versionToTouch,
					futureLRUAccessTime().Add(24*time.Hour),
				)
			})
			return nil
		},
	}
	env = newTestWorkerEnvWithMockCache(t, mc)

	var versionIDs []string
	base := futureLRUAccessTime()
	for index := range 3 {
		_, _, versionID := seedStoredObject(t, env)
		versionIDs = append(versionIDs, versionID)
		if err := env.repos.Objects.RecordVersionCacheAccess(
			context.Background(),
			versionID,
			base.Add(time.Duration(index)*time.Hour),
		); err != nil {
			t.Fatalf("RecordVersionCacheAccess(%s): %v", versionID, err)
		}
	}
	versionToTouch = versionIDs[1]

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 100, 90, 80, 3),
	)
	runWorkerUntilCondition(t, evictor, 10*time.Millisecond, 3*time.Second, func() (bool, error) {
		if used.Load() > 80 || deleteCalls.Load() != 2 {
			return false, nil
		}
		tasks, total, err := env.repos.Tasks.List(
			context.Background(),
			string(model.TaskTypeEvictCache),
			cacheeviction.StageLRU,
			"",
			10,
			0,
		)
		if err != nil {
			return false, err
		}
		var completed, cancelled int
		for _, task := range tasks {
			switch task.Status {
			case model.TaskStatusCompleted:
				completed++
			case model.TaskStatusCancelled:
				cancelled++
			}
		}
		return total == 3 && completed == 2 && cancelled == 1, nil
	})

	select {
	case err := <-touchResult:
		if err != nil {
			t.Fatalf("touching planned LRU candidate: %v", err)
		}
	default:
		t.Fatal("planned LRU candidate was not touched during the first deletion")
	}

	tasks, total, err := env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		cacheeviction.StageLRU,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List LRU tasks: %v", err)
	}
	var completed, cancelled int
	for _, task := range tasks {
		switch task.Status {
		case model.TaskStatusCompleted:
			completed++
		case model.TaskStatusCancelled:
			cancelled++
		}
	}
	if total != 3 || completed != 2 || cancelled != 1 {
		t.Fatalf(
			"LRU tasks total/completed/cancelled = %d/%d/%d, want 3/2/1",
			total,
			completed,
			cancelled,
		)
	}
}

func TestEvictor_LRUSkipsUnexpectedNullAccessTime(t *testing.T) {
	var used atomic.Int64
	used.Store(11)
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			used.Store(0)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, _, versionID := seedStoredObject(t, env)
	if _, err := env.db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("cache_accessed_at = NULL").
		Where("version_id = ?", versionID).
		Exec(context.Background()); err != nil {
		t.Fatalf("clear cache access time: %v", err)
	}
	candidates, err := env.repos.CacheEvictions.ListLRUCandidates(
		context.Background(),
		time.Now().Add(-time.Hour),
		10,
	)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("NULL-access LRU candidates = %#v err=%v, want none", candidates, err)
	}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 10, 90, 50, 3),
	)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(runCtx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	waitForSignal(t, done, time.Second, "NULL-access LRU test shutdown")

	tasks, total, err := env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		cacheeviction.StageLRU,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List NULL-access LRU tasks: %v", err)
	}
	if total != 0 || len(tasks) != 0 {
		t.Fatalf("NULL-access LRU tasks total=%d tasks=%#v, want none", total, tasks)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("NULL-access LRU delete calls = %d, want 0", deleteCalls.Load())
	}
	version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || version == nil || version.State != model.ObjectStateStored || !version.InCache {
		t.Fatalf("NULL-access LRU version = %#v err=%v, want stored in cache", version, err)
	}
}

func TestEvictor_LRUReactivatesCancelledStableTask(t *testing.T) {
	var used atomic.Int64
	used.Store(11)
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			used.Store(0)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objectID, versionID := seedStoredObject(t, env)
	accessedAt := futureLRUAccessTime()
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}
	completedAt := time.Now()
	task := cacheeviction.NewLRUTask(cacheeviction.Candidate{
		ObjectID:   objectID,
		VersionID:  versionID,
		AccessedAt: accessedAt,
	}, 3, completedAt)
	task.Status = model.TaskStatusCancelled
	task.CompletedAt = &completedAt
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create cancelled LRU task: %v", err)
	}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 10, 90, 50, 3),
	)
	runWorkerUntilCondition(t, evictor, 10*time.Millisecond, 3*time.Second, func() (bool, error) {
		got, err := env.repos.Tasks.GetByID(context.Background(), task.ID)
		return got != nil && got.Status == model.TaskStatusCompleted, err
	})

	tasks, total, err := env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		cacheeviction.StageLRU,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List reactivated LRU tasks: %v", err)
	}
	if total != 1 || len(tasks) != 1 || tasks[0].ID != task.ID || tasks[0].Status != model.TaskStatusCompleted {
		t.Fatalf("reactivated LRU tasks total=%d tasks=%#v, want original completed task", total, tasks)
	}
}

func TestEvictor_LRUExhaustedTaskWaitsForCooldownBeforeReplanning(t *testing.T) {
	var used atomic.Int64
	used.Store(11)
	var deleteCalls atomic.Int64
	var deleteSucceeds atomic.Bool
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			if deleteSucceeds.Load() {
				used.Store(0)
				return nil
			}
			return errors.New("permission denied")
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, _, versionID := seedStoredObject(t, env)
	accessedAt := futureLRUAccessTime()
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}

	pollInterval := 10 * time.Millisecond
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		pollInterval,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 10, 90, 50, 1),
	)
	var exhaustedAt time.Time
	runWorkerUntilCondition(t, evictor, pollInterval, 3*time.Second, func() (bool, error) {
		_, exhausted, err := env.repos.Tasks.List(
			context.Background(),
			string(model.TaskTypeEvictCache),
			cacheeviction.StageLRU,
			string(model.TaskStatusExhausted),
			10,
			0,
		)
		if err != nil || exhausted != 1 {
			return false, err
		}
		if exhaustedAt.IsZero() {
			exhaustedAt = time.Now()
		}
		return time.Since(exhaustedAt) >= 8*pollInterval, nil
	})

	tasks, total, err := env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		cacheeviction.StageLRU,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List LRU tasks after exhaustion: %v", err)
	}
	if total != 1 || len(tasks) != 1 || tasks[0].Status != model.TaskStatusExhausted {
		t.Fatalf("LRU tasks after exhaustion total=%d tasks=%#v, want one exhausted task", total, tasks)
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("LRU delete calls after exhaustion = %d, want 1", deleteCalls.Load())
	}

	if _, err := env.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("completed_at = ?", time.Now().Add(-2*time.Hour)).
		Where("id = ?", tasks[0].ID).
		Exec(context.Background()); err != nil {
		t.Fatalf("age exhausted LRU task: %v", err)
	}
	deleteSucceeds.Store(true)
	recoveryEvictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		pollInterval,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 10, 90, 50, 1),
	)
	runWorkerUntilCondition(t, recoveryEvictor, pollInterval, 3*time.Second, func() (bool, error) {
		_, completed, err := env.repos.Tasks.List(
			context.Background(),
			string(model.TaskTypeEvictCache),
			cacheeviction.StageLRU,
			string(model.TaskStatusCompleted),
			10,
			0,
		)
		return completed == 1, err
	})
	tasks, total, err = env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		cacheeviction.StageLRU,
		"",
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List LRU tasks after cooldown recovery: %v", err)
	}
	if total != 1 || len(tasks) != 1 || tasks[0].Status != model.TaskStatusCompleted {
		t.Fatalf("LRU tasks after cooldown recovery total=%d tasks=%#v, want original completed task", total, tasks)
	}
	if deleteCalls.Load() != 2 {
		t.Fatalf("LRU delete calls after cooldown recovery = %d, want 2 total attempts", deleteCalls.Load())
	}
}

func runWorkerUntilCondition(
	t *testing.T,
	w worker.Worker,
	pollInterval time.Duration,
	timeout time.Duration,
	condition func() (bool, error),
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	defer waitForSignal(t, done, time.Second, "worker condition shutdown")
	defer cancel()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		ok, err := condition()
		if err != nil {
			t.Fatalf("checking worker condition: %v", err)
		}
		if ok {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for worker condition")
		case <-ticker.C:
		}
	}
}
