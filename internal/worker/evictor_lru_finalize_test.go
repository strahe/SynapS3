package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

func TestEvictor_LRUAccessAfterPlanningCancelsStaleTask(t *testing.T) {
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		UsedBytesFunc: func() int64 { return 11 },
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objectID, versionID := seedStoredObject(t, env)
	plannedAt := futureLRUAccessTime()
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, plannedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess(planned): %v", err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, plannedAt)
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, plannedAt.Add(time.Minute)); err != nil {
		t.Fatalf("RecordVersionCacheAccess(after plan): %v", err)
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
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 20, 90, 50, 3),
	)
	got := runWorkerUntilTask(t, env, evictor, task.ID, 3*time.Second)
	if got.Status != model.TaskStatusCancelled {
		t.Fatalf("stale LRU task status = %s, want cancelled", got.Status)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls = %d, want 0 for stale LRU task", deleteCalls.Load())
	}
	version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || version == nil || !version.InCache || version.State != model.ObjectStateStored {
		t.Fatalf("version after stale LRU task = %#v err=%v", version, err)
	}
}

func TestEvictor_LRUTimestampPrecisionMatchesDatabaseValue(t *testing.T) {
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		UsedBytesFunc: func() int64 { return 11 },
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objectID, versionID := seedStoredObject(t, env)
	plannedAt := futureLRUAccessTime().Add(789 * time.Nanosecond)
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, plannedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess(planned): %v", err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, plannedAt)

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 20, 90, 50, 3),
	)
	got := runWorkerUntilTask(t, env, evictor, task.ID, 3*time.Second)
	if got.Status != model.TaskStatusCompleted {
		t.Fatalf("precision-normalized LRU task status = %s, want completed", got.Status)
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("cache Delete calls = %d, want 1", deleteCalls.Load())
	}
}

func TestEvictor_LRUWaitsForOpenBodyAndPreservesRecentAccess(t *testing.T) {
	var used atomic.Int64
	used.Store(11)
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 6}, nil
		},
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	bucket, objectID, versionID := seedStoredObject(t, env)
	plannedAt := futureLRUAccessTime()
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, plannedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess(planned): %v", err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, plannedAt)
	reader := objectreader.New(
		env.repos,
		env.cache,
		nil,
		env.cacheGate,
		env.accessTracker,
		slog.Default(),
	)

	opened, err := reader.Open(
		context.Background(),
		bucket.Name,
		"hello-"+versionID+".txt",
		objectreader.S3Visibility,
	)
	if err != nil {
		t.Fatalf("cache Open: %v", err)
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
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 20, 90, 50, 3),
	)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		_ = opened.Body.Close()
		waitForSignal(t, done, time.Second, "LRU race test shutdown")
	}()

	waitForTaskStatus(t, env, task.ID, model.TaskStatusRunning, 3*time.Second)
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls while response body is open = %d, want 0", deleteCalls.Load())
	}
	used.Store(0)
	if err := opened.Body.Close(); err != nil {
		t.Fatalf("close cache body: %v", err)
	}

	waitForTaskStatus(t, env, task.ID, model.TaskStatusCancelled, 3*time.Second)
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls = %d, want 0 after protected access", deleteCalls.Load())
	}
}

func TestEvictor_LRUChecksRemoteSafetyAfterWaitingForOpenBody(t *testing.T) {
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
	version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || version == nil || version.StorageUploadID == nil {
		t.Fatalf("GetVersionByID: version=%#v err=%v", version, err)
	}
	copies, err := env.repos.Uploads.ListCopies(context.Background(), *version.StorageUploadID)
	if err != nil || len(copies) == 0 || copies[0].StorageDataSetID == nil {
		t.Fatalf("ListCopies: copies=%#v err=%v", copies, err)
	}
	dataSetID := *copies[0].StorageDataSetID

	plannedAt := futureLRUAccessTime()
	if err := env.repos.Objects.RecordVersionCacheAccess(
		context.Background(),
		versionID,
		plannedAt,
	); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, plannedAt)
	checks := &observableReadableCheckRepo{
		StorageUploadRepository: env.repos.Uploads,
		started:                 make(chan struct{}),
	}
	env.repos.Uploads = checks

	opened, err := env.cacheGate.Open(
		versionID,
		func() (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: version.Size}, nil
		},
	)
	if err != nil {
		t.Fatalf("cache gate Open: %v", err)
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
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 20, 90, 50, 3),
	)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		_ = opened.Body.Close()
		waitForSignal(t, done, time.Second, "LRU remote-safety test shutdown")
	}()

	waitForTaskStatus(t, env, task.ID, model.TaskStatusRunning, 3*time.Second)
	select {
	case <-checks.started:
		t.Fatal("remote safety was checked before the open cache body released the deletion gate")
	case <-time.After(50 * time.Millisecond):
	}
	if err := env.repos.Uploads.MarkDataSetUnavailable(
		context.Background(),
		dataSetID,
		"provider unavailable",
	); err != nil {
		t.Fatalf("MarkDataSetUnavailable: %v", err)
	}
	if err := opened.Body.Close(); err != nil {
		t.Fatalf("close cache body: %v", err)
	}
	waitForSignal(t, checks.started, time.Second, "LRU final remote-safety check")
	waitForTaskStatus(t, env, task.ID, model.TaskStatusCancelled, 3*time.Second)
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls after remote safety changed = %d, want 0", deleteCalls.Load())
	}
}

type cacheAccessRecordingObjectRepo struct {
	repository.ObjectRepository

	accessWrites atomic.Int64
}

func (r *cacheAccessRecordingObjectRepo) RecordVersionCacheAccess(
	ctx context.Context,
	versionID string,
	accessedAt time.Time,
) error {
	r.accessWrites.Add(1)
	return r.ObjectRepository.RecordVersionCacheAccess(ctx, versionID, accessedAt)
}

func TestEvictor_LRUFlushesNewerMemoryAccessBeforeDatabaseStaleCancellation(t *testing.T) {
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		UsedBytesFunc: func() int64 { return 11 },
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return io.NopCloser(strings.NewReader("cached")), &cache.ObjectInfo{Size: 6}, nil
		},
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	bucket, objectID, versionID := seedStoredObject(t, env)
	recordingRepo := &cacheAccessRecordingObjectRepo{
		ObjectRepository: env.repos.Objects,
	}
	env.repos.Objects = recordingRepo
	cacheGate := cacheaccess.NewGate()
	accessTracker := cacheaccess.NewTracker(cacheaccess.DefaultPersistenceInterval, recordingRepo)
	reader := objectreader.New(
		env.repos,
		env.cache,
		nil,
		cacheGate,
		accessTracker,
		slog.Default(),
	)

	first, err := reader.Open(
		context.Background(),
		bucket.Name,
		"hello-"+versionID+".txt",
		objectreader.S3Visibility,
	)
	if err != nil {
		t.Fatalf("initial cache Open: %v", err)
	}
	_ = first.Body.Close()
	plannedVersion, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || plannedVersion == nil || plannedVersion.CacheAccessedAt == nil {
		t.Fatalf("version after initial cache access: version=%#v err=%v", plannedVersion, err)
	}
	task := seedLRUEvictionTask(
		t,
		env,
		objectID,
		versionID,
		*plannedVersion.CacheAccessedAt,
	)

	durableAfterPlan := plannedVersion.CacheAccessedAt.Add(time.Minute)
	if err := env.repos.Objects.RecordVersionCacheAccess(
		context.Background(),
		versionID,
		durableAfterPlan,
	); err != nil {
		t.Fatalf("RecordVersionCacheAccess(after plan): %v", err)
	}
	second, err := reader.Open(
		context.Background(),
		bucket.Name,
		"hello-"+versionID+".txt",
		objectreader.S3Visibility,
	)
	if err != nil {
		t.Fatalf("second cache Open: %v", err)
	}
	_ = second.Body.Close()

	beforeEviction, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || beforeEviction == nil || beforeEviction.CacheAccessedAt == nil {
		t.Fatalf("version before eviction: version=%#v err=%v", beforeEviction, err)
	}
	if !beforeEviction.CacheAccessedAt.Equal(durableAfterPlan) {
		t.Fatalf(
			"coalesced cache access persisted early at %v, want durable time %v",
			beforeEviction.CacheAccessedAt,
			durableAfterPlan,
		)
	}

	recordingRepo.accessWrites.Store(0)
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		cacheGate,
		accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 20, 90, 50, 3),
	)
	got := runWorkerUntilTask(t, env, evictor, task.ID, 3*time.Second)
	if got.Status != model.TaskStatusCancelled {
		t.Fatalf("stale LRU task status = %s, want cancelled", got.Status)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls = %d, want 0 after newer cache access", deleteCalls.Load())
	}
	if recordingRepo.accessWrites.Load() != 1 {
		t.Fatalf(
			"access writes before stale cancellation = %d, want one deferred flush",
			recordingRepo.accessWrites.Load(),
		)
	}
	afterEviction, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || afterEviction == nil || afterEviction.CacheAccessedAt == nil {
		t.Fatalf("version after stale cancellation: version=%#v err=%v", afterEviction, err)
	}
	if !afterEviction.CacheAccessedAt.After(durableAfterPlan) {
		t.Fatalf(
			"flushed cache access = %v, want after durable time %v",
			afterEviction.CacheAccessedAt,
			durableAfterPlan,
		)
	}
}

func TestEvictor_LRUCanEvictRehydratedCacheEvictedVersion(t *testing.T) {
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
	previousAccess := futureLRUAccessTime()
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, previousAccess); err != nil {
		t.Fatalf("RecordVersionCacheAccess(previous): %v", err)
	}
	if err := env.repos.Objects.UpdateVersionState(
		context.Background(),
		versionID,
		model.ObjectStateStored,
		model.ObjectStateCacheEvicted,
	); err != nil {
		t.Fatalf("UpdateVersionState(cache_evicted): %v", err)
	}
	completedAt := time.Now()
	previousTask := cacheeviction.NewLRUTask(cacheeviction.Candidate{
		ObjectID:   objectID,
		VersionID:  versionID,
		AccessedAt: previousAccess,
	}, 3, completedAt)
	previousTask.Status = model.TaskStatusCompleted
	previousTask.CompletedAt = &completedAt
	if err := env.repos.Tasks.Create(context.Background(), previousTask); err != nil {
		t.Fatalf("Create previous completed LRU task: %v", err)
	}
	accessedAt := previousAccess.Add(time.Hour)
	if err := env.repos.Objects.RecordVersionCacheCommit(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheCommit(rehydrated): %v", err)
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
		version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
		return version != nil && !version.InCache, err
	})
	tasks, total, err := env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		cacheeviction.StageLRU,
		"",
		10,
		0,
	)
	if err != nil || total != 1 || len(tasks) != 1 || tasks[0].ID != previousTask.ID {
		t.Fatalf("stable rehydrated LRU task total=%d tasks=%#v err=%v, want reused task", total, tasks, err)
	}
	version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	if version.State != model.ObjectStateCacheEvicted || version.InCache {
		t.Fatalf("version state/cache = %s/%v, want cache_evicted/false", version.State, version.InCache)
	}
}

func TestEvictor_LRUReconcilesAlreadyMissingCacheEntry(t *testing.T) {
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		UsedBytesFunc: func() int64 { return 11 },
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return false
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objectID, versionID := seedStoredObject(t, env)
	accessedAt := futureLRUAccessTime()
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, accessedAt)

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 20, 90, 50, 3),
	)
	got := runWorkerUntilTask(t, env, evictor, task.ID, 3*time.Second)
	if got.Status != model.TaskStatusCompleted {
		t.Fatalf("missing-entry LRU task status = %s, want completed", got.Status)
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("idempotent cache Delete calls = %d, want 1", deleteCalls.Load())
	}
	version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	if version.State != model.ObjectStateCacheEvicted || version.InCache {
		t.Fatalf("version state/cache = %s/%v, want cache_evicted/false", version.State, version.InCache)
	}
}

func TestEvictor_LRUTransientVersionLookupFailureSchedulesRetry(t *testing.T) {
	mc := &testutil.MockCache{
		UsedBytesFunc: func() int64 { return 11 },
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objectID, versionID := seedStoredObject(t, env)
	accessedAt := futureLRUAccessTime()
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, accessedAt)
	env.repos.Objects = &failOnceVersionLookupRepo{ObjectRepository: env.repos.Objects}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 20, 90, 50, 3),
	)
	runWorkerUntilTaskRetryCount(t, env, evictor, task.ID, 1, 3*time.Second)
	got, err := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if err != nil || got == nil {
		t.Fatalf("GetByID after transient lookup: task=%v err=%v", got, err)
	}
	if got.Status != model.TaskStatusScheduled || got.RetryCount != 1 {
		t.Fatalf("transient lookup LRU task status/retries = %s/%d, want scheduled/1", got.Status, got.RetryCount)
	}
}

type failOnceVersionLookupRepo struct {
	repository.ObjectRepository
	failed atomic.Bool
}

func (r *failOnceVersionLookupRepo) GetVersionByID(ctx context.Context, versionID string) (*model.ObjectVersion, error) {
	if r.failed.CompareAndSwap(false, true) {
		return nil, errors.New("transient version lookup failure")
	}
	return r.ObjectRepository.GetVersionByID(ctx, versionID)
}

func TestEvictor_LRUConcurrentWorkersStopAtLowWatermark(t *testing.T) {
	var used atomic.Int64
	used.Store(33)
	var deleteCalls atomic.Int64
	var activeDeletes atomic.Int64
	var concurrentOnce sync.Once
	concurrentDeletes := make(chan struct{})
	releaseDeletes := make(chan struct{})
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(ctx context.Context, _, _ string) error {
			deleteCalls.Add(1)
			if activeDeletes.Add(1) == 2 {
				concurrentOnce.Do(func() { close(concurrentDeletes) })
			}
			defer activeDeletes.Add(-1)
			select {
			case <-releaseDeletes:
			case <-ctx.Done():
				return ctx.Err()
			}
			used.Add(-11)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	accessedAt := futureLRUAccessTime()
	var taskIDs []int64
	for range 3 {
		_, objectID, versionID := seedStoredObject(t, env)
		if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
			t.Fatalf("RecordVersionCacheAccess(%s): %v", versionID, err)
		}
		taskIDs = append(taskIDs, seedLRUEvictionTask(t, env, objectID, versionID, accessedAt).ID)
	}
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		3,
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
	select {
	case <-concurrentDeletes:
		close(releaseDeletes)
	case <-time.After(time.Second):
		close(releaseDeletes)
		cancel()
		waitForSignal(t, done, time.Second, "concurrent LRU evictor shutdown")
		t.Fatal("LRU cache deletes did not run concurrently")
	}
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		terminal := 0
		for _, taskID := range taskIDs {
			task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
			if err == nil && task != nil && !taskStatusActive(task.Status) {
				terminal++
			}
		}
		if terminal == len(taskIDs) {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("timed out waiting for concurrent LRU tasks; terminal=%d used=%d", terminal, used.Load())
		case <-ticker.C:
		}
	}
	cancel()
	waitForSignal(t, done, time.Second, "concurrent LRU evictor shutdown")

	if deleteCalls.Load() != 2 || used.Load() != 11 {
		t.Fatalf("concurrent LRU deletes/used = %d/%d, want 2/11", deleteCalls.Load(), used.Load())
	}
	var completed, cancelled int
	for _, taskID := range taskIDs {
		task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
		if err != nil || task == nil {
			t.Fatalf("GetByID(%d): task=%v err=%v", taskID, task, err)
		}
		switch task.Status {
		case model.TaskStatusCompleted:
			completed++
		case model.TaskStatusCancelled:
			cancelled++
		default:
			t.Fatalf("task %d status = %s, want completed/cancelled", taskID, task.Status)
		}
	}
	if completed != 2 || cancelled != 1 {
		t.Fatalf("concurrent LRU terminal counts completed/cancelled = %d/%d, want 2/1", completed, cancelled)
	}
}

func TestEvictor_CancelsTaskThatDoesNotMatchCurrentPolicy(t *testing.T) {
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objectID, versionID := seedStoredObject(t, env)
	task := seedTask(t, env, model.TaskTypeEvictCache, objectID, versionID, 3, 0)
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.cacheGate,
		env.accessTracker,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyNone, 20, 90, 80, 3),
	)

	got := runWorkerUntilTask(t, env, evictor, task.ID, 3*time.Second)
	if got.Status != model.TaskStatusCancelled {
		t.Fatalf("incompatible eviction task status = %s, want cancelled", got.Status)
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls = %d, want 0 for incompatible policy", deleteCalls.Load())
	}
}
