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
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectreader"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

// seedStoredObject creates a bucket+object version in stored state with an accepted upload.
func seedStoredObject(t *testing.T, env *testWorkerEnv) (*model.Bucket, int64, string) {
	t.Helper()
	ctx := context.Background()

	versionID := model.NewVersionID()
	bucket := &model.Bucket{Name: "b-" + strings.ToLower(versionID), Status: model.BucketStatusActive}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	version := &model.ObjectVersion{
		VersionID:   versionID,
		BucketID:    bucket.ID,
		Key:         "hello-" + versionID + ".txt",
		Size:        11,
		ETag:        "etag-" + versionID,
		Checksum:    "sha256-" + versionID,
		ContentType: "text/plain",
		CacheKey:    ".versions/" + versionID,
	}
	objID, err := env.repos.Objects.CreateVersionAndSetCurrent(ctx, version)
	if err != nil {
		t.Fatalf("creating object version: %v", err)
	}

	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition to uploading: %v", err)
	}
	pieceCID := testCID(t).String()
	acceptWorkerVersionUpload(t, env, versionID, pieceCID, "https://provider.example/pieces/1")
	return bucket, objID, versionID
}

func newAfterUploadEvictor(
	env *testWorkerEnv,
	concurrency int,
	pollInterval time.Duration,
) *worker.Evictor {
	return worker.NewEvictor(
		env.repos,
		env.cache,
		env.sm,
		concurrency,
		pollInterval,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyAfterUpload, 0, 90, 80, 3),
	)
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

	evictor := newAfterUploadEvictor(env, 1, 50*time.Millisecond)
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	ctx := context.Background()

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if obj.State != model.ObjectStateCacheEvicted {
		t.Errorf("expected object state cache_evicted, got %s", obj.State)
	}
	if obj.InCache {
		t.Error("expected object cache location to be false after successful eviction")
	}
}

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
		repository.CacheEvictionStageLRU,
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
	base := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
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
			repository.CacheEvictionStageLRU,
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
		repository.CacheEvictionStageLRU,
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

func TestEvictor_LRUEvictsLegacyNullAccessTime(t *testing.T) {
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
		Set("cache_access_generation = NULL").
		Where("version_id = ?", versionID).
		Exec(context.Background()); err != nil {
		t.Fatalf("clear legacy cache access time: %v", err)
	}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 10, 90, 50, 3),
	)
	runWorkerUntilCondition(t, evictor, 10*time.Millisecond, 3*time.Second, func() (bool, error) {
		_, completed, err := env.repos.Tasks.List(
			context.Background(),
			string(model.TaskTypeEvictCache),
			repository.CacheEvictionStageLRU,
			string(model.TaskStatusCompleted),
			10,
			0,
		)
		return completed == 1, err
	})

	tasks, total, err := env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		repository.CacheEvictionStageLRU,
		string(model.TaskStatusCompleted),
		10,
		0,
	)
	if err != nil {
		t.Fatalf("List completed legacy NULL LRU tasks: %v", err)
	}
	if total != 1 || len(tasks) != 1 {
		t.Fatalf("completed legacy NULL LRU tasks total=%d tasks=%#v, want one", total, tasks)
	}
	if _, planned := tasks[0].Payload["cache_accessed_at"]; planned {
		t.Fatalf("legacy NULL LRU payload = %#v, want absent access snapshot", tasks[0].Payload)
	}
	if deleteCalls.Load() != 1 {
		t.Fatalf("legacy NULL LRU delete calls = %d, want 1", deleteCalls.Load())
	}
	version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || version == nil || version.State != model.ObjectStateCacheEvicted || version.InCache {
		t.Fatalf("legacy NULL LRU version after eviction = %#v err=%v", version, err)
	}
}

func TestEvictor_LRUReactivatesCancelledTaskForSameAccessGeneration(t *testing.T) {
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
	accessedAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}
	stage := repository.CacheEvictionStageLRU
	completedAt := time.Now()
	task := &model.Task{
		Type:         model.TaskTypeEvictCache,
		Stage:        &stage,
		RefType:      "object",
		RefID:        objectID,
		RefVersionID: versionID,
		IdempotencyKey: repository.LRUEvictionTaskKey(
			versionID,
			repository.CacheAccessGeneration(&accessedAt),
		),
		Payload: map[string]interface{}{
			"cache_accessed_at": accessedAt.Format(time.RFC3339Nano),
		},
		Status:      model.TaskStatusCancelled,
		MaxRetries:  3,
		ScheduledAt: completedAt,
		CompletedAt: &completedAt,
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create cancelled LRU task: %v", err)
	}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
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
		repository.CacheEvictionStageLRU,
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
	accessedAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}

	pollInterval := 10 * time.Millisecond
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
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
			repository.CacheEvictionStageLRU,
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
		repository.CacheEvictionStageLRU,
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
			repository.CacheEvictionStageLRU,
			string(model.TaskStatusCompleted),
			10,
			0,
		)
		return completed == 1, err
	})
	tasks, total, err = env.repos.Tasks.List(
		context.Background(),
		string(model.TaskTypeEvictCache),
		repository.CacheEvictionStageLRU,
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
	plannedAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
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

type blockingFirstVersionLookupRepo struct {
	repository.ObjectRepository

	loaded       chan struct{}
	release      chan struct{}
	blockOnce    sync.Once
	accessWrites atomic.Int64
}

func (r *blockingFirstVersionLookupRepo) GetVersionByID(
	ctx context.Context,
	versionID string,
) (*model.ObjectVersion, error) {
	version, err := r.ObjectRepository.GetVersionByID(ctx, versionID)
	blocked := false
	r.blockOnce.Do(func() {
		blocked = true
		close(r.loaded)
	})
	if blocked {
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return version, err
}

func (r *blockingFirstVersionLookupRepo) RecordVersionCacheAccess(
	ctx context.Context,
	versionID string,
	accessedAt time.Time,
) error {
	r.accessWrites.Add(1)
	return r.ObjectRepository.RecordVersionCacheAccess(ctx, versionID, accessedAt)
}

func TestEvictor_LRUFinalCheckProtectsAccessAfterInitialVersionLoad(t *testing.T) {
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
	coordinator := cacheaccess.NewCoordinator(cacheaccess.DefaultPersistenceInterval)
	reader := objectreader.New(
		env.repos,
		env.cache,
		nil,
		slog.Default(),
		objectreader.WithCacheAccessCoordinator(coordinator),
	)

	first, err := reader.Open(context.Background(), bucket.Name, "hello-"+versionID+".txt", objectreader.S3Visibility)
	if err != nil {
		t.Fatalf("initial cache Open: %v", err)
	}
	_ = first.Body.Close()
	version, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || version == nil || version.CacheAccessedAt == nil {
		t.Fatalf("version after initial cache access: version=%#v err=%v", version, err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, *version.CacheAccessedAt)

	blockingRepo := &blockingFirstVersionLookupRepo{
		ObjectRepository: env.repos.Objects,
		loaded:           make(chan struct{}),
		release:          make(chan struct{}),
	}
	env.repos.Objects = blockingRepo
	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 20, 90, 50, 3),
		worker.WithCacheAccessCoordinator(coordinator),
	)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var releaseOnce sync.Once
	go func() {
		_ = evictor.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(blockingRepo.release) })
		waitForSignal(t, done, time.Second, "LRU race test shutdown")
	}()

	waitForSignal(t, blockingRepo.loaded, time.Second, "initial LRU version load")
	second, err := reader.Open(context.Background(), bucket.Name, "hello-"+versionID+".txt", objectreader.S3Visibility)
	if err != nil {
		t.Fatalf("concurrent cache Open: %v", err)
	}
	_ = second.Body.Close()
	if blockingRepo.accessWrites.Load() != 0 {
		t.Fatalf("concurrent hot-path access writes = %d, want coalesced in memory", blockingRepo.accessWrites.Load())
	}
	releaseOnce.Do(func() { close(blockingRepo.release) })

	waitForTaskStatus(t, env, task.ID, model.TaskStatusCancelled, 3*time.Second)
	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls = %d, want 0 after concurrent access", deleteCalls.Load())
	}
	if blockingRepo.accessWrites.Load() != 1 {
		t.Fatalf("access writes after final LRU check = %d, want one deferred flush", blockingRepo.accessWrites.Load())
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
	previousAccess := time.Date(2026, time.June, 1, 0, 0, 0, 0, time.UTC)
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
	stage := repository.CacheEvictionStageLRU
	completedAt := time.Now()
	previousTask := &model.Task{
		Type:         model.TaskTypeEvictCache,
		Stage:        &stage,
		RefType:      "object",
		RefID:        objectID,
		RefVersionID: versionID,
		IdempotencyKey: repository.LRUEvictionTaskKey(
			versionID,
			repository.CacheAccessGeneration(&previousAccess),
		),
		Payload: map[string]interface{}{
			"cache_accessed_at": previousAccess.Format(time.RFC3339Nano),
		},
		Status:      model.TaskStatusCompleted,
		MaxRetries:  3,
		ScheduledAt: completedAt,
		CompletedAt: &completedAt,
	}
	if err := env.repos.Tasks.Create(context.Background(), previousTask); err != nil {
		t.Fatalf("Create previous completed LRU task: %v", err)
	}
	accessedAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess(rehydrated): %v", err)
	}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
		env.sm,
		1,
		10*time.Millisecond,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyLRU, 10, 90, 50, 3),
	)
	runWorkerUntilCondition(t, evictor, 10*time.Millisecond, 3*time.Second, func() (bool, error) {
		_, completed, err := env.repos.Tasks.List(
			context.Background(),
			string(model.TaskTypeEvictCache),
			repository.CacheEvictionStageLRU,
			string(model.TaskStatusCompleted),
			10,
			0,
		)
		return completed == 2, err
	})
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
	accessedAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, accessedAt)

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
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
	accessedAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	if err := env.repos.Objects.RecordVersionCacheAccess(context.Background(), versionID, accessedAt); err != nil {
		t.Fatalf("RecordVersionCacheAccess: %v", err)
	}
	task := seedLRUEvictionTask(t, env, objectID, versionID, accessedAt)
	env.repos.Objects = &failOnceVersionLookupRepo{ObjectRepository: env.repos.Objects}

	evictor := worker.NewEvictor(
		env.repos,
		env.cache,
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
	mc := &testutil.MockCache{
		UsedBytesFunc: used.Load,
		ExistsFunc: func(_ context.Context, _, _ string) bool {
			return true
		},
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalls.Add(1)
			used.Add(-11)
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	accessedAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
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

func seedLRUEvictionTask(
	t *testing.T,
	env *testWorkerEnv,
	objectID int64,
	versionID string,
	accessedAt time.Time,
) *model.Task {
	t.Helper()
	stage := repository.CacheEvictionStageLRU
	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          objectID,
		RefVersionID:   versionID,
		IdempotencyKey: "evict_cache:lru:test:" + model.NewVersionID(),
		Payload: map[string]interface{}{
			"cache_accessed_at": accessedAt.UTC().Format(time.RFC3339Nano),
		},
		Status:      model.TaskStatusQueued,
		MaxRetries:  3,
		ScheduledAt: time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("Create LRU eviction task: %v", err)
	}
	return task
}

func TestEvictor_ClaimsLaterPendingTaskWhileAnotherEvictionRuns(t *testing.T) {
	firstDeleteEntered := make(chan struct{})
	releaseFirstDelete := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once

	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, firstObjID, firstVersionID := seedStoredObject(t, env)
	firstTask := seedTask(t, env, model.TaskTypeEvictCache, firstObjID, firstVersionID, 5, 0)
	firstCacheKey := ".versions/" + firstVersionID
	_, secondObjID, secondVersionID := seedStoredObject(t, env)

	mc.DeleteFunc = func(ctx context.Context, _, key string) error {
		if key == firstCacheKey {
			enterOnce.Do(func() { close(firstDeleteEntered) })
			select {
			case <-releaseFirstDelete:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}

	evictor := newAfterUploadEvictor(env, 2, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(ctx)
		close(done)
	}()
	defer func() {
		releaseOnce.Do(func() { close(releaseFirstDelete) })
		cancel()
		waitForSignal(t, done, time.Second, "evictor shutdown")
	}()

	waitForSignal(t, firstDeleteEntered, time.Second, "first eviction delete to start")

	secondTask := seedTask(t, env, model.TaskTypeEvictCache, secondObjID, secondVersionID, 5, 0)

	waitForTaskStatus(t, env, secondTask.ID, model.TaskStatusCompleted, 500*time.Millisecond)

	got, err := env.repos.Tasks.GetByID(context.Background(), firstTask.ID)
	if err != nil {
		t.Fatalf("get first task: %v", err)
	}
	if got.Status != model.TaskStatusRunning {
		t.Fatalf("first task status = %s, want running while second task completed", got.Status)
	}
}

func TestEvictor_HealthyWhileEvictionTaskIsActive(t *testing.T) {
	deleteEntered := make(chan struct{})
	releaseDelete := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	pollInterval := 20 * time.Millisecond

	mc := &testutil.MockCache{
		DeleteFunc: func(ctx context.Context, _, _ string) error {
			enterOnce.Do(func() { close(deleteEntered) })
			select {
			case <-releaseDelete:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, versionID := seedStoredObject(t, env)
	_ = seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)

	evictor := newAfterUploadEvictor(env, 1, pollInterval)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(ctx)
		close(done)
	}()
	defer func() {
		releaseOnce.Do(func() { close(releaseDelete) })
		cancel()
		waitForSignal(t, done, time.Second, "evictor shutdown")
	}()

	waitForSignal(t, deleteEntered, time.Second, "eviction delete to start")
	time.Sleep(4 * pollInterval)

	if !evictor.Healthy() {
		t.Fatal("evictor should remain healthy while eviction task is active")
	}
}

func TestEvictor_ReplicatingVersionDefersEvictionAndKeepsCache(t *testing.T) {
	deleteCalled := false
	mc := &testutil.MockCache{
		DeleteFunc: func(_ context.Context, _, _ string) error {
			deleteCalled = true
			return nil
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	bucket, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
	ctx := context.Background()

	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("uploading: %v", err)
	}
	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateUploading, model.ObjectStateCommitting); err != nil {
		t.Fatalf("committing: %v", err)
	}
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil {
		t.Fatalf("GetVersionByID: version=%v err=%v", version, err)
	}
	upload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: versionID,
		ContentSize:     version.Size,
		Checksum:        version.Checksum,
		RequestedCopies: 3,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "101"),
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding primary: %v", err)
	}
	secondary, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        onChainID(t, "202"),
		CopyIndex:         1,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding secondary: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: onChainID(t, "1001"), ClientDataSetID: onChainIDPtr(t, "9001")}); err != nil {
		t.Fatalf("MarkDataSetReady primary: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, TransferMethod: model.StorageCopyTransferMethodIngress, ProviderID: onChainID(t, "101")},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: onChainID(t, "202")},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     testCID(t).String(),
		PieceID:      onChainIDPtr(t, "3001"),
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted primary: %v", err)
	}
	if _, err := env.repos.Uploads.BindReadableUploadForContent(ctx, repository.BindReadableUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindReadableUploadForContent: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)
	originalScheduledAt := task.ScheduledAt
	evictor := newAfterUploadEvictor(env, 1, 50*time.Millisecond)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		waitForSignal(t, done, time.Second, "evictor shutdown")
	}()

	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var gotTask *model.Task
	for gotTask == nil {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for replicating evict task to be deferred")
		case <-ticker.C:
			current, err := env.repos.Tasks.GetByID(ctx, task.ID)
			if err != nil || current == nil {
				continue
			}
			if current.Status != model.TaskStatusQueued && current.Status != model.TaskStatusWaiting {
				if current.Status != model.TaskStatusRunning {
					gotTask = current
				}
				continue
			}
			if current.StatusMessage != nil && strings.Contains(*current.StatusMessage, "waiting for all copies") {
				gotTask = current
			}
		}
	}
	if gotTask.Status != model.TaskStatusWaiting {
		t.Fatalf("task status = %s, want waiting deferred task", gotTask.Status)
	}
	if gotTask.RetryCount != 0 {
		t.Fatalf("task retry_count = %d, want 0", gotTask.RetryCount)
	}
	if gotTask.LastError != nil {
		t.Fatalf("task last_error = %v, want nil", gotTask.LastError)
	}
	if gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("task wait_reason = %v, want dependency", gotTask.WaitReason)
	}
	if gotTask.StatusMessage == nil || !strings.Contains(*gotTask.StatusMessage, "waiting for all copies") {
		t.Fatalf("task status_message = %v, want waiting-for-copies reason", gotTask.StatusMessage)
	}
	if !gotTask.ScheduledAt.After(originalScheduledAt) {
		t.Fatalf("task scheduled_at = %s, want after %s", gotTask.ScheduledAt, originalScheduledAt)
	}
	if gotTask.ScheduledAt.Before(originalScheduledAt.Add(20 * time.Second)) {
		t.Fatalf("task scheduled_at = %s, want a longer defer while replication is still running", gotTask.ScheduledAt)
	}
	gotVersion, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || gotVersion == nil {
		t.Fatalf("GetVersionByID after evict: version=%v err=%v", gotVersion, err)
	}
	if gotVersion.State != model.ObjectStateReplicating {
		t.Fatalf("version state = %s, want replicating", gotVersion.State)
	}
	if !gotVersion.InCache {
		t.Fatal("version in_cache = false, want cache retained while replicating")
	}
	if deleteCalled {
		t.Fatalf("cache delete was called for replicating version %s in bucket %s", version.CacheKey, bucket.Name)
	}
}

func TestEvictor_Preconditions(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(ctx context.Context, t *testing.T, env *testWorkerEnv) *model.Task
		wantLastError string
	}{
		{
			name: "MissingVersion",
			setup: func(ctx context.Context, t *testing.T, env *testWorkerEnv) *model.Task {
				_, objID, _ := seedStoredObject(t, env)
				stage := repository.CacheEvictionStageAfterUpload
				task := &model.Task{
					Type:           model.TaskTypeEvictCache,
					Stage:          &stage,
					RefType:        "object",
					RefID:          objID,
					RefVersionID:   "01J000000000000000MISSING1",
					IdempotencyKey: "evict_cache:missing",
					Status:         model.TaskStatusQueued,
					MaxRetries:     5,
					ScheduledAt:    time.Now(),
				}
				if err := env.repos.Tasks.Create(ctx, task); err != nil {
					t.Fatalf("creating task: %v", err)
				}
				return task
			},
			wantLastError: "object not found",
		},
		{
			name: "WrongState",
			setup: func(ctx context.Context, t *testing.T, env *testWorkerEnv) *model.Task {
				_, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
				if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
					t.Fatalf("transition: %v", err)
				}
				return seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)
			},
			wantLastError: "not stored",
		},
		{
			name: "NoReadableCopies",
			setup: func(ctx context.Context, t *testing.T, env *testWorkerEnv) *model.Task {
				_, objID, versionID := seedStoredObject(t, env)
				version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
				if err != nil || version == nil || version.StorageUploadID == nil {
					t.Fatalf("stored version upload: version=%v err=%v", version, err)
				}
				if _, err := env.db.NewDelete().Model((*model.StorageUploadCopy)(nil)).Where("upload_id = ?", *version.StorageUploadID).Exec(ctx); err != nil {
					t.Fatalf("remove readable copies: %v", err)
				}
				return seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)
			},
			wantLastError: "no readable upload copies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &testutil.MockCache{}
			env := newTestWorkerEnvWithMockCache(t, mc)
			ctx := context.Background()

			task := tt.setup(ctx, t, env)

			evictor := newAfterUploadEvictor(env, 1, 50*time.Millisecond)
			runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

			got, err := env.repos.Tasks.GetByID(ctx, task.ID)
			if err != nil {
				t.Fatalf("getting task: %v", err)
			}
			if got == nil {
				t.Fatalf("task %d not found", task.ID)
			}
			if got.Status != model.TaskStatusFailed {
				t.Errorf("expected task failed, got %s", got.Status)
			}
			if got.LastError == nil || !strings.Contains(*got.LastError, tt.wantLastError) {
				t.Errorf("expected last error to contain %q, got %v", tt.wantLastError, got.LastError)
			}
		})
	}
}

func TestEvictor_CacheDeleteFailureLeavesObjectUnchangedAndKeepsTaskRecoverable(t *testing.T) {
	for _, tc := range []struct {
		name        string
		retryCount  int
		wantStatus  model.TaskStatus
		wantRetries int
	}{
		{name: "requeue", retryCount: 0, wantStatus: model.TaskStatusScheduled, wantRetries: 1},
		{name: "exhausted", retryCount: 4, wantStatus: model.TaskStatusExhausted, wantRetries: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, tc.retryCount)

			evictor := newAfterUploadEvictor(env, 1, 50*time.Millisecond)
			if tc.wantStatus == model.TaskStatusScheduled {
				runWorkerUntilTaskRetryCount(t, env, evictor, task.ID, tc.wantRetries, 5*time.Second)
			} else {
				runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)
			}

			got, err := env.repos.Tasks.GetByID(context.Background(), task.ID)
			if err != nil || got == nil {
				t.Fatalf("get task after cache delete failure: task=%v err=%v", got, err)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("expected task %s after cache delete failure, got %s", tc.wantStatus, got.Status)
			}
			if got.RetryCount != tc.wantRetries {
				t.Errorf("expected retry_count=%d, got %d", tc.wantRetries, got.RetryCount)
			}

			obj, err := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
			if err != nil || obj == nil {
				t.Fatalf("get object after cache delete failure: object=%v err=%v", obj, err)
			}
			if obj.State != model.ObjectStateStored {
				t.Errorf("expected object state stored after cache delete failure, got %s", obj.State)
			}
			if !obj.InCache {
				t.Error("expected object cache location to remain true after cache delete failure")
			}
		})
	}
}
