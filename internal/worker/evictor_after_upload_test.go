package worker_test

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
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
		env.cacheGate,
		env.accessTracker,
		env.sm,
		concurrency,
		pollInterval,
		slog.Default(),
		worker.WithCacheEvictionPolicy(cache.EvictionPolicyAfterUpload, 0, 90, 80, 3),
	)
}

func futureLRUAccessTime() time.Time {
	return time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
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

type observableReadableCheckRepo struct {
	repository.StorageUploadRepository

	calls   atomic.Int64
	started chan struct{}
}

func (r *observableReadableCheckRepo) HasReadableCommittedCopy(
	ctx context.Context,
	uploadID int64,
) (bool, error) {
	if r.calls.Add(1) == 1 {
		close(r.started)
	}
	return r.StorageUploadRepository.HasReadableCommittedCopy(ctx, uploadID)
}

func TestEvictor_ChecksRemoteSafetyAfterWaitingForOpenBody(t *testing.T) {
	var deleteCalls atomic.Int64
	mc := &testutil.MockCache{
		ExistsFunc: func(context.Context, string, string) bool { return true },
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
	task := seedTask(t, env, model.TaskTypeEvictCache, objectID, versionID, 5, 0)
	evictor := newAfterUploadEvictor(env, 1, 10*time.Millisecond)

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = evictor.Run(runCtx)
		close(done)
	}()
	defer func() {
		cancel()
		_ = opened.Body.Close()
		waitForSignal(t, done, time.Second, "remote-safety eviction shutdown")
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
	waitForSignal(t, checks.started, time.Second, "final remote-safety check")
	waitForTaskStatus(t, env, task.ID, model.TaskStatusFailed, 3*time.Second)

	if deleteCalls.Load() != 0 {
		t.Fatalf("cache Delete calls after remote safety changed = %d, want 0", deleteCalls.Load())
	}
	gotVersion, err := env.repos.Objects.GetVersionByID(context.Background(), versionID)
	if err != nil || gotVersion == nil || !gotVersion.InCache ||
		gotVersion.State != model.ObjectStateStored {
		t.Fatalf("version after remote safety changed = %#v err=%v", gotVersion, err)
	}
}
