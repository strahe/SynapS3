package worker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
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

	obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(ctx, objID)
	if obj.State != model.ObjectStateCacheEvicted {
		t.Errorf("expected object state cache_evicted, got %s", obj.State)
	}
	if obj.InCache {
		t.Error("expected object cache location to be false after successful eviction")
	}
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

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 2, 20*time.Millisecond, slog.Default())
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

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, pollInterval, slog.Default())
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
	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())

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
				task := &model.Task{
					Type:           model.TaskTypeEvictCache,
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

			evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
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

			evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
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
