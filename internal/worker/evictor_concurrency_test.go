package worker_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
)

func seedLRUEvictionTask(
	t *testing.T,
	env *testWorkerEnv,
	objectID int64,
	versionID string,
	accessedAt time.Time,
) *model.Task {
	t.Helper()
	task := cacheeviction.NewLRUTask(cacheeviction.Candidate{
		ObjectID:   objectID,
		VersionID:  versionID,
		AccessedAt: accessedAt,
	}, 3, time.Now())
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
