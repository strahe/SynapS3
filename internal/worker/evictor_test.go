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
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
)

// seedStoredObject creates a bucket+object version in stored state with an accepted upload.
func seedStoredObject(t *testing.T, env *testWorkerEnv) (*model.Bucket, int64, string) {
	t.Helper()
	ctx := context.Background()

	bucket, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)

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

func TestEvictor_ReplicatingVersionEvictsCacheAndKeepsState(t *testing.T) {
	var deletedBucket, deletedKey string
	mc := &testutil.MockCache{
		DeleteFunc: func(_ context.Context, bucket, key string) error {
			deletedBucket = bucket
			deletedKey = key
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
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	primary, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        "101",
		CopyIndex:         0,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding primary: %v", err)
	}
	secondary, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:          bucket.ID,
		ProviderID:        "202",
		CopyIndex:         1,
		CreatedByUploadID: upload.ID,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding secondary: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{ID: primary.ID, UploadID: upload.ID, DataSetID: "1001", ClientDataSetID: "9001"}); err != nil {
		t.Fatalf("MarkDataSetReady primary: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, upload.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: primary.ID, CopyIndex: 0, Role: "primary", ProviderID: "101"},
		{StorageDataSetID: secondary.ID, CopyIndex: 1, Role: "secondary", ProviderID: "202"},
	}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     upload.ID,
		CopyIndex:    0,
		PieceCID:     testCID(t).String(),
		PieceID:      "3001",
		RetrievalURL: "https://primary.example/piece",
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted primary: %v", err)
	}
	if _, err := env.repos.Uploads.BindPrimaryCommittedUploadForContent(ctx, repository.BindPrimaryCommittedUploadInput{
		UploadID:    upload.ID,
		BucketID:    bucket.ID,
		ContentSize: version.Size,
		Checksum:    version.Checksum,
	}); err != nil {
		t.Fatalf("BindPrimaryCommittedUploadForContent: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)
	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	gotTask, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if gotTask.Status != model.TaskStatusCompleted {
		t.Fatalf("task status = %s, want completed", gotTask.Status)
	}
	gotVersion, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || gotVersion == nil {
		t.Fatalf("GetVersionByID after evict: version=%v err=%v", gotVersion, err)
	}
	if gotVersion.State != model.ObjectStateReplicating {
		t.Fatalf("version state = %s, want replicating", gotVersion.State)
	}
	if gotVersion.InCache {
		t.Fatal("version in_cache = true, want false")
	}
	if deletedBucket != bucket.Name || deletedKey != version.CacheKey {
		t.Fatalf("deleted cache = %s/%s, want %s/%s", deletedBucket, deletedKey, bucket.Name, version.CacheKey)
	}
}

func TestEvictor_MissingVersion(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, _ := seedStoredObject(t, env)

	task := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objID,
		RefVersionID:   "01J000000000000000MISSING1",
		IdempotencyKey: "evict_cache:missing",
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
	if got.LastError == nil || !strings.Contains(*got.LastError, "object not found") {
		t.Errorf("expected object not found error, got %v", got.LastError)
	}
}

func TestEvictor_WrongState(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	// Object in "uploading" state, not "stored"
	_, objID, versionID := seedObjectInDB(t, env, model.BucketStatusActive)
	ctx := context.Background()

	if err := env.repos.Objects.UpdateVersionState(ctx, versionID, model.ObjectStateCached, model.ObjectStateUploading); err != nil {
		t.Fatalf("transition: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "not stored") {
		t.Errorf("expected not stored error, got %v", got.LastError)
	}
}

func TestEvictor_NoReadableCopies(t *testing.T) {
	mc := &testutil.MockCache{}
	env := newTestWorkerEnvWithMockCache(t, mc)
	ctx := context.Background()

	_, objID, versionID := seedStoredObject(t, env)
	version, err := env.repos.Objects.GetVersionByID(ctx, versionID)
	if err != nil || version == nil || version.StorageUploadID == nil {
		t.Fatalf("stored version upload: version=%v err=%v", version, err)
	}
	if _, err := env.db.NewDelete().Model((*model.StorageUploadCopy)(nil)).Where("upload_id = ?", *version.StorageUploadID).Exec(ctx); err != nil {
		t.Fatalf("remove readable copies: %v", err)
	}

	task := seedTask(t, env, model.TaskTypeEvictCache, objID, versionID, 5, 0)

	evictor := worker.NewEvictor(env.repos, env.cache, env.sm, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, evictor, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "no readable upload copies") {
		t.Errorf("expected no readable upload copies error, got %v", got.LastError)
	}
}

func TestEvictor_CacheDeleteFailureLeavesObjectUnchangedAndKeepsTaskRecoverable(t *testing.T) {
	for _, tc := range []struct {
		name        string
		retryCount  int
		wantStatus  model.TaskStatus
		wantRetries int
	}{
		{name: "requeue", retryCount: 0, wantStatus: model.TaskStatusPending, wantRetries: 1},
		{name: "dead letter", retryCount: 4, wantStatus: model.TaskStatusDeadLetter, wantRetries: 5},
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
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_ = evictor.Run(ctx)

			got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
			if got.Status != tc.wantStatus {
				t.Errorf("expected task %s after cache delete failure, got %s", tc.wantStatus, got.Status)
			}
			if got.RetryCount != tc.wantRetries {
				t.Errorf("expected retry_count=%d, got %d", tc.wantRetries, got.RetryCount)
			}

			obj, _ := env.repos.Objects.GetCurrentVersionByObjectID(context.Background(), objID)
			if obj.State != model.ObjectStateStored {
				t.Errorf("expected object state stored after cache delete failure, got %s", obj.State)
			}
			if !obj.InCache {
				t.Error("expected object cache location to remain true after cache delete failure")
			}
		})
	}
}
