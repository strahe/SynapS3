package worker_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
)

func testCID(t *testing.T) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("test-data"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatalf("creating test multihash: %v", err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

// seedCachedObject creates a bucket, writes a file into the filesystem cache,
// and inserts an object in "cached" state.
func seedCachedObject(t *testing.T, env *testWorkerEnv) (*model.Bucket, int64, int64) {
	t.Helper()
	ctx := context.Background()

	bucket := &model.Bucket{Name: fmt.Sprintf("b-%d", time.Now().UnixNano()), Status: model.BucketStatusActive}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	key := "hello.txt"
	data := []byte("hello world")

	info, err := env.cache.Put(ctx, bucket.Name, key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("cache put: %v", err)
	}

	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         key,
		Size:        int64(len(data)),
		ETag:        info.ETag,
		Checksum:    info.Checksum,
		ContentType: "text/plain",
		CachePath:   info.Path,
		MaxRetries:  5,
	}
	objID, gen, err := env.repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("upserting object: %v", err)
	}
	return bucket, objID, gen
}

// seedObjectInDB inserts a bucket and object into the DB only (no cache write).
func seedObjectInDB(t *testing.T, env *testWorkerEnv, bucketStatus model.BucketStatus) (*model.Bucket, int64, int64) {
	t.Helper()
	ctx := context.Background()

	bucket := &model.Bucket{Name: fmt.Sprintf("b-%d", time.Now().UnixNano()), Status: bucketStatus}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("creating bucket: %v", err)
	}

	obj := &model.Object{
		BucketID:    bucket.ID,
		Key:         "hello.txt",
		Size:        11,
		ETag:        "abc123",
		Checksum:    "sha256-test",
		ContentType: "text/plain",
		CachePath:   "/fake/path",
		MaxRetries:  5,
	}
	objID, gen, err := env.repos.Objects.UpsertAndBumpGeneration(ctx, obj)
	if err != nil {
		t.Fatalf("upserting object: %v", err)
	}
	return bucket, objID, gen
}

// seedTask creates a pending task of the given type.
func seedTask(t *testing.T, env *testWorkerEnv, taskType model.TaskType, refID, gen int64, maxRetries, retryCount int) *model.Task {
	t.Helper()
	ctx := context.Background()
	task := &model.Task{
		Type:           taskType,
		RefType:        "object",
		RefID:          refID,
		RefGeneration:  gen,
		IdempotencyKey: fmt.Sprintf("%s:%d:%d", taskType, refID, gen),
		Status:         model.TaskStatusPending,
		RetryCount:     retryCount,
		MaxRetries:     maxRetries,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("creating task: %v", err)
	}
	return task
}

// runWorkerUntilTask runs a worker and waits until the given task
// leaves pending/running status, or times out.
func runWorkerUntilTask(t *testing.T, env *testWorkerEnv, w worker.Worker, taskID int64, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()

	deadline := time.After(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("timed out waiting for task %d to be processed", taskID)
		case <-ticker.C:
			task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
			if err != nil {
				continue
			}
			if task != nil && task.Status != model.TaskStatusPending && task.Status != model.TaskStatusRunning {
				cancel()
				<-done
				return
			}
		}
	}
}

func TestUploader_HappyPath(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 0)

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     11,
			Complete: true,
			Copies: []storage.CopyResult{
				{RetrievalURL: "https://provider.example/pieces/1"},
			},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()

	got, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed, got %s", got.Status)
	}

	obj, err := env.repos.Objects.GetByID(ctx, objID)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if obj.State != model.ObjectStateStored {
		t.Errorf("expected object state stored, got %s", obj.State)
	}
	if obj.PieceCID == nil || *obj.PieceCID != pieceCID.String() {
		t.Errorf("expected PieceCID %s, got %v", pieceCID.String(), obj.PieceCID)
	}
	var retrievalURL string
	if err := env.db.NewSelect().
		TableExpr("objects").
		Column("retrieval_url").
		Where("id = ?", objID).
		Scan(ctx, &retrievalURL); err != nil {
		t.Fatalf("select retrieval_url: %v", err)
	}
	if retrievalURL != "https://provider.example/pieces/1" {
		t.Fatalf("retrieval_url = %q, want persisted URL", retrievalURL)
	}

	// With autoEvict=true, an evict_cache task should be created
	evictTask, err := env.repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict_cache: %v", err)
	}
	if evictTask == nil {
		t.Fatal("expected evict_cache task to exist")
	}
	if evictTask.RefID != objID || evictTask.RefGeneration != gen {
		t.Errorf("evict task refs mismatch: got refID=%d gen=%d, want %d/%d",
			evictTask.RefID, evictTask.RefGeneration, objID, gen)
	}
}

func TestUploader_PartialSuccessDoesNotMarkObjectStored(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 1, 0)

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID:        pieceCID,
			Size:            11,
			RequestedCopies: 2,
			Complete:        false,
			Copies: []storage.CopyResult{
				{RetrievalURL: "https://provider.example/pieces/1"},
			},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if gotTask.Status != model.TaskStatusDeadLetter {
		t.Fatalf("expected task dead-lettered, got %s", gotTask.Status)
	}

	obj, err := env.repos.Objects.GetByID(ctx, objID)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if obj.State != model.ObjectStateFailed {
		t.Fatalf("expected object failed, got %s", obj.State)
	}

	evictTask, err := env.repos.Tasks.ClaimPending(ctx, model.TaskTypeEvictCache, time.Minute)
	if err != nil {
		t.Fatalf("claiming evict_cache: %v", err)
	}
	if evictTask != nil {
		t.Fatal("did not expect evict_cache task for partial upload")
	}
}

func TestUploader_StaleGeneration(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, _ := seedCachedObject(t, env)

	task := &model.Task{
		Type:           model.TaskTypeUpload,
		RefType:        "object",
		RefID:          objID,
		RefGeneration:  0, // stale — object is at gen 1
		IdempotencyKey: fmt.Sprintf("upload:%d:0", objID),
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), task); err != nil {
		t.Fatalf("creating task: %v", err)
	}

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		t.Error("upload should not be called for stale generation")
		return nil, errors.New("should not be called")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "stale generation") {
		t.Errorf("expected stale generation error, got %v", got.LastError)
	}
}

func TestUploader_NilStorageClient(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 0)

	uploader := worker.NewUploader(env.repos, env.cache, nil, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "storage client not configured") {
		t.Errorf("expected storage client error, got %v", got.LastError)
	}
}

func TestUploader_CacheReadFailure(t *testing.T) {
	mc := &testutil.MockCache{
		GetFunc: func(_ context.Context, _, _ string) (io.ReadCloser, *cache.ObjectInfo, error) {
			return nil, nil, errors.New("disk I/O error")
		},
	}
	env := newTestWorkerEnvWithMockCache(t, mc)
	_, objID, gen := seedObjectInDB(t, env, model.BucketStatusActive)
	// RetryCount at max-1 so this attempt triggers dead-letter terminal path.
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 4)

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		t.Error("upload should not be called after cache read failure")
		return nil, errors.New("should not be called")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusDeadLetter {
		t.Errorf("expected task dead_letter, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetByID(ctx, objID)
	if obj.State != model.ObjectStateFailed {
		t.Errorf("expected object state failed, got %s", obj.State)
	}
}

func TestUploader_SPUploadFailure_Retry(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 0)

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return nil, errors.New("SP unavailable")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	// After Fail+Requeue, task goes back to pending with future ScheduledAt.
	// Use a short Run since runWorkerUntilTask won't see it leave pending.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = uploader.Run(ctx)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusPending {
		t.Errorf("expected task requeued to pending, got %s", got.Status)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected retry_count=1, got %d", got.RetryCount)
	}
}

func TestUploader_SPUploadFailure_MaxRetries(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)

	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 4)

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return nil, errors.New("SP permanent failure")
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	ctx := context.Background()
	got, _ := env.repos.Tasks.GetByID(ctx, task.ID)
	if got.Status != model.TaskStatusDeadLetter {
		t.Errorf("expected task dead_letter, got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetByID(ctx, objID)
	if obj.State != model.ObjectStateFailed {
		t.Errorf("expected object state failed, got %s", obj.State)
	}
}

func TestUploader_EvictTaskIdempotency(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 0)

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     11,
			Complete: true,
			Copies:   []storage.CopyResult{{RetrievalURL: "https://provider.example/pieces/1"}},
		}, nil
	}

	// Pre-create conflicting evict_cache task to trigger idempotency collision
	conflict := &model.Task{
		Type:           model.TaskTypeEvictCache,
		RefType:        "object",
		RefID:          objID,
		RefGeneration:  gen,
		IdempotencyKey: fmt.Sprintf("evict_cache:%d:%d", objID, gen),
		Status:         model.TaskStatusPending,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
	}
	if err := env.repos.Tasks.Create(context.Background(), conflict); err != nil {
		t.Fatalf("creating conflict task: %v", err)
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, nil, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	// ErrAlreadyExists is treated as idempotent success — task completes
	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (evict task already exists = idempotent), got %s", got.Status)
	}

	obj, _ := env.repos.Objects.GetByID(context.Background(), objID)
	if obj.State != model.ObjectStateStored {
		t.Errorf("expected object in stored state, got %s", obj.State)
	}
}

func TestUploader_ZeroBalance_FailsTask(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return &synapse.WalletInfo{
				USDFCAccount: &synapse.TokenAccountInfo{
					Funds: big.NewInt(0),
				},
			}, nil
		},
	}

	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		t.Fatal("upload should not be called when balance is zero")
		return nil, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusFailed {
		t.Errorf("expected task failed for zero balance, got %s", got.Status)
	}
	if got.LastError == nil || !strings.Contains(*got.LastError, "insufficient payment account balance") {
		le := ""
		if got.LastError != nil {
			le = *got.LastError
		}
		t.Errorf("expected balance error message, got: %s", le)
	}
}

func TestUploader_WalletError_ProceedsWithUpload(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return nil, errors.New("RPC timeout")
		},
	}

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     11,
			Complete: true,
			Copies:   []storage.CopyResult{{RetrievalURL: "https://provider.example/pieces/1"}},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (wallet error should not block upload), got %s", got.Status)
	}
}

func TestUploader_WalletNilInfo_NoPanic(t *testing.T) {
	env := newTestWorkerEnv(t)
	_, objID, gen := seedCachedObject(t, env)
	task := seedTask(t, env, model.TaskTypeUpload, objID, gen, 5, 0)

	wallet := &testutil.MockWalletQuerier{
		GetWalletInfoFunc: func(_ context.Context) (*synapse.WalletInfo, error) {
			return nil, nil // (nil, nil) — edge case
		},
	}

	pieceCID := testCID(t)
	env.storage.UploadFunc = func(_ context.Context, _ io.Reader, _ *storage.UploadOptions) (*storage.UploadResult, error) {
		return &storage.UploadResult{
			PieceCID: pieceCID,
			Size:     11,
			Complete: true,
			Copies:   []storage.CopyResult{{RetrievalURL: "https://provider.example/pieces/1"}},
		}, nil
	}

	uploader := worker.NewUploader(env.repos, env.cache, env.storage, wallet, env.sm, true, 1, 50*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, uploader, task.ID, 5*time.Second)

	got, _ := env.repos.Tasks.GetByID(context.Background(), task.ID)
	if got.Status != model.TaskStatusCompleted {
		t.Errorf("expected task completed (nil info should not panic), got %s", got.Status)
	}
}
