package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

func storeRecoveryFixture(t *testing.T, retries int, payload string, parked synapse.ParkedPieceChecker, cacheStore cache.Cache, target *testutil.MockStorageTarget) (handlerTestRuntime, seededCopyPipeline, cid.Cid) {
	t.Helper()
	storageClient := &testutil.MockStorageClient{}
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		cache: cacheStore, storage: storageClient, parkedPieces: parked, maxRetries: &retries,
		policy: cache.EvictionPolicyNone,
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	pipeline := seedCopyPipeline(t, runtime, model.StorageCopyStatusPending)
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: pipeline.upload.BucketID,
		Key: "store-recovery.bin", ContentID: &pipeline.upload.ID,
		Size: 128, ETag: "store-recovery", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	info, err := piece.Calculate(strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageCopy)(nil)).
		Set("transfer_method = ?", model.StorageCopyTransferMethodCacheRestore).
		Where("id = ?", pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewUpdate().Model((*model.StorageContent)(nil)).
		Set("piece_cid = ?", info.CIDv2.String()).Where("id = ?", pipeline.upload.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	target.ProviderIDValue = pipeline.targetSet.ProviderID.SDK()
	targetDataSetID := pipeline.targetSet.DataSetID.SDK()
	target.DataSetIDValue = &targetDataSetID
	target.ClientDataSetIDValue = pipeline.targetClient
	storageClient.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		return target, nil
	}
	return runtime, pipeline, info.CIDv2
}

func seedStoreCheckpoint(t *testing.T, runtime handlerTestRuntime, taskID, copyID int64, pieceCID cid.Cid, providerURL string, attemptedAt time.Time) {
	t.Helper()
	checkpoint, err := json.Marshal(map[string]any{
		"attempted_at": attemptedAt.UTC(), "intended_piece_cid": pieceCID.String(),
		"provider_service_url": providerURL, "ingress_attempt": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE task_payloads SET checkpoint_json = ? WHERE task_id = ?`, checkpoint, taskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET resume_mode = 'recover' WHERE id = ?`, taskID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, copyID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestStoreProcessingAndQueryFailureLeaveRetryableCheckpoint(t *testing.T) {
	for _, tt := range []struct {
		name   string
		state  synapse.ParkedPieceState
		err    error
		reason string
	}{
		{name: "processing", state: synapse.ParkedPieceProcessing, reason: "store_processing_timeout"},
		{name: "query failure", err: errors.New("provider unavailable"), reason: "store_check_failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := strings.Repeat("k", 128)
			target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example"}
			parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
				return tt.state, tt.err
			})
			runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, nil, target)
			taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
			seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now().Add(-31*time.Minute))
			cancel, done := runHandlerEngine(t, runtime)
			defer stopHandlerEngine(t, cancel, done)
			failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
			if failed.FailureReason == nil || *failed.FailureReason != tt.reason || !runtime.service.Retryable(failed) || len(failed.Checkpoint) == 0 {
				t.Fatalf("failed Store task = %#v", failed)
			}
			copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
			if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID {
				t.Fatalf("Store copy after timeout = %#v, err=%v", copyRow, err)
			}
		})
	}
}

func TestStoreManualRetryAllowsOneUploadWhenAutomaticRetriesDisabled(t *testing.T) {
	payload := strings.Repeat("r", 128)
	var stores atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example", StoreFunc: func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
		stores.Add(1)
		return nil, errors.New("connection closed after upload")
	}}
	runtime, pipeline, _ := storeRecoveryFixture(t, 0, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && len(task.Checkpoint) > 0
	})
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != "store_retry_limit" || !runtime.service.Retryable(failed) || stores.Load() != 1 {
		t.Fatalf("zero-retry Store = task:%#v uploads:%d", failed, stores.Load())
	}
	if err := runtime.service.Retry(t.Context(), taskRow.ID); err != nil {
		t.Fatal(err)
	}
	retrying := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.RetryCount == 1 && stores.Load() == 2
	})
	if retrying.RetryLimit == nil || *retrying.RetryLimit != 1 {
		t.Fatalf("manual Store retry limit = %v, want 1", retrying.RetryLimit)
	}
}

func TestStoreStopsAfterConfiguredAutomaticRetransmissions(t *testing.T) {
	payload := strings.Repeat("l", 128)
	var stores atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example", StoreFunc: func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
		stores.Add(1)
		return nil, errors.New("connection closed after upload")
	}}
	runtime, pipeline, _ := storeRecoveryFixture(t, 1, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && stores.Load() == 1
	})
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && stores.Load() == 2 && task.RetryCount == 1
	})
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != "store_retry_limit" || !runtime.service.Retryable(failed) || stores.Load() != 2 {
		t.Fatalf("Store retry limit = task:%#v uploads:%d", failed, stores.Load())
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID {
		t.Fatalf("Store copy lost its retryable task: %#v, err:%v", copyRow, err)
	}
}

func TestNewStoreTaskAdoptsPreviousCheckpoint(t *testing.T) {
	payload := strings.Repeat("a", 128)
	var storeCalls atomic.Int64
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceReady, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://store.example",
		StoreFunc: func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
			storeCalls.Add(1)
			return nil, errors.New("unexpected upload")
		},
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xaa}, nil },
	}
	runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, nil, target)
	old := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	seedStoreCheckpoint(t, runtime, old.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now())
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET status = 'failed', finished_at = ?, resume_mode = 'recover' WHERE id = ?`, time.Now(), old.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET status = 'failed', active_task_id = NULL WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.repos.Contents.ReopenFailedUploadCopy(t.Context(), pipeline.target.ID); err != nil {
		t.Fatal(err)
	}
	newTask := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completed := waitForTask(t, runtime.repos, newTask.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if len(completed.Checkpoint) == 0 {
		t.Fatal("new Store task did not retain the previous checkpoint")
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status == model.StorageCopyStatusPending || copyRow.Status == model.StorageCopyStatusFailed || storeCalls.Load() != 0 {
		t.Fatalf("adopted Store copy = %#v, uploads:%d, err:%v", copyRow, storeCalls.Load(), err)
	}
}

func TestNewStoreTaskWithoutCheckpointNeedsReadableCache(t *testing.T) {
	payload := strings.Repeat("m", 128)
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return nil, nil, os.ErrNotExist
	}}
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example"}
	runtime, pipeline, _ := storeRecoveryFixture(t, 2, payload, nil, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	if failed.FailureReason == nil || *failed.FailureReason != "store_cache_missing" || len(failed.Checkpoint) != 0 {
		t.Fatalf("uncached Store task = %#v", failed)
	}
}

func TestNewStoreTaskWithoutCheckpointChecksProviderBeforeUploading(t *testing.T) {
	payload := strings.Repeat("n", 128)
	var stores, checks atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		checks.Add(1)
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://store.example",
		StoreFunc: func(_ context.Context, _ io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
			stores.Add(1)
			return &storage.StoreResult{PieceCID: options.PieceCID, Size: 128}, nil
		},
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xaa}, nil },
	}
	runtime, pipeline, _ := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if stores.Load() != 1 || checks.Load() == 0 || completed.RetryCount != 1 || len(completed.Checkpoint) == 0 {
		t.Fatalf("Store without old checkpoint = task:%#v uploads:%d checks:%d", completed, stores.Load(), checks.Load())
	}
}

func TestNewStoreTaskKeepsRetryAfterReadyPiecePresignFailure(t *testing.T) {
	payload := strings.Repeat("s", 128)
	var presigns, stores atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceReady, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://store.example",
		StoreFunc: func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
			stores.Add(1)
			return nil, errors.New("unexpected upload")
		},
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) {
			if presigns.Add(1) == 1 {
				return nil, errors.New("temporary presign failure")
			}
			return []byte{0xaa}, nil
		},
	}
	runtime, pipeline, _ := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	failed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusFailed })
	var checkpoint struct {
		IngressAttempt int `json:"ingress_attempt"`
	}
	if failed.FailureReason == nil || *failed.FailureReason != "commit_presign_failed" ||
		!runtime.service.Retryable(failed) || json.Unmarshal(failed.Checkpoint, &checkpoint) != nil ||
		checkpoint.IngressAttempt != 1 || failed.RetryCount != 0 || stores.Load() != 0 {
		t.Fatalf("ready piece after presign failure = task:%#v checkpoint:%#v uploads:%d", failed, checkpoint, stores.Load())
	}
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status != model.StorageCopyStatusPending || copyRow.ActiveTaskID == nil || *copyRow.ActiveTaskID != taskRow.ID {
		t.Fatalf("ready piece copy after presign failure = %#v, err:%v", copyRow, err)
	}
	if err := runtime.service.Retry(t.Context(), taskRow.ID); err != nil {
		t.Fatal(err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if presigns.Load() != 2 || stores.Load() != 0 {
		t.Fatalf("manual retry = presigns:%d uploads:%d, want 2/0", presigns.Load(), stores.Load())
	}
}

func TestNewStoreTaskWaitsForUnavailableProviderAfterEarlierUpload(t *testing.T) {
	payload := strings.Repeat("u", 128)
	var providerReady atomic.Bool
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceReady, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue:      "https://store.example",
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xaa}, nil },
	}
	runtime, pipeline, _ := storeRecoveryFixture(t, 0, payload, parked, cacheStore, target)
	runtime.storage.OpenDataSetTargetFunc = func(context.Context, sdktypes.BigInt, storage.NewDataSetContextOptions) (synapse.DataSetTarget, error) {
		if !providerReady.Load() {
			return nil, storage.ErrDataSetUnavailable
		}
		return target, nil
	}
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	if _, err := runtime.db.NewRaw(`UPDATE storage_copies SET ingress_store_attempt = 1 WHERE id = ?`, pipeline.target.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waiting := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "provider"
	})
	if waiting.RetryCount != 0 || len(waiting.Checkpoint) != 0 {
		t.Fatalf("unavailable provider consumed retry or checkpointed Store: %#v", waiting)
	}
	providerReady.Store(true)
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), taskRow.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
}

func TestStoreDoesNotUseOldProviderPieceForChangedTarget(t *testing.T) {
	payload := strings.Repeat("p", 128)
	var stores, oldChecks, currentChecks atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(_ context.Context, serviceURL string, _ cid.Cid) (synapse.ParkedPieceState, error) {
		if serviceURL == "https://old.example" {
			oldChecks.Add(1)
			return synapse.ParkedPieceReady, nil
		}
		currentChecks.Add(1)
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://current.example",
		StoreFunc: func(_ context.Context, _ io.Reader, options *storage.StoreOptions) (*storage.StoreResult, error) {
			stores.Add(1)
			return &storage.StoreResult{PieceCID: options.PieceCID, Size: 128}, nil
		},
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xaa}, nil },
	}
	runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, "https://old.example", time.Now())
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	completed := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	copyRow, err := runtime.repos.Contents.GetUploadCopyByID(t.Context(), pipeline.target.ID)
	if err != nil || copyRow.Status == model.StorageCopyStatusPending || copyRow.Status == model.StorageCopyStatusFailed || stores.Load() != 1 || oldChecks.Load() == 0 || currentChecks.Load() == 0 || completed.RetryCount != 1 {
		t.Fatalf("changed provider Store = copy:%#v task:%#v uploads:%d old checks:%d current checks:%d err:%v", copyRow, completed, stores.Load(), oldChecks.Load(), currentChecks.Load(), err)
	}
}

func TestStoreRestartBeforeRetryCheckpointRechecksOldPiece(t *testing.T) {
	payload := strings.Repeat("b", 128)
	var stores atomic.Int64
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return synapse.ParkedPieceMissing, nil
	})
	target := &testutil.MockStorageTarget{ServiceURLValue: "https://store.example", StoreFunc: func(context.Context, io.Reader, *storage.StoreOptions) (*storage.StoreResult, error) {
		stores.Add(1)
		return nil, errors.New("transfer interrupted")
	}}
	runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now())
	limitedRepos := *runtime.repos
	limitedRepos.Tasks = &limitedClaimRepository{TaskRepository: runtime.repos.Tasks, maximum: 1}
	firstEngine, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, &limitedRepos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runEngine(t, firstEngine)
	before := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeExecute
	})
	stopHandlerEngine(t, cancel, done)
	var checkpoint struct {
		IngressAttempt int `json:"ingress_attempt"`
	}
	if err := json.Unmarshal(before.Checkpoint, &checkpoint); err != nil || checkpoint.IngressAttempt != 1 || stores.Load() != 0 {
		t.Fatalf("before new checkpoint = attempt:%d uploads:%d err:%v", checkpoint.IngressAttempt, stores.Load(), err)
	}
	cancel, done = runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	after := waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.ResumeMode == model.TaskResumeModeRecover && stores.Load() == 1
	})
	if err := json.Unmarshal(after.Checkpoint, &checkpoint); err != nil || checkpoint.IngressAttempt != 2 || after.RetryCount != 1 {
		t.Fatalf("after new checkpoint = attempt:%d retry:%d err:%v", checkpoint.IngressAttempt, after.RetryCount, err)
	}
}

func TestStoreRestartAfterRetryCheckpointOnlyQueriesProvider(t *testing.T) {
	payload := strings.Repeat("c", 128)
	var stores atomic.Int64
	var parkedState atomic.Value
	parkedState.Store(synapse.ParkedPieceMissing)
	enteredStore := make(chan struct{})
	cacheStore := &testutil.MockCache{GetFunc: func(context.Context, string, string) (io.ReadCloser, *cache.ObjectInfo, error) {
		return io.NopCloser(strings.NewReader(payload)), &cache.ObjectInfo{Size: 128}, nil
	}}
	parked := parkedPieceCheckerFunc(func(context.Context, string, cid.Cid) (synapse.ParkedPieceState, error) {
		return parkedState.Load().(synapse.ParkedPieceState), nil
	})
	target := &testutil.MockStorageTarget{
		ServiceURLValue: "https://store.example",
		StoreFunc: func(ctx context.Context, _ io.Reader, _ *storage.StoreOptions) (*storage.StoreResult, error) {
			if stores.Add(1) == 1 {
				close(enteredStore)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		PresignForCommitFunc: func(context.Context, []storage.PieceInput) ([]byte, error) { return []byte{0xaa}, nil },
	}
	runtime, pipeline, pieceCID := storeRecoveryFixture(t, 2, payload, parked, cacheStore, target)
	taskRow := bindCopyTask(t, runtime, pipeline.target, model.TaskTypeStorageStore)
	seedStoreCheckpoint(t, runtime, taskRow.ID, pipeline.target.ID, pieceCID, target.ServiceURL(), time.Now())
	cancel, done := runHandlerEngine(t, runtime)
	select {
	case <-enteredStore:
	case <-time.After(3 * time.Second):
		stopHandlerEngine(t, cancel, done)
		t.Fatal("Store did not start after the new checkpoint")
	}
	inFlight, err := runtime.repos.Tasks.GetByID(t.Context(), taskRow.ID)
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint struct {
		IngressAttempt int `json:"ingress_attempt"`
	}
	if err := json.Unmarshal(inFlight.Checkpoint, &checkpoint); err != nil || checkpoint.IngressAttempt != 2 || inFlight.RetryCount != 1 {
		t.Fatalf("in-flight checkpoint = attempt:%d retry:%d err:%v", checkpoint.IngressAttempt, inFlight.RetryCount, err)
	}
	stopHandlerEngine(t, cancel, done)
	parkedState.Store(synapse.ParkedPieceReady)
	time.Sleep(350 * time.Millisecond)
	restarted, err := taskengine.NewEngine(taskengine.EngineConfig{
		Concurrency: 1, PollInterval: 5 * time.Millisecond, LeaseDuration: 300 * time.Millisecond,
		Retention: time.Hour, ProviderMutationConcurrency: 4, DestructiveMutationConcurrency: 2,
	}, runtime.repos, runtime.registry, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cancel, done = runEngine(t, restarted)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, taskRow.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
	if stores.Load() != 1 {
		t.Fatalf("Store calls after restart = %d, want 1", stores.Load())
	}
}

func TestPeerPullWaitsUntilStoreCopyIsReadable(t *testing.T) {
	runtime := newHandlerTestRuntime(t, handlerRuntimeOptions{
		register: func(handlers *worker.TaskHandlers, registry *taskengine.Registry) error {
			return handlers.RegisterStorage(registry)
		},
	})
	bucket := &model.Bucket{Name: "peer-source-wait", Status: model.BucketStatusActive, DefaultCopies: 2, MinimumDurableCopies: 1}
	if err := runtime.repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	content, err := runtime.repos.Contents.EnsureContent(t.Context(), repository.EnsureContentInput{
		BucketID: bucket.ID, ContentSize: 128, Checksum: testutil.StorageChecksum("peer-source-wait"), RequestedCopies: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	version := &model.ObjectVersion{
		VersionID: model.NewVersionID(), BucketID: bucket.ID, Key: "peer.bin",
		ContentID: &content.ID, Size: 128, ETag: "peer", ContentType: "application/octet-stream",
	}
	if _, err := runtime.repos.Objects.CreateVersionAndSetCurrent(t.Context(), version); err != nil {
		t.Fatal(err)
	}
	bindings := make([]*model.StorageDataSet, 2)
	for i := range bindings {
		providerID := testOnChainID(t, int64(61001+i))
		binding, err := runtime.repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{
			BucketID: bucket.ID, ProviderID: providerID, CopyIndex: i, CreatedByContentID: content.ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		dataSetID := testOnChainID(t, int64(62001+i))
		clientID := testOnChainID(t, int64(63001+i))
		if err := runtime.repos.Contents.MarkDataSetReady(t.Context(), repository.MarkDataSetReadyInput{
			ID: binding.ID, ContentID: content.ID, DataSetID: dataSetID, ClientDataSetID: &clientID,
		}); err != nil {
			t.Fatal(err)
		}
		bindings[i] = binding
	}
	if err := runtime.repos.Contents.CreateUploadCopiesForBindings(t.Context(), content.ID, []repository.UploadCopyBindingInput{
		{StorageDataSetID: bindings[0].ID, CopyIndex: 0, ProviderID: bindings[0].ProviderID, TransferMethod: model.StorageCopyTransferMethodIngress},
		{StorageDataSetID: bindings[1].ID, CopyIndex: 1, ProviderID: bindings[1].ProviderID, TransferMethod: model.StorageCopyTransferMethodPeerPull},
	}); err != nil {
		t.Fatal(err)
	}
	copies, err := runtime.repos.Contents.ListCopies(t.Context(), content.ID)
	if err != nil || len(copies) != 2 {
		t.Fatalf("copies = %#v, err:%v", copies, err)
	}
	peerTask := bindCopyTask(t, runtime, &copies[1], model.TaskTypeStorageTransferPlan)
	cancel, done := runHandlerEngine(t, runtime)
	defer stopHandlerEngine(t, cancel, done)
	waitForTask(t, runtime.repos, peerTask.ID, func(task *model.Task) bool {
		return task.Status == model.TaskStatusPending && task.WaitReason != nil && *task.WaitReason == "source"
	})
	pieceID := testOnChainID(t, 64001)
	testutil.CommitStorageCopy(t, runtime.db, runtime.repos, repository.MarkUploadCopyCommittedInput{
		StorageCopyID: copies[0].ID, ContentID: content.ID, CopyIndex: 0,
		PieceCID: testPieceCID(t, "peer-source-wait").String(), PieceID: &pieceID, RetrievalURL: "https://source.example/piece",
	})
	if _, err := runtime.db.NewRaw(`UPDATE tasks SET available_at = ? WHERE id = ?`, time.Now().Add(-time.Second), peerTask.ID).Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitForTask(t, runtime.repos, peerTask.ID, func(task *model.Task) bool { return task.Status == model.TaskStatusCompleted })
}
