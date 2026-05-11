package worker_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type fakeCleanupContext struct {
	pieceStatusFunc     func(context.Context, cid.Cid) (*storage.PieceStatus, error)
	deletePieceByIDFunc func(context.Context, sdktypes.BigInt) (*sdktypes.WriteResult, error)
}

func (f fakeCleanupContext) PieceStatus(ctx context.Context, piece cid.Cid) (*storage.PieceStatus, error) {
	if f.pieceStatusFunc != nil {
		return f.pieceStatusFunc(ctx, piece)
	}
	return &storage.PieceStatus{Exists: false}, nil
}

func (f fakeCleanupContext) DeletePieceByID(ctx context.Context, pieceID sdktypes.BigInt) (*sdktypes.WriteResult, error) {
	if f.deletePieceByIDFunc != nil {
		return f.deletePieceByIDFunc(ctx, pieceID)
	}
	return &sdktypes.WriteResult{Hash: common.HexToHash("0xdelete")}, nil
}

func TestStorageCleanupWorkerMarksMissingPieceRemoved(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	pieceCID := testCID(t).String()
	uploadID := int64(77)
	task := &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          uploadID,
		IdempotencyKey: "storage_cleanup:77",
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
		Payload: map[string]interface{}{
			"storage_upload_id": uploadID,
			"piece_cid":         pieceCID,
		},
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create cleanup task: %v", err)
	}
	copy := &model.StorageCleanupCopy{
		TaskID:          task.ID,
		UploadID:        uploadID,
		CopyIndex:       0,
		ProviderID:      onChainIDPtr(t, "101"),
		DataSetID:       onChainIDPtr(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "5001"),
		PieceID:         onChainIDPtr(t, "2001"),
		PieceCID:        pieceCID,
		Status:          model.StorageCleanupCopyStatusPending,
	}
	if _, err := env.db.NewInsert().Model(copy).Exec(ctx); err != nil {
		t.Fatalf("insert cleanup copy: %v", err)
	}

	var requestedDataSet string
	env.storage.CreateCleanupContextFunc = func(_ context.Context, opts *storage.CreateContextOptions) (synapse.CleanupContext, error) {
		if opts.ProviderID != nil {
			t.Fatalf("ProviderID = %s, want none when deleting from a known dataset", opts.ProviderID)
		}
		if opts.DataSetID == nil {
			t.Fatal("DataSetID is nil, want one dataset")
		}
		requestedDataSet = opts.DataSetID.String()
		return fakeCleanupContext{
			pieceStatusFunc: func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
				return &storage.PieceStatus{Exists: false}, nil
			},
		}, nil
	}

	cleanup := worker.NewStorageCleanupWorker(env.repos, env.storage, 1, 20*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, cleanup, task.ID, 5*time.Second)

	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if gotTask.Status != model.TaskStatusCompleted {
		t.Fatalf("task status = %s, want completed", gotTask.Status)
	}
	gotCopy := new(model.StorageCleanupCopy)
	if err := env.db.NewSelect().Model(gotCopy).Where("id = ?", copy.ID).Scan(ctx); err != nil {
		t.Fatalf("select cleanup copy: %v", err)
	}
	if gotCopy.Status != model.StorageCleanupCopyStatusRemoved || gotCopy.RemovedAt == nil {
		t.Fatalf("copy cleanup = status:%s removed:%v, want removed timestamp", gotCopy.Status, gotCopy.RemovedAt)
	}
	if requestedDataSet != "1001" {
		t.Fatalf("cleanup context dataset = %q, want 1001", requestedDataSet)
	}
}

func TestStorageCleanupWorkerContinuesAfterUnsupportedCopy(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	pieceCID := testCID(t).String()
	uploadID := int64(88)
	task := &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          uploadID,
		IdempotencyKey: "storage_cleanup:88",
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
		Payload: map[string]interface{}{
			"storage_upload_id": uploadID,
			"piece_cid":         pieceCID,
		},
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create cleanup task: %v", err)
	}
	unsupported := &model.StorageCleanupCopy{
		TaskID:    task.ID,
		UploadID:  uploadID,
		CopyIndex: 0,
		PieceCID:  pieceCID,
		Status:    model.StorageCleanupCopyStatusPending,
	}
	removable := &model.StorageCleanupCopy{
		TaskID:          task.ID,
		UploadID:        uploadID,
		CopyIndex:       1,
		ProviderID:      onChainIDPtr(t, "101"),
		DataSetID:       onChainIDPtr(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "5001"),
		PieceID:         onChainIDPtr(t, "2001"),
		PieceCID:        pieceCID,
		Status:          model.StorageCleanupCopyStatusPending,
	}
	if _, err := env.db.NewInsert().Model(unsupported).Exec(ctx); err != nil {
		t.Fatalf("insert unsupported cleanup copy: %v", err)
	}
	if _, err := env.db.NewInsert().Model(removable).Exec(ctx); err != nil {
		t.Fatalf("insert removable cleanup copy: %v", err)
	}
	env.storage.CreateCleanupContextFunc = func(context.Context, *storage.CreateContextOptions) (synapse.CleanupContext, error) {
		return fakeCleanupContext{
			pieceStatusFunc: func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
				return &storage.PieceStatus{Exists: false}, nil
			},
		}, nil
	}

	cleanup := worker.NewStorageCleanupWorker(env.repos, env.storage, 1, 20*time.Millisecond, slog.Default())
	runWorkerUntilTask(t, env, cleanup, task.ID, 5*time.Second)

	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if gotTask.Status != model.TaskStatusFailed {
		t.Fatalf("task status = %s, want failed after unsupported copy", gotTask.Status)
	}
	var copies []model.StorageCleanupCopy
	if err := env.db.NewSelect().Model(&copies).Where("task_id = ?", task.ID).OrderExpr("copy_index ASC").Scan(ctx); err != nil {
		t.Fatalf("select cleanup copies: %v", err)
	}
	if copies[0].Status != model.StorageCleanupCopyStatusUnsupported {
		t.Fatalf("unsupported copy status = %s, want unsupported", copies[0].Status)
	}
	if copies[1].Status != model.StorageCleanupCopyStatusRemoved {
		t.Fatalf("removable copy status = %s, want removed", copies[1].Status)
	}
}

func TestStorageCleanupWorkerWaitsWhenUnsupportedTaskStillHasScheduledDeletion(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	pieceCID := testCID(t).String()
	uploadID := int64(89)
	task := &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          uploadID,
		IdempotencyKey: "storage_cleanup:89",
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
		Payload: map[string]interface{}{
			"storage_upload_id": uploadID,
			"piece_cid":         pieceCID,
		},
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create cleanup task: %v", err)
	}
	unsupported := &model.StorageCleanupCopy{
		TaskID:    task.ID,
		UploadID:  uploadID,
		CopyIndex: 0,
		PieceCID:  pieceCID,
		Status:    model.StorageCleanupCopyStatusPending,
	}
	scheduled := &model.StorageCleanupCopy{
		TaskID:          task.ID,
		UploadID:        uploadID,
		CopyIndex:       1,
		ProviderID:      onChainIDPtr(t, "101"),
		DataSetID:       onChainIDPtr(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "5001"),
		PieceID:         onChainIDPtr(t, "2001"),
		PieceCID:        pieceCID,
		Status:          model.StorageCleanupCopyStatusPending,
	}
	if _, err := env.db.NewInsert().Model(unsupported).Exec(ctx); err != nil {
		t.Fatalf("insert unsupported cleanup copy: %v", err)
	}
	if _, err := env.db.NewInsert().Model(scheduled).Exec(ctx); err != nil {
		t.Fatalf("insert scheduled cleanup copy: %v", err)
	}
	env.storage.CreateCleanupContextFunc = func(context.Context, *storage.CreateContextOptions) (synapse.CleanupContext, error) {
		return fakeCleanupContext{
			pieceStatusFunc: func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
				return &storage.PieceStatus{Exists: true}, nil
			},
		}, nil
	}

	cleanup := worker.NewStorageCleanupWorker(env.repos, env.storage, 1, 20*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, cleanup, task.ID, model.TaskStatusWaiting, 5*time.Second)

	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if gotTask.Status != model.TaskStatusWaiting {
		t.Fatalf("task status = %s, want waiting", gotTask.Status)
	}
	var copies []model.StorageCleanupCopy
	if err := env.db.NewSelect().Model(&copies).Where("task_id = ?", task.ID).OrderExpr("copy_index ASC").Scan(ctx); err != nil {
		t.Fatalf("select cleanup copies: %v", err)
	}
	if copies[0].Status != model.StorageCleanupCopyStatusUnsupported {
		t.Fatalf("unsupported copy status = %s, want unsupported", copies[0].Status)
	}
	if copies[1].Status != model.StorageCleanupCopyStatusDeleteScheduled {
		t.Fatalf("scheduled copy status = %s, want delete_scheduled", copies[1].Status)
	}
}

func TestStorageCleanupWorkerSchedulesDeletionByPieceID(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	pieceCID := testCID(t).String()
	uploadID := int64(90)
	task := &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          uploadID,
		IdempotencyKey: "storage_cleanup:90",
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
		Payload: map[string]interface{}{
			"storage_upload_id": uploadID,
			"piece_cid":         pieceCID,
		},
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create cleanup task: %v", err)
	}
	copy := &model.StorageCleanupCopy{
		TaskID:          task.ID,
		UploadID:        uploadID,
		CopyIndex:       0,
		ProviderID:      onChainIDPtr(t, "101"),
		DataSetID:       onChainIDPtr(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "5001"),
		PieceID:         onChainIDPtr(t, "2001"),
		PieceCID:        pieceCID,
		Status:          model.StorageCleanupCopyStatusPending,
	}
	if _, err := env.db.NewInsert().Model(copy).Exec(ctx); err != nil {
		t.Fatalf("insert cleanup copy: %v", err)
	}

	var deletedPieceID string
	env.storage.CreateCleanupContextFunc = func(context.Context, *storage.CreateContextOptions) (synapse.CleanupContext, error) {
		return fakeCleanupContext{
			pieceStatusFunc: func(context.Context, cid.Cid) (*storage.PieceStatus, error) {
				return &storage.PieceStatus{Exists: true}, nil
			},
			deletePieceByIDFunc: func(_ context.Context, pieceID sdktypes.BigInt) (*sdktypes.WriteResult, error) {
				deletedPieceID = pieceID.String()
				return &sdktypes.WriteResult{Hash: common.HexToHash("0xdelete")}, nil
			},
		}, nil
	}

	cleanup := worker.NewStorageCleanupWorker(env.repos, env.storage, 1, 20*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, cleanup, task.ID, model.TaskStatusWaiting, 5*time.Second)

	if deletedPieceID != "2001" {
		t.Fatalf("deleted piece ID = %q, want 2001", deletedPieceID)
	}
	gotCopy := new(model.StorageCleanupCopy)
	if err := env.db.NewSelect().Model(gotCopy).Where("id = ?", copy.ID).Scan(ctx); err != nil {
		t.Fatalf("select cleanup copy: %v", err)
	}
	if gotCopy.Status != model.StorageCleanupCopyStatusDeleteScheduled {
		t.Fatalf("copy status = %s, want delete_scheduled", gotCopy.Status)
	}
}

func TestStorageCleanupWorkerWaitsWhenActiveUploadUsesSamePiece(t *testing.T) {
	env := newTestWorkerEnv(t)
	ctx := context.Background()
	pieceCID := testCID(t).String()

	bucket := &model.Bucket{Name: "storage-cleanup-active-upload-bucket", Status: model.BucketStatusActive}
	if err := env.repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	targetUpload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "deleted-version",
		ContentSize:     10,
		Checksum:        "same-checksum",
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt(target): %v", err)
	}
	seedStorageCleanupWorkerCommittedCopy(t, env, bucket.ID, targetUpload.ID, pieceCID, "101", "1001", "2001")
	if finalized, _, err := env.repos.Uploads.FinalizeUploadIfTargetCopiesMet(ctx, repository.FinalizeUploadInput{UploadID: targetUpload.ID}); err != nil {
		t.Fatalf("FinalizeUploadIfTargetCopiesMet(target): %v", err)
	} else if !finalized {
		t.Fatal("target upload finalized = false, want true")
	}

	task := &model.Task{
		Type:           model.TaskTypeStorageCleanup,
		RefType:        "storage_upload",
		RefID:          targetUpload.ID,
		IdempotencyKey: "storage_cleanup:active-upload-ref",
		Status:         model.TaskStatusQueued,
		MaxRetries:     3,
		ScheduledAt:    time.Now(),
		Payload: map[string]interface{}{
			"storage_upload_id": targetUpload.ID,
			"piece_cid":         pieceCID,
		},
	}
	if err := env.repos.Tasks.Create(ctx, task); err != nil {
		t.Fatalf("Create cleanup task: %v", err)
	}
	copy := &model.StorageCleanupCopy{
		TaskID:          task.ID,
		UploadID:        targetUpload.ID,
		CopyIndex:       0,
		ProviderID:      onChainIDPtr(t, "101"),
		DataSetID:       onChainIDPtr(t, "1001"),
		ClientDataSetID: onChainIDPtr(t, "5001"),
		PieceID:         onChainIDPtr(t, "2001"),
		PieceCID:        pieceCID,
		Status:          model.StorageCleanupCopyStatusPending,
	}
	if _, err := env.db.NewInsert().Model(copy).Exec(ctx); err != nil {
		t.Fatalf("insert cleanup copy: %v", err)
	}

	activeUpload, err := env.repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "active-version",
		ContentSize:     10,
		Checksum:        "same-checksum",
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt(active): %v", err)
	}
	seedStorageCleanupWorkerCommittedCopy(t, env, bucket.ID, activeUpload.ID, pieceCID, "101", "1001", "2001")

	var cleanupContextCalls atomic.Int32
	env.storage.CreateCleanupContextFunc = func(context.Context, *storage.CreateContextOptions) (synapse.CleanupContext, error) {
		cleanupContextCalls.Add(1)
		return fakeCleanupContext{}, nil
	}

	cleanup := worker.NewStorageCleanupWorker(env.repos, env.storage, 1, 20*time.Millisecond, slog.Default())
	runWorkerUntilTaskStatus(t, env, cleanup, task.ID, model.TaskStatusWaiting, 5*time.Second)

	gotTask, err := env.repos.Tasks.GetByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if gotTask.Status != model.TaskStatusWaiting {
		t.Fatalf("task status = %s, want waiting", gotTask.Status)
	}
	if gotTask.WaitReason == nil || *gotTask.WaitReason != model.TaskWaitReasonDependency {
		t.Fatalf("task wait reason = %v, want dependency", gotTask.WaitReason)
	}
	if gotTask.StatusMessage == nil || *gotTask.StatusMessage != "Waiting for shared data references to clear" {
		t.Fatalf("task status message = %v, want shared data reference wait message", gotTask.StatusMessage)
	}
	gotCopy := new(model.StorageCleanupCopy)
	if err := env.db.NewSelect().Model(gotCopy).Where("id = ?", copy.ID).Scan(ctx); err != nil {
		t.Fatalf("select cleanup copy: %v", err)
	}
	if gotCopy.Status != model.StorageCleanupCopyStatusPending {
		t.Fatalf("copy status = %s, want pending while waiting for references", gotCopy.Status)
	}
	if cleanupContextCalls.Load() != 0 {
		t.Fatalf("cleanup context calls = %d, want 0 while active upload references piece", cleanupContextCalls.Load())
	}
}

func seedStorageCleanupWorkerCommittedCopy(t *testing.T, env *testWorkerEnv, bucketID int64, uploadID int64, pieceCID string, providerID string, dataSetID string, pieceID string) {
	t.Helper()
	ctx := context.Background()
	provider := onChainID(t, providerID)
	binding, err := env.repos.Uploads.EnsureDataSetBinding(ctx, repository.EnsureDataSetBindingInput{
		BucketID:   bucketID,
		ProviderID: provider,
		CopyIndex:  0,
	})
	if err != nil {
		t.Fatalf("EnsureDataSetBinding: %v", err)
	}
	if err := env.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:        binding.ID,
		UploadID:  uploadID,
		DataSetID: onChainID(t, dataSetID),
	}); err != nil {
		t.Fatalf("MarkDataSetReady: %v", err)
	}
	if err := env.repos.Uploads.CreateUploadCopiesForBindings(ctx, uploadID, []repository.UploadCopyBindingInput{{
		StorageDataSetID: binding.ID,
		CopyIndex:        0,
		TransferMethod:   model.StorageCopyTransferMethodIngress,
		ProviderID:       provider,
	}}); err != nil {
		t.Fatalf("CreateUploadCopiesForBindings: %v", err)
	}
	if err := env.repos.Uploads.MarkUploadCopyCommitted(ctx, repository.MarkUploadCopyCommittedInput{
		UploadID:     uploadID,
		CopyIndex:    0,
		PieceCID:     pieceCID,
		PieceID:      onChainIDPtr(t, pieceID),
		RetrievalURL: "https://provider.example/" + pieceCID,
	}); err != nil {
		t.Fatalf("MarkUploadCopyCommitted: %v", err)
	}
}

func runWorkerUntilTaskStatus(t *testing.T, env *testWorkerEnv, w worker.Worker, taskID int64, status model.TaskStatus, timeout time.Duration) {
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
			t.Fatalf("timed out waiting for task %d to reach status %s", taskID, status)
		case <-ticker.C:
			task, err := env.repos.Tasks.GetByID(context.Background(), taskID)
			if err != nil {
				continue
			}
			if task != nil && task.Status == status {
				cancel()
				<-done
				return
			}
		}
	}
}
