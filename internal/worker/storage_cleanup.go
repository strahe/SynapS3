package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/storage"
)

type StorageCleanupWorker struct {
	repos        *repository.Repositories
	storage      synapse.StorageClient
	concurrency  int
	pollInterval time.Duration
	leaseTTL     time.Duration
	logger       *slog.Logger
	*livenessTracker
}

const (
	storageCleanupConfirmationDelay = time.Minute
	storageCleanupReferenceDelay    = time.Minute
)

var errStorageCleanupCopyUnsupported = errors.New("storage cleanup copy unsupported")

func NewStorageCleanupWorker(repos *repository.Repositories, storageClient synapse.StorageClient, concurrency int, pollInterval time.Duration, logger *slog.Logger) *StorageCleanupWorker {
	return &StorageCleanupWorker{
		repos:           repos,
		storage:         storageClient,
		concurrency:     concurrency,
		pollInterval:    pollInterval,
		leaseTTL:        5 * time.Minute,
		logger:          logger,
		livenessTracker: newLivenessTracker(pollInterval),
	}
}

func (w *StorageCleanupWorker) Name() string { return "storage_cleanup" }

func (w *StorageCleanupWorker) Healthy() bool { return w.healthy() }

func (w *StorageCleanupWorker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for slot := range w.concurrency {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			if !sleepStorageCleanupInitialStagger(ctx, slot) {
				return
			}
			w.runSlot(ctx)
		}(slot)
	}
	wg.Wait()
	return ctx.Err()
}

func sleepStorageCleanupInitialStagger(ctx context.Context, slot int) bool {
	if slot <= 0 {
		return true
	}
	timer := time.NewTimer(time.Duration(slot) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *StorageCleanupWorker) runSlot(ctx context.Context) {
	if !sleepUntilNextWorkerPoll(ctx, w.pollInterval) {
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		w.recordTick()
		task, err := w.repos.Tasks.ClaimReady(ctx, model.TaskTypeStorageCleanup, w.leaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("claiming storage cleanup task", "error", err)
			if !sleepUntilNextWorkerPoll(ctx, w.pollInterval) {
				return
			}
			continue
		}
		if task == nil {
			if !sleepUntilNextWorkerPoll(ctx, w.pollInterval) {
				return
			}
			continue
		}
		w.recordWorkStarted()
		func() {
			defer w.recordWorkFinished()
			stopLeaseRenewal := startTaskLeaseRenewal(w.logger, w.repos, task, w.leaseTTL)
			defer stopLeaseRenewal()
			w.processTask(ctx, task)
		}()
		releaseTaskOnWorkerShutdown(ctx, w.logger, w.repos, task)
	}
}

func (w *StorageCleanupWorker) processTask(ctx context.Context, task *model.Task) {
	start := time.Now()
	defer func() {
		admin.WorkerTaskDuration.WithLabelValues("storage_cleanup").Observe(time.Since(start).Seconds())
	}()

	logger := w.logger.With("taskID", task.ID, "uploadID", task.RefID)
	hasRefs, err := w.repos.StorageCleanup.UploadHasObjectReferences(ctx, task.RefID)
	if err != nil {
		logger.Error("checking storage cleanup references failed", "error", err)
		scheduleTaskRetry(ctx, w.repos, task, "storage_cleanup", logger, err)
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return
	}
	if hasRefs {
		if !w.waitForReferences(ctx, task, logger, "Waiting for object references to clear") {
			return
		}
		logger.Info("storage cleanup waiting because object references remain")
		return
	}
	hasRefs, err = w.repos.StorageCleanup.TaskHasObjectReferences(ctx, task.ID, task.RefID)
	if err != nil {
		logger.Error("checking storage cleanup piece references failed", "error", err)
		scheduleTaskRetry(ctx, w.repos, task, "storage_cleanup", logger, err)
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return
	}
	if hasRefs {
		if !w.waitForReferences(ctx, task, logger, "Waiting for shared data references to clear") {
			return
		}
		logger.Info("storage cleanup waiting because shared data references remain")
		return
	}

	copies, err := w.repos.StorageCleanup.ListCopiesForTask(ctx, task.ID)
	if err != nil {
		logger.Error("loading storage cleanup copies failed", "error", err)
		scheduleTaskRetry(ctx, w.repos, task, "storage_cleanup", logger, err)
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return
	}
	if len(copies) == 0 {
		if !w.completeTask(ctx, task, logger, "No remote replicas to delete") {
			return
		}
		return
	}

	waiting := false
	unsupported := false
	for _, copy := range copies {
		switch copy.Status {
		case model.StorageCleanupCopyStatusRemoved:
			continue
		case model.StorageCleanupCopyStatusUnsupported:
			unsupported = true
			continue
		}
		copyWaiting, err := w.processCopy(ctx, copy)
		if err != nil {
			logger.Warn("storage cleanup copy failed", "copyID", copy.ID, "error", err)
			if errors.Is(err, errStorageCleanupCopyUnsupported) {
				unsupported = true
				continue
			}
			scheduleTaskRetry(ctx, w.repos, task, "storage_cleanup", logger, err)
			admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
			return
		}
		waiting = waiting || copyWaiting
	}
	if waiting {
		if err := w.repos.Tasks.WaitRunning(ctx, task, model.TaskWaitReasonExternalConfirmation, "Waiting for remote replica deletion", storageCleanupConfirmationDelay); err != nil {
			logger.Error("waiting for storage cleanup confirmation failed", "error", err)
			_ = w.repos.Tasks.FailRunning(ctx, task, err.Error())
			admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
			return
		}
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "success").Inc()
		return
	}
	if unsupported {
		_ = w.repos.Tasks.FailRunning(ctx, task, "Remote replica deletion is not supported for this provider")
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return
	}
	if err := w.repos.StorageCleanup.DeleteUploadProvenanceIfUnreferenced(ctx, task.RefID); err != nil {
		logger.Error("deleting storage cleanup provenance failed", "error", err)
		scheduleTaskRetry(ctx, w.repos, task, "storage_cleanup", logger, err)
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return
	}
	if !w.completeTask(ctx, task, logger, "Remote replicas deleted") {
		return
	}
}

func (w *StorageCleanupWorker) completeTask(ctx context.Context, task *model.Task, logger *slog.Logger, message string) bool {
	if err := w.repos.Tasks.CompleteWithMessage(ctx, task, message); err != nil {
		if logger != nil {
			logger.Error("failed to complete storage cleanup task", "taskID", task.ID, "error", err)
		}
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return false
	}
	admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "success").Inc()
	return true
}

func (w *StorageCleanupWorker) waitForReferences(ctx context.Context, task *model.Task, logger *slog.Logger, message string) bool {
	if err := w.repos.Tasks.WaitRunning(ctx, task, model.TaskWaitReasonDependency, message, storageCleanupReferenceDelay); err != nil {
		if logger != nil {
			logger.Error("failed to wait storage cleanup task", "taskID", task.ID, "error", err)
		}
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return false
	}
	admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "success").Inc()
	return true
}

func (w *StorageCleanupWorker) processCopy(ctx context.Context, copy model.StorageCleanupCopy) (bool, error) {
	if copy.DataSetID == nil || copy.ClientDataSetID == nil || copy.PieceID == nil || copy.ProviderID == nil || copy.PieceCID == "" {
		msg := "Storage provider details are incomplete for this version"
		if err := w.repos.StorageCleanup.MarkCopyUnsupported(ctx, copy.ID, msg); err != nil {
			return false, err
		}
		return false, fmt.Errorf("%w: %s", errStorageCleanupCopyUnsupported, msg)
	}
	pieceCID, err := cid.Parse(copy.PieceCID)
	if err != nil {
		msg := "Stored data identifier is invalid"
		if markErr := w.repos.StorageCleanup.MarkCopyUnsupported(ctx, copy.ID, msg); markErr != nil {
			return false, markErr
		}
		return false, fmt.Errorf("%w: %v", errStorageCleanupCopyUnsupported, err)
	}
	cleanupCtx, err := w.storage.CreateCleanupContext(ctx, &storage.CreateContextOptions{
		DataSetID: sdkBigIntPtr(copy.DataSetID),
	})
	if err != nil {
		return false, err
	}
	status, err := cleanupCtx.PieceStatus(ctx, pieceCID)
	if err != nil {
		return false, err
	}
	if status == nil || !status.Exists {
		return false, w.repos.StorageCleanup.MarkCopyRemoved(ctx, copy.ID)
	}
	if copy.Status == model.StorageCleanupCopyStatusDeleteScheduled {
		return true, nil
	}
	result, err := cleanupCtx.DeletePieceByID(ctx, copy.PieceID.SDK())
	if err != nil {
		return false, err
	}
	txHash := ""
	if result != nil {
		txHash = result.Hash.String()
	}
	if err := w.repos.StorageCleanup.MarkCopyDeleteScheduled(ctx, copy.ID, txHash); err != nil {
		return false, err
	}
	return true, nil
}
