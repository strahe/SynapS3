package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/storage"
	"golang.org/x/sync/errgroup"
)

// Uploader claims upload tasks, uploads objects to Storage Providers via the
// synapse-go SDK (which handles both SP upload and on-chain commit), records the
// PieceCID, and optionally enqueues cache eviction.
type Uploader struct {
	repos        *repository.Repositories
	cache        cache.Cache
	storage      synapse.StorageClient
	wallet       synapse.WalletQuerier // optional; nil skips balance pre-check
	stateMachine *state.Machine
	autoEvict    bool
	concurrency  int
	pollInterval time.Duration
	leaseTTL     time.Duration
	logger       *slog.Logger
	*livenessTracker
}

// NewUploader creates a new upload worker.
func NewUploader(repos *repository.Repositories, c cache.Cache, sc synapse.StorageClient, wallet synapse.WalletQuerier, sm *state.Machine, autoEvict bool, concurrency int, pollInterval time.Duration, logger *slog.Logger) *Uploader {
	return &Uploader{
		repos:           repos,
		cache:           c,
		storage:         sc,
		wallet:          wallet,
		stateMachine:    sm,
		autoEvict:       autoEvict,
		concurrency:     concurrency,
		pollInterval:    pollInterval,
		leaseTTL:        10 * time.Minute,
		logger:          logger,
		livenessTracker: newLivenessTracker(pollInterval),
	}
}

func (u *Uploader) Name() string { return "uploader" }

func (u *Uploader) Run(ctx context.Context) error {
	return pollLoop(ctx, u.pollInterval, u.tick)
}

func (u *Uploader) tick(ctx context.Context) error {
	u.recordTick()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(u.concurrency)

	for range u.concurrency {
		task, err := u.repos.Tasks.ClaimPending(gctx, model.TaskTypeUpload, u.leaseTTL)
		if err != nil {
			u.logger.Error("claiming upload task", "error", err)
			break
		}
		if task == nil {
			break
		}

		g.Go(func() error {
			u.processTask(gctx, task)
			return nil
		})
	}

	err := g.Wait()
	return err
}

// Healthy returns true if the worker has ticked recently.
func (u *Uploader) Healthy() bool { return u.healthy() }

func (u *Uploader) processTask(ctx context.Context, task *model.Task) {
	start := time.Now()
	defer func() {
		admin.WorkerTaskDuration.WithLabelValues("uploader").Observe(time.Since(start).Seconds())
	}()

	logger := u.logger.With("taskID", task.ID, "objectID", task.RefID, "gen", task.RefGeneration)

	if u.storage == nil {
		logger.Warn("storage client not configured, failing task")
		_ = u.repos.Tasks.Fail(ctx, task.ID, "storage client not configured")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	obj, err := u.repos.Objects.GetByID(ctx, task.RefID)
	if err != nil || obj == nil {
		logger.Warn("object not found for upload task", "error", err)
		_ = u.repos.Tasks.Fail(ctx, task.ID, "object not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	if obj.Generation != task.RefGeneration {
		logger.Warn("stale generation, skipping", "objGen", obj.Generation)
		_ = u.repos.Tasks.Fail(ctx, task.ID, "stale generation")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	bucket, err := u.repos.Buckets.GetByID(ctx, obj.BucketID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "bucketID", obj.BucketID, "error", err)
		_ = u.repos.Tasks.Fail(ctx, task.ID, "bucket not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	// Transition cached → uploading (on retry, object may already be in uploading state)
	if obj.State != model.ObjectStateUploading {
		if err := state.TransitionState(ctx, u.stateMachine, u.repos.Objects, task.RefID, task.RefGeneration,
			model.ObjectStateCached, model.ObjectStateUploading); err != nil {
			logger.Warn("state transition cached→uploading failed", "error", err)
			_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
			admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
			return
		}
	}

	// Pre-flight: check payment account has deposited funds.
	if u.wallet != nil {
		info, wErr := u.wallet.GetWalletInfo(ctx)
		if wErr != nil {
			logger.Warn("wallet balance pre-check failed, proceeding with upload", "error", wErr)
		} else if info != nil && info.USDFCAccount != nil &&
			info.USDFCAccount.Funds != nil && info.USDFCAccount.Funds.Sign() == 0 {
			msg := "insufficient payment account balance: USDFC deposited funds = 0, please deposit USDFC into your payment account before uploading"
			logger.Warn(msg)
			_ = u.repos.Tasks.Fail(ctx, task.ID, msg)
			admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
			return
		}
	}

	// Read from cache
	rc, _, err := u.cache.Get(ctx, bucket.Name, obj.Key)
	if err != nil {
		u.handleFailure(ctx, task, logger, model.ObjectStateUploading, "cache read", err)
		return
	}
	defer func() { _ = rc.Close() }()

	// Upload to provider (SDK handles store + on-chain commit)
	uploadOpts := &storage.UploadOptions{
		DataSetMetadata: map[string]string{"bucket": bucket.Name},
	}
	result, err := u.storage.Upload(ctx, rc, uploadOpts)
	if err != nil {
		u.handleFailure(ctx, task, logger, model.ObjectStateUploading, "upload",
			fmt.Errorf("%s", decodeRevertReason(err.Error())))
		return
	}
	if result == nil {
		u.handleFailure(ctx, task, logger, model.ObjectStateUploading, "upload", errors.New("upload returned nil result"))
		return
	}
	if !result.Complete {
		u.handleFailure(ctx, task, logger, model.ObjectStateUploading, "upload",
			fmt.Errorf("upload incomplete: %d/%d copies committed", result.SuccessCount(), requestedCopies(result)))
		return
	}

	pieceCID := result.PieceCID.String()
	retrievalURL := firstRetrievalURL(result)
	if retrievalURL == "" {
		u.handleFailure(ctx, task, logger, model.ObjectStateUploading, "upload", errors.New("upload completed without retrieval URL"))
		return
	}

	// Atomic: update storage info + transition uploading → stored
	if err := u.repos.Objects.SetStorageInfoAndTransition(ctx, task.RefID, task.RefGeneration,
		pieceCID, retrievalURL, model.ObjectStateUploading, model.ObjectStateStored); err != nil {
		u.handleFailure(ctx, task, logger, model.ObjectStateUploading, "state transition uploading→stored", err)
		return
	}

	// Lazy-populate bucket ProofSetID from first upload result
	if bucket.ProofSetID == nil && len(result.Copies) > 0 && result.Copies[0].DataSetID != nil {
		dsID := result.Copies[0].DataSetID.String()
		if updateErr := u.repos.Buckets.SetProofSetID(ctx, bucket.ID, dsID); updateErr != nil {
			logger.Warn("failed to populate bucket ProofSetID (non-fatal)", "error", updateErr)
		}
	}

	// Enqueue cache eviction task
	if u.autoEvict {
		evictTask := &model.Task{
			Type:           model.TaskTypeEvictCache,
			RefType:        "object",
			RefID:          task.RefID,
			RefGeneration:  task.RefGeneration,
			IdempotencyKey: fmt.Sprintf("evict_cache:%d:%d", task.RefID, task.RefGeneration),
			Status:         model.TaskStatusPending,
			MaxRetries:     3,
			ScheduledAt:    time.Now(),
		}
		if err := u.repos.Tasks.Create(ctx, evictTask); err != nil {
			if !errors.Is(err, repository.ErrAlreadyExists) {
				logger.Warn("failed to enqueue eviction task (non-fatal)", "error", err)
			}
		}
	}

	_ = u.repos.Tasks.Complete(ctx, task.ID)
	logger.Info("upload complete", "pieceCID", pieceCID)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func requestedCopies(result *storage.UploadResult) int {
	if result == nil {
		return 0
	}
	if result.RequestedCopies > 0 {
		return result.RequestedCopies
	}
	return len(result.Copies)
}

func firstRetrievalURL(result *storage.UploadResult) string {
	if result == nil {
		return ""
	}
	for _, copy := range result.Copies {
		if copy.RetrievalURL != "" {
			return copy.RetrievalURL
		}
	}
	return ""
}

func (u *Uploader) handleFailure(ctx context.Context, task *model.Task, logger *slog.Logger, currentState model.ObjectState, stage string, err error) {
	logger.Error(stage+" failed", "error", err)
	if task.RetryCount+1 >= task.MaxRetries {
		_ = state.TransitionToFailed(ctx, u.stateMachine, u.repos.Objects, task.RefID, task.RefGeneration,
			currentState, fmt.Sprintf("%s: %v (max retries reached)", stage, err))
		if ftErr := u.repos.Tasks.FailTerminal(ctx, task.ID, err.Error()); ftErr != nil {
			logger.Error("failed to mark task as dead-letter", "error", ftErr)
		} else {
			admin.DeadLetterTotal.WithLabelValues("uploader", string(model.TaskTypeUpload)).Inc()
		}
	} else {
		_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
		_ = u.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}
