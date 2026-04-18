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
	"golang.org/x/sync/errgroup"
)

// Uploader claims upload_to_sp tasks, uploads objects to the Storage Provider,
// records the PieceCID, and chains to add_roots.
type Uploader struct {
	repos        *repository.Repositories
	cache        cache.Cache
	storage      synapse.StorageClient
	wallet       synapse.WalletQuerier // optional; nil skips balance pre-check
	stateMachine *state.Machine
	concurrency  int
	pollInterval time.Duration
	leaseTTL     time.Duration
	logger       *slog.Logger
	*livenessTracker
}

// NewUploader creates a new SP upload worker.
func NewUploader(repos *repository.Repositories, c cache.Cache, sc synapse.StorageClient, wallet synapse.WalletQuerier, sm *state.Machine, concurrency int, pollInterval time.Duration, logger *slog.Logger) *Uploader {
	return &Uploader{
		repos:           repos,
		cache:           c,
		storage:         sc,
		wallet:          wallet,
		stateMachine:    sm,
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
		task, err := u.repos.Tasks.ClaimPending(gctx, model.TaskTypeUploadToSP, u.leaseTTL)
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

	// Get object metadata to read from cache
	obj, err := u.repos.Objects.GetByID(ctx, task.RefID)
	if err != nil || obj == nil {
		logger.Warn("object not found for upload task", "error", err)
		_ = u.repos.Tasks.Fail(ctx, task.ID, "object not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	// Verify generation matches
	if obj.Generation != task.RefGeneration {
		logger.Warn("stale generation, skipping", "objGen", obj.Generation)
		_ = u.repos.Tasks.Fail(ctx, task.ID, "stale generation")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	// Look up bucket for cache path
	bucket, err := u.repos.Buckets.GetByID(ctx, obj.BucketID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "bucketID", obj.BucketID, "error", err)
		_ = u.repos.Tasks.Fail(ctx, task.ID, "bucket not found")
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	// Recovery: if object is already uploaded (previous run succeeded at upload but
	// failed to create the chain task), skip upload and just re-create the chain task.
	if obj.State == model.ObjectStateUploaded {
		logger.Info("object already uploaded, recovering chain task creation")
		chainTask := &model.Task{
			Type:           model.TaskTypeAddRoots,
			RefType:        "object",
			RefID:          task.RefID,
			RefGeneration:  task.RefGeneration,
			IdempotencyKey: fmt.Sprintf("add_roots:%d:%d", task.RefID, task.RefGeneration),
			Status:         model.TaskStatusPending,
			MaxRetries:     5,
			ScheduledAt:    time.Now(),
		}
		if err := u.repos.Tasks.Create(ctx, chainTask); err != nil {
			if errors.Is(err, repository.ErrAlreadyExists) {
				// Chain task already exists — this upload task is done.
				logger.Info("chain task already exists, completing upload task")
			} else {
				_ = u.repos.Tasks.Fail(ctx, task.ID, fmt.Sprintf("recovery chain task creation: %v", err))
				admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
				return
			}
		}
		_ = u.repos.Tasks.Complete(ctx, task.ID)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
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
	// This is a fast-fail gate for the obvious zero-balance case, not exact cost estimation.
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
		logger.Error("cache read failed", "error", err)
		if task.RetryCount+1 >= task.MaxRetries {
			_ = state.TransitionToFailed(ctx, u.stateMachine, u.repos.Objects, task.RefID, task.RefGeneration,
				model.ObjectStateUploading, fmt.Sprintf("cache read: %v (max retries reached)", err))
			if ftErr := u.repos.Tasks.FailTerminal(ctx, task.ID, err.Error()); ftErr != nil {
				logger.Error("failed to mark task as dead-letter", "error", ftErr)
			} else {
				admin.DeadLetterTotal.WithLabelValues("uploader", string(model.TaskTypeUploadToSP)).Inc()
			}
		} else {
			_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
			_ = u.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
		}
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}
	defer func() { _ = rc.Close() }()

	// Upload to SP
	result, err := u.storage.Upload(ctx, rc, nil)
	if err != nil {
		enriched := decodeRevertReason(err.Error())
		logger.Error("SP upload failed", "error", enriched)
		if task.RetryCount+1 >= task.MaxRetries {
			_ = state.TransitionToFailed(ctx, u.stateMachine, u.repos.Objects, task.RefID, task.RefGeneration,
				model.ObjectStateUploading, fmt.Sprintf("SP upload: %v (max retries reached)", enriched))
			if ftErr := u.repos.Tasks.FailTerminal(ctx, task.ID, enriched); ftErr != nil {
				logger.Error("failed to mark task as dead-letter", "error", ftErr)
			} else {
				admin.DeadLetterTotal.WithLabelValues("uploader", string(model.TaskTypeUploadToSP)).Inc()
			}
		} else {
			_ = u.repos.Tasks.Fail(ctx, task.ID, enriched)
			_ = u.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
		}
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	// Atomic: set PieceCID + transition uploading → uploaded
	pieceCID := result.PieceCID.String()
	if err := u.repos.Objects.SetPieceCIDAndTransition(ctx, task.RefID, task.RefGeneration,
		pieceCID, model.ObjectStateUploading, model.ObjectStateUploaded); err != nil {
		logger.Error("SetPieceCIDAndTransition failed", "error", err)
		if task.RetryCount+1 >= task.MaxRetries {
			_ = state.TransitionToFailed(ctx, u.stateMachine, u.repos.Objects, task.RefID, task.RefGeneration,
				model.ObjectStateUploading, fmt.Sprintf("SetPieceCIDAndTransition: %v (max retries reached)", err))
			if ftErr := u.repos.Tasks.FailTerminal(ctx, task.ID, err.Error()); ftErr != nil {
				logger.Error("failed to mark task as dead-letter", "error", ftErr)
			} else {
				admin.DeadLetterTotal.WithLabelValues("uploader", string(model.TaskTypeUploadToSP)).Inc()
			}
		} else {
			_ = u.repos.Tasks.Fail(ctx, task.ID, err.Error())
			_ = u.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
		}
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}

	// Chain: enqueue add_roots task
	chainTask := &model.Task{
		Type:           model.TaskTypeAddRoots,
		RefType:        "object",
		RefID:          task.RefID,
		RefGeneration:  task.RefGeneration,
		IdempotencyKey: fmt.Sprintf("add_roots:%d:%d", task.RefID, task.RefGeneration),
		Status:         model.TaskStatusPending,
		MaxRetries:     5,
		ScheduledAt:    time.Now(),
	}
	if err := u.repos.Tasks.Create(ctx, chainTask); err != nil {
		if errors.Is(err, repository.ErrAlreadyExists) {
			// Chain task already exists (e.g., from reconciliation) — treat as success.
			logger.Info("chain task already exists, completing upload task")
		} else {
			logger.Error("enqueuing add_roots task", "error", err)
			_ = u.repos.Tasks.Fail(ctx, task.ID, fmt.Sprintf("chain task creation: %v", err))
			admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
			return
		}
	}

	_ = u.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
	logger.Info("upload completed", "pieceCID", pieceCID)
}
