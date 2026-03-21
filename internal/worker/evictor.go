package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
	"golang.org/x/sync/errgroup"
)

// Evictor claims evict_cache tasks and removes objects from local cache
// after they have been successfully stored on-chain.
type Evictor struct {
	repos        *repository.Repositories
	cache        cache.Cache
	stateMachine *state.Machine
	concurrency  int
	pollInterval time.Duration
	leaseTTL     time.Duration
	logger       *slog.Logger
	*livenessTracker
}

// NewEvictor creates a new cache evictor worker.
func NewEvictor(repos *repository.Repositories, c cache.Cache, sm *state.Machine, concurrency int, pollInterval time.Duration, logger *slog.Logger) *Evictor {
	return &Evictor{
		repos:           repos,
		cache:           c,
		stateMachine:    sm,
		concurrency:     concurrency,
		pollInterval:    pollInterval,
		leaseTTL:        5 * time.Minute,
		logger:          logger,
		livenessTracker: newLivenessTracker(pollInterval),
	}
}

func (e *Evictor) Name() string { return "evictor" }

func (e *Evictor) Run(ctx context.Context) error {
	return pollLoop(ctx, e.pollInterval, e.tick)
}

func (e *Evictor) tick(ctx context.Context) error {
	e.recordTick()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(e.concurrency)

	for range e.concurrency {
		task, err := e.repos.Tasks.ClaimPending(gctx, model.TaskTypeEvictCache, e.leaseTTL)
		if err != nil {
			e.logger.Error("claiming evict_cache task", "error", err)
			break
		}
		if task == nil {
			break
		}

		g.Go(func() error {
			e.processTask(gctx, task)
			return nil
		})
	}

	err := g.Wait()
	return err
}

// Healthy returns true if the worker has ticked recently.
func (e *Evictor) Healthy() bool { return e.healthy() }

func (e *Evictor) processTask(ctx context.Context, task *model.Task) {
	start := time.Now()
	defer func() {
		admin.WorkerTaskDuration.WithLabelValues("evictor").Observe(time.Since(start).Seconds())
	}()

	logger := e.logger.With("taskID", task.ID, "objectID", task.RefID, "gen", task.RefGeneration)

	obj, err := e.repos.Objects.GetByID(ctx, task.RefID)
	if err != nil || obj == nil {
		logger.Warn("object not found for evict task", "error", err)
		_ = e.repos.Tasks.Fail(ctx, task.ID, "object not found")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	if obj.Generation != task.RefGeneration {
		logger.Warn("stale generation, skipping")
		_ = e.repos.Tasks.Fail(ctx, task.ID, "stale generation")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	// Dual safety check: must be onchained AND have PieceCID
	if obj.State != model.ObjectStateOnChained {
		logger.Warn("object not in onchained state", "state", obj.State)
		_ = e.repos.Tasks.Fail(ctx, task.ID, "not onchained")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}
	if obj.PieceCID == nil || *obj.PieceCID == "" {
		logger.Error("object has no PieceCID, refusing to evict")
		_ = e.repos.Tasks.Fail(ctx, task.ID, "no PieceCID")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	bucket, err := e.repos.Buckets.GetByID(ctx, obj.BucketID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "bucketID", obj.BucketID, "error", err)
		_ = e.repos.Tasks.Fail(ctx, task.ID, "bucket not found")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	// CAS transition first (acts as generation guard), then delete cache.
	// This prevents a TOCTOU race where a concurrent PutObject could write new data
	// between our generation check and cache deletion.
	if err := state.TransitionState(ctx, e.stateMachine, e.repos.Objects, task.RefID, task.RefGeneration,
		model.ObjectStateOnChained, model.ObjectStateCacheEvicted); err != nil {
		logger.Error("state transition onchained→cache_evicted failed", "error", err)
		if task.RetryCount+1 >= task.MaxRetries {
			_ = state.TransitionToFailed(ctx, e.stateMachine, e.repos.Objects, task.RefID, task.RefGeneration,
				model.ObjectStateOnChained, fmt.Sprintf("cache eviction: %v (max retries reached)", err))
			if ftErr := e.repos.Tasks.FailTerminal(ctx, task.ID, err.Error()); ftErr != nil {
				logger.Error("failed to mark task as dead-letter", "error", ftErr)
			} else {
				admin.DeadLetterTotal.WithLabelValues("evictor", string(model.TaskTypeEvictCache)).Inc()
			}
		} else {
			_ = e.repos.Tasks.Fail(ctx, task.ID, err.Error())
			_ = e.repos.Tasks.Requeue(ctx, task.ID, retryDelay(task.RetryCount))
		}
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	if err := e.cache.Delete(ctx, bucket.Name, obj.Key); err != nil {
		logger.Warn("cache delete failed (state already transitioned)", "error", err)
	}

	_ = e.repos.Tasks.Complete(ctx, task.ID)
	admin.WorkerTasksProcessed.WithLabelValues("evictor", "success").Inc()
	logger.Info("cache eviction completed")
}
