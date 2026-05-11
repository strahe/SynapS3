package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
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

const replicatingEvictDeferDelay = 30 * time.Second

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
	var wg sync.WaitGroup
	for range e.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.runSlot(ctx)
		}()
	}

	wg.Wait()
	return ctx.Err()
}

func (e *Evictor) runSlot(ctx context.Context) {
	if !sleepUntilNextWorkerPoll(ctx, e.pollInterval) {
		return
	}

	for {
		if ctx.Err() != nil {
			return
		}

		e.recordTick()
		task, err := e.repos.Tasks.ClaimReady(ctx, model.TaskTypeEvictCache, e.leaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			e.logger.Error("claiming evict_cache task", "error", err)
			if !sleepUntilNextWorkerPoll(ctx, e.pollInterval) {
				return
			}
			continue
		}
		if task == nil {
			if !sleepUntilNextWorkerPoll(ctx, e.pollInterval) {
				return
			}
			continue
		}

		e.recordWorkStarted()
		func() {
			defer e.recordWorkFinished()
			stopLeaseRenewal := startTaskLeaseRenewal(e.logger, e.repos, task, e.leaseTTL)
			defer stopLeaseRenewal()
			e.processTask(ctx, task)
		}()
		releaseTaskOnWorkerShutdown(ctx, e.logger, e.repos, task)
	}
}

// Healthy returns true if the worker has ticked recently.
func (e *Evictor) Healthy() bool { return e.healthy() }

func (e *Evictor) processTask(ctx context.Context, task *model.Task) {
	start := time.Now()
	defer func() {
		admin.WorkerTaskDuration.WithLabelValues("evictor").Observe(time.Since(start).Seconds())
	}()

	logger := e.logger.With("taskID", task.ID, "objectID", task.RefID, "versionID", task.RefVersionID)

	version, err := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil || version == nil {
		logger.Warn("object version not found for evict task", "error", err)
		_ = e.repos.Tasks.FailRunning(ctx, task, "object not found")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	if version.State == model.ObjectStateReplicating {
		if err := e.repos.Tasks.WaitRunning(ctx, task, model.TaskWaitReasonDependency, "waiting for all copies to commit", replicatingEvictDeferDelay); err != nil {
			logger.Error("failed to defer replicating cache eviction", "error", err)
			_ = e.repos.Tasks.FailRunning(ctx, task, err.Error())
			admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
			return
		}
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "success").Inc()
		logger.Info("cache eviction deferred until replication completes")
		return
	}
	if version.State != model.ObjectStateStored {
		logger.Warn("object version not in stored state", "state", version.State)
		_ = e.repos.Tasks.FailRunning(ctx, task, "not stored")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}
	if version.StorageUploadID == nil {
		logger.Error("object version has no accepted upload, refusing to evict")
		_ = e.repos.Tasks.FailRunning(ctx, task, "no accepted upload")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}
	copies, err := e.repos.Uploads.ListReadableCommittedCopies(ctx, *version.StorageUploadID)
	if err != nil {
		logger.Error("storage upload copy lookup failed", "error", err)
		_ = e.repos.Tasks.FailRunning(ctx, task, err.Error())
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}
	if len(copies) == 0 {
		logger.Error("object version has no readable upload copies, refusing to evict")
		_ = e.repos.Tasks.FailRunning(ctx, task, "no readable upload copies")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	bucket, err := e.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil || bucket == nil {
		logger.Error("bucket not found", "bucketID", version.BucketID, "error", err)
		_ = e.repos.Tasks.FailRunning(ctx, task, "bucket not found")
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	if err := e.cache.Delete(ctx, bucket.Name, version.CacheKey); err != nil {
		logger.Warn("cache delete failed", "error", err)
		scheduleTaskRetry(ctx, e.repos, task, "evictor", logger, err)
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	// Delete first; the DB transition records both lifecycle and cache location atomically.
	if err := state.TransitionState(ctx, e.stateMachine, e.repos.Objects, task.RefVersionID,
		model.ObjectStateStored, model.ObjectStateCacheEvicted); err != nil {
		logger.Error("state transition stored→cache_evicted failed", "error", err)
		if latest, latestErr := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID); latestErr == nil && latest != nil && latest.State == model.ObjectStateCacheEvicted {
			if !completeWorkerTask(ctx, e.repos, task, "evictor", logger) {
				return
			}
			logger.Info("cache eviction already recorded")
			return
		}
		scheduleTaskRetry(ctx, e.repos, task, "evictor", logger, err)
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}

	if !completeWorkerTask(ctx, e.repos, task, "evictor", logger) {
		return
	}
	logger.Info("cache eviction completed")
}
