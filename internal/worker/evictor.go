package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheaccess"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/state"
)

const (
	replicatingEvictDeferDelay = 30 * time.Second
	lruCandidateBatchSize      = 100
	lruTerminalRetryDelay      = time.Hour
)

// Evictor claims cache eviction tasks and removes remotely durable objects
// according to the configured policy.
type Evictor struct {
	repos                *repository.Repositories
	cache                cache.Cache
	cacheAccess          *cacheaccess.Coordinator
	stateMachine         *state.Machine
	policy               cache.EvictionPolicy
	maxCacheBytes        int64
	highWatermarkPercent int
	lowWatermarkPercent  int
	maxRetries           int
	concurrency          int
	pollInterval         time.Duration
	leaseTTL             time.Duration
	logger               *slog.Logger
	lruCapacityMu        sync.Mutex
	lruReservedBytes     int64
	*livenessTracker
}

// EvictorOption configures cache eviction behavior.
type EvictorOption func(*Evictor)

// WithCacheEvictionPolicy configures the eviction strategy and LRU capacity
// thresholds. The thresholds are ignored outside LRU mode.
func WithCacheEvictionPolicy(
	policy cache.EvictionPolicy,
	maxCacheBytes int64,
	highWatermarkPercent int,
	lowWatermarkPercent int,
	maxRetries int,
) EvictorOption {
	return func(e *Evictor) {
		e.policy = policy
		e.maxCacheBytes = maxCacheBytes
		e.highWatermarkPercent = highWatermarkPercent
		e.lowWatermarkPercent = lowWatermarkPercent
		e.maxRetries = maxRetries
	}
}

// NewEvictor creates a new cache evictor worker.
func NewEvictor(
	repos *repository.Repositories,
	c cache.Cache,
	cacheAccess *cacheaccess.Coordinator,
	sm *state.Machine,
	concurrency int,
	pollInterval time.Duration,
	logger *slog.Logger,
	opts ...EvictorOption,
) *Evictor {
	if cacheAccess == nil {
		panic("evictor requires a cache access coordinator")
	}
	e := &Evictor{
		repos:                repos,
		cache:                c,
		cacheAccess:          cacheAccess,
		stateMachine:         sm,
		policy:               cache.EvictionPolicyNone,
		highWatermarkPercent: 90,
		lowWatermarkPercent:  80,
		maxRetries:           defaultEvictMaxRetries,
		concurrency:          concurrency,
		pollInterval:         pollInterval,
		leaseTTL:             5 * time.Minute,
		logger:               logger,
		livenessTracker:      newLivenessTracker(pollInterval),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *Evictor) Name() string { return "evictor" }

func (e *Evictor) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	if e.policy == cache.EvictionPolicyLRU {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.runLRUPlanner(ctx)
		}()
	}
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

	stagePolicy, ok := evictionPolicyForTask(task)
	if !ok {
		e.applyEvictionDecision(
			ctx,
			task,
			cancelEviction("Cache eviction task uses an unsupported stage"),
		)
		return
	}

	prepared, decision := e.prepareEviction(ctx, task, stagePolicy)
	if decision != nil {
		e.applyEvictionDecision(ctx, task, decision)
		return
	}
	if decision = e.remoteSafetyDecision(
		ctx,
		stagePolicy,
		prepared.version,
		"rechecking readable remote copies before cache eviction",
	); decision != nil {
		e.applyEvictionDecision(ctx, task, decision)
		return
	}

	var deleted bool
	e.cacheAccess.GuardDeletion(task.RefVersionID, func(lastAccess time.Time) bool {
		decision, deleted = e.finalizePreparedEviction(
			ctx,
			task,
			prepared,
			stagePolicy,
			lastAccess,
		)
		return deleted
	})
	e.applyEvictionDecision(ctx, task, decision)
}

func (e *Evictor) reserveLRUDeletion(size int64) bool {
	e.lruCapacityMu.Lock()
	defer e.lruCapacityMu.Unlock()

	usedBytes := e.cache.UsedBytes()
	lowBytes := watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent)
	if usedBytes-e.lruReservedBytes <= lowBytes {
		return false
	}
	if size > 0 {
		e.lruReservedBytes += size
	}
	return true
}

func (e *Evictor) releaseLRUDeletion(size int64) {
	if size <= 0 {
		return
	}
	e.lruCapacityMu.Lock()
	e.lruReservedBytes -= size
	if e.lruReservedBytes < 0 {
		e.lruReservedBytes = 0
	}
	e.lruCapacityMu.Unlock()
}

func (e *Evictor) deferReplicatingEviction(ctx context.Context, task *model.Task) {
	logger := e.taskLogger(task)
	if err := e.repos.Tasks.WaitRunning(
		ctx,
		task,
		model.TaskWaitReasonDependency,
		"waiting for all copies to commit",
		replicatingEvictDeferDelay,
	); err != nil {
		logger.Error("failed to defer replicating cache eviction", "error", err)
		_ = e.repos.Tasks.FailRunning(ctx, task, err.Error())
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("evictor", "success").Inc()
	logger.Info("cache eviction deferred until replication completes")
}

func (e *Evictor) recordDeletedCacheState(
	ctx context.Context,
	task *model.Task,
	version *model.ObjectVersion,
) error {
	switch version.State {
	case model.ObjectStateStored:
		if err := state.TransitionState(
			ctx,
			e.stateMachine,
			e.repos.Objects,
			task.RefVersionID,
			model.ObjectStateStored,
			model.ObjectStateCacheEvicted,
		); err != nil {
			if latest, latestErr := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID); latestErr == nil &&
				latest != nil &&
				latest.State == model.ObjectStateCacheEvicted &&
				!latest.InCache {
				return nil
			}
			return err
		}
	case model.ObjectStateCacheEvicted:
		if err := e.repos.Objects.SetVersionCachePresence(ctx, task.RefVersionID, false); err != nil {
			return err
		}
	default:
		return fmt.Errorf("object is no longer eligible for cache eviction: state %s", version.State)
	}
	return nil
}

func (e *Evictor) completeTask(ctx context.Context, task *model.Task, logger *slog.Logger, message string) {
	if !completeWorkerTask(ctx, e.repos, task, "evictor", logger) {
		return
	}
	logger.Info(message)
}

func (e *Evictor) cancelTask(ctx context.Context, task *model.Task, message string) {
	logger := e.taskLogger(task)
	if err := e.repos.Tasks.CancelRunning(ctx, task, message); err != nil {
		logger.Error("failed to cancel cache eviction task", "error", err)
		admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("evictor", "success").Inc()
	logger.Info("cache eviction task cancelled", "reason", message)
}

func (e *Evictor) failTask(ctx context.Context, task *model.Task, lastError string, logMessage string) {
	logger := e.taskLogger(task)
	logger.Warn(logMessage)
	if err := e.repos.Tasks.FailRunning(ctx, task, lastError); err != nil {
		logger.Error("failed to record cache eviction failure", "error", err)
	}
	admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
}

func (e *Evictor) retryTask(ctx context.Context, task *model.Task, err error, logMessage string) {
	logger := e.taskLogger(task)
	logger.Warn(logMessage, "error", err)
	scheduleTaskRetry(ctx, e.repos, task, "evictor", logger, err)
	admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
}

func (e *Evictor) taskLogger(task *model.Task) *slog.Logger {
	return e.logger.With("taskID", task.ID, "objectID", task.RefID, "versionID", task.RefVersionID)
}
