package worker

import (
	"context"
	"errors"
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

const lruAccessedAtPayloadKey = "cache_accessed_at"

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
		e.cancelTask(ctx, task, "Cache eviction task uses an unsupported stage")
		return
	}
	if e.policy != stagePolicy.requiredPolicy {
		e.cancelTask(ctx, task, stagePolicy.policyMismatchReason)
		return
	}
	if stagePolicy.capacityBound &&
		e.cache.UsedBytes() <= watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent) {
		e.cancelTask(ctx, task, "LRU cache usage already reached the low watermark")
		return
	}
	e.guardEviction(ctx, task, stagePolicy)
}

func (e *Evictor) guardEviction(
	ctx context.Context,
	task *model.Task,
	stagePolicy evictionStagePolicy,
) {
	prepared, ok := e.prepareEviction(ctx, task, stagePolicy)
	if !ok {
		return
	}

	var result guardedEvictionResult
	e.cacheAccess.GuardDeletion(task.RefVersionID, func(lastAccess time.Time) bool {
		result = e.deletePreparedEviction(ctx, task, prepared, stagePolicy, lastAccess)
		return result.deleted
	})
	e.finishGuardedEviction(ctx, task, stagePolicy, result)
}

type preparedEviction struct {
	version    *model.ObjectVersion
	bucketName string
}

func (e *Evictor) prepareEviction(
	ctx context.Context,
	task *model.Task,
	stagePolicy evictionStagePolicy,
) (*preparedEviction, bool) {
	version, err := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil {
		e.retryTask(ctx, task, err, "loading object version for cache eviction")
		return nil, false
	}
	if version == nil {
		stagePolicy.reject(e, ctx, task, "object not found", "object version not found for cache eviction")
		return nil, false
	}

	eligibility := stagePolicy.eligibility(version)
	if eligibility.action != guardedEvictionNone {
		eligibility.version = version
		e.finishGuardedEviction(ctx, task, stagePolicy, eligibility)
		return nil, false
	}

	if !e.ensureRemoteReadable(ctx, task, stagePolicy, version) {
		return nil, false
	}

	bucket, err := e.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil {
		e.retryTask(ctx, task, err, "loading object bucket for cache eviction")
		return nil, false
	}
	if bucket == nil {
		stagePolicy.reject(e, ctx, task, "bucket not found", "bucket not found for cache eviction")
		return nil, false
	}
	return &preparedEviction{version: version, bucketName: bucket.Name}, true
}

type guardedEvictionAction uint8

const (
	guardedEvictionNone guardedEvictionAction = iota
	guardedEvictionRetry
	guardedEvictionReject
	guardedEvictionAccessed
	guardedEvictionDefer
	guardedEvictionLowWatermark
)

type guardedEvictionResult struct {
	action       guardedEvictionAction
	version      *model.ObjectVersion
	lastAccess   time.Time
	err          error
	reason       string
	logMessage   string
	deleted      bool
	cachePresent bool
}

func (e *Evictor) deletePreparedEviction(
	ctx context.Context,
	task *model.Task,
	prepared *preparedEviction,
	stagePolicy evictionStagePolicy,
	lastAccess time.Time,
) guardedEvictionResult {
	result := guardedEvictionResult{lastAccess: lastAccess}
	version, err := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil {
		result.action = guardedEvictionRetry
		result.err = err
		result.logMessage = "reloading object version before cache eviction"
		return result
	}
	if version == nil {
		result.action = guardedEvictionReject
		result.reason = "object not found"
		result.logMessage = "object version not found before cache eviction"
		return result
	}
	result.version = version

	eligibility := stagePolicy.eligibility(version)
	if eligibility.action != guardedEvictionNone {
		eligibility.version = version
		eligibility.lastAccess = lastAccess
		return eligibility
	}
	if stagePolicy.capacityBound {
		effectiveAccess := effectiveLRUAccessTime(version)
		if !lruAccessSnapshotMatches(task, effectiveAccess) ||
			lruAccessOccurredAfterSnapshot(task, lastAccess) {
			result.action = guardedEvictionAccessed
			return result
		}
	}

	if !sameEvictionTarget(prepared.version, version) {
		result.action = guardedEvictionRetry
		result.err = errors.New("object version changed while preparing cache eviction")
		result.logMessage = "object version changed before cache eviction"
		return result
	}
	remoteReadable, err := e.repos.Uploads.HasReadableCommittedCopy(
		ctx,
		*version.StorageUploadID,
	)
	if err != nil {
		result.action = guardedEvictionRetry
		result.err = err
		result.logMessage = "rechecking readable remote copies before cache eviction"
		return result
	}
	if !remoteReadable {
		result.action = guardedEvictionReject
		result.reason = "no readable upload copies"
		result.logMessage = "object version no longer has readable upload copies"
		return result
	}

	reserved := false
	if stagePolicy.capacityBound {
		if !e.reserveLRUDeletion(version.Size) {
			result.action = guardedEvictionLowWatermark
			return result
		}
		reserved = true
	}
	if reserved {
		defer func() {
			if reserved {
				e.releaseLRUDeletion(version.Size)
			}
		}()
	}

	result.cachePresent = e.cache.Exists(ctx, prepared.bucketName, version.CacheKey)
	if err := e.cache.Delete(ctx, prepared.bucketName, version.CacheKey); err != nil {
		result.action = guardedEvictionRetry
		result.err = err
		result.logMessage = "deleting cache entry"
		return result
	}
	if reserved {
		e.releaseLRUDeletion(version.Size)
		reserved = false
	}
	result.deleted = true
	if err := e.recordDeletedCacheState(ctx, task, version); err != nil {
		result.action = guardedEvictionRetry
		result.err = err
		result.logMessage = "recording cache eviction state"
	}
	return result
}

func sameEvictionTarget(before, after *model.ObjectVersion) bool {
	if before == nil || after == nil ||
		before.BucketID != after.BucketID ||
		before.CacheKey != after.CacheKey {
		return false
	}
	if before.StorageUploadID == nil || after.StorageUploadID == nil {
		return before.StorageUploadID == nil && after.StorageUploadID == nil
	}
	return *before.StorageUploadID == *after.StorageUploadID
}

func (e *Evictor) finishGuardedEviction(
	ctx context.Context,
	task *model.Task,
	stagePolicy evictionStagePolicy,
	result guardedEvictionResult,
) {
	switch result.action {
	case guardedEvictionRetry:
		e.retryTask(ctx, task, result.err, result.logMessage)
	case guardedEvictionReject:
		stagePolicy.reject(e, ctx, task, result.reason, result.logMessage)
	case guardedEvictionAccessed:
		e.persistNewerInMemoryAccess(
			ctx,
			task,
			result.version.CacheAccessedAt,
			result.lastAccess,
		)
		e.cancelTask(ctx, task, "Object was accessed after this LRU eviction was planned")
	case guardedEvictionDefer:
		e.deferReplicatingEviction(ctx, task)
	case guardedEvictionLowWatermark:
		e.cancelTask(ctx, task, "LRU cache usage already reached the low watermark")
	case guardedEvictionNone:
		if !result.deleted {
			return
		}
		if !result.cachePresent {
			e.taskLogger(task).Info("cache entry already absent; reconciling eviction state")
		}
		e.completeTask(ctx, task, e.taskLogger(task), "cache eviction completed")
	}
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

func (e *Evictor) persistNewerInMemoryAccess(
	ctx context.Context,
	task *model.Task,
	durableAccess *time.Time,
	inMemoryAccess time.Time,
) {
	inMemoryAccess = normalizeLRUAccessTime(inMemoryAccess)
	if inMemoryAccess.IsZero() ||
		(durableAccess != nil &&
			!inMemoryAccess.After(normalizeLRUAccessTime(*durableAccess))) {
		return
	}
	if err := e.repos.Objects.RecordVersionCacheAccess(
		ctx,
		task.RefVersionID,
		inMemoryAccess,
	); err != nil {
		e.taskLogger(task).Warn(
			"persisting recent cache access before cancelling LRU eviction",
			"error",
			err,
		)
	}
}

func (e *Evictor) ensureRemoteReadable(
	ctx context.Context,
	task *model.Task,
	stagePolicy evictionStagePolicy,
	version *model.ObjectVersion,
) bool {
	if version.StorageUploadID == nil {
		stagePolicy.reject(e, ctx, task, "no accepted upload", "object version has no accepted upload, refusing to evict")
		return false
	}
	readable, err := e.repos.Uploads.HasReadableCommittedCopy(ctx, *version.StorageUploadID)
	if err != nil {
		e.retryTask(ctx, task, err, "checking readable remote copies before cache eviction")
		return false
	}
	if !readable {
		stagePolicy.reject(e, ctx, task, "no readable upload copies", "object version has no readable upload copies, refusing to evict")
		return false
	}
	return true
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

func normalizeLRUAccessTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func lruAccessSnapshot(task *model.Task) (time.Time, bool) {
	if task == nil {
		return time.Time{}, false
	}
	raw, planned := task.Payload[lruAccessedAtPayloadKey]
	if !planned {
		return time.Time{}, false
	}
	text, ok := raw.(string)
	if !ok {
		return time.Time{}, false
	}
	expected, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return time.Time{}, false
	}
	return normalizeLRUAccessTime(expected), true
}

func effectiveLRUAccessTime(version *model.ObjectVersion) time.Time {
	if version == nil {
		return time.Time{}
	}
	if version.CacheAccessedAt != nil {
		return *version.CacheAccessedAt
	}
	return version.CreatedAt
}

func lruAccessSnapshotMatches(task *model.Task, current time.Time) bool {
	expected, ok := lruAccessSnapshot(task)
	if !ok || current.IsZero() {
		return false
	}
	return normalizeLRUAccessTime(current).Equal(expected)
}

func lruAccessOccurredAfterSnapshot(task *model.Task, current time.Time) bool {
	if current.IsZero() {
		return false
	}
	expected, ok := lruAccessSnapshot(task)
	if !ok {
		return true
	}
	return normalizeLRUAccessTime(current).After(expected)
}
