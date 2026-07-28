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
	// lruFinalizeMu serializes only the final low-watermark check and local
	// deletion so concurrent slots cannot all cross the low watermark.
	lruFinalizeMu sync.Mutex
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

func (e *Evictor) runLRUPlanner(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		e.recordTick()
		if err := e.planLRUEvictions(ctx); err != nil && ctx.Err() == nil {
			e.logger.Error("planning LRU cache evictions", "error", err)
		}
		if !sleepUntilNextWorkerPoll(ctx, e.pollInterval) {
			return
		}
	}
}

func (e *Evictor) planLRUEvictions(ctx context.Context) error {
	if e.policy != cache.EvictionPolicyLRU || e.maxCacheBytes <= 0 {
		return nil
	}
	usedBytes := e.cache.UsedBytes()
	highBytes := watermarkBytes(e.maxCacheBytes, e.highWatermarkPercent)
	if usedBytes < highBytes {
		return nil
	}
	lowBytes := watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent)
	activeBytes, err := e.repos.Tasks.ActiveEvictionBytes(ctx, repository.CacheEvictionStageLRU)
	if err != nil {
		return err
	}
	bytesToPlan := usedBytes - lowBytes - activeBytes
	if bytesToPlan <= 0 {
		return nil
	}

	var plannedBytes int64
	plannedTasks := 0
	for bytesToPlan > 0 {
		createdThisBatch := 0
		terminalSince := time.Now().Add(-lruTerminalRetryDelay)
		candidates, err := e.repos.Objects.ListLRUEvictionCandidates(
			ctx,
			terminalSince,
			lruCandidateBatchSize,
		)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			break
		}
		for _, candidate := range candidates {
			activated, err := e.repos.Tasks.CreateOrReactivateLRU(
				ctx,
				newLRUEvictionTask(candidate, e.maxRetries),
				terminalSince,
			)
			if err != nil {
				return fmt.Errorf("creating LRU eviction task for version %s: %w", candidate.VersionID, err)
			}
			if !activated {
				continue
			}
			plannedTasks++
			createdThisBatch++
			plannedBytes += candidate.Size
			bytesToPlan -= candidate.Size
			if bytesToPlan <= 0 {
				break
			}
		}
		if len(candidates) < lruCandidateBatchSize || createdThisBatch == 0 {
			break
		}
	}

	if plannedTasks == 0 {
		e.logger.Warn(
			"cache is above the LRU high watermark but no remotely safe entries can be evicted",
			"usedBytes",
			usedBytes,
			"highBytes",
			highBytes,
			"lowBytes",
			lowBytes,
		)
		return nil
	}
	e.logger.Info(
		"planned LRU cache evictions",
		"tasks",
		plannedTasks,
		"plannedBytes",
		plannedBytes,
		"usedBytes",
		usedBytes,
		"targetBytes",
		lowBytes,
	)
	return nil
}

func newLRUEvictionTask(candidate repository.CacheEvictionCandidate, maxRetries int) *model.Task {
	stage := repository.CacheEvictionStageLRU
	accessedAt := candidate.CreatedAt
	if candidate.CacheAccessedAt != nil {
		accessedAt = *candidate.CacheAccessedAt
	}
	accessedAt = normalizeLRUAccessTime(accessedAt)
	return &model.Task{
		Type:           model.TaskTypeEvictCache,
		Stage:          &stage,
		RefType:        "object",
		RefID:          candidate.ObjectID,
		RefVersionID:   candidate.VersionID,
		IdempotencyKey: repository.LRUEvictionTaskKey(candidate.VersionID),
		Payload: map[string]interface{}{
			lruAccessedAtPayloadKey: accessedAt.Format(time.RFC3339Nano),
		},
		Status:      model.TaskStatusQueued,
		MaxRetries:  maxRetries,
		ScheduledAt: time.Now(),
	}
}

func watermarkBytes(maxBytes int64, percent int) int64 {
	if maxBytes <= 0 || percent <= 0 {
		return 0
	}
	if percent >= 100 {
		return maxBytes
	}
	return (maxBytes/100)*int64(percent) + (maxBytes%100)*int64(percent)/100
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

	stage := taskStage(task)
	switch stage {
	case repository.CacheEvictionStageLRU:
		if e.policy != cache.EvictionPolicyLRU {
			e.cancelTask(ctx, task, "Cache eviction policy no longer uses LRU")
			return
		}
		e.processLRUTask(ctx, task)
	case repository.CacheEvictionStageAfterUpload:
		if e.policy != cache.EvictionPolicyAfterUpload {
			e.cancelTask(ctx, task, "Cache eviction policy no longer removes objects after upload")
			return
		}
		e.processAfterUploadTask(ctx, task)
	default:
		e.cancelTask(ctx, task, "Cache eviction task uses an unsupported stage")
	}
}

func taskStage(task *model.Task) string {
	if task == nil || task.Stage == nil {
		return ""
	}
	return *task.Stage
}

func (e *Evictor) processLRUTask(ctx context.Context, task *model.Task) {
	if e.cache.UsedBytes() <= watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent) {
		e.cancelTask(ctx, task, "LRU cache usage already reached the low watermark")
		return
	}
	e.guardEviction(ctx, task)
}

func (e *Evictor) processAfterUploadTask(ctx context.Context, task *model.Task) {
	e.guardEviction(ctx, task)
}

func (e *Evictor) guardEviction(ctx context.Context, task *model.Task) {
	e.cacheAccess.GuardDeletion(task.RefVersionID, func(lastAccess time.Time) bool {
		return e.finalizeEviction(ctx, task, lastAccess)
	})
}

func (e *Evictor) finalizeEviction(ctx context.Context, task *model.Task, lastAccess time.Time) bool {
	stage := taskStage(task)
	version, err := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID)
	if err != nil {
		e.retryTask(ctx, task, err, "loading object version for cache eviction")
		return false
	}
	if version == nil {
		e.rejectEviction(ctx, task, "object not found", "object version not found for cache eviction")
		return false
	}

	if !e.validateEvictionState(ctx, task, version) {
		return false
	}

	if stage == repository.CacheEvictionStageLRU {
		if !lruAccessSnapshotMatches(task, effectiveLRUAccessTime(version)) ||
			lruAccessOccurredAfterSnapshot(task, lastAccess) {
			e.persistNewerInMemoryAccess(ctx, task, version.CacheAccessedAt, lastAccess)
			e.cancelTask(ctx, task, "Object was accessed after this LRU eviction was planned")
			return false
		}
	}

	if !e.ensureRemoteReadable(ctx, task, version) {
		return false
	}

	bucket, err := e.repos.Buckets.GetByID(ctx, version.BucketID)
	if err != nil {
		e.retryTask(ctx, task, err, "loading object bucket for cache eviction")
		return false
	}
	if bucket == nil {
		e.rejectEviction(ctx, task, "bucket not found", "bucket not found for cache eviction")
		return false
	}

	if !e.deleteCacheEntry(ctx, task, bucket.Name, version) {
		return false
	}
	e.recordDeletedCache(ctx, task, version)
	return true
}

func (e *Evictor) validateEvictionState(
	ctx context.Context,
	task *model.Task,
	version *model.ObjectVersion,
) bool {
	switch taskStage(task) {
	case repository.CacheEvictionStageLRU:
		if !version.InCache {
			e.cancelTask(ctx, task, "Object is no longer present in the local cache")
			return false
		}
		if version.State != model.ObjectStateStored &&
			version.State != model.ObjectStateCacheEvicted {
			e.cancelTask(ctx, task, "Object is no longer eligible for LRU eviction")
			return false
		}
		return true
	case repository.CacheEvictionStageAfterUpload:
		if version.State == model.ObjectStateReplicating {
			e.deferReplicatingEviction(ctx, task)
			return false
		}
		if version.State != model.ObjectStateStored {
			e.failTask(ctx, task, "not stored", "object version not in stored state")
			return false
		}
		return true
	default:
		e.cancelTask(ctx, task, "Cache eviction task uses an unsupported stage")
		return false
	}
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

func (e *Evictor) ensureRemoteReadable(ctx context.Context, task *model.Task, version *model.ObjectVersion) bool {
	if version.StorageUploadID == nil {
		e.rejectEviction(ctx, task, "no accepted upload", "object version has no accepted upload, refusing to evict")
		return false
	}
	copies, err := e.repos.Uploads.ListReadableCommittedCopies(ctx, *version.StorageUploadID)
	if err != nil {
		e.retryTask(ctx, task, err, "checking readable remote copies before cache eviction")
		return false
	}
	if len(copies) == 0 {
		e.rejectEviction(ctx, task, "no readable upload copies", "object version has no readable upload copies, refusing to evict")
		return false
	}
	return true
}

func (e *Evictor) deleteCacheEntry(
	ctx context.Context,
	task *model.Task,
	bucketName string,
	version *model.ObjectVersion,
) bool {
	if taskStage(task) == repository.CacheEvictionStageLRU {
		e.lruFinalizeMu.Lock()
		defer e.lruFinalizeMu.Unlock()
		if e.cache.UsedBytes() <= watermarkBytes(e.maxCacheBytes, e.lowWatermarkPercent) {
			e.cancelTask(ctx, task, "LRU cache usage already reached the low watermark")
			return false
		}
	}

	cachePresent := e.cache.Exists(ctx, bucketName, version.CacheKey)
	if err := e.cache.Delete(ctx, bucketName, version.CacheKey); err != nil {
		e.retryTask(ctx, task, err, "deleting cache entry")
		return false
	}
	if !cachePresent {
		e.taskLogger(task).Info("cache entry already absent; reconciling eviction state")
	}
	return true
}

func (e *Evictor) rejectEviction(
	ctx context.Context,
	task *model.Task,
	reason string,
	logMessage string,
) {
	if taskStage(task) == repository.CacheEvictionStageLRU {
		e.cancelTask(ctx, task, reason)
		return
	}
	e.failTask(ctx, task, reason, logMessage)
}

func (e *Evictor) recordDeletedCache(ctx context.Context, task *model.Task, version *model.ObjectVersion) {
	logger := e.taskLogger(task)
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
			logger.Error("state transition stored→cache_evicted failed", "error", err)
			if latest, latestErr := e.repos.Objects.GetVersionByID(ctx, task.RefVersionID); latestErr == nil &&
				latest != nil &&
				latest.State == model.ObjectStateCacheEvicted &&
				!latest.InCache {
				e.completeTask(ctx, task, logger, "cache eviction already recorded")
				return
			}
			scheduleTaskRetry(ctx, e.repos, task, "evictor", logger, err)
			admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
			return
		}
	case model.ObjectStateCacheEvicted:
		if err := e.repos.Objects.SetVersionCachePresence(ctx, task.RefVersionID, false); err != nil {
			logger.Error("recording repeated cache eviction failed", "error", err)
			scheduleTaskRetry(ctx, e.repos, task, "evictor", logger, err)
			admin.WorkerTasksProcessed.WithLabelValues("evictor", "failure").Inc()
			return
		}
	default:
		e.cancelTask(ctx, task, "Object is no longer eligible for cache eviction")
		return
	}

	e.completeTask(ctx, task, logger, "cache eviction completed")
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
