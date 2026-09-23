package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/cache"
	"github.com/strahe/synaps3/internal/cacheeviction"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectdeletion"
	"github.com/strahe/synaps3/internal/storagecleanup"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/systemtask"
	taskengine "github.com/strahe/synaps3/internal/task"
	"github.com/strahe/synaps3/internal/walletoperation"
	"github.com/strahe/synapse-go/payments"
	"github.com/strahe/synapse-go/storage"
)

const (
	dependencyWait        = time.Minute
	externalPollInterval  = 5 * time.Second
	cleanupPollInterval   = time.Minute
	cleanupAttentionAfter = 24 * time.Hour
	taskGCInterval        = time.Hour
	cleanupGCPageSize     = 500
)

type cleanupCheckpoint struct {
	CopyID        int64     `json:"copy_id"`
	AttemptedAt   time.Time `json:"attempted_at"`
	RetryOfTxHash string    `json:"retry_of_tx_hash,omitempty"`
	// Finalized records that the content's rows were deleted.
	Finalized bool `json:"finalized,omitempty"`
}

type cacheCapacityCheckpoint struct {
	CycleActive bool `json:"cycle_active"`
}

type cacheEvictionCheckpoint struct {
	AttemptedAt time.Time `json:"attempted_at"`
}

func (h *TaskHandlers) cacheCapacityHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeCacheCapacityReconcile, InputVersion: 1,
		Codec:      taskengine.StrictJSONCodec(func(input *systemtask.Input) error { return systemtask.ValidateInput(*input) }),
		RetryLimit: nil, AllowRetry: true,
	}
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		if h.deps.EvictionPolicy != cache.EvictionPolicyLRU {
			return taskengine.Suspend(model.TaskResumeModeExecute, taskGCInterval, "scheduled", "Automatic cache cleanup is disabled", nil)
		}
		if h.deps.Cache == nil || h.deps.CacheTracker == nil || h.taskService == nil || h.deps.MaxCacheBytes <= 0 {
			return taskengine.Fail(errors.New("cache capacity dependencies are unavailable"), "dependency_unavailable", nil)
		}
		checkpoint, _, err := taskengine.DecodeCheckpoint[cacheCapacityCheckpoint](execution)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		if !h.deps.CacheTracker.SafeForLRU() {
			return taskengine.Suspend(model.TaskResumeModeRecover, dependencyWait, "cache_access", "Waiting for reliable cache access records", nil)
		}
		usedBytes := h.deps.Cache.UsedBytes()
		highBytes := cacheWatermarkBytes(h.deps.MaxCacheBytes, h.deps.LRUHighPercent)
		lowBytes := cacheWatermarkBytes(h.deps.MaxCacheBytes, h.deps.LRULowPercent)
		cycleActive := checkpoint.CycleActive
		switch {
		case usedBytes <= lowBytes:
			cycleActive = false
		case usedBytes >= highBytes:
			cycleActive = true
		}
		if cycleActive != checkpoint.CycleActive {
			checkpoint.CycleActive = cycleActive
			if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
				return retryTask(err, "cache_capacity_checkpoint_failed")
			}
		}
		if !cycleActive {
			return taskengine.Suspend(model.TaskResumeModeExecute, externalPollInterval, "scheduled", "Local cache usage is within its target", nil)
		}
		activeBytes, err := h.deps.Repositories.CacheEvictions.ActiveEvictionBytes(ctx)
		if err != nil {
			return retryTask(err, "cache_capacity_scan_failed")
		}
		bytesToPlan := usedBytes - lowBytes - activeBytes
		if bytesToPlan <= 0 {
			return taskengine.Suspend(model.TaskResumeModeExecute, externalPollInterval, "cache_cleanup", "Local cache cleanup is in progress", nil)
		}
		plannedBytes, plannedTasks, err := h.planLRUEvictions(ctx, bytesToPlan)
		if err != nil {
			return retryTask(err, "cache_capacity_plan_failed")
		}
		message := "Waiting for remotely safe cached data"
		if plannedTasks > 0 {
			message = fmt.Sprintf("Scheduled cleanup for %d cached items (%d bytes)", plannedTasks, plannedBytes)
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, externalPollInterval, "cache_cleanup", message, nil)
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func (h *TaskHandlers) planLRUEvictions(ctx context.Context, bytesToPlan int64) (int64, int, error) {
	const candidateBatchSize = 100
	var plannedBytes int64
	var plannedTasks int
	for bytesToPlan > 0 {
		candidates, err := h.deps.Repositories.CacheEvictions.ListLRUCandidates(ctx, candidateBatchSize)
		if err != nil {
			return plannedBytes, plannedTasks, err
		}
		if len(candidates) == 0 {
			break
		}
		createdThisBatch := 0
		for i := range candidates {
			candidate := candidates[i]
			scheduled := false
			err := h.deps.Repositories.WithTx(ctx, func(repos *repository.Repositories) error {
				reservation, err := repos.CacheEvictions.PrepareEviction(ctx, candidate.ContentID)
				if err != nil {
					return err
				}
				if reservation.ActiveTaskID != nil {
					return nil
				}
				generation := reservation.Generation
				accessedAt := cacheeviction.NormalizeAccessTime(candidate.AccessedAt)
				taskRow, _, err := h.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
					Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(candidate.ContentID, generation),
					Input:       cacheeviction.EvictInput{ContentID: candidate.ContentID, Generation: generation, AccessedAt: &accessedAt},
					SubjectType: "storage_content", SubjectKey: strconv.FormatInt(candidate.ContentID, 10),
				})
				if err != nil {
					return err
				}
				if err := repos.CacheEvictions.BindEvictionTask(ctx, candidate.ContentID, generation, taskRow.ID); err != nil {
					return err
				}
				scheduled = true
				return nil
			})
			if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
				continue
			}
			if err != nil {
				return plannedBytes, plannedTasks, fmt.Errorf("planning cache cleanup for content %d: %w", candidate.ContentID, err)
			}
			if !scheduled {
				continue
			}
			plannedBytes += candidate.Size
			bytesToPlan -= candidate.Size
			plannedTasks++
			createdThisBatch++
			if bytesToPlan <= 0 {
				break
			}
		}
		if len(candidates) < candidateBatchSize || createdThisBatch == 0 {
			break
		}
	}
	return plannedBytes, plannedTasks, nil
}

func (h *TaskHandlers) cacheEvictHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeCacheEvict, InputVersion: 1,
		Codec:      taskengine.StrictJSONCodec(cacheeviction.ValidateEvictInput),
		RetryLimit: h.retryLimit(), AllowRetry: true,
	}
	return taskHandler{
		definition: definition,
		execute: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runCacheEviction(ctx, execution, true)
		},
		recover: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runCacheEviction(ctx, execution, false)
		},
	}
}

func (h *TaskHandlers) runCacheEviction(
	ctx context.Context,
	execution taskengine.Execution,
	allowDelete bool,
) taskengine.Result {
	input, err := taskengine.DecodeInput[cacheeviction.EvictInput](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeCacheEvict), err)
	}
	if h.deps.Cache == nil || h.deps.CacheGate == nil || h.deps.CacheTracker == nil {
		return taskengine.Fail(
			errors.New("cache deletion dependencies are unavailable"),
			"dependency_unavailable",
			h.releaseCacheEvictionSettlement(input, execution.ID()),
		)
	}
	if _, _, checkpointErr := taskengine.DecodeCheckpoint[cacheEvictionCheckpoint](execution); checkpointErr != nil {
		return taskengine.Fail(checkpointErr, "invalid_checkpoint", h.releaseCacheEvictionSettlement(input, execution.ID()))
	}
	if allowDelete {
		checkpoint := cacheEvictionCheckpoint{AttemptedAt: time.Now().UTC()}
		if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_checkpoint_failed")
		}
	}
	var result taskengine.Result
	h.deps.CacheGate.GuardDeletion(model.ContentCacheKey(input.ContentID), func() {
		if input.AccessedAt != nil {
			if h.deps.EvictionPolicy != cache.EvictionPolicyLRU || !h.deps.CacheTracker.SafeForLRU() {
				result = h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
				return
			}
			if h.deps.Cache.UsedBytes() <= cacheWatermarkBytes(h.deps.MaxCacheBytes, h.deps.LRULowPercent) {
				result = h.cancelCacheEviction(input, execution.ID(), "Local cache usage reached its target")
				return
			}
		}
		result = h.deleteAuthorizedCacheEntry(ctx, execution, input, allowDelete)
	})
	return result
}

func (h *TaskHandlers) deleteAuthorizedCacheEntry(
	ctx context.Context,
	execution taskengine.Execution,
	input cacheeviction.EvictInput,
	allowDelete bool,
) taskengine.Result {
	deletionSucceeded := false
	if input.AccessedAt != nil && allowDelete {
		if !h.deps.CacheTracker.SafeForLRU() {
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		}
		entry, err := h.deps.Repositories.CacheEvictions.GetCacheEntry(ctx, input.ContentID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_entry_load_failed")
		}
		content, err := h.deps.Repositories.Contents.GetByID(ctx, input.ContentID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_content_load_failed")
		}
		if entry == nil || content == nil || entry.CacheAccessedAt == nil ||
			!cacheeviction.NormalizeAccessTime(*entry.CacheAccessedAt).Equal(*input.AccessedAt) ||
			cacheeviction.NormalizeAccessTime(h.deps.CacheTracker.Latest(input.ContentID)).After(*input.AccessedAt) {
			if entry != nil && entry.CacheAccessedAt != nil &&
				cacheeviction.NormalizeAccessTime(h.deps.CacheTracker.Latest(input.ContentID)).After(cacheeviction.NormalizeAccessTime(*entry.CacheAccessedAt)) {
				if flushErr := h.deps.CacheTracker.FlushWhileGuarded(ctx, input.ContentID); flushErr != nil {
					return h.retryCacheEviction(execution, input, flushErr, "cache_access_flush_failed")
				}
			}
			return h.cancelCacheEviction(input, execution.ID(), "Cached data was used after cleanup was scheduled")
		}
		if !h.reserveLRUDeletion(content.ContentSize) {
			return h.cancelCacheEviction(input, execution.ID(), "Local cache usage reached its target")
		}
		defer func() { h.finishLRUDeletion(content.ContentSize, deletionSucceeded) }()
	}
	if !allowDelete {
		entry, err := h.deps.Repositories.CacheEvictions.GetCacheEntry(ctx, input.ContentID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_entry_load_failed")
		}
		if entry == nil {
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		}
		content, err := h.deps.Repositories.Contents.GetByID(ctx, input.ContentID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_content_load_failed")
		}
		if content == nil {
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		}
		bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, content.BucketID)
		if err != nil {
			return h.retryCacheEviction(execution, input, err, "cache_bucket_load_failed")
		}
		if bucket == nil {
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		}
		body, _, err := h.deps.Cache.Get(ctx, bucket.Name, model.ContentCacheKey(input.ContentID))
		switch {
		case err == nil:
			if body == nil {
				return h.retryCacheEviction(execution, input, errors.New("cache returned an empty read handle"), "cache_observation_failed")
			}
			if closeErr := body.Close(); closeErr != nil {
				return h.retryCacheEviction(execution, input, closeErr, "cache_observation_failed")
			}
			return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Local cache removal is ready", nil)
		case os.IsNotExist(err):
			finalizeErr := h.deps.Repositories.WithTx(ctx, func(repos *repository.Repositories) error {
				if err := repos.Tasks.ValidateClaim(ctx, execution.ID(), execution.ClaimGeneration()); err != nil {
					return err
				}
				return repos.CacheEvictions.RecordDeletion(ctx, input.ContentID, input.Generation, execution.ID())
			})
			if errors.Is(finalizeErr, repository.ErrConflict) || errors.Is(finalizeErr, repository.ErrNotFound) {
				return h.cancelCacheEviction(input, execution.ID(), "Cache removal was superseded")
			}
			if finalizeErr != nil {
				return h.retryCacheEviction(execution, input, finalizeErr, "cache_record_failed")
			}
			h.deps.CacheTracker.Forget(input.ContentID)
			return taskengine.Complete("Local cache removed", nil)
		default:
			return h.retryCacheEviction(execution, input, err, "cache_observation_failed")
		}
	}
	deleteErr := h.deps.Repositories.WithTx(ctx, func(repos *repository.Repositories) error {
		if err := repos.Tasks.ValidateClaim(ctx, execution.ID(), execution.ClaimGeneration()); err != nil {
			return err
		}
		authorized, err := repos.CacheEvictions.AuthorizeDeletion(
			ctx, input.ContentID, input.Generation, execution.ID(), input.AccessedAt,
		)
		if err != nil {
			return err
		}
		if err := h.deps.Cache.Delete(ctx, authorized.BucketName, model.ContentCacheKey(authorized.Content.ID)); err != nil {
			return fmt.Errorf("deleting cache file: %w", err)
		}
		return repos.CacheEvictions.RecordDeletion(ctx, input.ContentID, input.Generation, execution.ID())
	})
	if deleteErr != nil {
		recorded, checkErr := h.deps.Repositories.CacheEvictions.DeletionRecorded(ctx, input.ContentID, input.Generation)
		if checkErr == nil && recorded {
			h.deps.CacheTracker.Forget(input.ContentID)
			if input.AccessedAt != nil {
				deletionSucceeded = true
			}
			return taskengine.Complete("Local cache removed", nil)
		}
		switch {
		case errors.Is(deleteErr, cacheeviction.ErrDurabilityThreshold) && input.AccessedAt == nil:
			return taskengine.Suspend(model.TaskResumeModeExecute, dependencyWait, "durability", "Waiting for durable storage", nil)
		case errors.Is(deleteErr, cacheeviction.ErrDurabilityThreshold), errors.Is(deleteErr, cacheeviction.ErrNoLongerEligible),
			errors.Is(deleteErr, cacheeviction.ErrAccessChanged), errors.Is(deleteErr, repository.ErrNotFound), errors.Is(deleteErr, repository.ErrConflict):
			return h.cancelCacheEviction(input, execution.ID(), "Cache removal is no longer needed")
		default:
			return h.retryCacheEviction(execution, input, errors.Join(deleteErr, checkErr), "cache_delete_failed")
		}
	}
	h.deps.CacheTracker.Forget(input.ContentID)
	if input.AccessedAt != nil {
		deletionSucceeded = true
	}
	return taskengine.Complete("Local cache removed", nil)
}

func (h *TaskHandlers) cancelCacheEviction(
	input cacheeviction.EvictInput,
	taskID int64,
	message string,
) taskengine.Result {
	return taskengine.Cancel(message, h.releaseCacheEvictionSettlement(input, taskID))
}

func (h *TaskHandlers) releaseCacheEvictionSettlement(input cacheeviction.EvictInput, taskID int64) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		err := repos.CacheEvictions.ReleaseEviction(ctx, input.ContentID, input.Generation, taskID)
		if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
			return nil
		}
		return err
	}
}

func (h *TaskHandlers) retryCacheEviction(
	execution taskengine.Execution,
	input cacheeviction.EvictInput,
	err error,
	reason string,
) taskengine.Result {
	if execution.RetryWillFail() {
		return taskengine.Fail(err, reason, h.releaseCacheEvictionSettlement(input, execution.ID()))
	}
	return taskengine.RetryBackoff(err, reason, nil)
}

func (h *TaskHandlers) reserveLRUDeletion(size int64) bool {
	h.lruCapacityMu.Lock()
	defer h.lruCapacityMu.Unlock()
	if h.lruInFlightDeletes == 0 {
		h.lruProjectedBytes = h.deps.Cache.UsedBytes()
	}
	if h.lruProjectedBytes <= cacheWatermarkBytes(h.deps.MaxCacheBytes, h.deps.LRULowPercent) {
		return false
	}
	h.lruProjectedBytes -= size
	h.lruInFlightDeletes++
	return true
}

func (h *TaskHandlers) finishLRUDeletion(size int64, deleted bool) {
	h.lruCapacityMu.Lock()
	defer h.lruCapacityMu.Unlock()
	if !deleted {
		h.lruProjectedBytes += size
	}
	h.lruInFlightDeletes--
	if h.lruInFlightDeletes <= 0 {
		h.lruInFlightDeletes = 0
		h.lruProjectedBytes = 0
	}
}

func cacheWatermarkBytes(maxBytes int64, percent int) int64 {
	if maxBytes <= 0 || percent <= 0 {
		return 0
	}
	if percent >= 100 {
		return maxBytes
	}
	return (maxBytes/100)*int64(percent) + (maxBytes%100)*int64(percent)/100
}

func (h *TaskHandlers) cacheDurabilityHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeCacheReconcileDurability, InputVersion: 1,
		Codec:      taskengine.StrictJSONCodec(cacheeviction.ValidateDurabilityInput),
		RetryLimit: h.retryLimit(), AllowRetry: true,
	}
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, err := taskengine.DecodeInput[cacheeviction.DurabilityInput](execution)
		if err != nil {
			return decodeFailure(string(definition.Type), err)
		}
		candidate, err := h.deps.Repositories.CacheEvictions.NextBucketDurabilityCandidate(ctx, input.BucketID, input.Generation, execution.ID())
		if err != nil {
			if errors.Is(err, repository.ErrConflict) {
				return taskengine.Cancel("A newer storage policy update replaced this operation", nil)
			}
			return retryTask(err, "durability_scan_failed")
		}
		if candidate == nil {
			return taskengine.Complete("Bucket storage policy applied", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.CacheEvictions.CompleteBucketDurability(ctx, input.BucketID, input.Generation, execution.ID())
			})
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, 0, "more_work", "Applying bucket storage policy", func(ctx context.Context, repos *repository.Repositories) error {
			if h.deps.EvictionPolicy != cache.EvictionPolicyAfterUpload {
				return nil
			}
			if h.taskService == nil {
				return errors.New("task service is unavailable")
			}
			reservation, err := repos.CacheEvictions.PrepareEviction(ctx, candidate.ID)
			if err != nil {
				return err
			}
			if reservation.ActiveTaskID != nil {
				return nil
			}
			generation := reservation.Generation
			taskRow, _, err := h.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
				Type: model.TaskTypeCacheEvict, IdempotencyKey: cacheeviction.EvictTaskKey(candidate.ID, generation),
				Input:       cacheeviction.EvictInput{ContentID: candidate.ID, Generation: generation},
				SubjectType: "storage_content", SubjectKey: strconv.FormatInt(candidate.ID, 10),
			})
			if err != nil {
				return err
			}
			return repos.CacheEvictions.BindEvictionTask(ctx, candidate.ID, generation, taskRow.ID)
		})
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func (h *TaskHandlers) storageCleanupHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageCleanup, InputVersion: 1,
		Codec:      taskengine.StrictJSONCodec(func(input *storagecleanup.Input) error { return storagecleanup.ValidateInput(*input) }),
		RetryLimit: h.retryLimit(), AllowRetry: true,
	}
	return taskHandler{
		definition: definition,
		execute: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runStorageCleanup(ctx, execution, true)
		},
		recover: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runStorageCleanup(ctx, execution, false)
		},
	}
}

func (h *TaskHandlers) runStorageCleanup(ctx context.Context, execution taskengine.Execution, allowDelete bool) taskengine.Result {
	input, err := taskengine.DecodeInput[storagecleanup.Input](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeStorageCleanup), err)
	}
	if h.deps.Storage == nil {
		return taskengine.Fail(errors.New("storage cleanup client is unavailable"), "dependency_unavailable", nil)
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[cleanupCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	// Finalizing deleted the content's rows, so there is nothing left to
	// authorize against.
	if checkpoint.Finalized {
		return taskengine.Complete("Stored data removed", nil)
	}
	copies, err := h.deps.Repositories.StorageCleanup.AuthorizeTask(ctx, input.ContentID, input.Generation, execution.ID())
	if err != nil {
		if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
			return taskengine.Cancel("Remote cleanup was superseded", nil)
		}
		return retryTask(err, "cleanup_authorization_failed")
	}
	hasReferences, err := h.deps.Repositories.StorageCleanup.UploadHasObjectReferences(ctx, input.ContentID)
	if err == nil && !hasReferences {
		hasReferences, err = h.deps.Repositories.StorageCleanup.CleanupHasObjectReferences(ctx, input.ContentID)
	}
	if err != nil {
		return retryTask(err, "cleanup_reference_check_failed")
	}
	if hasReferences {
		return taskengine.Suspend(model.TaskResumeModeRecover, dependencyWait, "references", "Waiting for stored data references", nil)
	}
	for i := range copies {
		copyRow := copies[i]
		switch copyRow.Status {
		// A deletion the provider cannot perform stays recorded as unsupported
		// and does not keep the content from being finalized.
		case model.StorageCleanupCopyStatusRemoved, model.StorageCleanupCopyStatusUnsupported:
			continue
		}
		// Zero is a legal on-chain ID; a missing data set is the only
		// identity gap that prevents an exact piece-ID lookup.
		if copyRow.DataSetID == nil {
			message := "Storage provider details are incomplete"
			if err := h.deps.Repositories.StorageCleanup.MarkCopyUnsupported(ctx, copyRow.ID, message); err != nil {
				return retryTask(err, "cleanup_evidence_failed")
			}
			continue
		}
		state, err := h.deps.Storage.DeletionState(ctx, copyRow.DataSetID.SDK(), copyRow.PieceID.SDK())
		if err != nil {
			return taskengine.Suspend(model.TaskResumeModeRecover, cleanupPollInterval, "provider_confirmation", "Checking remote cleanup", nil)
		}
		if !state.Live {
			if err := h.deps.Repositories.StorageCleanup.MarkCopyRemoved(ctx, copyRow.ID); err != nil {
				return retryTask(err, "cleanup_evidence_failed")
			}
			continue
		}
		if state.Queued {
			return taskengine.Suspend(model.TaskResumeModeRecover, cleanupPollInterval, "provider_confirmation", "Waiting for remote cleanup", nil)
		}
		previousHash := ""
		if copyRow.DeleteTxHash != nil {
			previousHash = *copyRow.DeleteTxHash
		}
		retryingFailedCopy := copyRow.Status == model.StorageCleanupCopyStatusFailed
		// Older attempts could checkpoint a retry without changing the failed row.
		// Do not treat that in-flight attempt as a fresh manual retry.
		if retryingFailedCopy && hasCheckpoint && checkpoint.CopyID == copyRow.ID &&
			checkpoint.RetryOfTxHash == previousHash && !checkpoint.AttemptedAt.IsZero() &&
			!checkpoint.AttemptedAt.Before(copyRow.UpdatedAt) {
			return waitForStorageCleanupOutcome(copyRow, checkpoint, hasCheckpoint, "Waiting for remote cleanup")
		}
		if !retryingFailedCopy && previousHash != "" {
			hashBytes, hashErr := hexutil.Decode(previousHash)
			if h.deps.Receipts == nil || hashErr != nil || len(hashBytes) != common.HashLength {
				return waitForStorageCleanupOutcome(copyRow, checkpoint, hasCheckpoint, "Checking remote cleanup")
			}
			requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletReceiptTimeout)
			receipt, receiptErr := h.deps.Receipts.TransactionReceipt(requestCtx, common.BytesToHash(hashBytes))
			cancel()
			if receiptErr != nil || receipt == nil || receipt.BlockNumber == nil || receipt.BlockNumber.Uint64() > state.BlockNumber {
				return waitForStorageCleanupOutcome(copyRow, checkpoint, hasCheckpoint, "Waiting for remote cleanup")
			}
			message := "Removal was not queued. Recover may submit another paid request."
			reason := "cleanup_transaction_not_scheduled"
			if receipt.Status == ethtypes.ReceiptStatusFailed {
				message = "Removal transaction failed. Recover may submit another paid request."
				reason = "cleanup_transaction_reverted"
			}
			return taskengine.Fail(errors.New(message), reason, func(ctx context.Context, repos *repository.Repositories) error {
				return repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, message)
			})
		} else if !retryingFailedCopy && (copyRow.Status != model.StorageCleanupCopyStatusPending || (hasCheckpoint && checkpoint.CopyID == copyRow.ID)) {
			return waitForStorageCleanupOutcome(copyRow, checkpoint, hasCheckpoint, "Waiting for remote cleanup")
		}
		if !allowDelete {
			return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Remote cleanup is ready", nil)
		}
		providerID := copyRow.ProviderID.SDK()
		cleanupContext, err := h.deps.Storage.OpenCleanupContext(ctx, copyRow.DataSetID.SDK(), storage.NewDataSetContextOptions{ProviderID: &providerID})
		if err != nil {
			return retryTask(err, "cleanup_context_failed")
		}
		checkpoint = cleanupCheckpoint{CopyID: copyRow.ID, AttemptedAt: time.Now().UTC(), RetryOfTxHash: previousHash}
		var settlement taskengine.Settlement
		if retryingFailedCopy {
			settlement = func(ctx context.Context, repos *repository.Repositories) error {
				return repos.StorageCleanup.BeginFailedCopyRetry(ctx, copyRow.ID, previousHash)
			}
		}
		var txHash string
		attempted, err := execution.WithCheckpointedEffect(ctx, taskengine.ResourceDestructiveMutation, checkpoint, settlement, func(ctx context.Context) error {
			result, deleteErr := cleanupContext.DeletePieceByID(ctx, copyRow.PieceID.SDK())
			if result != nil && result.Hash != (common.Hash{}) {
				txHash = result.Hash.String()
			}
			return deleteErr
		})
		if txHash != "" {
			err = errors.Join(err, h.deps.Repositories.StorageCleanup.MarkCopyDeleteScheduled(ctx, copyRow.ID, txHash))
		}
		if err != nil {
			if !attempted {
				if errors.Is(err, taskengine.ErrResourceBusy) {
					return taskengine.ResourceWait("Waiting for other removal operations to finish")
				}
				return retryTask(err, "cleanup_not_started")
			}
			return taskengine.Suspend(model.TaskResumeModeRecover, externalPollInterval, "provider_confirmation", "Checking remote cleanup", nil)
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, externalPollInterval, "provider_confirmation", "Waiting for remote cleanup", nil)
	}
	return h.finishStorageCleanup(ctx, execution, input)
}

func waitForStorageCleanupOutcome(copyRow model.StorageCleanupCopy, checkpoint cleanupCheckpoint, hasCheckpoint bool, waitingMessage string) taskengine.Result {
	attemptedAt := time.Time{}
	if hasCheckpoint && checkpoint.CopyID == copyRow.ID {
		attemptedAt = checkpoint.AttemptedAt
	}
	if attemptedAt.IsZero() && copyRow.ScheduledAt != nil {
		attemptedAt = *copyRow.ScheduledAt
	}
	if attemptedAt.IsZero() || time.Since(attemptedAt) >= cleanupAttentionAfter {
		message := "removal unconfirmed after 24 hours. Recover may submit another paid request"
		if attemptedAt.IsZero() {
			message = "removal outcome cannot be confirmed. Recover may submit another paid request"
		}
		return taskengine.Fail(errors.New(message), "cleanup_outcome_unknown", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.StorageCleanup.MarkCopyFailed(ctx, copyRow.ID, message)
		})
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, cleanupPollInterval, "provider_confirmation", waitingMessage, nil)
}

// finishStorageCleanup releases the content's cached bytes and then deletes its
// current-state rows, so writing the same bytes again starts new content. The
// ledgers keep their rows, including any remote deletion left unsupported.
func (h *TaskHandlers) finishStorageCleanup(ctx context.Context, execution taskengine.Execution, input storagecleanup.Input) taskengine.Result {
	if h.deps.Cache == nil || h.deps.CacheGate == nil || h.deps.CacheTracker == nil {
		return taskengine.Fail(errors.New("cache dependencies are unavailable"), "dependency_unavailable", nil)
	}
	content, err := h.deps.Repositories.Contents.GetByID(ctx, input.ContentID)
	if err != nil || content == nil {
		return retryTask(errors.Join(err, repository.ErrNotFound), "cleanup_content_load_failed")
	}
	bucket, err := h.deps.Repositories.Buckets.GetByID(ctx, content.BucketID)
	if err != nil || bucket == nil {
		return retryTask(errors.Join(err, repository.ErrNotFound), "cleanup_bucket_load_failed")
	}
	if _, err := objectdeletion.ReleaseContentCache(
		ctx, h.deps.Cache, h.deps.CacheGate, h.deps.CacheTracker, h.deps.Repositories.Objects, bucket.Name, input.ContentID,
	); err != nil {
		return retryTask(err, "cleanup_cache_release_failed")
	}
	finalized := cleanupCheckpoint{AttemptedAt: time.Now().UTC(), Finalized: true}
	err = execution.WriteCheckpointWith(ctx, finalized, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.StorageCleanup.FinalizeContent(ctx, input.ContentID, input.Generation, execution.ID())
	})
	if errors.Is(err, repository.ErrContentCleanupNotReady) {
		return taskengine.Suspend(model.TaskResumeModeRecover, dependencyWait, "references", "Waiting for other work on the stored data to finish", nil)
	}
	if err != nil {
		return retryTask(err, "cleanup_finalize_failed")
	}
	return taskengine.Complete("Stored data removed", nil)
}

func (h *TaskHandlers) walletHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeWalletOperation, InputVersion: 1,
		Codec:      taskengine.StrictJSONCodec(func(input *walletoperation.Input) error { return walletoperation.ValidateInput(*input) }),
		RetryLimit: h.retryLimit(), AllowRetry: true,
		CanManualRetry: func(task *model.Task) bool {
			return task != nil && task.FailureReason != nil && *task.FailureReason == "wallet_broadcast_not_started"
		},
	}
	return taskHandler{
		definition: definition,
		execute: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.executeWalletOperation(ctx, execution)
		},
		recover: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.recoverWalletOperation(ctx, execution)
		},
	}
}

func (h *TaskHandlers) executeWalletOperation(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[walletoperation.Input](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeWalletOperation), err)
	}
	op, err := h.deps.Repositories.WalletOperations.GetByID(ctx, input.OperationID)
	if err != nil {
		return h.retryWalletOperation(execution, input.OperationID, err, "wallet_load_failed")
	}
	if result, done := h.walletTerminalResult(op, execution.ID()); done {
		return result
	}
	if op.Status == model.WalletOperationStatusSubmitted || op.BroadcastAttemptedAt != nil {
		return h.recoverWalletOperation(ctx, execution)
	}
	if h.deps.Wallet == nil {
		return taskengine.Fail(errors.New("wallet operator is unavailable"), "dependency_unavailable", nil)
	}
	amount, ok := new(big.Int).SetString(op.Amount, 10)
	if !ok || !validTaskWalletAmount(op.Type, amount) {
		return taskengine.Fail(errors.New("invalid wallet operation amount"), "invalid_input", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkFailed(ctx, op.ID, execution.ID(), "invalid wallet operation amount")
		})
	}
	checkpoint := walletoperation.Checkpoint{BroadcastAttempted: true}
	var txHash string
	var alreadyComplete bool
	attempted, err := execution.WithCheckpointedEffect(ctx, taskengine.ResourceWallet, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkBroadcastAttempted(ctx, op.ID, execution.ID())
	}, func(ctx context.Context) error {
		requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletBroadcastTimeout)
		defer cancel()
		var broadcastErr error
		txHash, alreadyComplete, broadcastErr = broadcastWalletOperation(requestCtx, h.deps.Wallet, op.Type, amount)
		return broadcastErr
	})
	if err != nil {
		if !attempted {
			if errors.Is(err, taskengine.ErrResourceBusy) {
				return taskengine.ResourceWait("Waiting for another wallet operation to finish")
			}
			return retryTask(err, "wallet_broadcast_not_started")
		}
		message := fmt.Sprintf("wallet broadcast outcome is unknown: %v", err)
		return taskengine.Fail(err, "wallet_broadcast_unknown", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkUnknown(ctx, op.ID, execution.ID(), message)
		})
	}
	if alreadyComplete {
		return taskengine.Complete("Wallet authorization already satisfied", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkConfirmedWithoutTransaction(ctx, op.ID, execution.ID())
		})
	}
	if txHash == "" {
		err := errors.New("wallet broadcast returned no transaction hash")
		return taskengine.Fail(err, "wallet_broadcast_unknown", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkUnknown(ctx, op.ID, execution.ID(), err.Error())
		})
	}
	checkpoint.TransactionHash = txHash
	if err := execution.WriteCheckpointWith(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkSubmitted(ctx, op.ID, execution.ID(), txHash)
	}); err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Recording wallet transaction", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkSubmitted(ctx, op.ID, execution.ID(), txHash)
		})
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Waiting for wallet transaction", nil)
}

func (h *TaskHandlers) recoverWalletOperation(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[walletoperation.Input](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeWalletOperation), err)
	}
	op, err := h.deps.Repositories.WalletOperations.GetByID(ctx, input.OperationID)
	if err != nil {
		return h.retryWalletOperation(execution, input.OperationID, err, "wallet_load_failed")
	}
	if result, done := h.walletTerminalResult(op, execution.ID()); done {
		return result
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[walletoperation.Checkpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	txHash := ""
	if op.TxHash != nil {
		txHash = *op.TxHash
	}
	if txHash == "" && hasCheckpoint {
		txHash = checkpoint.TransactionHash
	}
	if txHash == "" {
		if !hasCheckpoint && op.BroadcastAttemptedAt == nil {
			return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Wallet operation is ready", nil)
		}
		err := errors.New("wallet transaction identity could not be recovered")
		return taskengine.Fail(err, "wallet_broadcast_unknown", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkUnknown(ctx, op.ID, execution.ID(), err.Error())
		})
	}
	if h.deps.Receipts == nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Waiting for wallet transaction", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkSubmitted(ctx, op.ID, execution.ID(), txHash)
		})
	}
	requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletReceiptTimeout)
	receipt, err := h.deps.Receipts.TransactionReceipt(requestCtx, common.HexToHash(txHash))
	cancel()
	if errors.Is(err, ethereum.NotFound) || receipt == nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Waiting for wallet transaction", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkSubmitted(ctx, op.ID, execution.ID(), txHash)
		})
	}
	if err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Checking wallet transaction", nil)
	}
	if receipt.Status == ethtypes.ReceiptStatusSuccessful {
		return taskengine.Complete("Wallet transaction confirmed", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkConfirmed(ctx, op.ID, execution.ID(), txHash)
		})
	}
	message := fmt.Sprintf("wallet transaction reverted: status=%d transaction=%s", receipt.Status, txHash)
	return taskengine.Fail(errors.New(message), "wallet_transaction_reverted", func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkFailed(ctx, op.ID, execution.ID(), message)
	})
}

func (h *TaskHandlers) retryWalletOperation(execution taskengine.Execution, operationID int64, err error, reason string) taskengine.Result {
	if !execution.RetryWillFail() {
		return retryTask(err, reason)
	}
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkFailed(ctx, operationID, execution.ID(), err.Error())
	})
}

func (h *TaskHandlers) walletTerminalResult(op *model.WalletOperation, taskID int64) (taskengine.Result, bool) {
	if op == nil {
		return taskengine.Fail(repository.ErrNotFound, "wallet_operation_missing", nil), true
	}
	if op.TaskID == nil || *op.TaskID != taskID {
		switch op.Status {
		case model.WalletOperationStatusConfirmed:
			return taskengine.Complete("Wallet operation completed", nil), true
		case model.WalletOperationStatusFailed, model.WalletOperationStatusUnknown:
			return taskengine.Fail(errors.New("wallet operation requires attention"), "wallet_operation_terminal", nil), true
		default:
			return taskengine.Fail(repository.ErrConflict, "wallet_task_superseded", nil), true
		}
	}
	return taskengine.Result{}, false
}

func validTaskWalletAmount(operationType model.WalletOperationType, amount *big.Int) bool {
	if amount == nil {
		return false
	}
	switch operationType {
	case model.WalletOperationTypeApprove:
		return amount.Sign() == 0
	case model.WalletOperationTypeFund, model.WalletOperationTypeWithdraw:
		return amount.Sign() > 0
	default:
		return false
	}
}

func broadcastWalletOperation(ctx context.Context, operator synapse.WalletOperator, operationType model.WalletOperationType, amount *big.Int) (string, bool, error) {
	switch operationType {
	case model.WalletOperationTypeFund:
		hash, err := operator.FundUSDFC(ctx, amount)
		return hash, false, err
	case model.WalletOperationTypeWithdraw:
		hash, err := operator.WithdrawUSDFC(ctx, amount)
		return hash, false, err
	case model.WalletOperationTypeApprove:
		hash, err := operator.ApproveFWSS(ctx)
		if errors.Is(err, payments.ErrNothingToFund) {
			return "", true, nil
		}
		return hash, false, err
	default:
		return "", false, fmt.Errorf("unsupported wallet operation type %q", operationType)
	}
}

func (h *TaskHandlers) observabilityHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeObservabilityRefresh, InputVersion: 1,
		Codec:      taskengine.StrictJSONCodec(func(input *systemtask.Input) error { return systemtask.ValidateInput(*input) }),
		RetryLimit: nil, AllowRetry: true,
	}
	run := func(ctx context.Context, _ taskengine.Execution) taskengine.Result {
		if h.deps.Observability == nil {
			return taskengine.Fail(errors.New("observability service is unavailable"), "dependency_unavailable", nil)
		}
		if err := h.deps.Observability.RefreshAll(ctx); err != nil {
			return retryTask(err, "observability_refresh_failed")
		}
		if err := h.scheduleMissingProviderSpeedTests(ctx); err != nil {
			return retryTask(err, "provider_speed_schedule_failed")
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, h.deps.Observability.RefreshInterval(), "scheduled", "Storage health refreshed", nil)
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func (h *TaskHandlers) gcHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeGC, InputVersion: 1,
		Codec:      taskengine.StrictJSONCodec(func(input *systemtask.Input) error { return systemtask.ValidateInput(*input) }),
		RetryLimit: nil, AllowRetry: true,
	}
	run := func(ctx context.Context, _ taskengine.Execution) taskengine.Result {
		deleted, err := h.deps.Repositories.Tasks.DeleteRetained(ctx, time.Now(), cleanupGCPageSize)
		if err != nil {
			return retryTask(err, "task_gc_failed")
		}
		delay := taskGCInterval
		if deleted == cleanupGCPageSize {
			delay = 0
		}
		return taskengine.Suspend(model.TaskResumeModeExecute, delay, "scheduled", fmt.Sprintf("Removed %d expired task records", deleted), nil)
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}
