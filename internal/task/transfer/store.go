package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/piece"
	"github.com/strahe/synapse-go/storage"
)

func (h *StoreHandler) storeHandler() *taskengine.FuncHandler {
	definition := copyDefinition(model.TaskTypeStorageStore)
	definition.AllowRetry = true
	definition.Policy.ObservationWindow = storeAttentionAfter
	definition.CanManualRetry = func(task *model.Task) bool {
		if task == nil || task.FailureReason == nil {
			return false
		}
		if taskengine.RecoverableEngineFailure(*task.FailureReason) {
			return true
		}
		switch *task.FailureReason {
		case "store_not_started":
			return len(task.Checkpoint) == 0
		case "store_retry_limit", "store_cache_missing", "store_checkpoint_write_failed", "copy_authorization_failed":
			return true
		case "store_outcome_unknown", "store_processing_timeout", "store_check_failed", "store_result_invalid", "store_cache_read_failed", "store_identity_mismatch", "commit_presign_failed", "copy_owner_missing", "copy_context_failed":
			return len(task.Checkpoint) > 0
		default:
			return false
		}
	}
	return taskengine.NewFuncHandler(definition, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.runStore(ctx, execution, true)
	}, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.runStore(ctx, execution, false)
	},
	)
}

func (h *StoreHandler) runStore(ctx context.Context, execution taskengine.Execution, mayStore bool) taskengine.Result {
	input, copyRow, handled, result := h.authorizeCopyTask(ctx, execution)
	if handled {
		return result
	}
	if copyRow.Status == model.StorageCopyStatusCommitted {
		return h.completeCopyTask(input, execution.ID(), "Storage copy is complete")
	}
	if copyRow.Status == model.StorageCopyStatusPieceReady || copyRow.Status == model.StorageCopyStatusCommitting {
		return h.handOffToCommit(input, execution.ID())
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[storeCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	_, target, content, bucket, err := copyContext(ctx, h.CopyCoordinator.deps.Repositories, h.deps.Resolver, copyRow)
	if err != nil {
		if !hasCheckpoint && copyRow.IngressStoreAttempt > 0 {
			if synapse.IsProviderUnavailable(err) || errors.Is(err, storage.ErrDataSetUnavailable) {
				return h.copyContextFailure(execution, input, copyRow, err, false)
			}
			return h.retryStoreNotStarted(execution, err)
		}
		return h.copyContextFailure(execution, input, copyRow, err, !hasCheckpoint)
	}
	if hasCheckpoint && !mayStore {
		return h.recoverStore(ctx, execution, input, copyRow, target, checkpoint)
	}
	if !mayStore {
		return taskengine.Wait(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage transfer is ready", nil)
	}
	if hasCheckpoint {
		if err := validateStoreCheckpoint(checkpoint, copyRow.ContentSize); err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		pieceCID, err := cid.Parse(checkpoint.IntendedPieceCID)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		state, checkErr := h.storePieceState(ctx, target, checkpoint, pieceCID)
		if checkErr != nil || state == synapse.ParkedPieceProcessing {
			return h.waitForStoreStatus(ctx, execution, checkpoint, state, checkErr)
		}
		if state == synapse.ParkedPieceReady {
			return h.finishPieceTransfer(execution, input, copyRow, target.PieceURL(pieceCID), pieceCID, "")
		}
	}
	if !hasCheckpoint && copyRow.IngressStoreAttempt > 0 {
		previous, err := h.CopyCoordinator.deps.Repositories.Tasks.PreviousStoreCheckpoints(ctx, copyRow.ID, execution.ID())
		if err != nil {
			return h.retryStoreNotStarted(execution, err)
		}
		for _, old := range previous {
			var oldInput storagepipeline.CopyGenerationInput
			var candidate storeCheckpoint
			if err := json.Unmarshal(old.Checkpoint, &candidate); err != nil || candidate.IngressAttempt != copyRow.IngressStoreAttempt {
				continue
			}
			if err := json.Unmarshal(old.Input, &oldInput); err != nil || oldInput.CopyID != copyRow.ID || oldInput.Generation >= input.Generation {
				return taskengine.Fail(errors.New("previous Store task identity conflicts with this copy"), "store_checkpoint_conflict", nil)
			}
			if err := validateStoreCheckpoint(candidate, copyRow.ContentSize); err != nil {
				return taskengine.Fail(err, "invalid_checkpoint", nil)
			}
			if hasCheckpoint && checkpoint != candidate {
				return taskengine.Fail(errors.New("previous Store checkpoints disagree"), "store_checkpoint_conflict", nil)
			}
			checkpoint, hasCheckpoint = candidate, true
		}
		if hasCheckpoint {
			if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
				return h.retryStoreNotStarted(execution, err)
			}
			return taskengine.Wait(model.TaskResumeModeRecover, 0, "provider_confirmation", "Checking storage transfer", nil)
		}
	}
	if !hasCheckpoint && !copyRow.CommitSealed() {
		backlogged, err := commitBacklogFull(ctx, h.CopyCoordinator.deps.Repositories, h.deps.CommitMaxBacklog, copyRow)
		if err != nil {
			return h.retryStoreNotStarted(execution, err)
		}
		if backlogged {
			return waitForCommitBacklog()
		}
	}
	if h.deps.Cache == nil || h.deps.CacheGate == nil {
		if hasCheckpoint {
			return taskengine.Fail(errors.New("cache reader is unavailable"), "store_cache_read_failed", nil)
		}
		return h.retryStoreNotStarted(execution, errors.New("cache reader is unavailable"))
	}
	cacheKey := model.ContentCacheKey(content.ID)
	releaseCache := h.deps.CacheGate.HoldRead(cacheKey)
	defer releaseCache()
	// One provider slot spans identity calculation and the transfer, so a task
	// that has to wait for a slot never hashes the bytes first.
	var outcome taskengine.Result
	err = execution.WithResource(ctx, taskengine.ResourceProviderMutation, func(ctx context.Context) error {
		outcome = h.storeWithProviderSlot(ctx, execution, input, copyRow, target, content, bucket, cacheKey, checkpoint, hasCheckpoint)
		return nil
	})
	if errors.Is(err, taskengine.ErrResourceBusy) {
		return taskengine.ResourceWait("Waiting for other storage operations to finish")
	}
	if err != nil {
		if hasCheckpoint {
			return taskengine.Fail(err, "store_checkpoint_write_failed", nil)
		}
		return h.retryStoreNotStarted(execution, err)
	}
	return outcome
}

func (h *StoreHandler) storeWithProviderSlot(
	ctx context.Context,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	target synapse.DataSetTarget,
	content *model.StorageContent,
	bucket *model.Bucket,
	cacheKey string,
	previous storeCheckpoint,
	hasCheckpoint bool,
) taskengine.Result {
	if hasCheckpoint {
		pieceCID, err := cid.Parse(previous.IntendedPieceCID)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		state, checkErr := h.storePieceState(ctx, target, previous, pieceCID)
		if checkErr != nil || state == synapse.ParkedPieceProcessing {
			return h.waitForStoreStatus(ctx, execution, previous, state, checkErr)
		}
		if state == synapse.ParkedPieceReady {
			return h.finishPieceTransfer(execution, input, copyRow, target.PieceURL(pieceCID), pieceCID, "")
		}
	}
	calculateReader, _, err := h.deps.Cache.Get(ctx, bucket.Name, cacheKey)
	if err != nil {
		if os.IsNotExist(err) {
			if result, stop := h.storeMissingCache(ctx, execution, input, copyRow, hasCheckpoint); stop {
				return result
			}
			return taskengine.Wait(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for retained cache data", nil)
		}
		return h.storeCacheFailure(execution, hasCheckpoint, err)
	}
	pieceInfo, calculateErr := piece.Calculate(calculateReader)
	closeErr := calculateReader.Close()
	if calculateErr != nil {
		return h.storeCacheFailure(execution, hasCheckpoint, calculateErr)
	}
	if closeErr != nil {
		return h.storeCacheFailure(execution, hasCheckpoint, closeErr)
	}
	if !pieceInfo.CIDv2.Defined() || content.ContentSize < 0 || pieceInfo.RawSize != uint64(content.ContentSize) {
		err := fmt.Errorf("calculated storage identity has size %d, expected %d", pieceInfo.RawSize, content.ContentSize)
		if hasCheckpoint {
			return taskengine.Fail(err, "store_identity_mismatch", nil)
		}
		return h.failCopyTask(execution, input, copyRow, err, "store_identity_mismatch")
	}
	if hasCheckpoint && pieceInfo.CIDv2.String() != previous.IntendedPieceCID {
		return taskengine.Fail(errors.New("cached bytes differ from the checkpointed piece"), "store_identity_mismatch", nil)
	}
	if !hasCheckpoint && copyRow.IngressStoreAttempt > 0 {
		if h.deps.ParkedPieces == nil {
			return h.retryStoreNotStarted(execution, errors.New("storage provider status checker is unavailable"))
		}
		state, checkErr := h.deps.ParkedPieces.FindParkedPiece(ctx, target.ServiceURL(), pieceInfo.CIDv2)
		if checkErr != nil || state == synapse.ParkedPieceProcessing {
			if checkErr == nil {
				checkErr = errors.New("storage provider is still processing the piece")
			}
			return h.retryStoreNotStarted(execution, checkErr)
		}
		if state == synapse.ParkedPieceReady {
			checkpoint := storeCheckpoint{
				AttemptedAt: time.Now().UTC(), IntendedPieceCID: pieceInfo.CIDv2.String(),
				ProviderServiceURL: target.ServiceURL(), IngressAttempt: copyRow.IngressStoreAttempt,
			}
			if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
				return taskengine.Fail(err, "store_checkpoint_write_failed", nil)
			}
			return h.finishPieceTransfer(execution, input, copyRow, target.PieceURL(pieceInfo.CIDv2), pieceInfo.CIDv2, "")
		}
		if state != synapse.ParkedPieceMissing {
			return h.retryStoreNotStarted(execution, fmt.Errorf("unexpected storage provider state %q", state))
		}
	}
	storeReader, _, err := h.deps.Cache.Get(ctx, bucket.Name, cacheKey)
	if err != nil {
		if os.IsNotExist(err) {
			if result, stop := h.storeMissingCache(ctx, execution, input, copyRow, hasCheckpoint); stop {
				return result
			}
			return taskengine.Wait(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for retained cache data", nil)
		}
		return h.storeCacheFailure(execution, hasCheckpoint, err)
	}
	defer func() { _ = storeReader.Close() }()
	checkpoint := storeCheckpoint{
		AttemptedAt: time.Now().UTC(), IntendedPieceCID: pieceInfo.CIDv2.String(),
		ProviderServiceURL: target.ServiceURL(),
	}
	var checkpointSettlement taskengine.Settlement
	if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress || copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
		checkpoint.IngressAttempt = copyRow.IngressStoreAttempt + 1
		checkpointSettlement = func(ctx context.Context, repos *repository.Repositories) error {
			_, err := repos.Contents.BeginIngressStoreProgress(ctx, repository.BeginIngressStoreProgressInput{
				CopyID: copyRow.ID, Generation: input.Generation, TaskID: execution.ID(), Attempt: checkpoint.IngressAttempt,
			})
			return err
		}
	}
	var stored *storage.StoreResult
	var progress *uploadProgressReporter
	attempted, err := execution.WithCheckpointedEffect(ctx, taskengine.ResourceProviderMutation, "store:"+checkpoint.IntendedPieceCID, checkpoint, checkpointSettlement, func(ctx context.Context) error {
		if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress || copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
			progress = h.newIngressProgressReporter(ctx, execution.ID(), input.Generation, copyRow.ID, checkpoint.IngressAttempt, content, bucket)
		}
		options := &storage.StoreOptions{PieceCID: pieceInfo.CIDv2}
		if progress != nil {
			options.OnProgress = progress.OnProgress
		}
		var storeErr error
		stored, storeErr = target.Store(ctx, storeReader, options)
		return storeErr
	})
	defer progress.Close()
	if err != nil {
		if !attempted {
			if errors.Is(err, taskengine.ErrEffectAlreadyAdmitted) {
				return taskengine.RetryInMode(err, "store_resend_required", model.TaskResumeModeRecover, 0, nil)
			}
			if hasCheckpoint || copyRow.IngressStoreAttempt > 0 {
				return taskengine.Fail(err, "store_checkpoint_write_failed", nil)
			}
			return h.retryStoreNotStarted(execution, err)
		}
		if ctx.Err() == nil {
			h.deps.Logger.Warn("storage transfer request failed",
				"task_id", execution.ID(), "copy_id", copyRow.ID, "content_id", copyRow.ContentID,
				"provider_id", copyRow.ProviderID, "storage_data_set_id", copyRow.StorageDataSetID, "error", synapse.ErrorSummary(err))
		}
		return taskengine.RetryInMode(synapse.SummarizedError(err), "provider_confirmation", model.TaskResumeModeRecover, storagePollInterval, nil)
	}
	if stored == nil || !stored.PieceCID.Equals(pieceInfo.CIDv2) || stored.Size != content.ContentSize {
		err := errors.New("storage provider returned a mismatched piece identity or size")
		return taskengine.Fail(err, "store_result_invalid", nil)
	}
	progress.Flush(content.ContentSize, true)
	return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Waiting for storage transfer", nil)
}

func (h *StoreHandler) recoverStore(
	ctx context.Context,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	target synapse.DataSetTarget,
	checkpoint storeCheckpoint,
) (result taskengine.Result) {
	if err := validateStoreCheckpoint(checkpoint, copyRow.ContentSize); err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	pieceCID, err := cid.Parse(checkpoint.IntendedPieceCID)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	startedAt := time.Now().UTC()
	defer func() { result = result.WithWorkStartedAt(startedAt) }()
	state, findErr := h.storePieceState(ctx, target, checkpoint, pieceCID)
	if findErr == nil && state == synapse.ParkedPieceReady {
		return h.finishPieceTransfer(execution, input, copyRow, target.PieceURL(pieceCID), pieceCID, "")
	}
	if findErr == nil && state == synapse.ParkedPieceMissing {
		return taskengine.Wait(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage transfer is ready to resend", nil)
	}
	return h.waitForStoreStatus(ctx, execution, checkpoint, state, findErr)
}

func (h *StoreHandler) storePieceState(ctx context.Context, target synapse.DataSetTarget, checkpoint storeCheckpoint, pieceCID cid.Cid) (synapse.ParkedPieceState, error) {
	if h.deps.ParkedPieces == nil {
		return "", errors.New("storage provider status checker is unavailable")
	}
	oldState, oldErr := h.deps.ParkedPieces.FindParkedPiece(ctx, checkpoint.ProviderServiceURL, pieceCID)
	if checkpoint.ProviderServiceURL == target.ServiceURL() {
		if oldErr == nil && oldState != synapse.ParkedPieceMissing && oldState != synapse.ParkedPieceProcessing && oldState != synapse.ParkedPieceReady {
			return "", fmt.Errorf("unexpected storage provider state %q", oldState)
		}
		return oldState, oldErr
	}
	currentState, currentErr := h.deps.ParkedPieces.FindParkedPiece(ctx, target.ServiceURL(), pieceCID)
	if currentErr == nil && currentState != synapse.ParkedPieceMissing && currentState != synapse.ParkedPieceProcessing && currentState != synapse.ParkedPieceReady {
		return "", fmt.Errorf("unexpected storage provider state %q", currentState)
	}
	if currentErr == nil && currentState == synapse.ParkedPieceReady {
		return currentState, nil
	}
	if currentErr != nil {
		return "", currentErr
	}
	if oldErr != nil {
		return "", oldErr
	}
	if oldState != synapse.ParkedPieceMissing && oldState != synapse.ParkedPieceProcessing && oldState != synapse.ParkedPieceReady {
		return "", fmt.Errorf("unexpected storage provider state %q", oldState)
	}
	if currentState == synapse.ParkedPieceMissing && (oldState == synapse.ParkedPieceReady || oldState == synapse.ParkedPieceMissing) {
		return synapse.ParkedPieceMissing, nil
	}
	return synapse.ParkedPieceProcessing, nil
}

func (h *StoreHandler) waitForStoreStatus(ctx context.Context, execution taskengine.Execution, checkpoint storeCheckpoint, state synapse.ParkedPieceState, checkErr error) taskengine.Result {
	if checkErr != nil {
		return retryTask(checkErr, "store_check_failed")
	}
	startedAt, err := execution.ObserveOperation(ctx, fmt.Sprintf("store:%s", checkpoint.IntendedPieceCID))
	if err != nil {
		return retryTask(err, "store_observation_failed")
	}
	window := execution.Policy().ObservationWindow
	if window > 0 && time.Since(startedAt) >= window {
		return taskengine.Fail(fmt.Errorf("storage provider still reports %s", state), "store_processing_timeout", nil)
	}
	return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
}

func (h *StoreHandler) retryStoreNotStarted(_ taskengine.Execution, err error) taskengine.Result {
	return retryTask(err, "store_not_started")
}

func (h *StoreHandler) storeCacheFailure(execution taskengine.Execution, hasCheckpoint bool, err error) taskengine.Result {
	if hasCheckpoint {
		return taskengine.Fail(err, "store_cache_read_failed", nil)
	}
	return h.retryStoreNotStarted(execution, err)
}

func (h *StoreHandler) storeMissingCache(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput, copyRow *model.StorageCopy, hasCheckpoint bool) (taskengine.Result, bool) {
	err := errors.New("storage transfer needs local bytes that are no longer cached")
	if hasCheckpoint || copyRow.IngressStoreAttempt > 0 {
		return taskengine.Fail(err, "store_cache_missing", nil), true
	}
	if copyRow.TransferMethod != model.StorageCopyTransferMethodCacheRestore {
		return taskengine.Result{}, false
	}
	migration, loadErr := h.CopyCoordinator.deps.Repositories.Contents.IsPendingReplacementCopy(ctx, copyRow.ID)
	if loadErr != nil {
		return h.retryCopyTask(execution, input, copyRow, loadErr, "replacement_load_failed"), true
	}
	if migration {
		return taskengine.Fail(err, "store_cache_missing", nil), true
	}
	return h.failCopyTask(execution, input, copyRow, err, "store_cache_missing"), true
}
