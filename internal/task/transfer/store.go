package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
)

func (h *StoreHandler) storeHandler() *taskengine.FuncHandler {
	definition := h.copyDefinition(model.TaskTypeStorageStore)
	definition.MaxConcurrency = h.deps.UploadConcurrency
	definition.AllowRetry = true
	definition.Policy = taskengine.ExecutionPolicy{
		MaxAttempts:       10,
		Backoff:           taskengine.BackoffPolicy{InitialDelay: 30 * time.Second, Multiplier: 2, MaximumDelay: 5 * time.Minute, Jitter: 0.2},
		InvocationTimeout: 3 * time.Hour, ObservationWindow: storeAttentionAfter,
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
		return h.completeCopyTask(input, execution.ID(), "Replica is complete")
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
		return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Storage transfer is ready", nil)
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
		return h.executeStoreSession(ctx, execution, input, copyRow, target, content, bucket, checkpoint, true, nil)
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
			return taskengine.Wait(model.TaskResumeModeRecover, "provider_confirmation", "Checking storage transfer", nil)
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
	err = ctx.Err()
	if err == nil {
		err = h.CopyCoordinator.deps.Repositories.Tasks.ValidateClaim(ctx, execution.ID(), execution.ClaimGeneration())
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		if hasCheckpoint {
			return retryTask(err, "store_checkpoint_write_failed")
		}
		return h.retryStoreNotStarted(execution, err)
	}
	return h.storePiece(ctx, execution, input, copyRow, target, content, bucket)
}

func (h *StoreHandler) storePiece(
	ctx context.Context,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	target synapse.DataSetTarget,
	content *model.StorageContent,
	bucket *model.Bucket,
) taskengine.Result {
	body, pieceCID, err := h.openStoreBody(ctx, execution, copyRow, content, bucket, "")
	if err != nil {
		return h.storeBodyFailure(ctx, execution, input, copyRow, false, err)
	}
	defer func() { _ = body.Close() }()
	checkpoint := storeCheckpoint{
		AttemptedAt: time.Now().UTC(), IntendedPieceCID: pieceCID.String(),
		ProviderServiceURL: target.ServiceURL(), Stage: storeStageCreate,
	}
	if copyRow.IngressStoreAttempt > 0 {
		state, err := h.storePieceState(ctx, target, checkpoint, pieceCID)
		if err != nil {
			return h.retryStoreNotStarted(execution, err)
		}
		if state == synapse.ParkedPieceReady {
			checkpoint.IngressAttempt = copyRow.IngressStoreAttempt
			if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
				return retryTask(err, "store_checkpoint_write_failed")
			}
			return h.finishPieceTransfer(execution, input, copyRow, target.PieceURL(pieceCID), pieceCID, "")
		}
		if state != synapse.ParkedPieceMissing {
			return h.retryStoreNotStarted(execution, errors.New("storage provider is still processing the piece"))
		}
	}
	return h.executeStoreSession(ctx, execution, input, copyRow, target, content, bucket, checkpoint, false, body)
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
		if checkpoint.Stage != storeStageExpired && !checkpoint.ObservationStartedAt.IsZero() && time.Since(checkpoint.ObservationStartedAt) >= execution.Policy().ObservationWindow {
			return taskengine.Fail(errors.New("upload session recovery remains unconfirmed"), "store_processing_timeout", nil)
		}
		return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Storage transfer is ready to resume", nil)
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
	if checkpoint.Stage == storeStageExpired && checkErr != nil {
		return retryStoreCheckpoint(execution, checkpoint, checkErr, "store_check_failed")
	}
	if checkpoint.ObservationStartedAt.IsZero() {
		checkpoint.ObservationStartedAt = time.Now().UTC()
		if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
			return retryStoreCheckpoint(execution, checkpoint, err, "store_observation_failed")
		}
	}
	if checkErr != nil {
		return retryStoreCheckpoint(execution, checkpoint, checkErr, "store_check_failed")
	}
	window := execution.Policy().ObservationWindow
	if window > 0 && time.Since(checkpoint.ObservationStartedAt) >= window {
		return taskengine.Fail(fmt.Errorf("storage provider still reports %s", state), "store_processing_timeout", nil)
	}
	return taskengine.WaitForObservation(model.TaskResumeModeRecover, "provider_confirmation", "Checking storage transfer", nil)
}

func (h *StoreHandler) retryStoreNotStarted(_ taskengine.Execution, err error) taskengine.Result {
	return retryTask(err, "store_not_started")
}

func (h *StoreHandler) storeCacheFailure(execution taskengine.Execution, hasCheckpoint bool, err error) taskengine.Result {
	if hasCheckpoint {
		return retryTask(err, "store_cache_read_failed")
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
