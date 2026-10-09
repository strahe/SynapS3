package transfer

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func (h *PlanHandler) transferPlanHandler() *taskengine.FuncHandler {
	definition := h.copyDefinition(model.TaskTypeStorageTransferPlan)
	definition.WorkStart = taskengine.WorkStartOnHandler
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
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
		binding, err := h.CopyCoordinator.deps.Repositories.Contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "dataset_load_failed")
		}
		if binding == nil {
			return h.failCopyTask(execution, input, copyRow, repository.ErrNotFound, "dataset_missing")
		}
		if binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || binding.DataSetID.IsZero() {
			return taskengine.Wait(model.TaskResumeModeExecute, storagePollInterval, "dataset", "Waiting for storage service", nil)
		}
		unreferenced, err := h.CopyCoordinator.deps.Repositories.Objects.ContentIsUnreferenced(ctx, copyRow.ContentID)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "copy_owner_load_failed")
		}
		// A signed member is transferred again whatever names its bytes: its
		// request can only be sent whole.
		if unreferenced && !copyRow.CommitSealed() {
			return h.completeCopyTask(input, execution.ID(), "Storage copy is no longer required")
		}
		//exhaustive:enforce
		switch copyRow.TransferMethod {
		case model.StorageCopyTransferMethodIngress, model.StorageCopyTransferMethodCacheRestore:
			// Stored from the local cache below.
		case model.StorageCopyTransferMethodPeerPull:
			sources, err := h.CopyCoordinator.deps.Repositories.Contents.ListReadableCommittedCopies(ctx, copyRow.ContentID)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "copy_source_load_failed")
			}
			if len(sources) > 0 {
				return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStoragePull, "Storage copy is ready to transfer")
			}
			migration, err := h.CopyCoordinator.deps.Repositories.Contents.IsPendingReplacementCopy(ctx, copyRow.ID)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "replacement_load_failed")
			}
			recovery, err := h.copyRetryAllowsCache(ctx, execution, input)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "copy_recovery_load_failed")
			}
			if !migration && !recovery {
				return taskengine.Wait(model.TaskResumeModeExecute, storageSourcePollInterval, "source", "Waiting for a readable storage source", nil)
			}
			available, err := copyCacheAvailable(ctx, h.CopyCoordinator.deps.Repositories, h.deps.Cache, copyRow)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "copy_cache_load_failed")
			}
			if !available {
				if !migration {
					return taskengine.Wait(model.TaskResumeModeExecute, storageSourcePollInterval, "source", "Waiting for a readable storage source", nil)
				}
				return taskengine.Fail(errors.New("stored content migration has no readable source or local cache"), "migration_cache_missing", nil)
			}
			return h.advanceToCacheRestore(input, execution.ID(), "Storage copy is recovering from cache", "")
		default:
			// A method this version does not know may not be a store, so the
			// copy waits instead of being uploaded.
			return taskengine.Wait(model.TaskResumeModeExecute, storageDependencyWait, "transfer_method",
				"Waiting for a newer version that supports this copy's transfer method", nil)
		}
		available, err := copyCacheAvailable(ctx, h.CopyCoordinator.deps.Repositories, h.deps.Cache, copyRow)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "copy_cache_load_failed")
		}
		if available {
			return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageStore, "Storage copy is ready to transfer")
		}
		if copyRow.TransferMethod == model.StorageCopyTransferMethodCacheRestore {
			migration, err := h.CopyCoordinator.deps.Repositories.Contents.IsPendingReplacementCopy(ctx, copyRow.ID)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "replacement_load_failed")
			}
			if !migration {
				return h.failCopyTask(execution, input, copyRow, errors.New("storage transfer has no local cache"), "copy_cache_missing")
			}
			return taskengine.Fail(errors.New("stored content migration cannot read its local cache"), "migration_cache_missing", nil)
		}
		return taskengine.Wait(model.TaskResumeModeExecute, storageDependencyWait, "source", "Waiting for a readable storage source", nil)
	}
	return taskengine.NewFuncHandler(definition, run, run)
}

func (h *CopyCoordinator) copyRetryAllowsCache(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput) (bool, error) {
	current, err := h.deps.Repositories.Tasks.GetByID(ctx, execution.ID())
	if err != nil {
		return false, err
	}
	if current == nil {
		return false, repository.ErrNotFound
	}
	if current.RetryOfTaskID != nil {
		return true, nil
	}
	if current.Type != model.TaskTypeStoragePull || input.Generation <= 1 {
		return false, nil
	}
	// A Pull phase keeps the authorization of the recovery plan that created it.
	previous, err := h.deps.Repositories.Tasks.GetByIdentity(ctx, model.TaskTypeStorageTransferPlan, storagepipeline.TransferPlanKey(input.CopyID, input.Generation-1))
	if err != nil || previous == nil || previous.RetryOfTaskID == nil {
		return false, err
	}
	var previousInput storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(previous.Input, &previousInput); err != nil {
		return false, err
	}
	if previousInput.CopyID != input.CopyID || previousInput.Generation != input.Generation-1 {
		return false, repository.ErrTaskDataCorrupted
	}
	return true, nil
}
