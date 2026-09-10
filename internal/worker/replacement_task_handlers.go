package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	taskengine "github.com/strahe/synaps3/internal/task"
)

const replacementSeedBatchSize = 100

type retirementCheckpoint struct {
	AttemptedAt      time.Time `json:"attempted_at"`
	TerminationEpoch *int64    `json:"termination_epoch,omitempty"`
	TransactionHash  string    `json:"transaction_hash,omitempty"`
}

func (h *TaskHandlers) replacementCoordinateHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeProviderReplacementCoordinate, InputVersion: 1,
		Codec: taskengine.StrictJSONCodec(func(input *storagereplacement.CoordinateInput) error {
			return storagereplacement.ValidateCoordinateInput(*input)
		}),
		RetryLimit: h.retryLimit(), AllowRetry: false,
	}
	run := func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		input, err := taskengine.DecodeInput[storagereplacement.CoordinateInput](execution)
		if err != nil {
			return decodeFailure(string(definition.Type), err)
		}
		row, err := h.deps.Repositories.Replacements.AuthorizeTask(ctx, input.ReplacementID, input.Generation, execution.ID())
		if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
			return taskengine.Cancel("Provider replacement was superseded", nil)
		}
		if err != nil {
			return h.retryReplacement(execution, input.ReplacementID, err, "replacement_authorization_failed")
		}
		if row.Status == storagereplacement.StatusSuperseded {
			return h.scheduleReplacementRetirement(input, execution.ID(), row, row.TargetDataSetID, "Unused storage service cleanup scheduled")
		}
		if row.Status == storagereplacement.StatusCompleted {
			return taskengine.Complete("Provider replacement completed", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.CompleteTask(ctx, row.ID, input.Generation, execution.ID())
			})
		}
		if row.Status == storagereplacement.StatusFailed || row.Status == storagereplacement.StatusCleanupAttention {
			return taskengine.Fail(errors.New("provider replacement requires operator action"), "replacement_attention", nil)
		}
		if row.Status == storagereplacement.StatusWaiting && row.WaitReason != nil && !row.WaitReason.Valid() {
			return taskengine.Fail(fmt.Errorf("provider replacement has unknown wait reason %q", *row.WaitReason), "replacement_unknown_wait_reason", nil)
		}

		target, err := h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
		if err != nil || target == nil {
			if err == nil {
				err = repository.ErrNotFound
			}
			return h.failReplacement(row.ID, err, "replacement_target_missing")
		}
		if target.Status != model.StorageDataSetStatusReady || target.DataSetID == nil || target.DataSetID.IsZero() {
			if target.Status == model.StorageDataSetStatusFailed {
				return h.failReplacement(row.ID, errors.New("replacement target storage service failed"), "replacement_target_failed")
			}
			if target.EnsureTaskID == nil {
				return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "target", "Preparing replacement storage service", func(ctx context.Context, repos *repository.Repositories) error {
					if err := h.enqueueDataSetEnsure(ctx, repos, target); err != nil {
						return err
					}
					return repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonTargetCreating)
				})
			}
			return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "target", "Waiting for replacement storage service", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonTargetCreating)
			})
		}

		if !target.IsCurrent {
			return taskengine.Suspend(model.TaskResumeModeRecover, 0, "activation", "Activating replacement storage service", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.Activate(ctx, row.ID)
			})
		}
		if row.Status == storagereplacement.StatusWaiting {
			if err := h.deps.Repositories.Replacements.MarkMigrating(ctx, row.ID); err != nil && !errors.Is(err, repository.ErrConflict) {
				return h.retryReplacement(execution, row.ID, err, "replacement_state_failed")
			}
		}

		inserted, seeded, err := h.deps.Repositories.Replacements.SeedMigrationBatch(ctx, row.ID, replacementSeedBatchSize)
		if err != nil {
			return h.retryReplacement(execution, row.ID, err, "replacement_seed_failed")
		}
		if !seeded || inserted > 0 {
			return taskengine.Suspend(model.TaskResumeModeRecover, 0, "seeding", "Preparing stored content for migration", nil)
		}
		item, err := h.deps.Repositories.Replacements.NextPendingReplacementItem(ctx, row.ID)
		if err != nil {
			return h.retryReplacement(execution, row.ID, err, "replacement_item_scan_failed")
		}
		if item != nil {
			return h.coordinateReplacementItem(ctx, execution, row, item)
		}
		snapshot, err := h.deps.Repositories.Replacements.ReplacementExecution(ctx, row.ID)
		if err != nil {
			return h.retryReplacement(execution, row.ID, err, "replacement_progress_failed")
		}
		if snapshot.HasFailed {
			return h.failReplacement(row.ID, errors.New("stored content migration requires attention"), "replacement_item_attention")
		}
		if !snapshot.SeedingComplete || snapshot.HasPending {
			return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "copy_work", "Waiting for stored content migration", nil)
		}
		return h.scheduleReplacementRetirement(input, execution.ID(), row, row.SourceDataSetID, "Old storage service retirement scheduled")
	}
	return taskHandler{definition: definition, execute: run, recover: run}
}

func (h *TaskHandlers) coordinateReplacementItem(
	ctx context.Context,
	execution taskengine.Execution,
	replacement *storagereplacement.Replacement,
	item *storagereplacement.Item,
) taskengine.Result {
	snapshot, err := h.deps.Repositories.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{
		ReplacementID: replacement.ID, ItemID: item.ID,
	})
	if errors.Is(err, storagereplacement.ErrItemCancelled) {
		return taskengine.Suspend(model.TaskResumeModeRecover, 0, "copy_work", "Continuing stored content migration", nil)
	}
	if errors.Is(err, storagereplacement.ErrItemDeferred) {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "source", "Waiting for readable stored content", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Replacements.MarkWaiting(ctx, replacement.ID, storagereplacement.WaitReasonReadableSource)
		})
	}
	if err != nil {
		return h.retryReplacement(execution, replacement.ID, err, "replacement_item_load_failed")
	}
	if snapshot == nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, 0, "copy_work", "Continuing stored content migration", nil)
	}
	copyRow, err := h.deps.Repositories.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{
		ReplacementID: replacement.ID, ItemID: item.ID, ContentID: snapshot.Upload.ID,
	})
	if err != nil {
		return h.retryReplacement(execution, replacement.ID, err, "replacement_copy_attach_failed")
	}
	if copyRow.Status == model.StorageCopyStatusCommitted {
		return taskengine.Suspend(model.TaskResumeModeRecover, 0, "copy_work", "Stored content migrated", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Replacements.MarkReplacementItemCopied(ctx, replacement.ID, item.ID, copyRow.ID)
		})
	}
	if copyRow.Status == model.StorageCopyStatusFailed && copyRow.ActiveTaskID == nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, 0, "copy_work", "Restarting stored content migration", func(ctx context.Context, repos *repository.Repositories) error {
			if err := repos.Contents.ReopenFailedUploadCopy(ctx, copyRow.ID); err != nil {
				return err
			}
			return h.enqueueInitialCopyTask(ctx, repos, copyRow.ID, model.TaskTypeStorageTransferPlan)
		})
	}
	if copyRow.ActiveTaskID == nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, 0, "copy_work", "Migrating stored content", func(ctx context.Context, repos *repository.Repositories) error {
			return h.enqueueInitialCopyTask(ctx, repos, copyRow.ID, model.TaskTypeStorageTransferPlan)
		})
	}
	copyTask, err := h.deps.Repositories.Tasks.GetByID(ctx, *copyRow.ActiveTaskID)
	if err != nil {
		return h.retryReplacement(execution, replacement.ID, err, "replacement_copy_task_load_failed")
	}
	if copyTask != nil && copyTask.Status == model.TaskStatusFailed {
		message := "stored content migration failed"
		if copyTask.LastError != nil {
			message = *copyTask.LastError
		}
		return taskengine.Fail(errors.New(message), "replacement_copy_failed", func(ctx context.Context, repos *repository.Repositories) error {
			if err := repos.Replacements.MarkReplacementItemAttention(ctx, replacement.ID, item.ID, message); err != nil {
				return err
			}
			return repos.Replacements.MarkFailed(ctx, replacement.ID, nil, message)
		})
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "copy_work", "Waiting for stored content migration", nil)
}

func (h *TaskHandlers) failReplacement(replacementID int64, err error, reason string) taskengine.Result {
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Replacements.MarkFailed(ctx, replacementID, nil, err.Error())
	})
}

func (h *TaskHandlers) retryReplacement(
	execution taskengine.Execution,
	replacementID int64,
	err error,
	reason string,
) taskengine.Result {
	if !execution.RetryWillFail() {
		return retryTask(err, reason)
	}
	return h.failReplacement(replacementID, err, reason)
}

func (h *TaskHandlers) scheduleReplacementRetirement(
	input storagereplacement.CoordinateInput,
	taskID int64,
	row *storagereplacement.Replacement,
	dataSetID int64,
	message string,
) taskengine.Result {
	return taskengine.Complete(message, func(ctx context.Context, repos *repository.Repositories) error {
		if h.taskService == nil {
			return errors.New("task service is unavailable")
		}
		dataSet, err := repos.Contents.GetDataSetBindingByID(ctx, dataSetID)
		if err != nil || dataSet == nil {
			return errors.Join(err, repository.ErrNotFound)
		}
		if dataSet.Status == model.StorageDataSetStatusRetired {
			return repos.Replacements.CompleteTask(ctx, row.ID, input.Generation, taskID)
		}
		if dataSet.RetirementTaskID == nil {
			generation, err := repos.Contents.NextDataSetRetirementGeneration(ctx, dataSetID)
			if err != nil {
				return err
			}
			retireInput := storagereplacement.RetireInput{
				ReplacementID: row.ID, DataSetID: dataSetID, Generation: generation,
			}
			retireTask, _, err := h.taskService.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
				Type:           model.TaskTypeStorageDataSetRetire,
				IdempotencyKey: storagereplacement.RetireTaskKey(dataSetID, generation), Input: retireInput,
				SubjectType: "storage_data_set", SubjectKey: fmt.Sprintf("%d", dataSetID),
			})
			if err != nil {
				return err
			}
			if err := repos.Contents.BindDataSetRetirementTask(ctx, dataSetID, generation, retireTask.ID); err != nil {
				return err
			}
		}
		if row.Status != storagereplacement.StatusSuperseded {
			if err := repos.Replacements.BeginRetirement(ctx, row.ID); err != nil && !errors.Is(err, repository.ErrConflict) {
				return err
			}
		}
		return repos.Replacements.CompleteTask(ctx, row.ID, input.Generation, taskID)
	})
}

func (h *TaskHandlers) dataSetRetireHandler() taskengine.Handler {
	definition := taskengine.Definition{
		Type: model.TaskTypeStorageDataSetRetire, InputVersion: 1,
		Codec: taskengine.StrictJSONCodec(func(input *storagereplacement.RetireInput) error {
			return storagereplacement.ValidateRetireInput(*input)
		}),
		RetryLimit: h.retryLimit(), AllowRetry: true,
		CanManualRetry: func(task *model.Task) bool {
			return task == nil || task.FailureReason == nil || *task.FailureReason != "termination_outcome_unknown"
		},
	}
	return taskHandler{
		definition: definition,
		execute: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runDataSetRetirement(ctx, execution, true)
		},
		recover: func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.runDataSetRetirement(ctx, execution, false)
		},
	}
}

func (h *TaskHandlers) runDataSetRetirement(ctx context.Context, execution taskengine.Execution, mayTerminate bool) taskengine.Result {
	input, err := taskengine.DecodeInput[storagereplacement.RetireInput](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeStorageDataSetRetire), err)
	}
	dataSet, err := h.deps.Repositories.Contents.AuthorizeDataSetRetirementTask(ctx, input.DataSetID, input.Generation, execution.ID())
	if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
		return taskengine.Cancel("Storage service retirement was superseded", nil)
	}
	if err != nil {
		return h.retryRetirement(execution, input.ReplacementID, err, "retirement_authorization_failed")
	}
	row, err := h.deps.Repositories.Replacements.GetByID(ctx, input.ReplacementID)
	if err != nil || row == nil {
		if err == nil {
			err = repository.ErrNotFound
		}
		return taskengine.Fail(err, "replacement_missing", nil)
	}
	abandoned := row.Status == storagereplacement.StatusSuperseded && input.DataSetID == row.TargetDataSetID
	if !abandoned && input.DataSetID != row.SourceDataSetID {
		return taskengine.Fail(errors.New("retirement data set does not belong to replacement"), "retirement_identity_mismatch", nil)
	}
	if dataSet.Status == model.StorageDataSetStatusRetired {
		return taskengine.Complete("Storage service retired", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.CompleteDataSetRetirementTask(ctx, input.DataSetID, input.Generation, execution.ID())
		})
	}
	if dataSet.DataSetID == nil || dataSet.DataSetID.IsZero() {
		return taskengine.Fail(errors.New("storage service has no remote identity"), "retirement_identity_missing", nil)
	}

	var terminationEpoch *int64
	if abandoned {
		terminationEpoch = row.AbandonedTerminationEpoch
		sole, err := h.deps.Repositories.Replacements.CountAbandonedTargetSoleCopies(ctx, row.TargetDataSetID)
		if err != nil {
			return h.retryRetirement(execution, row.ID, err, "retirement_gate_failed")
		}
		if sole > 0 {
			return taskengine.Fail(fmt.Errorf("unused storage service holds %d sole copies", sole), "retirement_coverage", nil)
		}
	} else {
		terminationEpoch = row.TerminationEpoch
		gate, err := h.deps.Repositories.Replacements.EvaluateRetirementGate(ctx, row.ID, nil)
		if err != nil {
			return h.retryRetirement(execution, row.ID, err, "retirement_gate_failed")
		}
		if len(gate.Blockers) > 0 {
			if containsString(gate.Blockers, "slot_ownership") {
				err := fmt.Errorf("retirement safety gate failed: %s", strings.Join(gate.Blockers, ", "))
				return taskengine.Fail(err, "retirement_gate_invalid", func(ctx context.Context, repos *repository.Repositories) error {
					return repos.Replacements.MarkCleanupAttention(ctx, row.ID, err.Error())
				})
			}
			return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "retirement_gate", "Waiting for safe storage service retirement", nil)
		}
	}

	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[retirementCheckpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if terminationEpoch == nil && checkpoint.TerminationEpoch != nil {
		if *checkpoint.TerminationEpoch < 0 {
			return taskengine.Fail(errors.New("storage service retirement checkpoint has an invalid epoch"), "invalid_checkpoint", nil)
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, 0, "termination_epoch", "Recording storage service retirement", retirementEvidenceSettlement(
			abandoned, row.ID, *checkpoint.TerminationEpoch, checkpoint.TransactionHash,
		))
	}
	if terminationEpoch == nil {
		if hasCheckpoint {
			err := errors.New("storage service termination outcome could not be recovered")
			if abandoned {
				return taskengine.Fail(err, "termination_outcome_unknown", nil)
			}
			return taskengine.Fail(err, "termination_outcome_unknown", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.MarkCleanupAttention(ctx, row.ID, err.Error())
			})
		}
		if !mayTerminate {
			return taskengine.Suspend(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage service is ready to retire", nil)
		}
		if h.deps.Terminator == nil {
			return taskengine.Fail(errors.New("storage service terminator is unavailable"), "dependency_unavailable", nil)
		}
		checkpoint = retirementCheckpoint{AttemptedAt: time.Now().UTC()}
		var terminationEpochValue int64
		var txHash string
		attempted, err := execution.WithCheckpointedEffect(ctx, taskengine.ResourceDestructiveMutation, checkpoint, nil, func(ctx context.Context) error {
			result, terminateErr := h.deps.Terminator.TerminateService(ctx, dataSet.DataSetID.SDK())
			if result != nil {
				terminationEpochValue = result.EndEpoch
				txHash = result.TxHash
			}
			return terminateErr
		})
		if err != nil && !attempted {
			return retryTask(err, "termination_not_started")
		}
		if err != nil || terminationEpochValue < 0 {
			if err == nil {
				err = errors.New("storage service termination returned an invalid epoch")
			}
			if abandoned {
				return taskengine.Fail(err, "termination_outcome_unknown", nil)
			}
			return taskengine.Fail(err, "termination_outcome_unknown", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.MarkCleanupAttention(ctx, row.ID, err.Error())
			})
		}
		checkpoint.TerminationEpoch = &terminationEpochValue
		checkpoint.TransactionHash = txHash
		settlement := retirementEvidenceSettlement(abandoned, row.ID, terminationEpochValue, txHash)
		if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
			return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "termination_epoch", "Recording storage service retirement", settlement)
		}
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "termination_epoch", "Waiting for storage service retirement", settlement)
	}
	if h.deps.Epochs == nil {
		return taskengine.Fail(errors.New("chain epoch reader is unavailable"), "dependency_unavailable", nil)
	}
	observedEpoch, err := h.deps.Epochs.CurrentEpoch(ctx)
	if err != nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "termination_epoch", "Checking storage service retirement", nil)
	}
	if observedEpoch < *terminationEpoch {
		return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "termination_epoch", "Waiting for storage service retirement", nil)
	}
	return taskengine.Complete("Storage service retired", func(ctx context.Context, repos *repository.Repositories) error {
		if abandoned {
			if err := repos.Replacements.CompleteAbandonedTargetTermination(ctx, row.ID); err != nil {
				return err
			}
		} else if err := repos.Replacements.CompleteRetirement(ctx, row.ID, observedEpoch); err != nil {
			return err
		}
		return repos.Contents.CompleteDataSetRetirementTask(ctx, input.DataSetID, input.Generation, execution.ID())
	})
}

func retirementEvidenceSettlement(abandoned bool, replacementID, epoch int64, transactionHash string) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		evidence := repository.RecordTerminationEpochInput{
			ReplacementID: replacementID,
			TxHash:        transactionHash,
			Epoch:         epoch,
		}
		if abandoned {
			return repos.Replacements.RecordAbandonedTerminationEpoch(ctx, evidence)
		}
		return repos.Replacements.RecordTerminationEpoch(ctx, evidence)
	}
}

func (h *TaskHandlers) retryRetirement(
	execution taskengine.Execution,
	replacementID int64,
	err error,
	reason string,
) taskengine.Result {
	if !execution.RetryWillFail() {
		return retryTask(err, reason)
	}
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		replacement, loadErr := repos.Replacements.GetByID(ctx, replacementID)
		if loadErr != nil {
			return loadErr
		}
		if replacement == nil || replacement.Status == storagereplacement.StatusSuperseded {
			return nil
		}
		return repos.Replacements.MarkCleanupAttention(ctx, replacementID, err.Error())
	})
}

func containsString(values []string, wanted string) bool {
	return slices.Contains(values, wanted)
}
