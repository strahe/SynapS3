package replacement

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"

	taskengine "github.com/strahe/synaps3/internal/worker"

	"github.com/strahe/synaps3/internal/storagepipeline"

	"github.com/strahe/synaps3/internal/storagereplacement"
)

type (
	TaskRetryPolicy interface{ Retryable(*model.Task) bool }
	Dependencies    struct {
		Repositories *repository.Repositories
		Messenger    *taskengine.Messenger
		RetryPolicy  TaskRetryPolicy
	}
)

const (
	storageDependencyWait    = time.Minute
	storagePollInterval      = 5 * time.Second
	replacementSeedBatchSize = 100
)

type Handler struct {
	*taskengine.FuncHandler
	deps Dependencies
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("task repositories are required")
	}
	if deps.Messenger == nil || deps.RetryPolicy == nil {
		return nil, errors.New("replacement scheduling dependencies are required")
	}
	h := &Handler{deps: deps}
	h.FuncHandler = h.replacementCoordinateHandler()
	return h, nil
}

func (h *Handler) replacementCoordinateHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeProviderReplacementCoordinate, InputVersion: 1, WorkStart: taskengine.WorkStartOnHandler,
		Codec: taskengine.StrictJSONCodec(func(input *storagereplacement.CoordinateInput) error {
			return storagereplacement.ValidateCoordinateInput(*input)
		}),
		// The coordinator re-reads the replacement ledger on every wake and a
		// replacement can run for days, so transient errors must not exhaust it.
		RetryLimit: nil, AllowRetry: false,
		// A replacement resumes only through the Data Sets retry, so a claim the
		// Engine fails itself must leave the replacement in a state that retry
		// accepts rather than in progress with no coordinator.
		// The reason stays on the failed task; the replacement records what the
		// operator can do about it.
		OnEngineFailure: func(task *model.Task, _ string) taskengine.Settlement {
			return func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.FailForEngineTask(ctx, task.ID,
					"Replacement work stopped because of an internal error. Retry the replacement; if it stops again, check its task on the Tasks page.")
			}
		},
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

		source, err := h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, row.SourceDataSetID)
		if err != nil || source == nil {
			if err == nil {
				err = repository.ErrNotFound
			}
			return h.retryReplacement(execution, row.ID, err, "replacement_source_load_failed")
		}
		localSource := source.Status == model.StorageDataSetStatusRetired && source.DataSetID == nil
		target, err := h.deps.Repositories.Contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
		if err != nil || target == nil {
			if err == nil {
				err = repository.ErrNotFound
			}
			return h.failReplacement(row.ID, err, "replacement_target_missing")
		}
		if target.Status == model.StorageDataSetStatusRetired || target.Status == model.StorageDataSetStatusFailed {
			return h.failReplacement(row.ID, errors.New("replacement target storage setup stopped; choose another provider"), "replacement_target_failed")
		}
		if !source.IsCurrent && !target.IsCurrent {
			return h.failReplacement(row.ID, errors.New("replacement has no current replica; storage setup requires attention"), "replacement_slot_missing")
		}
		if target.EnsureTaskID != nil && target.Status != model.StorageDataSetStatusReady {
			ensure, err := h.deps.Repositories.Tasks.GetByID(ctx, *target.EnsureTaskID)
			if err != nil {
				return h.retryReplacement(execution, row.ID, err, "replacement_target_task_load_failed")
			}
			if ensure != nil && (ensure.Status == model.TaskStatusFailed || ensure.Status == model.TaskStatusCancelled) && len(target.CreationRejection) == 0 {
				if h.deps.RetryPolicy.Retryable(ensure) {
					return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "target", "Waiting for storage setup to be retried", func(ctx context.Context, repos *repository.Repositories) error {
						return repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonTargetCreating)
					})
				}
				return h.failReplacement(row.ID, errors.New("replacement storage setup stopped; check its task before continuing"), "replacement_target_failed")
			}
		}
		if localSource && target.IsCurrent && target.Status == model.StorageDataSetStatusReady && target.DataSetID != nil && !target.DataSetID.IsZero() {
			var done bool
			err := execution.WriteCheckpointWith(ctx, struct{}{}, func(ctx context.Context, repos *repository.Repositories) error {
				var err error
				done, err = repos.Replacements.AbandonUncreatedCopiesBatch(ctx, row.ID, input.Generation, execution.ID(), replacementSeedBatchSize)
				return err
			})
			if err != nil {
				return h.retryReplacement(execution, row.ID, err, "replacement_source_release_failed")
			}
			if !done {
				return taskengine.Suspend(model.TaskResumeModeRecover, 0, "source_copies", "Preparing replacement replicas", nil)
			}
		}
		if target.Status != model.StorageDataSetStatusReady || target.DataSetID == nil || target.DataSetID.IsZero() {
			if evidence, err := target.CreationRejectionEvidence(); err == nil && evidence != nil {
				reason := storagereplacement.FailureReasonTargetRejected
				return taskengine.Fail(errors.New("storage provider rejected setup"), "replacement_target_rejected",
					func(ctx context.Context, repos *repository.Repositories) error {
						return repos.Replacements.MarkFailed(ctx, row.ID, &reason, storagereplacement.ProviderRejectedMessage)
					})
			}
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
			if source.DataSetID != nil {
				incomplete, err := h.deps.Repositories.Contents.ListIncompleteCopiesForDataSet(ctx, row.SourceDataSetID)
				if err != nil {
					return h.retryReplacement(execution, row.ID, err, "replacement_source_writes_failed")
				}
				if len(incomplete) > 0 {
					return h.waitForSourceWrites(ctx, execution, row, incomplete)
				}
			}
			return taskengine.Suspend(model.TaskResumeModeRecover, 0, "activation", "Activating replacement storage service", func(ctx context.Context, repos *repository.Repositories) error {
				if err := repos.Replacements.Activate(ctx, row.ID, input.Generation, execution.ID()); err != nil {
					return err
				}
				return h.deps.Messenger.Notify(ctx, repos, storagereplacement.ReplacementActivated{BucketID: row.BucketID, CopyIndex: row.CopyIndex})
			})
		}
		if row.Status == storagereplacement.StatusWaiting || row.Status == storagereplacement.StatusPreparingTarget {
			return taskengine.Suspend(model.TaskResumeModeRecover, 0, "migration", "Preparing replacement replicas", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.MarkMigrating(ctx, row.ID)
			})
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
		if localSource {
			return taskengine.Complete("Provider replacement completed", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.CompleteWithoutRemoteSource(ctx, row.ID, input.Generation, execution.ID())
			})
		}
		return h.scheduleReplacementRetirement(input, execution.ID(), row, row.SourceDataSetID, "Old storage service retirement scheduled")
	}
	return taskengine.NewFuncHandler(definition, run, run)
}

func (h *Handler) coordinateReplacementItem(
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
			return h.startCopyTransfer(ctx, repos, copyRow.ID)
		})
	}
	workTaskID := copyRow.WorkTaskID()
	if workTaskID == nil {
		return taskengine.Suspend(model.TaskResumeModeRecover, 0, "copy_work", "Migrating stored content", func(ctx context.Context, repos *repository.Repositories) error {
			if copyRow.Status != model.StorageCopyStatusPending {
				return h.joinCommit(ctx, repos, copyRow.ID)
			}
			return h.startCopyTransfer(ctx, repos, copyRow.ID)
		})
	}
	copyTask, err := h.deps.Repositories.Tasks.GetByID(ctx, *workTaskID)
	if err != nil {
		return h.retryReplacement(execution, replacement.ID, err, "replacement_copy_task_load_failed")
	}
	if copyTask != nil && copyTask.Status == model.TaskStatusFailed {
		// The copy task still holds the copy and an operator can retry it, for
		// example after an unknown transfer outcome, so the replacement waits
		// for that retry instead of failing as a whole.
		if h.deps.RetryPolicy.Retryable(copyTask) {
			return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "copy_work", "Waiting for a failed migration task to be retried", nil)
		}
		message := "stored content migration failed"
		if copyTask.LastError != nil {
			message = *copyTask.LastError
		}
		return taskengine.Fail(errors.New(message), "replacement_copy_failed", func(ctx context.Context, repos *repository.Repositories) error {
			if copyTask.FailureReason != nil && *copyTask.FailureReason == "migration_cache_missing" && copyRow.ActiveTaskID != nil {
				if err := repos.Contents.CompleteCopyTask(ctx, copyRow.ID, copyRow.WorkGeneration, copyTask.ID); err != nil {
					return err
				}
			}
			if err := repos.Replacements.MarkReplacementItemAttention(ctx, replacement.ID, item.ID, message); err != nil {
				return err
			}
			return repos.Replacements.MarkFailed(ctx, replacement.ID, nil, message)
		})
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "copy_work", "Waiting for stored content migration", nil)
}

func (h *Handler) waitForSourceWrites(
	ctx context.Context,
	execution taskengine.Execution,
	replacement *storagereplacement.Replacement,
	incomplete []model.StorageCopy,
) taskengine.Result {
	waitingForRetry := false
	for i := range incomplete {
		taskID := incomplete[i].WorkTaskID()
		if taskID == nil {
			continue
		}
		copyTask, err := h.deps.Repositories.Tasks.GetByID(ctx, *taskID)
		if err != nil {
			return h.retryReplacement(execution, replacement.ID, err, "replacement_source_writes_failed")
		}
		if copyTask == nil || copyTask.Status != model.TaskStatusFailed {
			continue
		}
		if h.deps.RetryPolicy.Retryable(copyTask) {
			waitingForRetry = true
			continue
		}
		message := "a storage write to the current provider failed"
		if copyTask.LastError != nil {
			message += ": " + *copyTask.LastError
		}
		return h.failReplacement(replacement.ID, errors.New(message), "replacement_source_write_failed")
	}
	if waitingForRetry {
		return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "source_writes", "Waiting for a failed storage write to be retried", nil)
	}
	return taskengine.Suspend(model.TaskResumeModeRecover, storagePollInterval, "source_writes", "Waiting for current storage writes", nil)
}

func (h *Handler) failReplacement(replacementID int64, err error, reason string) taskengine.Result {
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Replacements.MarkFailed(ctx, replacementID, nil, err.Error())
	})
}

func (h *Handler) retryReplacement(
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

func (h *Handler) scheduleReplacementRetirement(
	input storagereplacement.CoordinateInput,
	taskID int64,
	row *storagereplacement.Replacement,
	dataSetID int64,
	message string,
) taskengine.Result {
	return taskengine.Complete(message, func(ctx context.Context, repos *repository.Repositories) error {
		dataSet, err := repos.Contents.GetDataSetBindingByID(ctx, dataSetID)
		if err != nil || dataSet == nil {
			return errors.Join(err, repository.ErrNotFound)
		}
		if dataSet.Status == model.StorageDataSetStatusRetired {
			return repos.Replacements.CompleteTask(ctx, row.ID, input.Generation, taskID)
		}
		if err := h.deps.Messenger.Handover(ctx, repos, storagereplacement.RetireDataSet{ReplacementID: row.ID, BindingID: dataSetID}); err != nil {
			return err
		}

		if row.Status != storagereplacement.StatusSuperseded {
			if err := repos.Replacements.BeginRetirement(ctx, row.ID); err != nil && !errors.Is(err, repository.ErrConflict) {
				return err
			}
		}
		return repos.Replacements.CompleteTask(ctx, row.ID, input.Generation, taskID)
	})
}

func (h *Handler) enqueueDataSetEnsure(ctx context.Context, tx *repository.Repositories, row *model.StorageDataSet) error {
	if row == nil {
		return repository.ErrInvalidInput
	}
	return h.deps.Messenger.Handover(ctx, tx, storagepipeline.EnsureDataSet{BindingID: row.ID})
}

func (h *Handler) startCopyTransfer(ctx context.Context, tx *repository.Repositories, copyID int64) error {
	return h.deps.Messenger.Handover(ctx, tx, storagepipeline.StartCopyTransfer{CopyID: copyID})
}

func (h *Handler) joinCommit(ctx context.Context, tx *repository.Repositories, copyID int64) error {
	return h.deps.Messenger.Handover(ctx, tx, storagepipeline.JoinCommit{CopyID: copyID})
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}
