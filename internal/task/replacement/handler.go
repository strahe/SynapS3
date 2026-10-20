package replacement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"

	taskengine "github.com/strahe/synaps3/internal/worker"

	"github.com/strahe/synaps3/internal/storagepipeline"

	"github.com/strahe/synaps3/internal/storagereplacement"
)

type (
	TaskRetryPolicy interface {
		RetryableContext(context.Context, *model.Task) (bool, error)
		InspectRetryWithRepositories(context.Context, *repository.Repositories, *model.Task) error
	}
	Dependencies struct {
		Repositories *repository.Repositories
		Messenger    *taskengine.Messenger
		RetryPolicy  TaskRetryPolicy
		Scheduler    *taskengine.Scheduler
	}
)

const (
	replacementSeedBatchSize        = 100
	replacementInternalErrorMessage = "Replacement work stopped because of an internal error. Retry the replacement; if it stops again, check its task on the Tasks page."
)

type Handler struct {
	*taskengine.FuncHandler
	deps Dependencies
}

type replacementProgressCheckpoint struct {
	ScanAfterItemID   int64          `json:"scan_after_item_id,omitempty"`
	DependencyRetries map[string]int `json:"dependency_retries,omitempty"`
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("task repositories are required")
	}
	if deps.Messenger == nil || deps.RetryPolicy == nil || deps.Scheduler == nil {
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
		Identity: taskengine.IdentityFromInput("storage_replacement", func(input storagereplacement.CoordinateInput) (int64, int64) {
			return input.ReplacementID, input.Generation
		}),
		Policy: taskengine.ExecutionPolicy{MaxAttempts: 6, DependencyRetries: 5, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
		OnEngineFailure: func(task *model.Task, _ string) taskengine.Settlement {
			return func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.FailForEngineTask(ctx, task.ID, replacementInternalErrorMessage)
			}
		},
	}
	definition.InspectRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
		var input storagereplacement.CoordinateInput
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return err
		}
		row, err := repos.Replacements.GetByID(ctx, input.ReplacementID)
		if err != nil {
			return err
		}
		if row == nil || row.TaskID == nil || *row.TaskID != source.ID || row.TaskGeneration != input.Generation {
			return repository.ErrConflict
		}
		if row.Status == storagereplacement.StatusFailed {
			err := repos.Replacements.RetryEligibility(ctx, row.ID)
			if errors.Is(err, storagereplacement.ErrNotRetryable) || errors.Is(err, storagereplacement.ErrSuperseded) || errors.Is(err, storagereplacement.ErrTargetInUse) {
				return errors.Join(repository.ErrConflict, err)
			}
			if err != nil {
				return err
			}
		}
		if row.Status == storagereplacement.StatusCompleted || row.Status == storagereplacement.StatusSuperseded || row.Status == storagereplacement.StatusRetiring || row.Status == storagereplacement.StatusCleanupAttention {
			return repository.ErrConflict
		}
		target, err := repos.Contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
		if err != nil || target == nil {
			return errors.Join(err, repository.ErrNotFound)
		}
		if target.Status != model.StorageDataSetStatusReady && target.EnsureTaskID != nil {
			child, err := repos.Tasks.GetByID(ctx, *target.EnsureTaskID)
			if err != nil {
				return err
			}
			if child == nil {
				return repository.ErrConflict
			}
			if child.Status == model.TaskStatusFailed {
				return h.deps.RetryPolicy.InspectRetryWithRepositories(ctx, repos, child)
			}
			if child.Status != model.TaskStatusPending && child.Status != model.TaskStatusRunning {
				return taskengine.ErrRetryUnsupported
			}
		}
		return nil
	}
	bind := func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
		var input storagereplacement.CoordinateInput
		if err := json.Unmarshal(old.Input, &input); err != nil {
			return err
		}
		return repos.Replacements.ResumeCoordinatorTask(ctx, input.ReplacementID, input.Generation, old.ID, next.ID)
	}
	definition.BindSuccessor = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
		var input storagereplacement.CoordinateInput
		if err := json.Unmarshal(old.Input, &input); err != nil {
			return err
		}
		return repos.Replacements.TransferTaskOwner(ctx, input.ReplacementID, input.Generation, old.ID, next.ID)
	}

	definition.PrepareRetry = func(_ context.Context, _ *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
		var input storagereplacement.CoordinateInput
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return taskengine.RetryPreparation{}, err
		}
		checkpoint := source.Checkpoint
		if len(checkpoint) > 0 {
			var progress replacementProgressCheckpoint
			if err := json.Unmarshal(checkpoint, &progress); err == nil {
				progress.DependencyRetries = nil
				var err error
				checkpoint, err = json.Marshal(progress)
				if err != nil {
					return taskengine.RetryPreparation{}, err
				}
			}
		}
		return taskengine.RetryPreparation{
			Request: taskengine.EnqueueRequest{Type: source.Type, Input: source.Input}, Checkpoint: checkpoint,
			ResumeMode: model.TaskResumeModeRecover,
			Validate: func(ctx context.Context, repos *repository.Repositories, current *model.Task) error {
				_, err := repos.Replacements.PrepareCoordinatorRetry(ctx, input.ReplacementID, input.Generation, current.ID)
				return err
			},
			Bind: func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
				row, err := repos.Replacements.GetByID(ctx, input.ReplacementID)
				if err != nil || row == nil {
					return errors.Join(err, repository.ErrNotFound)
				}
				target, err := repos.Contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
				if err != nil || target == nil {
					return errors.Join(err, repository.ErrNotFound)
				}
				if target.Status != model.StorageDataSetStatusReady && target.EnsureTaskID != nil {
					child, err := repos.Tasks.GetByID(ctx, *target.EnsureTaskID)
					if err != nil {
						return err
					}
					if child != nil && child.Status == model.TaskStatusFailed && (child.FailureReason == nil || !storagepipeline.ExecutionIdentityBlocked(*child.FailureReason)) {
						if _, err := h.deps.Scheduler.RetryInTransaction(ctx, repos, child.ID, time.Now().UTC()); err != nil {
							return err
						}
					}
				}
				return bind(ctx, repos, old, next)
			},
		}, nil
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
			return h.scheduleReplacementRetirement(input, execution.ID(), row, row.TargetDataSetID, "Unused data set cleanup scheduled")
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
		if target.Status == model.StorageDataSetStatusRetired {
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
			if ensure != nil && (ensure.Status == model.TaskStatusFailed || ensure.Status == model.TaskStatusCancelled) {
				return taskengine.Fail(errors.New("storage setup stopped"), "replacement_target_failed", func(ctx context.Context, repos *repository.Repositories) error {
					return repos.Replacements.MarkInitializationFailed(ctx, row.ID, input.Generation, execution.ID(), ensure.ID,
						"Storage setup stopped. Retry from Tasks or choose another provider.")
				})
			}
		}
		if localSource && target.IsCurrent && target.Status == model.StorageDataSetStatusReady && target.DataSetID != nil && !target.DataSetID.IsZero() {
			progress, _, err := taskengine.DecodeCheckpoint[replacementProgressCheckpoint](execution)
			if err != nil {
				return taskengine.Fail(err, "invalid_checkpoint", nil)
			}
			var done bool
			err = execution.WriteCheckpointWith(ctx, progress, func(ctx context.Context, repos *repository.Repositories) error {
				var err error
				done, err = repos.Replacements.AbandonUncreatedCopiesBatch(ctx, row.ID, input.Generation, execution.ID(), replacementSeedBatchSize)
				return err
			})
			if err != nil {
				return h.retryReplacement(execution, row.ID, err, "replacement_source_release_failed")
			}
			if !done {
				return taskengine.Wait(model.TaskResumeModeRecover, "source_copies", "Preparing replacement replicas", nil)
			}
		}
		remoteSource := source.DataSetID != nil && !source.IsCurrent && source.Status == model.StorageDataSetStatusDraining
		if remoteSource && target.IsCurrent && target.Status == model.StorageDataSetStatusReady && target.DataSetID != nil && !target.DataSetID.IsZero() {
			writes, err := h.deps.Repositories.Replacements.HasUnsignedSourceWrites(ctx, row.SourceDataSetID)
			if err != nil {
				return h.retryReplacement(execution, row.ID, err, "replacement_source_writes_failed")
			}
			if writes {
				progress, _, err := taskengine.DecodeCheckpoint[replacementProgressCheckpoint](execution)
				if err != nil {
					return taskengine.Fail(err, "invalid_checkpoint", nil)
				}
				var done bool
				err = execution.WriteCheckpointWith(ctx, progress, func(ctx context.Context, repos *repository.Repositories) error {
					copies, complete, err := repos.Replacements.HandOverSourceWritesBatch(ctx, row.ID, input.Generation, execution.ID(), replacementSeedBatchSize)
					if err != nil {
						return err
					}
					for _, copyRow := range copies {
						if copyRow.Status == model.StorageCopyStatusPending && copyRow.WorkTaskID() == nil {
							if err := h.startCopyTransfer(ctx, repos, copyRow.ID); err != nil {
								return err
							}
						}
					}
					done = complete
					return nil
				})
				if err != nil {
					return h.retryReplacement(execution, row.ID, err, "replacement_source_handover_failed")
				}
				if !done {
					return taskengine.Wait(model.TaskResumeModeRecover, "source_copies", "Preparing replacement replicas", nil)
				}
			}
		}
		if target.Status != model.StorageDataSetStatusReady || target.DataSetID == nil || target.DataSetID.IsZero() {
			if target.EnsureTaskID == nil {
				return taskengine.Wait(model.TaskResumeModeRecover, "target", "Preparing replacement data set", func(ctx context.Context, repos *repository.Repositories) error {
					if err := h.enqueueDataSetEnsure(ctx, repos, target); err != nil {
						return err
					}
					return repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonTargetCreating)
				})
			}
			return taskengine.Wait(model.TaskResumeModeRecover, "target", "Waiting for replacement data set", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.MarkWaiting(ctx, row.ID, storagereplacement.WaitReasonTargetCreating)
			})
		}

		if !target.IsCurrent {
			return taskengine.Wait(model.TaskResumeModeRecover, "activation", "Activating replacement data set", func(ctx context.Context, repos *repository.Repositories) error {
				if err := repos.Replacements.Activate(ctx, row.ID, input.Generation, execution.ID()); err != nil {
					return err
				}
				return h.deps.Messenger.Notify(ctx, repos, storagereplacement.ReplacementActivated{BucketID: row.BucketID, CopyIndex: row.CopyIndex})
			})
		}
		if row.Status == storagereplacement.StatusWaiting || row.Status == storagereplacement.StatusPreparingTarget {
			return taskengine.Wait(model.TaskResumeModeRecover, "migration", "Preparing replacement replicas", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.MarkMigrating(ctx, row.ID)
			})
		}

		if !row.SeedingComplete {
			return taskengine.Wait(model.TaskResumeModeRecover, "seeding", "Preparing stored content for migration", func(ctx context.Context, repos *repository.Repositories) error {
				if _, err := repos.Replacements.LockCoordinatorProgress(ctx, row.ID, input.Generation, execution.ID()); err != nil {
					return err
				}
				_, _, err := repos.Replacements.SeedMigrationBatch(ctx, row.ID, replacementSeedBatchSize)
				return err
			})
		}
		checkpoint, _, err := taskengine.DecodeCheckpoint[replacementProgressCheckpoint](execution)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		items, err := h.deps.Repositories.Replacements.ListPendingReplacementItemsAfter(ctx, row.ID, checkpoint.ScanAfterItemID, replacementSeedBatchSize)
		if err != nil {
			return h.retryReplacement(execution, row.ID, err, "replacement_item_scan_failed")
		}
		if len(items) > 0 {
			return h.coordinateReplacementItems(ctx, execution, row, checkpoint, items)
		}
		if checkpoint.ScanAfterItemID != 0 {
			return taskengine.Wait(model.TaskResumeModeRecover, "copy_work", "Continuing stored content migration", func(ctx context.Context, repos *repository.Repositories) error {
				if _, err := repos.Replacements.LockCoordinatorProgress(ctx, row.ID, input.Generation, execution.ID()); err != nil {
					return err
				}
				checkpoint.ScanAfterItemID = 0
				return writeProgressCheckpoint(ctx, repos, execution, checkpoint)
			})
		}
		snapshot, err := h.deps.Repositories.Replacements.ReplacementExecution(ctx, row.ID)
		if err != nil {
			return h.retryReplacement(execution, row.ID, err, "replacement_progress_failed")
		}
		if snapshot.HasFailed {
			return h.failReplacement(row.ID, errors.New("Some stored content could not be copied to the new provider. Retry the replacement to try it again."), "replacement_item_attention") //nolint:staticcheck // ST1005: this error is displayed directly to the user.
		}
		if !snapshot.SeedingComplete || snapshot.HasPending {
			return taskengine.Wait(model.TaskResumeModeRecover, "copy_work", "Waiting for stored content migration", nil)
		}
		if localSource {
			return taskengine.Complete("Provider replacement completed", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Replacements.CompleteWithoutRemoteSource(ctx, row.ID, input.Generation, execution.ID())
			})
		}
		writes, err := h.deps.Repositories.Contents.ListInFlightCopiesForDataSet(ctx, row.SourceDataSetID)
		if err != nil {
			return h.retryReplacement(execution, row.ID, err, "replacement_source_writes_failed")
		}
		if len(writes) > 0 {
			return h.waitForSourceWrites(ctx, execution, row, checkpoint, writes)
		}
		return h.scheduleReplacementRetirement(input, execution.ID(), row, row.SourceDataSetID, "Old data set retirement scheduled")
	}
	return taskengine.NewFuncHandler(definition, run, run)
}

type dependencyRetry struct {
	source  *model.Task
	key     string
	at      time.Time
	message string
}

type replacementItemRetry struct {
	itemID int64
	copyID int64
	dependencyRetry
}

func copyDependencyKey(copyID int64) string {
	return string(model.TaskSubjectStorageCopy) + ":" + strconv.FormatInt(copyID, 10)
}

func (h *Handler) planDependencyRetry(ctx context.Context, execution taskengine.Execution, checkpoint replacementProgressCheckpoint, source *model.Task) (dependencyRetry, error) {
	decision := dependencyRetry{
		source: source, key: string(source.SubjectType) + ":" + source.SubjectKey,
		message: "stored content migration failed",
	}
	if source.LastError != nil {
		decision.message = *source.LastError
	}
	if source.FailureReason != nil && storagepipeline.ExecutionIdentityBlocked(*source.FailureReason) {
		return decision, nil
	}
	eligible, err := h.deps.RetryPolicy.RetryableContext(ctx, source)
	if err != nil {
		return decision, err
	}
	if eligible {
		decision.at, _ = execution.DependencyRetryAt(checkpoint.DependencyRetries[decision.key])
	}
	return decision, nil
}

func (h *Handler) coordinateReplacementItems(ctx context.Context, execution taskengine.Execution, replacement *storagereplacement.Replacement, checkpoint replacementProgressCheckpoint, items []storagereplacement.Item) taskengine.Result {
	var decisions []replacementItemRetry
	dependencies := make(map[int64]dependencyRetry)
	for _, item := range items {
		_, err := h.deps.Repositories.Replacements.InspectItem(ctx, repository.AcquireReplacementItemInput{ReplacementID: replacement.ID, ItemID: item.ID})
		if errors.Is(err, storagereplacement.ErrItemCancelled) {
			continue
		}
		if err != nil {
			return h.retryReplacement(execution, replacement.ID, err, "replacement_item_load_failed")
		}
		copyRow, err := h.deps.Repositories.Contents.GetUploadCopyForDataSet(ctx, item.ContentID, replacement.TargetDataSetID)
		if err != nil {
			return h.retryReplacement(execution, replacement.ID, err, "replacement_copy_load_failed")
		}
		if copyRow == nil {
			continue
		}
		if copyRow.Status == model.StorageCopyStatusFailed && copyRow.ActiveTaskID == nil {
			decision := dependencyRetry{key: copyDependencyKey(copyRow.ID), message: "replacement copy transfer failed"}
			latest, loadErr := h.deps.Repositories.Tasks.LatestForSubject(ctx, model.TaskSubjectStorageCopy, strconv.FormatInt(copyRow.ID, 10), model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull)
			if loadErr != nil {
				return h.retryReplacement(execution, replacement.ID, loadErr, "replacement_copy_task_load_failed")
			}
			if latest != nil && latest.FailureReason != nil && storagepipeline.ExecutionIdentityBlocked(*latest.FailureReason) {
				decisions = append(decisions, replacementItemRetry{itemID: item.ID, copyID: copyRow.ID, dependencyRetry: decision})
				continue
			}
			decision.at, _ = execution.DependencyRetryAt(checkpoint.DependencyRetries[decision.key])
			decisions = append(decisions, replacementItemRetry{itemID: item.ID, copyID: copyRow.ID, dependencyRetry: decision})
			continue
		}
		if workID := copyRow.WorkTaskID(); workID != nil {
			decision, known := dependencies[*workID]
			if !known {
				work, err := h.deps.Repositories.Tasks.GetByID(ctx, *workID)
				if err != nil {
					return h.retryReplacement(execution, replacement.ID, err, "replacement_copy_task_load_failed")
				}
				if work == nil || work.Status != model.TaskStatusFailed {
					continue
				}
				decision, err = h.planDependencyRetry(ctx, execution, checkpoint, work)
				if err != nil {
					return h.retryReplacement(execution, replacement.ID, err, "replacement_retry_eligibility_failed")
				}
				dependencies[work.ID] = decision
			}
			decisions = append(decisions, replacementItemRetry{itemID: item.ID, copyID: copyRow.ID, dependencyRetry: decision})
		}
	}
	return taskengine.Wait(model.TaskResumeModeRecover, "copy_work", "Migrating stored content", func(ctx context.Context, repos *repository.Repositories) error {
		progress := checkpoint
		progress.DependencyRetries = maps.Clone(checkpoint.DependencyRetries)
		if progress.DependencyRetries == nil {
			progress.DependencyRetries = make(map[string]int)
		}
		batch, err := repos.Replacements.AcquireItemsForCoordinator(ctx, replacement.ID, replacement.TaskGeneration, execution.ID(), checkpoint.ScanAfterItemID, replacementSeedBatchSize)
		if err != nil {
			return err
		}
		for _, snapshot := range batch.Items {
			item := snapshot.Item
			copyRow, err := repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{ReplacementID: replacement.ID, ItemID: item.ID, ContentID: snapshot.Upload.ID})
			if err != nil {
				return err
			}
			if copyRow.Status == model.StorageCopyStatusCommitted {
				if err := repos.Replacements.MarkReplacementItemCopied(ctx, replacement.ID, item.ID, copyRow.ID); err != nil {
					return err
				}
				delete(progress.DependencyRetries, copyDependencyKey(copyRow.ID))
			} else if copyRow.WorkTaskID() == nil && copyRow.Status != model.StorageCopyStatusFailed {
				if copyRow.Status == model.StorageCopyStatusPending {
					err = h.startCopyTransfer(ctx, repos, copyRow.ID)
				} else {
					err = h.joinCommit(ctx, repos, copyRow.ID)
				}
				if err != nil {
					return err
				}
			}
		}
		if len(progress.DependencyRetries) > 0 {
			for _, item := range items {
				copyRow, err := repos.Contents.GetUploadCopyForDataSet(ctx, item.ContentID, replacement.TargetDataSetID)
				if err != nil {
					return err
				}
				if copyRow != nil && copyRow.Status == model.StorageCopyStatusCommitted {
					delete(progress.DependencyRetries, copyDependencyKey(copyRow.ID))
				}
			}
		}
		for _, decision := range decisions {
			if _, err := requiredReplacementItemCopy(ctx, repos, execution, replacement, decision.itemID); err != nil {
				return err
			}
		}
		current, err := lockFailedDependencies(ctx, repos, dependencies)
		if err != nil {
			return err
		}
		retried := make(map[int64]bool)
		for _, decision := range decisions {
			if decision.source != nil && (retried[decision.source.ID] || !current[decision.source.ID]) {
				continue
			}
			copyRow, err := repos.Contents.GetUploadCopyByID(ctx, decision.copyID)
			if err != nil {
				return err
			}
			if copyRow == nil {
				return repository.ErrConflict
			}
			if decision.source != nil {
				if workID := copyRow.WorkTaskID(); workID == nil || *workID != decision.source.ID {
					continue
				}
			} else if copyRow.Status != model.StorageCopyStatusFailed || copyRow.ActiveTaskID != nil {
				return repository.ErrConflict
			}
			if decision.at.IsZero() {
				if err := repos.Replacements.MarkReplacementItemAttention(ctx, replacement.ID, decision.itemID, decision.message); err != nil {
					return err
				}
				delete(progress.DependencyRetries, copyDependencyKey(copyRow.ID))
				continue
			}
			if decision.source != nil {
				if _, err := h.deps.Scheduler.RetryInTransaction(ctx, repos, decision.source.ID, decision.at); err != nil {
					return err
				}
				retried[decision.source.ID] = true
			} else {
				failed, err := repos.Tasks.LatestForSubject(ctx, model.TaskSubjectStorageCopy, strconv.FormatInt(copyRow.ID, 10), model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull)
				if err != nil {
					return err
				}
				var recoveryID int64
				if failed != nil && failed.Status == model.TaskStatusFailed {
					recoveryID = failed.ID
				}
				if err := repos.Contents.ReopenFailedUploadCopy(ctx, copyRow.ID); err != nil {
					return err
				}
				if err := h.deps.Messenger.Handover(ctx, repos, storagepipeline.StartCopyTransfer{CopyID: copyRow.ID, AvailableAt: decision.at, RecoveryTaskID: recoveryID}); err != nil {
					return err
				}
			}
			progress.DependencyRetries[decision.key]++
		}
		progress.ScanAfterItemID = batch.LastItemID
		if batch.Scanned < replacementSeedBatchSize {
			progress.ScanAfterItemID = 0
		}
		return writeProgressCheckpoint(ctx, repos, execution, progress)
	})
}

// Source task locks serialize the successor check with manual and coordinated retries.
func lockFailedDependencies(ctx context.Context, repos *repository.Repositories, dependencies map[int64]dependencyRetry) (map[int64]bool, error) {
	ids := slices.Sorted(maps.Keys(dependencies))
	current := make(map[int64]bool, len(ids))
	for _, id := range ids {
		source, err := repos.Tasks.GetForUpdate(ctx, id)
		if err != nil {
			return nil, err
		}
		if source == nil || source.Status != model.TaskStatusFailed || source.SupersededAt != nil {
			continue
		}
		next, err := repos.Tasks.GetDirectSuccessor(ctx, id)
		if err != nil {
			return nil, err
		}
		current[id] = next == nil
	}
	return current, nil
}

func writeProgressCheckpoint(ctx context.Context, repos *repository.Repositories, execution taskengine.Execution, checkpoint replacementProgressCheckpoint) error {
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	return repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), raw)
}

func requiredReplacementItemCopy(ctx context.Context, repos *repository.Repositories, execution taskengine.Execution, replacement *storagereplacement.Replacement, itemID int64) (*model.StorageCopy, error) {
	if _, err := repos.Replacements.LockCoordinatorProgress(ctx, replacement.ID, replacement.TaskGeneration, execution.ID()); err != nil {
		return nil, err
	}
	snapshot, err := repos.Replacements.AcquireItem(ctx, repository.AcquireReplacementItemInput{ReplacementID: replacement.ID, ItemID: itemID})
	if errors.Is(err, storagereplacement.ErrItemCancelled) {
		return nil, repository.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, repository.ErrConflict
	}
	return repos.Replacements.AttachTargetCopy(ctx, repository.AttachReplacementTargetCopyInput{ReplacementID: replacement.ID, ItemID: itemID, ContentID: snapshot.Upload.ID})
}

// waitForSourceWrites holds retirement while writes remain on the source. Once
// unsigned writes are handed over, only writes the old provider holds a signed
// authorization for are left, and they settle only through their own tasks.
func (h *Handler) waitForSourceWrites(
	ctx context.Context,
	execution taskengine.Execution,
	replacement *storagereplacement.Replacement,
	checkpoint replacementProgressCheckpoint,
	writes []model.StorageCopy,
) taskengine.Result {
	dependencies := make(map[int64]dependencyRetry)
	waiting := false
	retrying := false
	for i := range writes {
		taskID := writes[i].WorkTaskID()
		if taskID == nil {
			waiting = true
			continue
		}
		if _, known := dependencies[*taskID]; known {
			continue
		}
		source, err := h.deps.Repositories.Tasks.GetByID(ctx, *taskID)
		if err != nil {
			return h.retryReplacement(execution, replacement.ID, err, "replacement_source_writes_failed")
		}
		if source == nil || source.Status != model.TaskStatusFailed {
			waiting = true
			continue
		}
		decision, err := h.planDependencyRetry(ctx, execution, checkpoint, source)
		if err != nil {
			return h.retryReplacement(execution, replacement.ID, err, "replacement_retry_eligibility_failed")
		}
		dependencies[source.ID] = decision
		if !decision.at.IsZero() {
			waiting = true
			retrying = true
		}
	}
	const message = "The old provider did not finish writes it had already accepted, so it cannot be shut down yet. Retry the replacement after the old provider recovers."
	if !waiting && len(dependencies) > 0 {
		return taskengine.Fail(errors.New(message), "replacement_source_write_failed", func(ctx context.Context, repos *repository.Repositories) error { //nolint:staticcheck // ST1005: this error is displayed directly to the user.
			if _, err := repos.Replacements.LockCoordinatorTask(ctx, replacement.ID, replacement.TaskGeneration, execution.ID()); err != nil {
				return err
			}
			current, err := lockFailedDependencies(ctx, repos, dependencies)
			if err != nil {
				return err
			}
			for id, decision := range dependencies {
				if !current[id] {
					return repository.ErrConflict
				}
				owned, err := dependencyOwnerCurrent(ctx, repos, decision.source)
				if err != nil {
					return err
				}
				if !owned {
					return repository.ErrConflict
				}
			}
			return repos.Replacements.MarkFailed(ctx, replacement.ID, nil, message)
		})
	}
	var settlement taskengine.Settlement
	if retrying {
		settlement = func(ctx context.Context, repos *repository.Repositories) error {
			if _, err := repos.Replacements.LockCoordinatorTask(ctx, replacement.ID, replacement.TaskGeneration, execution.ID()); err != nil {
				return err
			}
			current, err := lockFailedDependencies(ctx, repos, dependencies)
			if err != nil {
				return err
			}
			progress := checkpoint
			progress.DependencyRetries = maps.Clone(checkpoint.DependencyRetries)
			if progress.DependencyRetries == nil {
				progress.DependencyRetries = make(map[string]int)
			}
			for _, id := range slices.Sorted(maps.Keys(dependencies)) {
				decision := dependencies[id]
				if !current[id] || decision.at.IsZero() {
					continue
				}
				owned, err := dependencyOwnerCurrent(ctx, repos, decision.source)
				if err != nil {
					return err
				}
				if !owned {
					continue
				}
				if _, err := h.deps.Scheduler.RetryInTransaction(ctx, repos, id, decision.at); err != nil {
					return err
				}
				progress.DependencyRetries[decision.key]++
			}
			return writeProgressCheckpoint(ctx, repos, execution, progress)
		}
	}
	return taskengine.Wait(model.TaskResumeModeRecover, "source_writes", "Waiting for writes on the old provider to finish", settlement)
}

func dependencyOwnerCurrent(ctx context.Context, repos *repository.Repositories, source *model.Task) (bool, error) {
	if source.SubjectType == model.TaskSubjectStorageCommitRequest {
		request, err := repos.Contents.GetCommitRequest(ctx, source.SubjectKey)
		return request != nil && request.TaskID != nil && *request.TaskID == source.ID, err
	}
	copyID, err := strconv.ParseInt(source.SubjectKey, 10, 64)
	if err != nil {
		return false, err
	}
	copyRow, err := repos.Contents.GetUploadCopyByID(ctx, copyID)
	if err != nil || copyRow == nil {
		return false, err
	}
	owner := copyRow.WorkTaskID()
	return owner != nil && *owner == source.ID, nil
}

func (h *Handler) failReplacement(replacementID int64, err error, reason string) taskengine.Result {
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Replacements.MarkFailed(ctx, replacementID, nil, errtext.Format(err))
	})
}

func (h *Handler) retryReplacement(execution taskengine.Execution, _ int64, err error, reason string) taskengine.Result {
	return taskengine.Retry(err, reason, nil).WithRetrySettlements(nil, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Replacements.FailForEngineTask(ctx, execution.ID(), replacementInternalErrorMessage)
	})
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
