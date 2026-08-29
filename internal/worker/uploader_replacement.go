package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/objectlimits"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
)

// One bounded window of upload history per seeding pass. Small enough that the
// write stays short on SQLite's single writer, large enough that a long history
// still drains in a reasonable number of passes.
const replacementSeedBatchSize = 200

func (u *Uploader) processReplacementTask(ctx context.Context, task *model.Task, logger *slog.Logger) {
	if task == nil || task.ClaimedAt == nil {
		return
	}
	payload, err := storagereplacement.ParseMigratePayload(task)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "parse replacement migration task", err)
		return
	}
	replacement, err := u.repos.Replacements.GetByID(ctx, payload.ReplacementID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load provider replacement", err)
		return
	}
	if replacement == nil || replacement.Status == storagereplacement.StatusCompleted || replacement.Status.Retryable() {
		// A finished or operator-owned replacement has no work the coordinator
		// may do on its own.
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	if replacement.Status == storagereplacement.StatusSuperseded {
		// The successor owns the slot. This coordinator's leftover is the unused
		// target, which must still be ended even if confirmation already queued it.
		if err := u.ensureAbandonedTargetTask(ctx, replacement.ID, replacement.BucketID, task.MaxRetries); err != nil {
			u.handleReplacementTaskFailure(ctx, task, 0, logger, "queue abandoned replacement cleanup", err)
			return
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
		return
	}
	target, err := u.repos.Uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "load replacement target", err)
		return
	}
	if target == nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "load replacement target",
			fmt.Errorf("target data set %d: %w", replacement.TargetDataSetID, repository.ErrNotFound))
		return
	}

	switch replacementPhase(replacement.Status, target) {
	case storagereplacement.PhasePrepare:
		u.prepareReplacementTarget(ctx, task, replacement, target, logger)
	case storagereplacement.PhaseMigrate:
		u.migrateReplacementItem(ctx, task, replacement, target, payload, logger)
	case storagereplacement.PhaseRetire:
		// Retirement belongs to the cleanup worker; the migration coordinator's
		// job is finished once it has handed over.
		if err := u.ensureReplacementRetirementTask(ctx, replacement.ID, replacement.BucketID, task.MaxRetries); err != nil {
			u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "queue replacement retirement", err)
			return
		}
		completeWorkerTask(ctx, u.repos, task, "uploader", logger)
	default:
		// An unrecognised phase is a bug, not an outage. Stop the task without
		// touching the replacement so the record still describes reality.
		u.failReplacementTaskWithoutMutation(ctx, task, logger,
			fmt.Errorf("replacement %d has no coordinator work in status %s", replacement.ID, replacement.Status))
	}
}

// A waiting replacement resumes at the phase it was waiting in, which is
// recovered from the data rather than remembered in the status.
func replacementPhase(status storagereplacement.Status, target *model.StorageDataSet) storagereplacement.Phase {
	if phase := storagereplacement.PhaseFor(status); phase != storagereplacement.PhaseNone {
		return phase
	}
	if status != storagereplacement.StatusWaiting {
		return storagereplacement.PhaseNone
	}
	if target != nil && target.IsCurrent {
		return storagereplacement.PhaseMigrate
	}
	return storagereplacement.PhasePrepare
}

// prepareReplacementTarget creates the approved service and, once it is
// writable, switches the slot over. Until that switch every write still goes to
// the source, so a failure here costs nothing but time.
func (u *Uploader) prepareReplacementTarget(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	logger *slog.Logger,
) {
	bucket, err := u.repos.Buckets.GetByID(ctx, replacement.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = fmt.Errorf("bucket %d: %w", replacement.BucketID, repository.ErrNotFound)
		}
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "load replacement bucket", err)
		return
	}
	if target.Status != model.StorageDataSetStatusReady {
		if target.Status == model.StorageDataSetStatusPending || target.Status == model.StorageDataSetStatusFailed {
			if !u.ensureReplacementFundingReady(ctx, task, replacement, target, bucket, logger) {
				return
			}
		}
		ready, err := u.createReplacementDataSet(ctx, replacement, target, bucket)
		if err != nil {
			if errors.Is(err, storagereplacement.ErrTargetInUse) {
				// Retrying cannot change this: the operator has to pick a
				// provider that is free. The source still owns the replica at
				// this point, so a new confirmation is available to them.
				u.failReplacementTarget(ctx, task, replacement.ID, logger, err.Error())
				return
			}
			u.handleReplacementProviderFailure(ctx, task, replacement, logger, "create replacement service", err)
			return
		}
		if !ready {
			u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonTargetCreating, logger,
				storagereplacement.WaitReasonTargetCreating.Message())
			return
		}
	}
	if err := u.repos.Replacements.Activate(ctx, replacement.ID); err != nil {
		if errors.Is(err, repository.ErrConflict) {
			u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonTargetWritable, logger,
				storagereplacement.WaitReasonTargetWritable.Message())
			return
		}
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "activate replacement target", err)
		return
	}
	logger.Info("provider replacement activated",
		"replacementID", replacement.ID, "bucketID", replacement.BucketID, "copyIndex", replacement.CopyIndex)
	u.continueReplacementTask(ctx, task, replacement.ID, "", logger)
}

// createReplacementDataSet drives one data set creation step and reports
// whether the service is ready. It mirrors the ordinary upload path but does
// not touch any copy row, because the target holds no data yet.
func (u *Uploader) createReplacementDataSet(
	ctx context.Context,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	bucket *model.Bucket,
) (bool, error) {
	storageCtx, err := u.contextForBindingProvider(ctx, target, bucket.Name)
	if err != nil {
		return false, err
	}
	switch target.Status {
	case model.StorageDataSetStatusPending, model.StorageDataSetStatusFailed:
		// Nothing has been submitted for this generation yet, so a context that
		// already carries a data set can only be somebody else's: this provider
		// still runs a live service for this bucket, a generation released
		// locally without being terminated on chain. A replacement must open its
		// own paid service -- attaching here would leave it paying for, and
		// later retiring, a service it does not own.
		//
		// A creation this replacement did submit is resumed by the creating
		// branch below, which resolves the recorded transaction instead.
		matchingRef, err := u.storage.FindMatchingDataSet(
			ctx,
			target.ProviderID.SDK(),
			map[string]string{"bucket": bucket.Name},
			storageCtx.CDNEnabled(),
		)
		if err != nil {
			return false, err
		}
		if matchingRef != nil {
			return false, fmt.Errorf("provider %s already runs data set %s for this bucket: %w",
				target.ProviderID.String(), idtypes.OnChainIDFromSDK(matchingRef.DataSetID()).String(),
				storagereplacement.ErrTargetInUse)
		}
		var submitted storage.CreateDataSetSubmission
		var submitErr error
		result, err := storageCtx.CreateDataSet(ctx, &storage.CreateDataSetOptions{
			OnSubmitted: func(sub storage.CreateDataSetSubmission) {
				submitted = sub
				submitErr = u.repos.Uploads.MarkDataSetCreating(ctx, repository.MarkDataSetCreatingInput{
					ID:              target.ID,
					TransactionID:   sub.TransactionID,
					StatusURL:       sub.StatusURL,
					ClientDataSetID: onChainIDPtrFromSDKPtr(sub.ClientDataSetID),
				})
			},
		})
		if submitted.TransactionID != "" && submitErr != nil {
			return false, fmt.Errorf("save replacement service submission: %w", submitErr)
		}
		if err != nil {
			return false, err
		}
		dataSetID, clientDataSetID, err := dataSetResultIDsForBinding(target, result)
		if err != nil {
			return false, err
		}
		return true, u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID:              target.ID,
			DataSetID:       dataSetID,
			ClientDataSetID: &clientDataSetID,
		})
	case model.StorageDataSetStatusCreating:
		if target.CreateTransactionID == nil || target.CreateStatusURL == nil || target.ClientDataSetID == nil {
			return false, errDataSetCreationIncomplete
		}
		result, err := storageCtx.WaitForDataSetCreated(ctx, storage.CreateDataSetSubmission{
			ProviderID:      target.ProviderID.SDK(),
			TransactionID:   *target.CreateTransactionID,
			StatusURL:       *target.CreateStatusURL,
			ClientDataSetID: sdkBigIntPtr(target.ClientDataSetID),
		})
		if err != nil {
			return false, err
		}
		dataSetID, clientDataSetID, err := dataSetResultIDsForBinding(target, result)
		if err != nil {
			return false, err
		}
		return true, u.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
			ID:              target.ID,
			DataSetID:       dataSetID,
			ClientDataSetID: &clientDataSetID,
		})
	default:
		return false, fmt.Errorf("replacement %d target status %s cannot be prepared", replacement.ID, target.Status)
	}
}

func (u *Uploader) migrateReplacementItem(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	_ storagereplacement.MigratePayload,
	logger *slog.Logger,
) {
	if replacement.Status == storagereplacement.StatusWaiting &&
		(replacement.WaitReason == nil || *replacement.WaitReason != storagereplacement.WaitReasonReadableSource) {
		bucket, err := u.repos.Buckets.GetByID(ctx, replacement.BucketID)
		if err != nil || bucket == nil {
			if err == nil {
				err = fmt.Errorf("bucket %d: %w", replacement.BucketID, repository.ErrNotFound)
			}
			u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "load replacement bucket", err)
			return
		}
		if _, err := u.contextForReadyBinding(ctx, target); err != nil {
			u.handleReplacementProviderFailure(ctx, task, replacement, logger, "check replacement target", err)
			return
		}
		if err := u.repos.Replacements.MarkMigrating(ctx, replacement.ID); err != nil {
			u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "resume replacement migration", err)
			return
		}
	}
	if !replacement.SeedingComplete {
		if _, _, err := u.repos.Replacements.SeedMigrationBatchWithBudget(
			ctx, replacement.ID, replacementSeedBatchSize, u.replacementMaxRetries,
		); err != nil {
			u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "seed replacement migration", err)
			return
		}
	}
	execution, err := u.repos.Replacements.ReplacementExecution(ctx, replacement.ID)
	if err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "inspect replacement migration", err)
		return
	}
	if execution.SeedingComplete && !execution.HasPending && !execution.HasActive &&
		!execution.HasRetrying && !execution.HasWaitingSource {
		if execution.HasFailed {
			message := "Stored content needs attention"
			u.failReplacementCoordinator(ctx, task, replacement.ID, nil, logger, message)
			return
		}
		u.finishReplacementMigration(ctx, task, replacement, logger)
		return
	}
	if execution.SeedingComplete && !execution.HasPending && !execution.HasActive &&
		!execution.HasRetrying && execution.HasWaitingSource {
		u.waitForReplacementDependencyAfter(ctx, task, replacement, storagereplacement.WaitReasonReadableSource,
			logger, storagereplacement.WaitReasonReadableSource.Message(), 5*time.Second)
		return
	}
	u.waitForReplacementCoordinatorPoll(ctx, task, logger, execution)
}

func (u *Uploader) waitForReplacementCoordinatorPoll(
	ctx context.Context,
	task *model.Task,
	logger *slog.Logger,
	execution storagereplacement.ExecutionSnapshot,
) {
	message := "Discovering stored content to move"
	if execution.SeedingComplete {
		message = fmt.Sprintf("Moving stored content (%d of %d copied)", execution.ItemsCopied, execution.ItemsTotal)
	}
	if err := u.repos.Tasks.WaitRunning(ctx, task, model.TaskWaitReasonDependency, message, 5*time.Second); err != nil {
		u.handleReplacementTaskFailure(ctx, task, execution.ReplacementID, logger, "wait for replacement items", err)
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

func (u *Uploader) finishReplacementMigration(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	logger *slog.Logger,
) {
	if err := u.repos.Replacements.BeginRetirement(ctx, replacement.ID); err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "begin replacement retirement", err)
		return
	}
	if err := u.ensureReplacementRetirementTask(ctx, replacement.ID, replacement.BucketID, task.MaxRetries); err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "queue replacement retirement", err)
		return
	}
	logger.Info("provider replacement migration complete", "replacementID", replacement.ID)
	completeWorkerTask(ctx, u.repos, task, "uploader", logger)
}

func (u *Uploader) ensureReplacementRetirementTask(ctx context.Context, replacementID, bucketID int64, maxRetries int) error {
	_, err := u.repos.Tasks.EnsureRecurring(ctx, storagereplacement.NewRetireTask(replacementID, bucketID, maxRetries, time.Now()))
	if err != nil {
		return fmt.Errorf("ensure replacement retirement task for replacement %d: %w", replacementID, err)
	}
	return nil
}

func (u *Uploader) ensureAbandonedTargetTask(ctx context.Context, replacementID, bucketID int64, maxRetries int) error {
	_, err := u.repos.Tasks.EnsureRecurring(ctx, storagereplacement.NewAbandonedTargetTask(replacementID, bucketID, maxRetries, time.Now()))
	if err != nil {
		return fmt.Errorf("ensure abandoned replacement cleanup for replacement %d: %w", replacementID, err)
	}
	return nil
}

// ensureReplacementFundingReady waits, without burning retries, until the wallet
// can pay for the approved service. Ordinary uploads already do this; creating
// a replacement service is the same kind of paid work.
func (u *Uploader) ensureReplacementFundingReady(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	bucket *model.Bucket,
	logger *slog.Logger,
) bool {
	storageCtx, err := u.contextForBindingProvider(ctx, target, bucket.Name)
	if err != nil {
		u.handleReplacementProviderFailure(ctx, task, replacement, logger, "open replacement funding context", err)
		return false
	}
	costs, err := u.storage.PrepareUpload(ctx, uint64(objectlimits.MinFOCUploadSize), []synapse.StorageTarget{storageCtx})
	if err != nil {
		u.handleReplacementProviderFailure(ctx, task, replacement, logger, "prepare replacement funding", err)
		return false
	}
	if costs == nil {
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, "prepare replacement funding",
			errors.New("missing storage cost estimate"))
		return false
	}
	if costs.Ready {
		return true
	}
	u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonFunding, logger,
		uploadFundingWaitMessage(costs))
	return false
}

func (u *Uploader) continueReplacementTask(
	ctx context.Context,
	task *model.Task,
	replacementID int64,
	versionID string,
	logger *slog.Logger,
) {
	err := u.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if err := txRepos.Tasks.LockRunningClaim(ctx, task); err != nil {
			return err
		}
		return txRepos.Tasks.ContinueRunning(ctx, task, versionID, storagereplacement.NewMigratePayload(replacementID))
	})
	if err != nil {
		u.handleReplacementTaskFailure(ctx, task, replacementID, logger, "advance replacement task", err)
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

// waitForReplacementDependency records why progress paused without consuming
// retry budget. Waiting is never a failure.
func (u *Uploader) waitForReplacementDependency(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	reason storagereplacement.WaitReason,
	logger *slog.Logger,
	message string,
) {
	u.waitForReplacementDependencyAfter(ctx, task, replacement, reason, logger, message, uploadDependencyWaitDelay)
}

func (u *Uploader) waitForReplacementDependencyAfter(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	reason storagereplacement.WaitReason,
	logger *slog.Logger,
	message string,
	delay time.Duration,
) {
	err := u.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if err := txRepos.Replacements.MarkWaiting(ctx, replacement.ID, reason); err != nil {
			return err
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := txRepos.Tasks.WaitRunning(ctx, task, model.TaskWaitReasonDependency, message, delay); err != nil {
			return err
		}
		return context.Cause(ctx)
	})
	if err != nil {
		logger.Error("failed to wait for replacement dependency",
			"replacementID", replacement.ID, "taskID", task.ID, "reason", reason, "error", err)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "success").Inc()
}

// handleReplacementProviderFailure keeps a recoverable provider problem in
// waiting and reserves retries for genuinely unknown failures.
func (u *Uploader) handleReplacementProviderFailure(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	logger *slog.Logger,
	stage string,
	err error,
) {
	if u.waitForPendingSubmittedCommit(ctx, task, logger, err) {
		return
	}
	switch {
	case synapse.IsProviderUnavailable(err), synapse.IsNoProviderCandidates(err):
		u.waitForReplacementDependency(ctx, task, replacement, storagereplacement.WaitReasonTarget, logger,
			storagereplacement.WaitReasonTarget.Message())
	case dataSetWriteBlockedError(err), synapse.IsDataSetServiceEnded(err):
		// The approved target itself ended. That needs a new confirmation, so it
		// is operator work rather than another attempt.
		u.failReplacementCoordinator(ctx, task, replacement.ID, nil, logger, fmt.Sprintf("%s: %v", stage, err))
	default:
		u.handleReplacementTaskFailure(ctx, task, replacement.ID, logger, stage, err)
	}
}

// handleReplacementTaskFailure retries, and records the replacement as failed
// only once the task has genuinely run out of attempts.
func (u *Uploader) handleReplacementTaskFailure(
	ctx context.Context,
	task *model.Task,
	replacementID int64,
	logger *slog.Logger,
	stage string,
	err error,
) {
	logger.Error(stage+" failed", "replacementID", replacementID, "error", err)
	if replacementID <= 0 {
		scheduleTaskRetry(ctx, u.repos, task, "uploader", logger, err)
		admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
		return
	}
	message := fmt.Sprintf("%s: %v", stage, err)
	status, retryErr := u.repos.Replacements.ScheduleCoordinatorRetry(ctx, repository.ReplacementCoordinatorRetryInput{
		ReplacementID: replacementID,
		Task:          task,
		LastError:     message,
		Backoff:       retryDelay(task.RetryCount),
	})
	if retryErr != nil {
		logger.Error("failed to schedule provider replacement coordinator retry",
			"replacementID", replacementID, "error", retryErr)
	} else if status == model.TaskStatusExhausted {
		admin.TasksExhaustedTotal.WithLabelValues("uploader", string(task.Type)).Inc()
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

func (u *Uploader) failReplacementCoordinator(
	ctx context.Context,
	task *model.Task,
	replacementID int64,
	reason *storagereplacement.FailureReason,
	logger *slog.Logger,
	message string,
) {
	err := u.repos.Replacements.FailCoordinator(ctx, repository.ReplacementCoordinatorFailureInput{
		ReplacementID: replacementID,
		Task:          task,
		FailureReason: reason,
		LastError:     message,
	})
	if err != nil {
		logger.Error("failed to stop provider replacement coordinator",
			"replacementID", replacementID, "error", err)
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

// failReplacementTarget records an approved target that can never work and
// stops its coordinator in the same transaction, so the record never shows work
// in progress with nothing queued to do it. Retrying is pointless here, so the
// retry budget is not spent first.
func (u *Uploader) failReplacementTarget(ctx context.Context, task *model.Task, replacementID int64, logger *slog.Logger, message string) {
	reason := storagereplacement.FailureReasonTargetInUse
	err := u.repos.Replacements.FailCoordinator(ctx, repository.ReplacementCoordinatorFailureInput{
		ReplacementID: replacementID,
		Task:          task,
		FailureReason: &reason,
		LastError:     message,
	})
	if err != nil {
		logger.Error("failed to record unusable replacement target",
			"replacementID", replacementID, "error", err)
	} else {
		logger.Warn("provider replacement needs a different provider",
			"replacementID", replacementID, "reason", message)
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

// failReplacementTaskWithoutMutation stops a task that must not retry while
// leaving the replacement record exactly as it is.
func (u *Uploader) failReplacementTaskWithoutMutation(ctx context.Context, task *model.Task, logger *slog.Logger, err error) {
	logger.Error("replacement coordinator stopped", "taskID", task.ID, "error", err)
	if failErr := u.repos.Tasks.FailRunning(ctx, task, err.Error()); failErr != nil {
		logger.Error("failed to stop replacement coordinator", "taskID", task.ID, "error", failErr)
	}
	admin.WorkerTasksProcessed.WithLabelValues("uploader", "failure").Inc()
}

// deferToReplacement yields an ordinary upload stage to the replacement
// coordinator when both target the same copy. It mirrors the recovered-replica
// gate so the two coordinators and normal uploads never write one row at once.
func (u *Uploader) deferToReplacement(ctx context.Context, task *model.Task, bucketID, uploadID int64, copyIndex int, logger *slog.Logger) bool {
	copyRow, err := u.taskUploadCopy(ctx, task, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load upload copy for replacement coordination", err)
		return true
	}
	// The write belongs to the generation its copy is bound to. Asking the slot
	// instead would stop deferring the moment the replacement takes the slot,
	// which is exactly when the two writers overlap.
	binding, err := u.taskCopyDataSet(ctx, task, bucketID, uploadID, copyIndex)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "load upload data set", err)
		return true
	}
	if binding == nil {
		return false
	}
	replacement, err := u.repos.Replacements.GetActiveForDataSet(ctx, binding.ID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "check provider replacement", err)
		return true
	}
	if replacement == nil {
		return false
	}
	if binding.ID != replacement.TargetDataSetID {
		return false
	}
	if copyRow == nil {
		u.handleTaskFailure(ctx, task, logger, "load upload copy for replacement coordination",
			fmt.Errorf("upload copy %d not found", copyIndex))
		return true
	}
	// The item claim is visible before AttachTargetCopy and is compared by claim time.
	itemClaim, err := u.repos.Replacements.RunningReplacementItemClaimForUpload(ctx, replacement.ID, uploadID)
	if err != nil {
		u.handleTaskFailure(ctx, task, logger, "check replacement item in progress", err)
		return true
	}
	// Ordinary uploads win an exact timestamp tie. The replacement side uses
	// claimed_at <= its own claim, so both paths now apply the same total order.
	if itemClaim == nil || task.ClaimedAt == nil || !itemClaim.ClaimedAt.Before(*task.ClaimedAt) {
		return false
	}
	u.waitForStorageDependency(ctx, task, logger, "Waiting for the approved provider replacement")
	return true
}
