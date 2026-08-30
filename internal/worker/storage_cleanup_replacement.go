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
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synapse-go/storage"
)

// processReplacementRetirementTask ends a replaced storage service. Every step
// is ordered so the destructive call sits between transactions and can never
// run against a stale view:
//
//	tx A   evaluate the safety gate, wait if anything still needs the source
//	 --    terminate the service with the provider
//	tx B   record the epoch at which the service ends
//	 --    observe the chain head
//	tx C   re-evaluate the whole gate, then retire and complete
//
// A blocker that can clear on its own returns to waiting; only a structural
// problem or a payment decision raises operator attention.
func (w *StorageCleanupWorker) processReplacementRetirementTask(ctx context.Context, task *model.Task) {
	logger := w.logger.With("taskID", task.ID)
	replacementID, err := storagereplacement.ParseRetirePayload(task)
	if err != nil {
		w.failReplacementRetirement(ctx, task, logger, "parse replacement retirement task", err)
		return
	}
	logger = logger.With("replacementID", replacementID)

	replacement, err := w.repos.Replacements.GetByID(ctx, replacementID)
	if err != nil {
		w.retryReplacementRetirement(ctx, task, replacementID, logger, "load provider replacement", err)
		return
	}
	if replacement == nil {
		w.completeTask(ctx, task, logger, "Replacement already retired")
		return
	}
	if task.Stage != nil && *task.Stage == storagereplacement.StageRetireAbandonedTarget {
		w.retireAbandonedTarget(ctx, task, replacement, logger)
		return
	}
	if replacement.Status == storagereplacement.StatusCompleted {
		w.completeTask(ctx, task, logger, "Replacement already retired")
		return
	}
	if replacement.Status == storagereplacement.StatusSuperseded {
		// A later confirmation owns this slot now. Retiring this replacement's
		// source would terminate a service the successor still depends on.
		w.completeTask(ctx, task, logger, "Replacement superseded")
		return
	}
	if replacement.Status == storagereplacement.StatusCleanupAttention {
		// Automatic retry is suppressed until an operator acts, so the task must
		// not keep re-running on its own.
		w.completeTask(ctx, task, logger, "Waiting for operator action")
		return
	}
	if w.terminator == nil || w.epochs == nil {
		w.retryReplacementRetirement(ctx, task, replacementID, logger, "retire replaced service",
			errors.New("service termination is not configured"))
		return
	}
	source, err := w.repos.Uploads.GetDataSetBindingByID(ctx, replacement.SourceDataSetID)
	if err != nil {
		w.retryReplacementRetirement(ctx, task, replacementID, logger, "load retiring data set", err)
		return
	}
	if source == nil || source.DataSetID == nil || source.DataSetID.IsZero() {
		w.retryReplacementRetirement(ctx, task, replacementID, logger, "load retiring data set",
			fmt.Errorf("data set %d has no on-chain identity: %w", replacement.SourceDataSetID, repository.ErrNotFound))
		return
	}
	if source.Status == model.StorageDataSetStatusRetired {
		w.completeTask(ctx, task, logger, "Replaced provider already retired")
		return
	}

	// tx A: nothing may still need the source before it is terminated.
	gate, err := w.repos.Replacements.EvaluateRetirementGate(ctx, replacementID, nil)
	if err != nil {
		w.retryReplacementRetirement(ctx, task, replacementID, logger, "evaluate retirement gate", err)
		return
	}
	if !gate.Passed() {
		w.waitForReplacementRetirement(ctx, task, replacementID, gate, logger)
		return
	}
	// Nothing needs the source any more, so the record should say retiring
	// rather than waiting while the destructive work runs.
	if replacement.Status != storagereplacement.StatusRetiring {
		if err := w.repos.Replacements.BeginRetirement(ctx, replacementID); err != nil {
			w.retryReplacementRetirement(ctx, task, replacementID, logger, "resume replacement retirement", err)
			return
		}
		replacement.Status = storagereplacement.StatusRetiring
	}

	if replacement.TerminationEpoch == nil {
		result, err := w.terminator.TerminateService(ctx, source.DataSetID.SDK())
		if err != nil {
			w.handleTerminationError(ctx, task, replacementID, logger, err)
			return
		}
		if result == nil {
			w.retryReplacementRetirement(ctx, task, replacementID, logger, "terminate replaced service",
				errors.New("storage service returned no termination result"))
			return
		}
		// tx B: the end of term is recorded before the service is treated as
		// terminated, so a crash here re-reads it instead of terminating twice.
		if err := w.repos.Replacements.RecordTerminationEpoch(ctx, repository.RecordTerminationEpochInput{
			ReplacementID: replacementID,
			TxHash:        result.TxHash,
			Epoch:         result.EndEpoch,
		}); err != nil {
			// A conflict means someone recorded an epoch first. Trust the stored
			// value rather than the one this attempt just observed, but never
			// continue without one.
			stored, loadErr := w.repos.Replacements.GetByID(ctx, replacementID)
			if loadErr != nil || stored == nil || stored.TerminationEpoch == nil {
				w.retryReplacementRetirement(ctx, task, replacementID, logger, "record termination epoch", err)
				return
			}
			replacement.TerminationEpoch = stored.TerminationEpoch
		} else {
			replacement.TerminationEpoch = &result.EndEpoch
		}
		logger.Info("replaced storage service termination submitted",
			"dataSetID", source.DataSetID.String(), "endEpoch", *replacement.TerminationEpoch)
	}

	observed, err := w.epochs.CurrentEpoch(ctx)
	if err != nil {
		w.retryReplacementRetirement(ctx, task, replacementID, logger, "read chain epoch", err)
		return
	}
	if replacement.TerminationEpoch == nil || observed < *replacement.TerminationEpoch {
		w.waitForReplacementEpoch(ctx, task, replacementID, logger)
		return
	}

	// tx C: the gate is evaluated again from scratch, this time including the
	// epoch, and the repository refuses the change if anything regressed.
	if err := w.repos.Replacements.CompleteRetirement(ctx, replacementID, observed); err != nil {
		w.handleRetirementCompletionError(ctx, task, replacementID, logger, err)
		return
	}
	logger.Info("replaced storage service retired", "dataSetID", source.DataSetID.String())
	w.completeTask(ctx, task, logger, "Replaced provider retired")
}

// retireAbandonedTarget ends the paid service of a target a later confirmation
// replaced. It asks a narrower question than source retirement: the abandoned
// generation only holds partly migrated data, so the gate is simply that
// nothing depends on it as its last readable copy. The replacement record stays
// superseded throughout.
func (w *StorageCleanupWorker) retireAbandonedTarget(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	logger *slog.Logger,
) {
	if w.terminator == nil || w.epochs == nil {
		// Abandoned cleanup has no operator retry: the replacement is superseded.
		// Waiting keeps the coordinator claimable until termination is configured.
		w.waitForAbandonedTarget(ctx, task, logger, "Waiting until unused storage services can be ended")
		return
	}
	target, err := w.repos.Uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil {
		w.waitForAbandonedTarget(ctx, task, logger, "Waiting to end the unused storage service")
		return
	}
	if target == nil || target.Status == model.StorageDataSetStatusRetired {
		w.completeTask(ctx, task, logger, "Abandoned provider already retired")
		return
	}
	if target.IsCurrent {
		// It took over the slot after all, so it is not abandoned.
		w.completeTask(ctx, task, logger, "Target is in use")
		return
	}
	if target.DataSetID == nil || target.DataSetID.IsZero() {
		if dataSetBindingHasCreationEvidence(target) {
			// A creation was submitted and may still land a paid service on this
			// provider. Retiring the local row now would release the provider
			// while that service exists and nothing is left watching for it, so
			// the submission is resolved first.
			w.resolveAbandonedTargetCreation(ctx, task, replacement, target, logger)
			return
		}
		// Nothing was ever asked of this provider, but the local generation still
		// holds it for this bucket: an unretired generation makes the provider
		// unavailable to both automatic selection and a manual choice. Release it.
		if err := w.releaseAbandonedTargetWithoutService(ctx, replacement, target,
			"provider replacement was superseded before a storage service was created"); err != nil {
			w.waitForAbandonedTarget(ctx, task, logger, "Waiting to release the unused storage provider")
			return
		}
		w.completeTask(ctx, task, logger, "Abandoned provider released without a service")
		return
	}

	sole, err := w.repos.Replacements.CountAbandonedTargetSoleCopies(ctx, target.ID)
	if err != nil {
		w.waitForAbandonedTarget(ctx, task, logger, "Waiting to end the unused storage service")
		return
	}
	if sole > 0 {
		logger.Warn("abandoned target still holds the only readable copy of some content",
			"dataSetID", target.DataSetID.String(), "count", sole)
		w.waitForAbandonedTarget(ctx, task, logger, "Waiting for content on the abandoned provider to be copied elsewhere")
		return
	}
	activeAttempts, err := w.repos.Uploads.CountActiveCommitAttemptsForDataSet(ctx, target.ID)
	if err != nil || activeAttempts > 0 {
		if err != nil {
			logger.Warn("failed to check abandoned target confirmation attempts", "error", err)
		} else {
			logger.Info("abandoned target still has active confirmation attempts", "count", activeAttempts)
		}
		w.waitForAbandonedTarget(ctx, task, logger, "Waiting for storage confirmations")
		return
	}

	endEpoch := replacement.AbandonedTerminationEpoch
	if endEpoch == nil {
		result, terminateErr := w.terminator.TerminateService(ctx, target.DataSetID.SDK())
		if terminateErr != nil {
			switch {
			case synapse.IsTerminationBlocked(terminateErr):
				w.waitForAbandonedTarget(ctx, task, logger, "Waiting for outstanding payment to be settled")
			case synapse.IsProviderUnavailable(terminateErr):
				w.waitForAbandonedTarget(ctx, task, logger, storagereplacement.WaitReasonProvider.Message())
			default:
				w.waitForAbandonedTarget(ctx, task, logger, "Waiting to end the unused storage service")
			}
			return
		}
		if result == nil {
			w.waitForAbandonedTarget(ctx, task, logger, "Waiting to record the unused storage service end epoch")
			return
		}
		if err := w.repos.Replacements.RecordAbandonedTerminationEpoch(ctx, repository.RecordTerminationEpochInput{
			ReplacementID: replacement.ID,
			TxHash:        result.TxHash,
			Epoch:         result.EndEpoch,
		}); err != nil {
			w.waitForAbandonedTarget(ctx, task, logger, "Waiting to record the unused storage service end epoch")
			return
		}
		endEpoch = &result.EndEpoch
	}
	observed, err := w.epochs.CurrentEpoch(ctx)
	if err != nil {
		w.waitForAbandonedTarget(ctx, task, logger, "Waiting to end the unused storage service")
		return
	}
	if observed < *endEpoch {
		w.waitForAbandonedTarget(ctx, task, logger, storagereplacement.WaitReasonTerminationEpoch.Message())
		return
	}
	if err := w.repos.Replacements.CompleteAbandonedTargetTermination(ctx, replacement.ID, time.Now()); err != nil {
		w.waitForAbandonedTarget(ctx, task, logger, "Waiting to end the unused storage service")
		return
	}
	logger.Info("abandoned replacement target retired", "dataSetID", target.DataSetID.String())
	w.completeTask(ctx, task, logger, "Abandoned provider retired")
}

// A recoverable blocker keeps the replacement waiting; the operator sees which
// predicate is holding it.
func (w *StorageCleanupWorker) waitForReplacementRetirement(
	ctx context.Context,
	task *model.Task,
	replacementID int64,
	gate repository.RetirementGate,
	logger *slog.Logger,
) {
	reason := retirementWaitReason(gate)
	if reason == storagereplacement.WaitReasonCoverage && !gate.SlotOwned {
		// The generations are not in the shape retirement assumes. Retrying
		// cannot fix that, so it needs an operator.
		w.raiseCleanupAttention(ctx, task, replacementID, logger,
			fmt.Sprintf("replica slot ownership is inconsistent: %v", gate.Blockers))
		return
	}
	if err := w.repos.Replacements.MarkWaiting(ctx, replacementID, reason); err != nil {
		logger.Warn("failed to record retirement wait reason", "reason", reason, "error", err)
	}
	logger.Debug("retirement gate is blocked", "blockers", gate.Blockers)
	w.waitForReferences(ctx, task, logger, reason.Message())
}

// retirementWaitReason names the first blocker so the operator sees the reason
// closest to the source of the delay.
func retirementWaitReason(gate repository.RetirementGate) storagereplacement.WaitReason {
	if !gate.SlotOwned {
		return storagereplacement.WaitReasonCoverage
	}
	switch {
	case gate.WaitingItems > 0:
		return storagereplacement.WaitReasonReadableSource
	case gate.CoverageGaps > 0:
		return storagereplacement.WaitReasonCoverage
	case gate.SourceWrites > 0:
		return storagereplacement.WaitReasonSourceWrites
	default:
		return storagereplacement.WaitReasonTerminationEpoch
	}
}

func (w *StorageCleanupWorker) waitForReplacementEpoch(ctx context.Context, task *model.Task, replacementID int64, logger *slog.Logger) {
	if err := w.repos.Replacements.MarkWaiting(ctx, replacementID, storagereplacement.WaitReasonTerminationEpoch); err != nil {
		logger.Warn("failed to record termination epoch wait", "error", err)
	}
	if err := w.repos.Tasks.WaitRunning(ctx, task, model.TaskWaitReasonExternalConfirmation,
		storagereplacement.WaitReasonTerminationEpoch.Message(), storageCleanupConfirmationDelay); err != nil {
		logger.Error("failed to wait for termination epoch", "taskID", task.ID, "error", err)
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return
	}
	admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "success").Inc()
}

func (w *StorageCleanupWorker) handleTerminationError(ctx context.Context, task *model.Task, replacementID int64, logger *slog.Logger, err error) {
	switch {
	case synapse.IsTerminationBlocked(err):
		// Settling debt is the operator's decision, never the gateway's.
		w.raiseCleanupAttention(ctx, task, replacementID, logger, err.Error())
	case synapse.IsProviderUnavailable(err):
		if markErr := w.repos.Replacements.MarkWaiting(ctx, replacementID, storagereplacement.WaitReasonProvider); markErr != nil {
			logger.Warn("failed to record provider wait reason", "error", markErr)
		}
		w.waitForReferences(ctx, task, logger, storagereplacement.WaitReasonProvider.Message())
	default:
		w.retryReplacementRetirement(ctx, task, replacementID, logger, "terminate replaced service", err)
	}
}

// CompleteRetirement re-checks everything itself, so a refusal here means the
// world changed between the first gate and the final transaction.
func (w *StorageCleanupWorker) handleRetirementCompletionError(ctx context.Context, task *model.Task, replacementID int64, logger *slog.Logger, err error) {
	if errors.Is(err, storagereplacement.ErrPrematureComplete) {
		gate, gateErr := w.repos.Replacements.EvaluateRetirementGate(ctx, replacementID, nil)
		if gateErr != nil {
			w.retryReplacementRetirement(ctx, task, replacementID, logger, "re-evaluate retirement gate", gateErr)
			return
		}
		w.waitForReplacementRetirement(ctx, task, replacementID, gate, logger)
		return
	}
	w.retryReplacementRetirement(ctx, task, replacementID, logger, "complete replacement retirement", err)
}

// raiseCleanupAttention stops the task in the same step that records why, so
// automatic retry can never resume suppressed cleanup.
func (w *StorageCleanupWorker) raiseCleanupAttention(ctx context.Context, task *model.Task, replacementID int64, logger *slog.Logger, message string) {
	err := w.repos.WithTx(ctx, func(txRepos *repository.Repositories) error {
		if err := txRepos.Replacements.MarkCleanupAttention(ctx, replacementID, message); err != nil {
			return err
		}
		return txRepos.Tasks.FailRunning(ctx, task, message)
	})
	if err != nil {
		logger.Error("failed to raise replacement cleanup attention", "error", err)
		admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
		return
	}
	logger.Warn("replacement retirement needs operator action", "reason", message)
	admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
}

func (w *StorageCleanupWorker) retryReplacementRetirement(
	ctx context.Context,
	task *model.Task,
	replacementID int64,
	logger *slog.Logger,
	stage string,
	err error,
) {
	logger.Error(stage+" failed", "error", err)
	status := scheduleTaskRetry(ctx, w.repos, task, "storage_cleanup", logger, err)
	// A zero replacement id means the caller owns no replacement state to move,
	// which is the case for abandoned target cleanup.
	if status == model.TaskStatusExhausted && replacementID > 0 {
		// Cleanup that ran out of attempts is an operator decision, not a
		// failure to retry, and the record must land even during shutdown.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalFailureCleanupTimeout)
		defer cancel()
		if markErr := w.repos.Replacements.MarkCleanupAttention(cleanupCtx, replacementID,
			fmt.Sprintf("%s: %v (max retries reached)", stage, err)); markErr != nil {
			logger.Error("failed to record replacement cleanup attention", "error", markErr)
		}
	}
	admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
}

func (w *StorageCleanupWorker) failReplacementRetirement(ctx context.Context, task *model.Task, logger *slog.Logger, stage string, err error) {
	logger.Error(stage+" failed", "error", err)
	if failErr := w.repos.Tasks.FailRunning(ctx, task, err.Error()); failErr != nil {
		logger.Error("failed to stop replacement retirement task", "error", failErr)
	}
	admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
}

// releaseAbandonedTargetWithoutService retires a local generation that never
// received an on-chain identity. Leaving it unretired would block that provider
// from every later confirmation for the bucket.
func (w *StorageCleanupWorker) releaseAbandonedTargetWithoutService(
	ctx context.Context,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	lastError string,
) error {
	if target != nil {
		switch target.Status {
		case model.StorageDataSetStatusPending, model.StorageDataSetStatusCreating, model.StorageDataSetStatusFailed:
			if err := w.repos.Uploads.MarkDataSetFailed(ctx, target.ID, lastError); err != nil && !errors.Is(err, repository.ErrConflict) {
				return err
			}
		}
	}
	return w.repos.Replacements.RetireAbandonedTarget(ctx, replacement.ID)
}

// Observation failures never produced a data set id. After the last retry the
// provider must be released: superseded replacements have no Data Sets retry.
func (w *StorageCleanupWorker) retryAbandonedTargetObservation(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	logger *slog.Logger,
	stage string,
	err error,
) {
	if synapse.IsProviderUnavailable(err) {
		w.waitForReferences(ctx, task, logger, storagereplacement.WaitReasonProvider.Message())
		return
	}
	logger.Error(stage+" failed", "error", err)
	status := scheduleTaskRetry(ctx, w.repos, task, "storage_cleanup", logger, err)
	if status == model.TaskStatusExhausted {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalFailureCleanupTimeout)
		defer cancel()
		if releaseErr := w.releaseAbandonedTargetWithoutService(cleanupCtx, replacement, target,
			fmt.Sprintf("%s: %v (max retries reached)", stage, err)); releaseErr != nil {
			logger.Error("failed to release abandoned target after observation retries", "error", releaseErr)
		}
	}
	admin.WorkerTasksProcessed.WithLabelValues("storage_cleanup", "failure").Inc()
}

// resolveAbandonedTargetCreation finishes a data set creation the superseded
// confirmation left in flight. Until it resolves, the generation has neither a
// service to end nor proof that none exists, and the provider stays reserved.
func (w *StorageCleanupWorker) resolveAbandonedTargetCreation(
	ctx context.Context,
	task *model.Task,
	replacement *storagereplacement.Replacement,
	target *model.StorageDataSet,
	logger *slog.Logger,
) {
	if target.CreateTransactionID == nil || target.CreateStatusURL == nil || target.ClientDataSetID == nil {
		// The submission left no way to observe its outcome. Treating it as a
		// live service would reserve the provider forever, so it is recorded as
		// failed and the generation released.
		if err := w.releaseAbandonedTargetWithoutService(ctx, replacement, target,
			"provider replacement was superseded before the service could be observed"); err != nil {
			w.waitForAbandonedTarget(ctx, task, logger, "Waiting to release the unused storage provider")
			return
		}
		w.completeTask(ctx, task, logger, "Abandoned provider released; its service could not be observed")
		return
	}
	bucket, err := w.repos.Buckets.GetByID(ctx, replacement.BucketID)
	if err != nil || bucket == nil {
		if err == nil {
			err = fmt.Errorf("bucket %d: %w", replacement.BucketID, repository.ErrNotFound)
		}
		w.retryAbandonedTargetObservation(ctx, task, replacement, target, logger, "load bucket for abandoned target", err)
		return
	}
	if w.storage == nil {
		w.retryAbandonedTargetObservation(ctx, task, replacement, target, logger, "open abandoned target context",
			errors.New("storage client is not configured"))
		return
	}
	storageCtx, err := w.storage.OpenProviderTarget(ctx, target.ProviderID.SDK(), storage.NewProviderContextOptions{
		DataSetMetadata: map[string]string{"bucket": bucket.Name},
	})
	if err != nil || storageCtx == nil {
		if err == nil {
			err = errors.New("storage context resolver returned no context")
		}
		w.retryAbandonedTargetObservation(ctx, task, replacement, target, logger, "open abandoned target context", err)
		return
	}
	result, err := storageCtx.WaitForDataSetCreated(ctx, storage.CreateDataSetSubmission{
		ProviderID:      target.ProviderID.SDK(),
		TransactionID:   *target.CreateTransactionID,
		StatusURL:       *target.CreateStatusURL,
		ClientDataSetID: sdkBigIntPtr(target.ClientDataSetID),
	})
	if err != nil {
		w.retryAbandonedTargetObservation(ctx, task, replacement, target, logger, "resolve abandoned target creation", err)
		return
	}
	dataSetID, clientDataSetID, err := dataSetResultIDsForBinding(target, result)
	if err != nil {
		w.retryAbandonedTargetObservation(ctx, task, replacement, target, logger, "validate abandoned target creation", err)
		return
	}
	if err := w.repos.Uploads.MarkDataSetReady(ctx, repository.MarkDataSetReadyInput{
		ID:              target.ID,
		DataSetID:       dataSetID,
		ClientDataSetID: &clientDataSetID,
	}); err != nil {
		// The chain service exists. Releasing the local row would orphan it, and
		// exhausting the task would leave no resume path, so wait and persist again.
		w.waitForAbandonedTarget(ctx, task, logger, "Waiting to record the unused storage service")
		return
	}
	// The service now has an identity, so the next run ends it through the gate.
	w.waitForAbandonedTarget(ctx, task, logger, "Ending the unused storage service")
}

// waitForAbandonedTarget parks leftover cleanup without burning retries or
// moving the superseded replacement. Startup can still see a waiting task, and
// ResumeCoordinator revives one that previously failed or exhausted.
func (w *StorageCleanupWorker) waitForAbandonedTarget(ctx context.Context, task *model.Task, logger *slog.Logger, message string) {
	w.waitForReferences(ctx, task, logger, message)
}
