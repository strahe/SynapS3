package transfer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagepull"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/pdp"
	"github.com/strahe/synapse-go/storage"
)

func (h *PullHandler) pullWithoutSource(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput, copyRow *model.StorageCopy) taskengine.Result {
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
		err := errors.New("stored content migration has no readable source or local cache")
		if copyRow.CommitDecidedByRequest() {
			return h.failPullTask(execution, input, copyRow, "", err, "migration_cache_missing")
		}
		return taskengine.Fail(err, "migration_cache_missing", nil)
	}
	return h.advanceToCacheRestore(input, execution.ID(), "Storage copy is recovering from cache", "")
}

func (h *PullHandler) recoverPullFromCache(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput, copyRow *model.StorageCopy, pullAttemptID, lastError string) (taskengine.Result, bool) {
	migration, err := h.CopyCoordinator.deps.Repositories.Contents.IsPendingReplacementCopy(ctx, copyRow.ID)
	if err != nil {
		return retryPullDependency(execution, err, "replacement_load_failed"), true
	}
	if !migration && execution.CancellationRequested() {
		return taskengine.Result{}, false
	}
	if !migration {
		entry, err := h.CopyCoordinator.deps.Repositories.CacheEvictions.GetCacheEntry(ctx, copyRow.ContentID)
		if err != nil {
			return retryPullDependency(execution, err, "copy_cache_load_failed"), true
		}
		if entry == nil || entry.CacheActiveTaskID != nil {
			return taskengine.Result{}, false
		}
	}
	available, err := copyCacheAvailable(ctx, h.CopyCoordinator.deps.Repositories, h.deps.Cache, copyRow)
	if err != nil {
		return retryPullDependency(execution, err, "copy_cache_load_failed"), true
	}
	if !available {
		if !migration {
			return taskengine.Result{}, false
		}
		if copyRow.CommitDecidedByRequest() {
			return h.failPullTask(execution, input, copyRow, pullAttemptID,
				errors.New("stored content migration failed and its local cache is unavailable"), "migration_cache_missing"), true
		}
		return taskengine.Fail(errors.New("stored content migration failed and its local cache is unavailable"), "migration_cache_missing", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.Contents.AbandonMigrationPull(ctx, copyRow.ID, input.Generation, execution.ID(), pullAttemptID)
		}), true
	}
	return h.advanceToCacheRestoreWithError(input, execution.ID(), "Storage copy is recovering from cache", pullAttemptID, lastError), true
}

func (h *PullHandler) pullHandler() *taskengine.FuncHandler {
	definition := h.copyDefinition(model.TaskTypeStoragePull)
	definition.MaxConcurrency = 4
	definition.Policy.MaxAttempts = 12
	return taskengine.NewFuncHandler(definition, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.runPull(ctx, execution, true)
	}, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.runPull(ctx, execution, false)
	},
	)
}

func (h *PullHandler) runPull(ctx context.Context, execution taskengine.Execution, mayPull bool) (result taskengine.Result) {
	input, copyRow, handled, result := h.authorizeCopyTask(ctx, execution)
	if handled {
		return result
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[pullCheckpoint](execution)
	if err != nil || (hasCheckpoint && checkpoint.AttemptID == "") {
		return failPullOutcome(execution, errors.Join(err, errors.New("pull checkpoint has no attempt identity")))
	}
	var attempt *storagepull.Attempt
	if hasCheckpoint {
		attempt, err = h.CopyCoordinator.deps.Repositories.Contents.GetPullAttempt(ctx, checkpoint.AttemptID, copyRow.ContentID, copyRow.StorageDataSetID)
	} else {
		attempt, err = h.CopyCoordinator.deps.Repositories.Contents.GetUnresolvedPullAttempt(ctx, copyRow.ContentID, copyRow.StorageDataSetID)
		if errors.Is(err, repository.ErrNotFound) {
			err = nil
		}
	}
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return failPullOutcome(execution, err)
		}
		return retryPullDependency(execution, err, "pull_attempt_load_failed")
	}
	var pieceCID cid.Cid
	var extra []byte
	if attempt != nil {
		pieceCID, extra, err = pullAttemptRequest(attempt)
		if err != nil {
			return failPullOutcome(execution, err)
		}
		if !hasCheckpoint {
			checkpoint.AttemptID = attempt.AttemptID
			if err := execution.WriteCheckpointWith(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Contents.ReservePullRequest(ctx, pullReservation(input, execution.ID(), attempt))
			}); err != nil {
				return retryPullDependency(execution, err, "pull_checkpoint_failed")
			}
		}
	}
	if copyRow.Status == model.StorageCopyStatusCommitted || copyRow.Status == model.StorageCopyStatusPieceReady || copyRow.Status == model.StorageCopyStatusCommitting {
		if attempt != nil {
			retrievalURL := ""
			if copyRow.RetrievalURL != nil {
				retrievalURL = *copyRow.RetrievalURL
			}
			return h.finishPieceTransfer(execution, input, copyRow, retrievalURL, pieceCID, attempt.AttemptID)
		}
		if copyRow.Status == model.StorageCopyStatusCommitted {
			return h.completeCopyTask(input, execution.ID(), "Storage copy is complete")
		}
		return h.handOffToCommit(input, execution.ID())
	}
	if attempt != nil && attempt.ResolvedAt != nil {
		return failPullOutcome(execution, errors.New("pull attempt is already finished"))
	}
	if attempt == nil && execution.CancellationRequested() {
		message := "Storage transfer cancelled"
		if copyRow.CommitDecidedByRequest() {
			return taskengine.Cancel(message, h.releaseMemberTransfer(execution, input, copyRow, message, ""))
		}
		return taskengine.Cancel(message, func(ctx context.Context, repos *repository.Repositories) error {
			return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message, "")
		})
	}
	if copyRow.TransferMethod == model.StorageCopyTransferMethodIngress && attempt == nil {
		return h.advanceCopyTask(input, execution.ID(), model.TaskTypeStorageStore, "Storage copy is ready for ingress")
	}
	if attempt == nil && !mayPull {
		return taskengine.Wait(model.TaskResumeModeExecute, 0, "safe_to_execute", "Storage transfer is ready", nil)
	}
	_, target, _, _, err := copyContext(ctx, h.CopyCoordinator.deps.Repositories, h.deps.Resolver, copyRow)
	if err != nil {
		if attempt != nil {
			if errors.Is(err, repository.ErrNotFound) {
				return failPullOutcome(execution, err)
			}
			return retryPullDependency(execution, err, "copy_context_failed")
		}
		return h.copyContextFailure(execution, input, copyRow, err, true)
	}
	freshAttempt := attempt == nil
	if freshAttempt {
		if !copyRow.CommitSealed() {
			backlogged, err := commitBacklogFull(ctx, h.CopyCoordinator.deps.Repositories, h.deps.CommitMaxBacklog, copyRow)
			if err != nil {
				return h.retryCopyTask(execution, input, copyRow, err, "commit_backlog_check_failed")
			}
			if backlogged {
				return waitForCommitBacklog()
			}
		}
		sources, err := h.CopyCoordinator.deps.Repositories.Contents.ListReadableCommittedCopies(ctx, copyRow.ContentID)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "copy_source_load_failed")
		}
		if len(sources) == 0 {
			return h.pullWithoutSource(ctx, execution, input, copyRow)
		}
		last, err := h.CopyCoordinator.deps.Repositories.Contents.GetLastAbandonedPullAttempt(ctx, copyRow.ContentID, copyRow.StorageDataSetID)
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "copy_source_load_failed")
		}
		source := choosePullSource(sources, last)
		pieceCID, err = cid.Parse(source.PieceCID)
		if err != nil {
			return h.failCopyTask(execution, input, copyRow, err, "source_identity_invalid")
		}
		extra, err = target.PresignForCommit(ctx, []storage.PieceInput{{PieceCID: pieceCID}})
		if err == nil {
			_, err = storagecommit.ExtraDataNonce(extra)
		}
		if err != nil {
			return h.retryCopyTask(execution, input, copyRow, err, "pull_presign_failed")
		}
		attemptID, err := newAttemptID()
		if err != nil {
			return h.failCopyTask(execution, input, copyRow, err, "pull_identity_failed")
		}
		attempt = &storagepull.Attempt{
			AttemptID: attemptID, ContentID: copyRow.ContentID, StorageDataSetID: copyRow.StorageDataSetID,
			Status:           storagepull.AttemptStatusAttempted,
			SourceProviderID: source.ProviderID, SourceDataSetID: source.DataSetID, SourcePieceID: source.PieceID,
			SourcePieceCID: source.PieceCID, SourceRetrievalURL: source.RetrievalURL,
			ExtraDataHex: hex.EncodeToString(extra),
		}
		checkpoint = pullCheckpoint{AttemptID: attemptID}
	}
	if (!mayPull && !checkpoint.Accepted) || execution.CancellationRequested() {
		return h.observePullPiece(ctx, execution, input, copyRow, target, attempt, pieceCID, false)
	}
	if delay := time.Until(checkpoint.NextRequestAt); delay > 0 {
		return taskengine.Wait(model.TaskResumeModeRecover, delay, storagepull.WaitQueueFull, "Waiting for the provider to accept storage transfers", nil)
	}
	var submitted *storage.PullResult
	var attempted bool
	submit := func(ctx context.Context) error {
		submitCtx, cancel := context.WithTimeout(ctx, pullSubmitTimeout)
		defer cancel()
		submitted, err = target.SubmitPull(submitCtx, storage.PullRequest{
			Pieces: []cid.Cid{pieceCID}, ExtraData: extra,
			From: func(cid.Cid) string { return attempt.SourceRetrievalURL },
		})
		return err
	}
	if !checkpoint.Accepted {
		attempted, err = execution.WithCheckpointedEffect(ctx, "pull:"+attempt.AttemptID, checkpoint,
			func(ctx context.Context, repos *repository.Repositories) error {
				return repos.Contents.ReservePullRequest(ctx, pullReservation(input, execution.ID(), attempt))
			}, submit)
	} else {
		err = submit(ctx)
	}
	if !checkpoint.Accepted && !attempted && err != nil {
		return h.retryUnsubmittedPull(execution, input, copyRow, attempt, err)
	}
	if errors.Is(err, pdp.ErrPullQueueFull) {
		delay := time.Minute
		if httpErr, ok := errors.AsType[*pdp.HTTPError](err); ok && httpErr.RetryAfter > 0 {
			delay = httpErr.RetryAfter
		}
		checkpoint.NextRequestAt = time.Now().UTC().Add(delay)
		var recordErr error
		if !checkpoint.Accepted {
			// Curio rolls back queue rejection, so only this admission is resolved.
			// The authorization and ownership remain available for recovery.
			recordErr = execution.ResolveOperation(ctx, "pull:"+attempt.AttemptID, checkpoint, nil)
		} else {
			recordErr = execution.WriteCheckpoint(ctx, checkpoint)
		}
		if recordErr != nil {
			return retryPullOutcome(execution, errors.Join(err, recordErr), "pull_queue_delay_record_failed")
		}
		return taskengine.Wait(model.TaskResumeModeRecover, delay, storagepull.WaitQueueFull, "Waiting for the provider to accept storage transfers", nil)
	}
	if err != nil {
		if synapse.ClassifyPullError(err) == synapse.PullErrorTerminal {
			return failPullOutcome(execution, err)
		}
		return retryPullOutcome(execution, err, "pull_request_failed")
	}
	status, err := pullResultStatus(submitted, pieceCID)
	if err != nil {
		return retryPullOutcome(execution, err, "pull_status_invalid")
	}
	if !checkpoint.Accepted {
		checkpoint.Accepted = true
		if err := execution.WriteCheckpoint(ctx, checkpoint); err != nil {
			return retryPullOutcome(execution, err, "pull_acceptance_record_failed")
		}
	}
	switch status {
	case storage.PullStatusPending, storage.PullStatusInProgress, storage.PullStatusRetrying:
		return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
	case storage.PullStatusFailed:
		return h.failConfirmedPull(ctx, execution, input, copyRow, attempt, pdp.ErrPullFailed)
	case storage.PullStatusComplete:
		return h.observePullPiece(ctx, execution, input, copyRow, target, attempt, pieceCID, true)
	default:
		return retryPullOutcome(execution, fmt.Errorf("unknown pull status %q", status), "pull_status_invalid")
	}
}

func (h *PullHandler) observePullPiece(
	ctx context.Context,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	target synapse.DataSetTarget,
	attempt *storagepull.Attempt,
	pieceCID cid.Cid,
	reportedComplete bool,
) taskengine.Result {
	state, err := h.pullPieceState(ctx, target, pieceCID)
	if err == nil && state == synapse.ParkedPieceReady {
		return h.finishPieceTransfer(execution, input, copyRow, target.PieceURL(pieceCID), pieceCID, attempt.AttemptID)
	}
	if execution.CancellationRequested() {
		return failPullOutcome(execution, errors.Join(err, errors.New("cancelled pull has no confirmed outcome")))
	}
	if err != nil {
		return retryPullOutcome(execution, err, "pull_status_failed")
	}
	switch state {
	case synapse.ParkedPieceProcessing:
		return taskengine.Wait(model.TaskResumeModeRecover, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
	case synapse.ParkedPieceMissing:
		if reportedComplete {
			return h.failConfirmedPull(ctx, execution, input, copyRow, attempt, errors.New("provider no longer has the completed pull piece"))
		}
		checkpoint, _, err := taskengine.DecodeCheckpoint[pullCheckpoint](execution)
		if err != nil {
			return taskengine.Fail(err, "invalid_checkpoint", nil)
		}
		if delay := time.Until(checkpoint.NextRequestAt); delay > 0 {
			return taskengine.Wait(model.TaskResumeModeExecute, delay, storagepull.WaitQueueFull, "Waiting for the provider to accept storage transfers", nil)
		}
		return taskengine.Wait(model.TaskResumeModeExecute, storagePollInterval, "provider_confirmation", "Checking storage transfer", nil)
	default:
		return retryPullOutcome(execution, fmt.Errorf("unknown parked piece state %q", state), "pull_status_invalid")
	}
}

func (h *PullHandler) pullPieceState(ctx context.Context, target synapse.DataSetTarget, pieceCID cid.Cid) (synapse.ParkedPieceState, error) {
	if h.deps.ParkedPieces == nil {
		return "", errors.New("parked piece checker is unavailable")
	}
	statusCtx, cancel := context.WithTimeout(ctx, pullStatusTimeout)
	defer cancel()
	return h.deps.ParkedPieces.FindParkedPiece(statusCtx, target.ServiceURL(), pieceCID)
}

func (h *PullHandler) failConfirmedPull(ctx context.Context, execution taskengine.Execution, input storagepipeline.CopyGenerationInput, copyRow *model.StorageCopy, attempt *storagepull.Attempt, err error) taskengine.Result {
	err = fmt.Errorf("storage provider %s could not copy the piece from provider %s: %w", copyRow.ProviderID, attempt.SourceProviderID, err)
	if result, recovered := h.recoverPullFromCache(ctx, execution, input, copyRow, attempt.AttemptID, err.Error()); recovered {
		return result
	}
	return h.failPullTask(execution, input, copyRow, attempt.AttemptID, err, "pull_failed")
}

func (h *PullHandler) retryUnsubmittedPull(_ taskengine.Execution, _ storagepipeline.CopyGenerationInput, _ *model.StorageCopy, _ *storagepull.Attempt, err error) taskengine.Result {
	return retryTask(err, "pull_checkpoint_failed")
}
