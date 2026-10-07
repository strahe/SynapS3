package transfer

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/synapse"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/storage"
)

func (h *CopyCoordinator) wakePeerPullPlans(ctx context.Context, repos *repository.Repositories, contentID int64) error {
	if h.deps.Scheduler == nil {
		return errors.New("task service is unavailable")
	}
	copies, err := repos.Contents.ListCopies(ctx, contentID)
	if err != nil {
		return err
	}
	wakeIDs := make([]int64, 0, len(copies))
	for i := range copies {
		copyRow := &copies[i]
		if copyRow.Status != model.StorageCopyStatusPending ||
			copyRow.TransferMethod != model.StorageCopyTransferMethodPeerPull ||
			copyRow.ActiveTaskID == nil {
			continue
		}
		taskRow, err := repos.Tasks.GetByID(ctx, *copyRow.ActiveTaskID)
		if err != nil {
			return err
		}
		if taskRow != nil && taskRow.Type == model.TaskTypeStorageTransferPlan &&
			taskRow.Status == model.TaskStatusPending && taskRow.WaitReason != nil && *taskRow.WaitReason == "source" {
			wakeIDs = append(wakeIDs, taskRow.ID)
		}
	}
	_, err = h.wake(ctx, repos, wakeIDs)
	return err
}

func (h *CopyCoordinator) advanceToCacheRestore(input storagepipeline.CopyGenerationInput, taskID int64, message, pullAttemptID string) taskengine.Result {
	return h.advanceToCacheRestoreWithError(input, taskID, message, pullAttemptID, "")
}

func (h *CopyCoordinator) advanceToCacheRestoreWithError(input storagepipeline.CopyGenerationInput, taskID int64, message, pullAttemptID, lastError string) taskengine.Result {
	return taskengine.Complete(message, func(ctx context.Context, repos *repository.Repositories) error {
		if err := repos.Contents.SetCopyCacheRestore(ctx, input.CopyID, input.Generation, taskID, pullAttemptID, lastError); err != nil {
			return err
		}
		return h.enqueueSuccessorCopyTask(ctx, repos, input, taskID, model.TaskTypeStorageStore)
	})
}

func (h *CopyCoordinator) authorizeCopyTask(
	ctx context.Context,
	execution taskengine.Execution,
) (storagepipeline.CopyGenerationInput, *model.StorageCopy, bool, taskengine.Result) {
	input, err := taskengine.DecodeInput[storagepipeline.CopyGenerationInput](execution)
	if err != nil {
		return input, nil, true, decodeFailure(string(execution.Type()), err)
	}
	copyRow, err := h.deps.Repositories.Contents.AuthorizeCopyTask(ctx, input.CopyID, input.Generation, execution.ID(), execution.ClaimGeneration())
	if errors.Is(err, repository.ErrConflict) || errors.Is(err, repository.ErrNotFound) {
		return input, nil, true, taskengine.Cancel("Storage work was superseded", nil)
	}
	if err != nil {
		if execution.Type() == model.TaskTypeStoragePull {
			return input, nil, true, retryPullDependency(execution, err, "copy_authorization_failed")
		}
		if execution.Type() == model.TaskTypeStorageStore && execution.RetryWillFail() {
			return input, nil, true, taskengine.Fail(err, "copy_authorization_failed", nil)
		}
		if execution.RetryWillFail() {
			message := err.Error()
			return input, nil, true, taskengine.Fail(err, "copy_authorization_failed", func(ctx context.Context, repos *repository.Repositories) error {
				copyRow, authorizeErr := repos.Contents.AuthorizeCopyTask(ctx, input.CopyID, input.Generation, execution.ID(), execution.ClaimGeneration())
				if errors.Is(authorizeErr, repository.ErrConflict) || errors.Is(authorizeErr, repository.ErrNotFound) {
					return nil
				}
				if authorizeErr != nil {
					return authorizeErr
				}
				if copyRow.CommitDecidedByRequest() {
					return nil
				}
				return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message, "")
			})
		}
		return input, nil, true, retryTask(err, "copy_authorization_failed")
	}
	return input, copyRow, false, taskengine.Result{}
}

func (h *CopyCoordinator) advanceCopyTask(
	input storagepipeline.CopyGenerationInput,
	taskID int64,
	nextType model.TaskType,
	message string,
) taskengine.Result {
	return taskengine.Complete(message, func(ctx context.Context, repos *repository.Repositories) error {
		return h.enqueueSuccessorCopyTask(ctx, repos, input, taskID, nextType)
	})
}

func (h *CopyCoordinator) completeCopyTask(input storagepipeline.CopyGenerationInput, taskID int64, message string) taskengine.Result {
	return taskengine.Complete(message, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Contents.CompleteCopyTask(ctx, input.CopyID, input.Generation, taskID)
	})
}

func (h *CopyCoordinator) enqueueInitialCopyTask(ctx context.Context, repos *repository.Repositories, copyID int64, taskType model.TaskType) error {
	return h.enqueueCopyTaskAt(ctx, repos, copyID, taskType, time.Time{})
}

func (h *CopyCoordinator) enqueueCopyTaskAt(ctx context.Context, repos *repository.Repositories, copyID int64, taskType model.TaskType, availableAt time.Time) error {
	if h.deps.Scheduler == nil {
		return errors.New("task service is unavailable")
	}
	generation, err := repos.Contents.NextCopyWorkGeneration(ctx, copyID)
	if err != nil {
		return err
	}
	input := storagepipeline.CopyGenerationInput{CopyID: copyID, Generation: generation}
	taskRow, _, err := h.deps.Scheduler.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type: taskType, IdempotencyKey: copyTaskKey(taskType, copyID, generation), Input: input,
		SubjectType: model.TaskSubjectStorageCopy, SubjectKey: strconv.FormatInt(copyID, 10),
		AvailableAt: availableAt,
	})
	if err != nil {
		return err
	}
	return repos.Contents.BindCopyTask(ctx, copyID, generation, taskRow.ID)
}

func (h *CopyCoordinator) enqueueSuccessorCopyTask(
	ctx context.Context,
	repos *repository.Repositories,
	current storagepipeline.CopyGenerationInput,
	currentTaskID int64,
	nextType model.TaskType,
) error {
	if h.deps.Scheduler == nil {
		return errors.New("task service is unavailable")
	}
	next := storagepipeline.CopyGenerationInput{CopyID: current.CopyID, Generation: current.Generation + 1}
	taskRow, _, err := h.deps.Scheduler.EnqueueInTransaction(ctx, repos, taskengine.EnqueueRequest{
		Type: nextType, IdempotencyKey: copyTaskKey(nextType, next.CopyID, next.Generation), Input: next,
		SubjectType: model.TaskSubjectStorageCopy, SubjectKey: strconv.FormatInt(next.CopyID, 10),
	})
	if err != nil {
		return err
	}
	return repos.Contents.ReplaceCopyTask(ctx, current.CopyID, current.Generation, currentTaskID, next.Generation, taskRow.ID)
}

func (h *CopyCoordinator) copyContextFailure(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	err error,
	settleSafe bool,
) taskengine.Result {
	if errors.Is(err, repository.ErrNotFound) {
		if settleSafe {
			return h.failCopyTask(execution, input, copyRow, err, "copy_owner_missing")
		}
		return taskengine.Fail(err, "copy_owner_missing", nil)
	}
	if synapse.IsProviderUnavailable(err) || errors.Is(err, storage.ErrDataSetUnavailable) {
		return taskengine.Suspend(model.TaskResumeModeRecover, storageDependencyWait, "provider", "Waiting for storage provider", nil)
	}
	if settleSafe {
		return h.retryCopyTask(execution, input, copyRow, err, "copy_context_failed")
	}
	return retryTask(err, "copy_context_failed")
}

func (h *CopyCoordinator) finishPieceTransfer(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	retrievalURL string,
	pieceCID cid.Cid,
	pullAttemptID string,
) taskengine.Result {
	pieceCIDString := pieceCID.String()
	return taskengine.Complete("Storage transfer completed", func(ctx context.Context, repos *repository.Repositories) error {
		if err := repos.Contents.MarkUploadCopyPieceReady(ctx, repository.MarkUploadCopyPieceReadyInput{
			StorageCopyID: copyRow.ID, RequireEligibleCopy: true, ContentID: copyRow.ContentID,
			CopyIndex: copyRow.CopyIndex, PieceCID: pieceCIDString, RetrievalURL: retrievalURL,
			PullAttemptID: pullAttemptID,
		}); err != nil {
			return err
		}
		if err := repos.Contents.CompleteCopyTask(ctx, input.CopyID, input.Generation, execution.ID()); err != nil {
			return err
		}
		return h.deps.Messenger.Handover(ctx, repos, storagepipeline.JoinCommit{CopyID: copyRow.ID})
	})
}

func (h *CopyCoordinator) handOffToCommit(input storagepipeline.CopyGenerationInput, taskID int64) taskengine.Result {
	return taskengine.Complete("Storage copy is ready to register", func(ctx context.Context, repos *repository.Repositories) error {
		if err := repos.Contents.CompleteCopyTask(ctx, input.CopyID, input.Generation, taskID); err != nil {
			return err
		}
		return h.deps.Messenger.Handover(ctx, repos, storagepipeline.JoinCommit{CopyID: input.CopyID})
	})
}

func (h *CopyCoordinator) retryCopyTask(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	err error,
	reason string,
) taskengine.Result {
	if !execution.RetryWillFail() {
		return retryTask(err, reason)
	}
	return h.failCopyTask(execution, input, copyRow, err, reason)
}

func (h *CopyCoordinator) failPullTask(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	pullAttemptID string,
	err error,
	reason string,
) taskengine.Result {
	if copyRow == nil {
		return taskengine.Fail(err, reason, nil)
	}
	message := err.Error()
	if copyRow.CommitDecidedByRequest() {
		return taskengine.Fail(err, reason, h.releaseMemberTransfer(execution, input, copyRow, message, pullAttemptID))
	}
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message, pullAttemptID)
	})
}

func (h *CopyCoordinator) failCopyTask(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	err error,
	reason string,
) taskengine.Result {
	if copyRow == nil {
		return taskengine.Fail(err, reason, nil)
	}
	message := err.Error()
	if copyRow.CommitDecidedByRequest() {
		return taskengine.Fail(err, reason, h.releaseMemberTransfer(execution, input, copyRow, message, ""))
	}
	return taskengine.Fail(err, reason, func(ctx context.Context, repos *repository.Repositories) error {
		return h.settleCopyFailure(ctx, repos, execution, input, copyRow, message, "")
	})
}

func (h *CopyCoordinator) releaseMemberTransfer(
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	message, pullAttemptID string,
) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		return repos.Contents.ReleaseMemberTransfer(ctx, repository.ReleaseMemberTransferInput{
			StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, Generation: input.Generation,
			TaskID: execution.ID(), LastError: message, PullAttemptID: pullAttemptID,
		})
	}
}

func (h *CopyCoordinator) settleCopyFailure(
	ctx context.Context,
	repos *repository.Repositories,
	execution taskengine.Execution,
	input storagepipeline.CopyGenerationInput,
	copyRow *model.StorageCopy,
	message string,
	pullAttemptID string,
) error {
	if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{
		StorageCopyID: copyRow.ID,
		ContentID:     copyRow.ContentID,
		CopyIndex:     copyRow.CopyIndex,
		LastError:     message,
		PullAttemptID: pullAttemptID,
	}); err != nil {
		return err
	}
	// The content itself is flagged by MarkUploadCopyFailed, and only once no
	// copy is left that holds or may still hold it.
	if err := repos.Contents.CompleteCopyTask(ctx, input.CopyID, input.Generation, execution.ID()); err != nil {
		return err
	}
	return h.recoverIngressAfterFailure(ctx, repos, copyRow)
}

func (h *CopyCoordinator) failCopy(ctx context.Context, repos *repository.Repositories, copyRow *model.StorageCopy, message, pullAttemptID string) error {
	if err := repos.Contents.MarkUploadCopyFailed(ctx, repository.MarkUploadCopyFailedInput{
		StorageCopyID: copyRow.ID,
		ContentID:     copyRow.ContentID,
		CopyIndex:     copyRow.CopyIndex,
		LastError:     message,
		PullAttemptID: pullAttemptID,
	}); err != nil {
		return err
	}
	return h.recoverIngressAfterFailure(ctx, repos, copyRow)
}

func (h *CopyCoordinator) recoverIngressAfterFailure(ctx context.Context, repos *repository.Repositories, copyRow *model.StorageCopy) error {
	if copyRow.TransferMethod != model.StorageCopyTransferMethodIngress {
		return nil
	}
	readable, err := repos.Contents.HasReadableCommittedCopy(ctx, copyRow.ContentID)
	if err != nil {
		return err
	}
	if readable {
		failed, err := repos.Contents.ReopenFailedIngressForPull(ctx, copyRow.ContentID)
		if err != nil {
			return err
		}
		for _, failedCopy := range failed {
			if err := h.enqueueInitialCopyTask(ctx, repos, failedCopy.ID, model.TaskTypeStorageTransferPlan); err != nil {
				return err
			}
		}
		return nil
	}
	promoted, err := repos.Contents.PromotePendingIngress(ctx, copyRow.ContentID)
	if err != nil || promoted == nil {
		return err
	}
	if promoted.ActiveTaskID != nil && h.deps.Scheduler != nil {
		_, err = h.wake(ctx, repos, []int64{*promoted.ActiveTaskID})
		return err
	}
	return h.enqueueInitialCopyTask(ctx, repos, promoted.ID, model.TaskTypeStorageTransferPlan)
}
