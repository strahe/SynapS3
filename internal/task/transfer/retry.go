package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func transferCopyOwner(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(old.Input, &input); err != nil {
		return err
	}
	return repos.Contents.TransferCopyTaskOwner(ctx, input.CopyID, input.Generation, old.ID, next.ID)
}

func (h *CopyCoordinator) inspectCopyRetry(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
	if source.FailureReason != nil && *source.FailureReason == "invalid_checkpoint" {
		return repository.ErrConflict
	}
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(source.Input, &input); err != nil {
		return err
	}
	row, err := repos.Contents.GetUploadCopyByID(ctx, input.CopyID)
	if err != nil {
		return err
	}
	if row == nil || row.WorkGeneration != input.Generation {
		return repository.ErrConflict
	}
	if row.ActiveTaskID != nil {
		if *row.ActiveTaskID == source.ID {
			return nil
		}
		return repository.ErrConflict
	}
	latest, err := repos.Tasks.LatestForSubject(ctx, model.TaskSubjectStorageCopy, strconv.FormatInt(input.CopyID, 10), model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull)
	if err != nil {
		return err
	}
	if latest == nil || latest.ID != source.ID {
		return repository.ErrConflict
	}
	states, err := repos.Contents.CopyRetryStates(ctx, []int64{input.CopyID})
	if err != nil {
		return err
	}
	if state := states[input.CopyID]; !state.Available {
		if state.Block != "" {
			return &repository.CopyRetryBlockedError{Block: state.Block}
		}
		return repository.ErrConflict
	}
	return nil
}

func (h *CopyCoordinator) prepareCopyRetry(ctx context.Context, repos *repository.Repositories, source *model.Task) (taskengine.RetryPreparation, error) {
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(source.Input, &input); err != nil {
		return taskengine.RetryPreparation{}, err
	}
	row, err := repos.Contents.GetUploadCopyByID(ctx, input.CopyID)
	if err != nil {
		return taskengine.RetryPreparation{}, err
	}
	if row == nil {
		return taskengine.RetryPreparation{}, repository.ErrNotFound
	}
	if row.ActiveTaskID != nil {
		return taskengine.RetryPreparation{
			Request:    taskengine.EnqueueRequest{Type: source.Type, IdempotencyKey: source.IdempotencyKey, Input: source.Input},
			Checkpoint: source.Checkpoint, ResumeMode: model.TaskResumeModeRecover,
			PreserveCancellation: true, Bind: transferCopyOwner,
		}, nil
	}
	if h.deps.CacheGate == nil {
		return taskengine.RetryPreparation{}, errors.New("cache admission unavailable")
	}
	release := h.deps.CacheGate.HoldRead(model.ContentCacheKey(row.ContentID))
	preparation, err := h.prepareCopyRecovery(ctx, repos, source, row)
	if err != nil {
		release()
		return taskengine.RetryPreparation{}, err
	}
	preparation.Release = release
	return preparation, nil
}

func (h *CopyCoordinator) prepareCopyRecovery(ctx context.Context, repos *repository.Repositories, source *model.Task, row *model.StorageCopy) (taskengine.RetryPreparation, error) {
	if source == nil || row == nil || row.ActiveTaskID != nil {
		return taskengine.RetryPreparation{}, repository.ErrConflict
	}
	var previous storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(source.Input, &previous); err != nil {
		return taskengine.RetryPreparation{}, err
	}
	if previous.CopyID != row.ID || previous.Generation != row.WorkGeneration {
		return taskengine.RetryPreparation{}, repository.ErrConflict
	}
	latest, err := repos.Tasks.LatestForSubject(ctx, model.TaskSubjectStorageCopy, strconv.FormatInt(row.ID, 10), model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull)
	if err != nil {
		return taskengine.RetryPreparation{}, err
	}
	if latest == nil || latest.ID != source.ID {
		return taskengine.RetryPreparation{}, repository.ErrConflict
	}
	input := storagepipeline.CopyGenerationInput{CopyID: row.ID, Generation: previous.Generation + 1}
	return taskengine.RetryPreparation{
		Request: taskengine.EnqueueRequest{
			Type: model.TaskTypeStorageTransferPlan, IdempotencyKey: storagepipeline.TransferPlanKey(input.CopyID, input.Generation), Input: input,
			SubjectType: model.TaskSubjectStorageCopy, SubjectKey: strconv.FormatInt(input.CopyID, 10),
		},
		ResumeMode: model.TaskResumeModeExecute,
		Bind: func(ctx context.Context, repos *repository.Repositories, _, next *model.Task) error {
			current, err := repos.Contents.GetUploadCopyByID(ctx, input.CopyID)
			if err != nil {
				return err
			}
			if current == nil || current.ActiveTaskID != nil || current.WorkGeneration != previous.Generation {
				return repository.ErrConflict
			}
			if current.Status == model.StorageCopyStatusFailed {
				current, err = repos.Contents.RetryFailedCopy(ctx, input.CopyID)
				if err != nil {
					return err
				}
			}
			if current == nil || current.Status != model.StorageCopyStatusPending || current.WorkGeneration != previous.Generation {
				return repository.ErrConflict
			}
			return repos.Contents.BindCopyTask(ctx, input.CopyID, input.Generation, next.ID)
		},
	}, nil
}
