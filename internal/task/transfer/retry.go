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
	_, session, err := copyRetryCheckpoint(source, row)
	if err != nil {
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
	if session {
		if row.TransferMethod != model.StorageCopyTransferMethodIngress && row.TransferMethod != model.StorageCopyTransferMethodCacheRestore {
			return repository.ErrConflict
		}
		if row.TransferMethod == model.StorageCopyTransferMethodIngress {
			ingress, err := repos.Contents.GetIngressCopy(ctx, row.ContentID)
			if err != nil {
				return err
			}
			if ingress != nil && ingress.ID != row.ID {
				return repository.ErrConflict
			}
		}
	}
	return nil
}

func copyRetryCheckpoint(source *model.Task, row *model.StorageCopy) (json.RawMessage, bool, error) {
	if source.Type != model.TaskTypeStorageStore || len(source.Checkpoint) == 0 {
		return source.Checkpoint, false, nil
	}
	var checkpoint storeCheckpoint
	if err := json.Unmarshal(source.Checkpoint, &checkpoint); err != nil {
		return nil, false, err
	}
	if err := validateStoreCheckpoint(checkpoint, row.ContentSize); err != nil {
		return nil, false, err
	}
	if checkpoint.IngressAttempt != 0 && checkpoint.IngressAttempt != row.IngressStoreAttempt {
		return nil, false, repository.ErrConflict
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(source.Checkpoint, &fields); err != nil {
		return nil, false, err
	}
	delete(fields, "observation_started_at")
	encoded, err := json.Marshal(fields)
	return encoded, checkpoint.SessionID != "", err
}

func loadCopyRetryCheckpoint(ctx context.Context, repos *repository.Repositories, source *model.Task) (json.RawMessage, error) {
	var input storagepipeline.CopyGenerationInput
	if err := json.Unmarshal(source.Input, &input); err != nil {
		return nil, err
	}
	row, err := repos.Contents.GetUploadCopyByID(ctx, input.CopyID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, repository.ErrNotFound
	}
	checkpoint, _, err := copyRetryCheckpoint(source, row)
	return checkpoint, err
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
	checkpoint, _, err := copyRetryCheckpoint(source, row)
	if err != nil {
		return taskengine.RetryPreparation{}, err
	}
	if row.ActiveTaskID != nil {
		return taskengine.RetryPreparation{
			Request:    taskengine.EnqueueRequest{Type: source.Type, Input: source.Input},
			Checkpoint: checkpoint, ResumeMode: model.TaskResumeModeRecover,
			Bind: transferCopyOwner,
		}, nil
	}
	if h.deps.CacheGate == nil {
		return taskengine.RetryPreparation{}, errors.New("cache admission unavailable")
	}
	release, err := h.deps.CacheGate.HoldRead(ctx, model.ContentCacheKey(row.ContentID))
	if err != nil {
		return taskengine.RetryPreparation{}, err
	}
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
	checkpoint, session, err := copyRetryCheckpoint(source, row)
	if err != nil {
		return taskengine.RetryPreparation{}, err
	}
	if session && !row.CommitSealed() {
		return taskengine.RetryPreparation{
			Request:    taskengine.EnqueueRequest{Type: model.TaskTypeStorageStore, Input: input},
			Checkpoint: checkpoint, ResumeMode: model.TaskResumeModeRecover,
			Bind: func(ctx context.Context, repos *repository.Repositories, current, next *model.Task) error {
				return repos.Contents.ResumeFailedStoreCopy(ctx, repository.ResumeFailedStoreCopyInput{
					CopyID: row.ID, Generation: previous.Generation, SourceTaskID: current.ID, TaskID: next.ID,
					SourceCheckpoint: source.Checkpoint, Checkpoint: checkpoint,
				})
			},
		}, nil
	}
	return taskengine.RetryPreparation{
		Request: taskengine.EnqueueRequest{
			Type: model.TaskTypeStorageTransferPlan, Input: input,
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
