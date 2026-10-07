package commit

import (
	"context"
	"errors"
	"fmt"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

func NewJoinCommitReceiver(handler *Handler) (taskengine.MessageHandler, error) {
	if handler == nil {
		return nil, errors.New("join commit requires a commit handler")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		request, ok := message.(storagepipeline.JoinCommit)
		if !ok || request.CopyID < 1 {
			return errors.New("invalid join commit message")
		}
		return handler.queueCommit(ctx, tx, request.CopyID)
	}, nil
}

func NewDataSetReadySubscriber(handler *Handler) (taskengine.MessageHandler, error) {
	if handler == nil {
		return nil, errors.New("data set ready continuation requires a commit handler")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		ready, ok := message.(storagepipeline.DataSetReady)
		if !ok || ready.BindingID < 1 {
			return errors.New("invalid data set ready message")
		}
		binding, err := tx.Contents.GetDataSetBindingByID(ctx, ready.BindingID)
		if err != nil {
			return err
		}
		if binding == nil || binding.Status != model.StorageDataSetStatusReady || binding.DataSetID == nil || binding.DataSetID.IsZero() {
			return fmt.Errorf("data set is not ready: %w", repository.ErrConflict)
		}
		copies, err := tx.Contents.ListIncompleteCopiesForDataSet(ctx, ready.BindingID)
		if err != nil {
			return err
		}
		ids := make([]int64, 0, len(copies))
		for i := range copies {
			copyRow := &copies[i]
			if copyRow.ContinuationKind() != model.CopyContinuationRegistration {
				continue
			}
			if id := copyRow.WorkTaskID(); id != nil {
				ids = append(ids, *id)
				continue
			}
			if err := handler.queueCommit(ctx, tx, copyRow.ID); err != nil {
				return err
			}
		}
		_, err = handler.deps.Scheduler.WakeInTransaction(ctx, tx, ids, taskengine.WakePendingFilter{Types: []model.TaskType{model.TaskTypeStorageCommit}})
		return err
	}, nil
}
