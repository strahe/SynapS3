package uploadplan

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/task/binding"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type WakeDependencies struct{ Scheduler *taskengine.Scheduler }

func NewDataSetReadySubscriber(deps WakeDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("upload plan scheduler is required")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		input, ok := message.(storagepipeline.DataSetReady)
		if !ok || input.BindingID <= 0 {
			return repository.ErrInvalidInput
		}
		row, err := binding.ReadReadyDataSet(ctx, tx, input.BindingID)
		if err != nil {
			return err
		}
		if !row.IsCurrent {
			return nil
		}
		return wakeReplacementUploadPlans(ctx, tx, deps.Scheduler, row.BucketID, row.CopyIndex)
	}, nil
}

func NewReplacementActivatedSubscriber(deps WakeDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("upload plan scheduler is required")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		input, ok := message.(storagereplacement.ReplacementActivated)
		if !ok || input.BucketID <= 0 || input.CopyIndex < 0 || input.CopyIndex >= model.StorageCopiesMax {
			return repository.ErrInvalidInput
		}
		return wakeReplacementUploadPlans(ctx, tx, deps.Scheduler, input.BucketID, input.CopyIndex)
	}, nil
}

func wakeReplacementUploadPlans(ctx context.Context, tx *repository.Repositories, scheduler *taskengine.Scheduler, bucketID int64, copyIndex int) error {
	ids, err := tx.Contents.PendingUploadPlansForReplica(ctx, bucketID, copyIndex)
	if err != nil {
		return err
	}
	_, err = scheduler.WakeInTransaction(ctx, tx, ids, taskengine.WakePendingFilter{Types: []model.TaskType{model.TaskTypeUploadPlan}})
	return err
}
