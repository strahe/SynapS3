package bucketprovision

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/strahe/synaps3/internal/task/binding"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type (
	ReadinessDependencies struct{ Messenger *taskengine.Messenger }
	WakeDependencies      struct{ Scheduler *taskengine.Scheduler }
)

func NewDataSetReadyPromoteSubscriber(deps ReadinessDependencies) (taskengine.MessageHandler, error) {
	if deps.Messenger == nil {
		return nil, errors.New("bucket readiness messenger is required")
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
		return promoteReadyBucket(ctx, tx, deps.Messenger, row.BucketID)
	}, nil
}

func NewDataSetReadyWakeSubscriber(deps WakeDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("bucket provision scheduler is required")
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
		return wakeProvision(ctx, tx, deps.Scheduler, row.BucketID)
	}, nil
}

func NewReplacementActivatedPromoteSubscriber(deps ReadinessDependencies) (taskengine.MessageHandler, error) {
	if deps.Messenger == nil {
		return nil, errors.New("bucket readiness messenger is required")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		input, ok := message.(storagereplacement.ReplacementActivated)
		if !ok || input.BucketID <= 0 || input.CopyIndex < 0 || input.CopyIndex >= model.StorageCopiesMax {
			return repository.ErrInvalidInput
		}
		return promoteReadyBucket(ctx, tx, deps.Messenger, input.BucketID)
	}, nil
}

func NewReplacementActivatedWakeSubscriber(deps WakeDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("bucket provision scheduler is required")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		input, ok := message.(storagereplacement.ReplacementActivated)
		if !ok || input.BucketID <= 0 || input.CopyIndex < 0 || input.CopyIndex >= model.StorageCopiesMax {
			return repository.ErrInvalidInput
		}
		return wakeProvision(ctx, tx, deps.Scheduler, input.BucketID)
	}, nil
}

func promoteReadyBucket(ctx context.Context, tx *repository.Repositories, messenger *taskengine.Messenger, bucketID int64) error {
	bucket, err := tx.Buckets.GetByID(ctx, bucketID)
	if err != nil {
		return err
	}
	if bucket == nil {
		return repository.ErrNotFound
	}
	return promoteBucketReady(ctx, tx, messenger, bucket.ID, model.ClampStorageCopies(bucket.DefaultCopies))
}

func promoteBucketReady(ctx context.Context, tx *repository.Repositories, messenger *taskengine.Messenger, bucketID int64, required int) error {
	promoted, err := tx.Buckets.PromoteReadyIfProvisioned(ctx, bucketID, required)
	if err != nil || !promoted {
		return err
	}
	return messenger.Notify(ctx, tx, bucketlifecycle.BucketReady{BucketID: bucketID})
}

func wakeProvision(ctx context.Context, tx *repository.Repositories, scheduler *taskengine.Scheduler, bucketID int64) error {
	bucket, err := tx.Buckets.GetByID(ctx, bucketID)
	if err != nil {
		return err
	}
	if bucket == nil {
		return repository.ErrNotFound
	}
	taskRow, err := tx.Tasks.GetByIdentity(ctx, model.TaskTypeBucketProvision, bucketlifecycle.ProvisionKey(bucket.ID, bucket.DefaultCopies))
	if err != nil || taskRow == nil {
		return err
	}
	_, err = scheduler.WakeInTransaction(ctx, tx, []int64{taskRow.ID}, taskengine.WakePendingFilter{Types: []model.TaskType{model.TaskTypeBucketProvision}})
	return err
}
