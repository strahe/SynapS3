package provider

import (
	"context"
	"errors"

	"github.com/strahe/synaps3/internal/bucketlifecycle"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	taskengine "github.com/strahe/synaps3/internal/worker"
)

type BucketReadyDependencies struct{ Scheduler *taskengine.Scheduler }

func NewBucketReadySubscriber(deps BucketReadyDependencies) (taskengine.MessageHandler, error) {
	if deps.Scheduler == nil {
		return nil, errors.New("bucket readiness subscriber requires a scheduler")
	}
	return func(ctx context.Context, tx *repository.Repositories, message taskengine.Message) error {
		input, ok := message.(bucketlifecycle.BucketReady)
		if !ok || input.BucketID <= 0 {
			return errors.New("invalid bucket readiness message")
		}
		refresh, err := tx.Tasks.GetByIdentity(ctx, model.TaskTypeObservabilityRefresh, "system:observability-refresh")
		if err != nil || refresh == nil {
			return err
		}
		_, err = deps.Scheduler.WakeInTransaction(ctx, tx, []int64{refresh.ID}, taskengine.WakePendingFilter{Types: []model.TaskType{model.TaskTypeObservabilityRefresh}})
		return err
	}, nil
}
