package provider

import (
	"context"
	"errors"
	"time"

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
		const scheduleKey = "system:observability-refresh"
		schedule, err := tx.TaskSchedules.GetForUpdate(ctx, scheduleKey)
		if err != nil {
			return err
		}
		if schedule == nil {
			return tx.TaskSchedules.Ensure(ctx, scheduleKey, time.Now().UTC())
		}
		if schedule.LatestTaskID == nil {
			return nil
		}
		refresh, err := tx.Tasks.GetByID(ctx, *schedule.LatestTaskID)
		if err != nil || refresh == nil {
			return err
		}
		if refresh.Status != model.TaskStatusPending && refresh.Status != model.TaskStatusRunning {
			return tx.TaskSchedules.ScheduleNext(ctx, scheduleKey, refresh.ID, time.Now().UTC())
		}
		_, err = deps.Scheduler.WakeInTransaction(ctx, tx, []int64{refresh.ID}, taskengine.WakePendingFilter{Types: []model.TaskType{model.TaskTypeObservabilityRefresh}})
		return err
	}, nil
}
