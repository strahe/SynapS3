package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

type BunTaskScheduleRepo struct{ db bun.IDB }

var _ TaskScheduleRepository = (*BunTaskScheduleRepo)(nil)

func (r *BunTaskScheduleRepo) Ensure(ctx context.Context, key string, nextRunAt time.Time) error {
	if key == "" || nextRunAt.IsZero() {
		return ErrInvalidInput
	}
	_, err := r.db.NewInsert().Model(&model.TaskSchedule{Key: key, NextRunAt: nextRunAt}).On("CONFLICT (key) DO NOTHING").Exec(ctx)
	return err
}

func (r *BunTaskScheduleRepo) GetForUpdate(ctx context.Context, key string) (*model.TaskSchedule, error) {
	row := new(model.TaskSchedule)
	q := r.db.NewSelect().Model(row).Where("key = ?", key)
	if r.db.Dialect().Name() == dialect.PG {
		q.For("UPDATE")
	}
	if err := q.Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *BunTaskScheduleRepo) GetByTaskID(ctx context.Context, taskID int64) (*model.TaskSchedule, error) {
	row := new(model.TaskSchedule)
	if err := r.db.NewSelect().Model(row).Where("latest_task_id = ?", taskID).Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *BunTaskScheduleRepo) ListDue(ctx context.Context, now time.Time) ([]model.TaskSchedule, error) {
	var rows []model.TaskSchedule
	err := r.db.NewSelect().Model(&rows).Where("next_run_at <= ?", now).OrderExpr("next_run_at, key").Scan(ctx)
	return rows, err
}

func (r *BunTaskScheduleRepo) SetHead(ctx context.Context, key string, expectedGeneration, generation int64, expectedTaskID, taskID *int64, nextRunAt time.Time) error {
	if key == "" || generation < 0 || generation < expectedGeneration || nextRunAt.IsZero() {
		return ErrInvalidInput
	}
	q := r.db.NewUpdate().Model((*model.TaskSchedule)(nil)).Set("latest_task_id = ?", taskID).Set("generation = ?", generation).Set("next_run_at = ?", nextRunAt).Where("key = ? AND generation = ?", key, expectedGeneration)
	if expectedTaskID == nil {
		q.Where("latest_task_id IS NULL")
	} else {
		q.Where("latest_task_id = ?", *expectedTaskID)
	}
	result, err := q.Exec(ctx)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunTaskScheduleRepo) ScheduleNext(ctx context.Context, key string, taskID int64, nextRunAt time.Time) error {
	if key == "" || taskID < 1 || nextRunAt.IsZero() {
		return ErrInvalidInput
	}
	result, err := r.db.NewUpdate().Model((*model.TaskSchedule)(nil)).Set("next_run_at = ?", nextRunAt).Where("key = ? AND latest_task_id = ?", key, taskID).Exec(ctx)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrConflict
	}
	return nil
}
