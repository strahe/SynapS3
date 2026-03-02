package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// BunTaskRepo implements TaskRepository using Bun ORM.
type BunTaskRepo struct {
	db bun.IDB
}

var _ TaskRepository = (*BunTaskRepo)(nil)

func (r *BunTaskRepo) Create(ctx context.Context, task *model.Task) error {
	_, err := r.db.NewInsert().Model(task).Exec(ctx)
	if err != nil {
		return fmt.Errorf("inserting task: %w", err)
	}
	return nil
}

func (r *BunTaskRepo) GetByID(ctx context.Context, id int64) (*model.Task, error) {
	task := new(model.Task)
	err := r.db.NewSelect().
		Model(task).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting task by id: %w", err)
	}
	return task, nil
}

// ClaimPending atomically claims one pending task of the given type.
// Uses a SQLite-safe atomic UPDATE (no SELECT FOR UPDATE).
// Returns nil, nil if no pending task is available.
func (r *BunTaskRepo) ClaimPending(ctx context.Context, taskType model.TaskType, leaseDuration time.Duration) (*model.Task, error) {
	now := time.Now()
	leaseUntil := now.Add(leaseDuration)

	task := new(model.Task)
	// Atomic claim: UPDATE ... WHERE id = (subquery) RETURNING *
	err := r.db.NewRaw(
		`UPDATE tasks SET status = ?, claimed_at = ?, lease_until = ?, started_at = ?
		 WHERE id = (
		     SELECT id FROM tasks
		     WHERE type = ? AND status = ?
		     ORDER BY scheduled_at ASC
		     LIMIT 1
		 )
		 RETURNING *`,
		model.TaskStatusRunning, now, leaseUntil, now,
		taskType, model.TaskStatusPending,
	).Scan(ctx, task)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("claiming pending task: %w", err)
	}
	return task, nil
}

// Complete marks a running task as completed.
func (r *BunTaskRepo) Complete(ctx context.Context, taskID int64) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusCompleted).
		Set("completed_at = ?", now).
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("completing task %d: not in running state", taskID)
	}
	return nil
}

// Fail marks a running task as failed, recording the error and incrementing retry count.
func (r *BunTaskRepo) Fail(ctx context.Context, taskID int64, lastError string) error {
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).
		Set("last_error = ?", lastError).
		Set("retry_count = retry_count + 1").
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failing task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("failing task %d: not in running state", taskID)
	}
	return nil
}

// ReleaseExpiredLeases resets running tasks whose lease has expired back to pending.
func (r *BunTaskRepo) ReleaseExpiredLeases(ctx context.Context) (int, error) {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusPending).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("status = ? AND lease_until < ?", model.TaskStatusRunning, now).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("releasing expired leases: %w", err)
	}
	rows, _ := res.RowsAffected()
	return int(rows), nil
}
