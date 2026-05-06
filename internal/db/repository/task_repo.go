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
	normalizeTaskStage(task)
	_, err := r.db.NewInsert().Model(task).Exec(ctx)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("inserting task %q: %w", task.IdempotencyKey, ErrAlreadyExists)
		}
		return fmt.Errorf("inserting task: %w", err)
	}
	return nil
}

func normalizeTaskStage(task *model.Task) {
	if task == nil || task.Stage != nil || task.Type != model.TaskTypeUpload {
		return
	}
	stage, _ := task.Payload["stage"].(string)
	if stage == "" {
		stage = "prepare_upload"
	}
	task.Stage = &stage
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
	// The scheduled_at filter ensures tasks with future backoff are not claimed prematurely.
	err := r.db.NewRaw(
		`UPDATE tasks SET status = ?, claimed_at = ?, lease_until = ?, started_at = ?
		 WHERE id = (
		     SELECT id FROM tasks
		     WHERE type = ? AND status = ? AND scheduled_at <= ?
		     ORDER BY scheduled_at ASC
		     LIMIT 1
		 )
		 AND status = ?
		 RETURNING *`,
		model.TaskStatusRunning, now, leaseUntil, now,
		taskType, model.TaskStatusPending, now,
		model.TaskStatusPending,
	).Scan(ctx, task)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("claiming pending task: %w", err)
	}
	return task, nil
}

func (r *BunTaskRepo) RenewLease(ctx context.Context, taskID int64, leaseDuration time.Duration) error {
	if leaseDuration < 0 {
		leaseDuration = 0
	}
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("lease_until = ?", time.Now().Add(leaseDuration)).
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("renewing task lease: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("renewing task %d lease: not in running state", taskID)
	}
	return nil
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

func (r *BunTaskRepo) DeferRunning(ctx context.Context, taskID int64, delay time.Duration, reason string) error {
	if delay < 0 {
		delay = 0
	}
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusPending).
		Set("last_error = ?", reason).
		Set("scheduled_at = ?", time.Now().Add(delay)).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("deferring running task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("deferring task %d: not in running state", taskID)
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
		Where("status = ? AND (lease_until IS NULL OR lease_until < ?)", model.TaskStatusRunning, now).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("releasing expired leases: %w", err)
	}
	rows, _ := res.RowsAffected()
	return int(rows), nil
}

// FailTerminal marks a running task as dead-letter (permanently failed after max retries).
func (r *BunTaskRepo) FailTerminal(ctx context.Context, taskID int64, lastError string) error {
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusDeadLetter).
		Set("last_error = ?", lastError).
		Set("retry_count = retry_count + 1").
		Set("completed_at = ?", time.Now()).
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking task dead-letter: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("marking task %d dead-letter: not in running state", taskID)
	}
	return nil
}

// ListDeadLetters returns dead-letter tasks, ordered by most recent first.
func (r *BunTaskRepo) ListDeadLetters(ctx context.Context, limit int) ([]model.Task, error) {
	var tasks []model.Task
	q := r.db.NewSelect().
		Model(&tasks).
		Where("status = ?", model.TaskStatusDeadLetter).
		OrderExpr("COALESCE(completed_at, started_at, scheduled_at) DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	err := q.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing dead-letter tasks: %w", err)
	}
	return tasks, nil
}

func (r *BunTaskRepo) RetryDeadLetter(ctx context.Context, taskID int64) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		task := new(model.Task)
		err := db.NewSelect().
			Model(task).
			Where("id = ? AND status = ?", taskID, model.TaskStatusDeadLetter).
			Scan(ctx)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("retrying dead-letter task %d: %w", taskID, ErrNotFound)
			}
			return fmt.Errorf("loading dead-letter task: %w", err)
		}

		now := time.Now()
		if err := resetFailedObjectForTaskRetry(ctx, db, task, now); err != nil {
			return err
		}

		res, err := db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("status = ?", model.TaskStatusPending).
			Set("retry_count = 0").
			Set("scheduled_at = ?", now).
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("started_at = NULL").
			Set("completed_at = NULL").
			Set("last_error = NULL").
			Where("id = ? AND status = ?", taskID, model.TaskStatusDeadLetter).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("retrying dead-letter task: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("retrying dead-letter task %d: %w", taskID, ErrNotFound)
		}
		return nil
	})
}

func resetFailedObjectForTaskRetry(ctx context.Context, db bun.IDB, task *model.Task, now time.Time) error {
	if task.Type != model.TaskTypeUpload || task.RefType != "object" || task.RefVersionID == "" {
		return nil
	}

	target := retryObjectState(task)
	q := db.NewUpdate().
		Model((*model.ObjectVersion)(nil)).
		Set("state = ?", target).
		Set("failed_at_state = NULL").
		Set("last_error = NULL").
		Set("updated_at = ?", now).
		Where("version_id = ? AND state = ?", task.RefVersionID, model.ObjectStateFailed)
	if target == model.ObjectStateReplicating {
		if uploadID := taskPayloadInt64(task.Payload, "upload_id"); uploadID > 0 {
			q = q.Set("storage_upload_id = ?", uploadID)
		}
	}
	if _, err := q.Exec(ctx); err != nil {
		return fmt.Errorf("resetting failed object for task retry: %w", err)
	}
	return nil
}

func retryObjectState(task *model.Task) model.ObjectState {
	stage := ""
	if task.Stage != nil {
		stage = *task.Stage
	}
	if stage == "" {
		stage, _ = task.Payload["stage"].(string)
	}
	switch stage {
	case "primary_commit":
		return model.ObjectStateCommitting
	case "secondary_pull", "secondary_commit":
		return model.ObjectStateReplicating
	case "ensure_dataset":
		if taskPayloadInt64(task.Payload, "copy_index") > 0 {
			return model.ObjectStateReplicating
		}
	}
	return model.ObjectStateUploading
}

func taskPayloadInt64(payload map[string]interface{}, key string) int64 {
	if payload == nil {
		return 0
	}
	raw, ok := payload[key]
	if !ok {
		return 0
	}
	switch v := raw.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	}
	return 0
}

func (r *BunTaskRepo) runMaybeTx(ctx context.Context, fn func(bun.IDB) error) error {
	if db, ok := r.db.(*bun.DB); ok {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(tx)
		})
	}
	return fn(r.db)
}

// Requeue resets a failed task back to pending with a scheduled backoff delay.
func (r *BunTaskRepo) Requeue(ctx context.Context, taskID int64, backoff time.Duration) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusPending).
		Set("scheduled_at = ?", now.Add(backoff)).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("id = ? AND status = ?", taskID, model.TaskStatusFailed).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("requeuing task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("requeuing task %d: not in failed state", taskID)
	}
	return nil
}

// CountByStatus returns task counts grouped by type and status.
func (r *BunTaskRepo) CountByStatus(ctx context.Context) ([]TaskStatusCount, error) {
	var counts []TaskStatusCount
	err := r.db.NewSelect().
		TableExpr("tasks").
		ColumnExpr("type, status, COUNT(*) AS count").
		GroupExpr("type, status").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting tasks by status: %w", err)
	}
	return counts, nil
}

func (r *BunTaskRepo) CountActiveObjectTasksByBucket(ctx context.Context, bucketID int64) (int64, error) {
	count, err := r.db.NewSelect().
		TableExpr("tasks AS t").
		Join("JOIN objects AS o ON o.id = t.ref_id").
		Where("t.ref_type = ?", "object").
		Where("o.bucket_id = ?", bucketID).
		Where("t.status IN (?)", bun.List([]model.TaskStatus{model.TaskStatusPending, model.TaskStatusRunning})).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting active object tasks by bucket: %w", err)
	}
	return int64(count), nil
}

func (r *BunTaskRepo) CountActiveBucketTasksByBucketID(ctx context.Context, bucketID int64) (int64, error) {
	count, err := r.db.NewSelect().
		TableExpr("tasks").
		Where("ref_type = ?", "bucket").
		Where("ref_id = ?", bucketID).
		Where("status IN (?)", bun.List([]model.TaskStatus{model.TaskStatusPending, model.TaskStatusRunning})).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting active bucket tasks by bucket ID: %w", err)
	}
	return int64(count), nil
}

// List returns tasks with optional type/stage/status filters, paginated by offset/limit.
func (r *BunTaskRepo) List(ctx context.Context, taskType string, stage string, status string, limit, offset int) ([]model.Task, int, error) {
	applyFilters := func(q *bun.SelectQuery) *bun.SelectQuery {
		if taskType != "" {
			q = q.Where("type = ?", taskType)
		}
		if stage != "" {
			q = q.Where("stage = ?", stage)
		}
		if status != "" {
			q = q.Where("status = ?", status)
		}
		return q
	}

	total, err := applyFilters(r.db.NewSelect().Model((*model.Task)(nil))).Count(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("counting tasks: %w", err)
	}
	var tasks []model.Task
	err = applyFilters(r.db.NewSelect().Model(&tasks)).OrderExpr("id DESC").Limit(limit).Offset(offset).Scan(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("listing tasks: %w", err)
	}
	return tasks, total, nil
}

func (r *BunTaskRepo) CompleteByRef(ctx context.Context, refType string, refID int64, taskType model.TaskType) error {
	now := time.Now()
	res, err := r.db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusCompleted).
		Set("completed_at = ?", now).
		Where("ref_type = ?", refType).
		Where("ref_id = ?", refID).
		Where("type = ?", taskType).
		Where("status IN (?, ?)", model.TaskStatusPending, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing tasks by ref: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("no matching %s task for %s/%d: %w", taskType, refType, refID, ErrNotFound)
	}
	return nil
}
