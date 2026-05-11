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
	if task != nil && task.Status == "" {
		task.Status = model.TaskStatusQueued
	}
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

// ClaimReady atomically claims one ready task of the given type.
// Uses a SQLite-safe atomic UPDATE (no SELECT FOR UPDATE).
// Returns nil, nil if no task is ready.
func (r *BunTaskRepo) ClaimReady(ctx context.Context, taskType model.TaskType, leaseDuration time.Duration) (*model.Task, error) {
	now := time.Now()
	leaseUntil := now.Add(leaseDuration)

	task := new(model.Task)
	// Atomic claim: UPDATE ... WHERE id = (subquery) RETURNING *
	// The scheduled_at filter ensures future retry/wait tasks are not claimed prematurely.
	err := r.db.NewRaw(
		`UPDATE tasks
		 SET status = ?, claimed_at = ?, lease_until = ?, started_at = ?,
		     last_error = NULL, wait_reason = NULL, status_message = NULL
		 WHERE id = (
		     SELECT id FROM tasks
		     WHERE type = ?
		       AND status IN (?, ?, ?)
		       AND scheduled_at <= ?
		     ORDER BY scheduled_at ASC
		     LIMIT 1
		 )
		 AND status IN (?, ?, ?)
		 RETURNING *`,
		model.TaskStatusRunning, now, leaseUntil, now,
		taskType,
		model.TaskStatusQueued, model.TaskStatusScheduled, model.TaskStatusWaiting,
		now,
		model.TaskStatusQueued, model.TaskStatusScheduled, model.TaskStatusWaiting,
	).Scan(ctx, task)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("claiming ready task: %w", err)
	}
	return task, nil
}

func (r *BunTaskRepo) RenewLease(ctx context.Context, claimedTask *model.Task, leaseDuration time.Duration) error {
	if leaseDuration < 0 {
		leaseDuration = 0
	}
	taskID, claimedAt, err := runningTaskClaim(claimedTask)
	if err != nil {
		return err
	}
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("lease_until = ?", now.Add(leaseDuration)).
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Where("claimed_at = ?", claimedAt).
		Where("lease_until IS NOT NULL AND lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("renewing task lease: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("renewing task %d lease: not in active running claim", taskID)
	}
	return nil
}

// Complete marks a running task as completed.
func (r *BunTaskRepo) Complete(ctx context.Context, claimedTask *model.Task) error {
	return r.complete(ctx, claimedTask, "")
}

func (r *BunTaskRepo) CompleteWithMessage(ctx context.Context, claimedTask *model.Task, message string) error {
	return r.complete(ctx, claimedTask, message)
}

func (r *BunTaskRepo) complete(ctx context.Context, claimedTask *model.Task, message string) error {
	taskID, claimedAt, err := runningTaskClaim(claimedTask)
	if err != nil {
		return err
	}
	now := time.Now()
	q := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusCompleted).
		Set("completed_at = ?", now).
		Set("last_error = NULL").
		Set("wait_reason = NULL").
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Where("claimed_at = ?", claimedAt).
		Where("lease_until IS NOT NULL AND lease_until > ?", now)
	if message == "" {
		q = q.Set("status_message = NULL")
	} else {
		q = q.Set("status_message = ?", message)
	}
	res, err := q.Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("completing task %d: not in active running claim", taskID)
	}
	return nil
}

// FailRunning marks a running task as failed without scheduling automatic retry.
func (r *BunTaskRepo) FailRunning(ctx context.Context, claimedTask *model.Task, lastError string) error {
	taskID, claimedAt, err := runningTaskClaim(claimedTask)
	if err != nil {
		return err
	}
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).
		Set("last_error = ?", lastError).
		Set("status_message = NULL").
		Set("wait_reason = NULL").
		Set("completed_at = ?", now).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Where("claimed_at = ?", claimedAt).
		Where("lease_until IS NOT NULL AND lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failing task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("failing task %d: not in active running claim", taskID)
	}
	return nil
}

func (r *BunTaskRepo) ScheduleRetryRunning(ctx context.Context, claimedTask *model.Task, lastError string, backoff time.Duration) (model.TaskStatus, error) {
	if backoff < 0 {
		backoff = 0
	}
	taskID, claimedAt, err := runningTaskClaim(claimedTask)
	if err != nil {
		return "", err
	}

	var nextStatus model.TaskStatus
	err = r.runMaybeTx(ctx, func(db bun.IDB) error {
		now := time.Now()
		task, err := loadRunningTaskClaim(ctx, db, taskID, claimedAt, now)
		if err != nil {
			return err
		}

		nextRetryCount := task.RetryCount + 1
		nextStatus = retryStatusForTask(task)
		completedAt := (*time.Time)(nil)
		scheduledAt := now.Add(backoff)
		if nextStatus == model.TaskStatusExhausted {
			completedAt = &now
			scheduledAt = now
		}

		q := db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("status = ?", nextStatus).
			Set("retry_count = ?", nextRetryCount).
			Set("last_error = ?", lastError).
			Set("status_message = NULL").
			Set("wait_reason = NULL").
			Set("scheduled_at = ?", scheduledAt).
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("started_at = NULL").
			Set("completed_at = ?", completedAt).
			Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
			Where("claimed_at = ?", claimedAt).
			Where("lease_until IS NOT NULL AND lease_until > ?", now)
		res, err := q.Exec(ctx)
		if err != nil {
			return fmt.Errorf("scheduling task retry: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("scheduling retry for task %d: not in same running claim", taskID)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return nextStatus, nil
}

func loadRunningTaskClaim(ctx context.Context, db bun.IDB, taskID int64, claimedAt time.Time, now time.Time) (*model.Task, error) {
	task := new(model.Task)
	if err := db.NewSelect().
		Model(task).
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Where("claimed_at = ?", claimedAt).
		Where("lease_until IS NOT NULL AND lease_until > ?", now).
		Scan(ctx); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("loading running task claim %d: not in active running claim", taskID)
		}
		return nil, fmt.Errorf("loading running task claim: %w", err)
	}
	return task, nil
}

func runningTaskClaim(task *model.Task) (int64, time.Time, error) {
	if task == nil || task.ID == 0 || task.ClaimedAt == nil {
		return 0, time.Time{}, fmt.Errorf("running task claim is required: %w", ErrInvalidInput)
	}
	return task.ID, *task.ClaimedAt, nil
}

func retryStatusForTask(task *model.Task) model.TaskStatus {
	if task.RetryCount+1 >= task.MaxRetries {
		return model.TaskStatusExhausted
	}
	return model.TaskStatusScheduled
}

func (r *BunTaskRepo) WaitRunning(ctx context.Context, claimedTask *model.Task, reason model.TaskWaitReason, message string, delay time.Duration) error {
	if delay < 0 {
		delay = 0
	}
	taskID, claimedAt, err := runningTaskClaim(claimedTask)
	if err != nil {
		return err
	}
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusWaiting).
		Set("last_error = NULL").
		Set("wait_reason = ?", reason).
		Set("status_message = ?", message).
		Set("scheduled_at = ?", now.Add(delay)).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Where("claimed_at = ?", claimedAt).
		Where("lease_until IS NOT NULL AND lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("waiting running task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("waiting task %d: not in active running claim", taskID)
	}
	return nil
}

func (r *BunTaskRepo) ReleaseRunning(ctx context.Context, claimedTask *model.Task) error {
	taskID, claimedAt, err := runningTaskClaim(claimedTask)
	if err != nil {
		return err
	}
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusQueued).
		Set("scheduled_at = ?", now).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Set("last_error = NULL").
		Set("wait_reason = NULL").
		Set("status_message = NULL").
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Where("claimed_at = ?", claimedAt).
		Where("lease_until IS NOT NULL AND lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("releasing running task: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("releasing task %d: not in active running claim", taskID)
	}
	return nil
}

// ReleaseExpiredLeases resets running tasks whose lease has expired back to queued.
func (r *BunTaskRepo) ReleaseExpiredLeases(ctx context.Context) (int, error) {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusQueued).
		Set("scheduled_at = ?", now).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Set("last_error = NULL").
		Set("wait_reason = NULL").
		Set("status_message = NULL").
		Where("status = ? AND (lease_until IS NULL OR lease_until < ?)", model.TaskStatusRunning, now).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("releasing expired leases: %w", err)
	}
	rows, _ := res.RowsAffected()
	return int(rows), nil
}

// MarkRunningExhausted marks a running task as exhausted.
func (r *BunTaskRepo) MarkRunningExhausted(ctx context.Context, claimedTask *model.Task, lastError string) error {
	taskID, claimedAt, err := runningTaskClaim(claimedTask)
	if err != nil {
		return err
	}
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusExhausted).
		Set("last_error = ?", lastError).
		Set("status_message = NULL").
		Set("wait_reason = NULL").
		Set("retry_count = retry_count + 1").
		Set("completed_at = ?", now).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("id = ? AND status = ?", taskID, model.TaskStatusRunning).
		Where("claimed_at = ?", claimedAt).
		Where("lease_until IS NOT NULL AND lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("marking task exhausted: %w", err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("marking task %d exhausted: not in active running claim", taskID)
	}
	return nil
}

// ListExhausted returns exhausted tasks, ordered by most recent first.
func (r *BunTaskRepo) ListExhausted(ctx context.Context, limit int) ([]model.Task, error) {
	var tasks []model.Task
	q := r.db.NewSelect().
		Model(&tasks).
		Where("status = ?", model.TaskStatusExhausted).
		OrderExpr("COALESCE(completed_at, started_at, scheduled_at) DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	err := q.Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing exhausted tasks: %w", err)
	}
	return tasks, nil
}

func (r *BunTaskRepo) RetryExhausted(ctx context.Context, taskID int64) error {
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		task := new(model.Task)
		err := db.NewSelect().
			Model(task).
			Where("id = ? AND status = ?", taskID, model.TaskStatusExhausted).
			Scan(ctx)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("retrying exhausted task %d: %w", taskID, ErrNotFound)
			}
			return fmt.Errorf("loading exhausted task: %w", err)
		}

		now := time.Now()
		if err := resetFailedObjectForTaskRetry(ctx, db, task, now); err != nil {
			return err
		}

		res, err := db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("status = ?", model.TaskStatusQueued).
			Set("retry_count = 0").
			Set("scheduled_at = ?", now).
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("started_at = NULL").
			Set("completed_at = NULL").
			Set("last_error = NULL").
			Set("wait_reason = NULL").
			Set("status_message = NULL").
			Where("id = ? AND status = ?", taskID, model.TaskStatusExhausted).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("retrying exhausted task: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("retrying exhausted task %d: %w", taskID, ErrNotFound)
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
	case "ingress_commit":
		return model.ObjectStateCommitting
	case "peer_pull", "peer_commit":
		return model.ObjectStateReplicating
	case "ensure_dataset":
		if taskPayloadString(task.Payload, "transfer_method") == string(model.StorageCopyTransferMethodPeerPull) {
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

func taskPayloadString(payload map[string]interface{}, key string) string {
	if payload == nil {
		return ""
	}
	raw, ok := payload[key]
	if !ok {
		return ""
	}
	value, _ := raw.(string)
	return value
}

func (r *BunTaskRepo) runMaybeTx(ctx context.Context, fn func(bun.IDB) error) error {
	if db, ok := r.db.(*bun.DB); ok {
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(tx)
		})
	}
	return fn(r.db)
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

func activeTaskStatuses() []model.TaskStatus {
	return []model.TaskStatus{
		model.TaskStatusQueued,
		model.TaskStatusScheduled,
		model.TaskStatusWaiting,
		model.TaskStatusRunning,
	}
}

func (r *BunTaskRepo) CountActiveObjectTasksByBucket(ctx context.Context, bucketID int64) (int64, error) {
	count, err := r.db.NewSelect().
		TableExpr("tasks AS t").
		Join("JOIN objects AS o ON o.id = t.ref_id").
		Where("t.ref_type = ?", "object").
		Where("o.bucket_id = ?", bucketID).
		Where("t.status IN (?)", bun.List(activeTaskStatuses())).
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
		Where("status IN (?)", bun.List(activeTaskStatuses())).
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
		Set("last_error = NULL").
		Set("wait_reason = NULL").
		Set("status_message = NULL").
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("started_at = NULL").
		Where("ref_type = ?", refType).
		Where("ref_id = ?", refID).
		Where("type = ?", taskType).
		Where("status IN (?)", bun.List(activeTaskStatuses())).
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
