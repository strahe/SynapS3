package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

const claimExpiredTaskSQLiteSQL = `UPDATE tasks
SET status = 'running',
    resume_mode = 'recover',
    claim_generation = claim_generation + 1,
    claimed_at = ?,
    lease_until = ?,
    started_at = COALESCE(started_at, ?),
    finished_at = NULL,
    updated_at = ?
WHERE id = (
    SELECT id
    FROM tasks
    WHERE status = 'running' AND lease_until <= ?
    ORDER BY lease_until, id
    LIMIT 1
)
AND status = 'running' AND lease_until <= ?
RETURNING *`

const claimPendingTaskSQLiteSQL = `UPDATE tasks
SET status = 'running',
    claim_generation = claim_generation + 1,
    claimed_at = ?,
    lease_until = ?,
    started_at = COALESCE(started_at, ?),
    finished_at = NULL,
    updated_at = ?
WHERE id = (
    SELECT id
    FROM tasks
    WHERE status = 'pending' AND available_at <= ?
    ORDER BY available_at, id
    LIMIT 1
)
AND status = 'pending' AND available_at <= ?
RETURNING *`

const claimExpiredTaskPostgresSQL = `UPDATE tasks
SET status = 'running',
    resume_mode = 'recover',
    claim_generation = claim_generation + 1,
    claimed_at = ?,
    lease_until = ?,
    started_at = COALESCE(started_at, ?),
    finished_at = NULL,
    updated_at = ?
WHERE id = (
    SELECT id
    FROM tasks
    WHERE status = 'running' AND lease_until <= ?
    ORDER BY lease_until, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
AND status = 'running' AND lease_until <= ?
RETURNING *`

const claimPendingTaskPostgresSQL = `UPDATE tasks
SET status = 'running',
    claim_generation = claim_generation + 1,
    claimed_at = ?,
    lease_until = ?,
    started_at = COALESCE(started_at, ?),
    finished_at = NULL,
    updated_at = ?
WHERE id = (
    SELECT id
    FROM tasks
    WHERE status = 'pending' AND available_at <= ?
    ORDER BY available_at, id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
AND status = 'pending' AND available_at <= ?
RETURNING *`

// BunTaskRepo is the persistence boundary used by TaskService and Engine.
type BunTaskRepo struct {
	db bun.IDB
}

var _ TaskRepository = (*BunTaskRepo)(nil)

func (r *BunTaskRepo) Enqueue(ctx context.Context, task *model.Task) (*model.Task, bool, error) {
	if task == nil || task.Type == "" || task.IdempotencyKey == "" || task.InputVersion < 1 || len(task.Input) == 0 || task.InputHash == "" {
		return nil, false, fmt.Errorf("task identity and canonical input are required: %w", ErrInvalidInput)
	}
	if task.Status == "" {
		task.Status = model.TaskStatusPending
	}
	if task.ResumeMode == "" {
		task.ResumeMode = model.TaskResumeModeExecute
	}
	if task.AvailableAt.IsZero() {
		task.AvailableAt = time.Now()
	}

	inserted := false
	if err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		result, err := db.NewInsert().
			Model(task).
			On("CONFLICT (type, idempotency_key) DO NOTHING").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("enqueuing task %s/%s: %w", task.Type, task.IdempotencyKey, err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return nil
		}
		inserted = true
		payload := &model.TaskPayload{TaskID: task.ID, Input: task.Input, Checkpoint: task.Checkpoint}
		if _, err := db.NewInsert().Model(payload).Exec(ctx); err != nil {
			return fmt.Errorf("enqueuing task %s/%s payload: %w", task.Type, task.IdempotencyKey, err)
		}
		return nil
	}); err != nil {
		return nil, false, err
	}
	if inserted {
		return task, true, nil
	}
	existing, err := r.GetByIdentity(ctx, task.Type, task.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	if existing == nil {
		return nil, false, fmt.Errorf("loading task after identity conflict: %w", ErrNotFound)
	}
	return existing, false, nil
}

// withTaskPayload projects the JSON a task carries from the row that holds it.
func withTaskPayload(q *bun.SelectQuery) *bun.SelectQuery {
	return q.
		ColumnExpr("task.*").
		ColumnExpr("task_payload.input_json AS input").
		ColumnExpr("task_payload.checkpoint_json AS checkpoint").
		Join("JOIN task_payloads AS task_payload ON task_payload.task_id = task.id")
}

// loadTaskPayload fills in the JSON for a task read without the join, such as
// one returned by the claim statement.
func loadTaskPayload(ctx context.Context, db bun.IDB, task *model.Task) error {
	payload := new(model.TaskPayload)
	if err := db.NewSelect().Model(payload).Where("task_id = ?", task.ID).Scan(ctx); err != nil {
		return fmt.Errorf("selecting task %d payload: %w", task.ID, err)
	}
	task.Input = payload.Input
	task.Checkpoint = payload.Checkpoint
	return nil
}

func (r *BunTaskRepo) GetByID(ctx context.Context, id int64) (*model.Task, error) {
	task := new(model.Task)
	err := withTaskPayload(r.db.NewSelect().Model(task)).Where("task.id = ?", id).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("selecting task %d: %w", id, err)
	}
	return task, nil
}

func (r *BunTaskRepo) GetByIdentity(ctx context.Context, taskType model.TaskType, key string) (*model.Task, error) {
	task := new(model.Task)
	err := withTaskPayload(r.db.NewSelect().Model(task)).
		Where("task.type = ? AND task.idempotency_key = ?", taskType, key).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("selecting task %s/%s: %w", taskType, key, err)
	}
	return task, nil
}

func (r *BunTaskRepo) ClaimNext(ctx context.Context, leaseDuration time.Duration) (*model.Task, error) {
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive: %w", ErrInvalidInput)
	}
	if r.db.Dialect().Name() != dialect.PG {
		return r.claimNextSQLite(ctx, r.db, leaseDuration)
	}
	db, ok := r.db.(*bun.DB)
	if !ok {
		return r.claimNextPostgres(ctx, r.db, leaseDuration)
	}
	var claimed *model.Task
	err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var err error
		claimed, err = r.claimNextPostgres(ctx, tx, leaseDuration)
		return err
	})
	return claimed, err
}

func (r *BunTaskRepo) claimNextPostgres(ctx context.Context, db bun.IDB, leaseDuration time.Duration) (*model.Task, error) {
	return claimNextTask(ctx, db, leaseDuration, claimExpiredTaskPostgresSQL, claimPendingTaskPostgresSQL)
}

func (r *BunTaskRepo) claimNextSQLite(ctx context.Context, db bun.IDB, leaseDuration time.Duration) (*model.Task, error) {
	return claimNextTask(ctx, db, leaseDuration, claimExpiredTaskSQLiteSQL, claimPendingTaskSQLiteSQL)
}

func claimNextTask(
	ctx context.Context,
	db bun.IDB,
	leaseDuration time.Duration,
	recoverySQL string,
	pendingSQL string,
) (*model.Task, error) {
	now := time.Now()
	leaseUntil := now.Add(leaseDuration)
	task, err := claimTaskWithSQL(ctx, db, recoverySQL, now, leaseUntil)
	if err != nil || task != nil {
		return task, err
	}
	return claimTaskWithSQL(ctx, db, pendingSQL, now, leaseUntil)
}

func claimTaskWithSQL(ctx context.Context, db bun.IDB, query string, now, leaseUntil time.Time) (*model.Task, error) {
	task := new(model.Task)
	err := db.NewRaw(query, now, leaseUntil, now, now, now, now).Scan(ctx, task)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claiming next task: %w", err)
	}
	if err := loadTaskPayload(ctx, db, task); err != nil {
		return nil, err
	}
	return task, nil
}

func (r *BunTaskRepo) RenewLease(ctx context.Context, id, generation int64, leaseDuration time.Duration) (time.Time, error) {
	now := time.Now()
	until := now.Add(leaseDuration)
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("lease_until = ?", until).
		Set("updated_at = ?", now).
		Where("id = ? AND status = ?", id, model.TaskStatusRunning).
		Where("claim_generation = ?", generation).
		Where("lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("renewing task %d lease: %w", id, err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return time.Time{}, ErrTaskLeaseLost
	}
	return until, nil
}

func (r *BunTaskRepo) WriteCheckpoint(ctx context.Context, id, generation int64, checkpoint []byte) error {
	if len(checkpoint) == 0 {
		return fmt.Errorf("checkpoint is required: %w", ErrInvalidInput)
	}
	now := time.Now()
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		result, err := db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("resume_mode = ?", model.TaskResumeModeRecover).
			Set("updated_at = ?", now).
			Where("id = ? AND status = ?", id, model.TaskStatusRunning).
			Where("claim_generation = ?", generation).
			Where("lease_until > ?", now).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("writing task %d checkpoint: %w", id, err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return ErrTaskLeaseLost
		}
		if _, err := db.NewUpdate().
			Model((*model.TaskPayload)(nil)).
			Set("checkpoint_json = ?", checkpoint).
			Where("task_id = ?", id).
			Exec(ctx); err != nil {
			return fmt.Errorf("writing task %d checkpoint: %w", id, err)
		}
		return nil
	})
}

func (r *BunTaskRepo) ValidateClaim(ctx context.Context, id, generation int64) error {
	now := time.Now()
	var found int64
	err := r.db.NewRaw(`UPDATE tasks
		SET updated_at = updated_at
		WHERE id = ? AND status = 'running' AND claim_generation = ? AND lease_until > ?
		RETURNING id`, id, generation, now).Scan(ctx, &found)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrTaskLeaseLost
	}
	if err != nil {
		return fmt.Errorf("validating task %d claim: %w", id, err)
	}
	return nil
}

func (r *BunTaskRepo) Settle(ctx context.Context, id, generation int64, transition TaskTransition) error {
	if !validTaskTransition(transition) {
		return fmt.Errorf("invalid task transition: %w", ErrInvalidInput)
	}
	now := time.Now()
	query := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", transition.Status).
		Set("wait_reason = ?", transition.WaitReason).
		Set("failure_reason = ?", transition.FailureReason).
		Set("last_error = ?", transition.LastError).
		Set("status_message = ?", transition.StatusMessage).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("updated_at = ?", now).
		Where("id = ? AND status = ?", id, model.TaskStatusRunning).
		Where("claim_generation = ?", generation).
		Where("lease_until > ?", now)
	if transition.IncrementRetry {
		query = query.Set("retry_count = retry_count + 1")
	}
	if transition.Status == model.TaskStatusPending {
		query = query.
			Set("resume_mode = CASE WHEN cancellation_requested_at IS NOT NULL THEN ? ELSE ? END", model.TaskResumeModeRecover, transition.ResumeMode).
			Set("available_at = CASE WHEN cancellation_requested_at IS NOT NULL THEN ? ELSE ? END", now, transition.AvailableAt).
			Set("finished_at = NULL").
			Set("retention_until = NULL").
			Set("acknowledged_at = NULL")
	} else {
		query = query.
			Set("resume_mode = ?", transition.ResumeMode).
			Set("finished_at = ?", now).
			Set("retention_until = ?", transition.RetentionUntil)
	}
	result, err := query.Exec(ctx)
	if err != nil {
		return fmt.Errorf("settling task %d: %w", id, err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrTaskLeaseLost
	}
	return nil
}

func validTaskTransition(transition TaskTransition) bool {
	switch transition.Status {
	case model.TaskStatusPending:
		return !transition.AvailableAt.IsZero() &&
			(transition.ResumeMode == model.TaskResumeModeExecute || transition.ResumeMode == model.TaskResumeModeRecover) &&
			transition.RetentionUntil == nil
	case model.TaskStatusCompleted, model.TaskStatusCancelled:
		return transition.RetentionUntil != nil
	case model.TaskStatusFailed:
		return transition.RetentionUntil == nil
	default:
		return false
	}
}

func (r *BunTaskRepo) ShortenLease(ctx context.Context, id, generation int64, duration time.Duration) error {
	if duration <= 0 {
		return fmt.Errorf("lease duration must be positive: %w", ErrInvalidInput)
	}
	now := time.Now()
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Set("lease_until = ?", now.Add(duration)).
		Set("updated_at = ?", now).
		Where("id = ? AND status = ?", id, model.TaskStatusRunning).
		Where("claim_generation = ?", generation).
		Where("lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("shortening task %d lease: %w", id, err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrTaskLeaseLost
	}
	return nil
}

func (r *BunTaskRepo) WakePending(ctx context.Context, ids []int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	for _, id := range ids {
		if id < 1 {
			return 0, fmt.Errorf("task IDs must be positive: %w", ErrInvalidInput)
		}
	}
	now := time.Now()
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("available_at = ?", now).
		Set("updated_at = ?", now).
		Where("id IN (?)", bun.List(ids)).
		Where("status = ?", model.TaskStatusPending).
		Where("available_at > ?", now).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("waking pending tasks: %w", err)
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

func (r *BunTaskRepo) RequestCancellation(ctx context.Context, id int64, reason string) error {
	now := time.Now()
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("cancellation_requested_at = COALESCE(cancellation_requested_at, ?)", now).
		Set("cancellation_reason = COALESCE(cancellation_reason, ?)", nullableText(reason)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Set("available_at = CASE WHEN status = ? THEN ? ELSE available_at END", model.TaskStatusPending, now).
		Set("updated_at = ?", now).
		Where("id = ? AND status IN (?, ?)", id, model.TaskStatusPending, model.TaskStatusRunning).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("requesting cancellation for task %d: %w", id, err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrNotFound
	}
	return nil
}

func (r *BunTaskRepo) RetryFailed(ctx context.Context, id int64) error {
	now := time.Now()
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusPending).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Set("available_at = ?", now).
		Set("retry_count = 0").
		Set("failure_reason = NULL").
		Set("last_error = NULL").
		Set("status_message = NULL").
		Set("wait_reason = NULL").
		Set("finished_at = NULL").
		Set("acknowledged_at = NULL").
		Set("retention_until = NULL").
		Set("updated_at = ?", now).
		Where("id = ? AND status = ?", id, model.TaskStatusFailed).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("retrying task %d: %w", id, err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrNotFound
	}
	return nil
}

// ReactivateTerminal reuses a terminal idempotency record as a fresh execute
// run. It is intentionally narrower than manual retry: callers must first
// verify that the task's immutable input still describes the desired work.
func (r *BunTaskRepo) ReactivateTerminal(ctx context.Context, id int64) error {
	if id < 1 {
		return fmt.Errorf("reactivating terminal task: %w", ErrInvalidInput)
	}
	now := time.Now()
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		result, err := db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("status = ?", model.TaskStatusPending).
			Set("resume_mode = ?", model.TaskResumeModeExecute).
			Set("available_at = ?", now).
			Set("wait_reason = NULL").
			Set("retry_count = 0").
			Set("failure_reason = NULL").
			Set("last_error = NULL").
			Set("status_message = NULL").
			Set("cancellation_requested_at = NULL").
			Set("cancellation_reason = NULL").
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("started_at = NULL").
			Set("finished_at = NULL").
			Set("acknowledged_at = NULL").
			Set("retention_until = NULL").
			Set("updated_at = ?", now).
			Where("id = ? AND type = ? AND status IN (?, ?)", id, model.TaskTypeUploadPlan, model.TaskStatusFailed, model.TaskStatusCancelled).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("reactivating terminal task %d: %w", id, err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return fmt.Errorf("reactivating terminal task %d: %w", id, ErrConflict)
		}
		result, err = db.NewUpdate().
			Model((*model.TaskPayload)(nil)).
			Set("checkpoint_json = NULL").
			Where("task_id = ?", id).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("clearing terminal task %d checkpoint: %w", id, err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return fmt.Errorf("clearing terminal task %d checkpoint: %w", id, ErrConflict)
		}
		return nil
	})
}

func (r *BunTaskRepo) AcknowledgeFailed(ctx context.Context, id int64, retention time.Duration) error {
	if retention <= 0 {
		return fmt.Errorf("retention must be positive: %w", ErrInvalidInput)
	}
	now := time.Now()
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("acknowledged_at = COALESCE(acknowledged_at, ?)", now).
		Set("retention_until = COALESCE(retention_until, ?)", now.Add(retention)).
		Set("updated_at = ?", now).
		Where("id = ? AND status = ?", id, model.TaskStatusFailed).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("acknowledging task %d: %w", id, err)
	}
	rows, _ := result.RowsAffected()
	if rows != 1 {
		return ErrNotFound
	}
	return nil
}

func (r *BunTaskRepo) DeleteRetained(ctx context.Context, now time.Time, limit int) (int, error) {
	if now.IsZero() || limit < 1 {
		return 0, fmt.Errorf("deleting retained tasks: %w", ErrInvalidInput)
	}
	if limit > 1000 {
		limit = 1000
	}
	var ids []int64
	err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		Column("id").
		Where("retention_until IS NOT NULL AND retention_until <= ?", now).
		Where(`NOT EXISTS (SELECT 1 FROM object_cache WHERE cache_active_task_id = task.id)`).
		Where(`NOT EXISTS (SELECT 1 FROM buckets WHERE durability_task_id = task.id)`).
		Where(`NOT EXISTS (SELECT 1 FROM storage_contents WHERE cleanup_task_id = task.id)`).
		Where(`NOT EXISTS (SELECT 1 FROM storage_copies WHERE active_task_id = task.id)`).
		Where(`NOT EXISTS (SELECT 1 FROM storage_data_sets WHERE ensure_task_id = task.id OR retirement_task_id = task.id)`).
		Where(`NOT EXISTS (SELECT 1 FROM wallet_operations WHERE task_id = task.id)`).
		Where(`NOT EXISTS (SELECT 1 FROM storage_replacements WHERE task_id = task.id)`).
		OrderExpr("retention_until, id").
		Limit(limit).
		Scan(ctx, &ids)
	if err != nil {
		return 0, fmt.Errorf("selecting retained tasks: %w", err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	result, err := r.db.NewDelete().
		Model((*model.Task)(nil)).
		Where("id IN (?)", bun.List(ids)).
		Where("retention_until IS NOT NULL AND retention_until <= ?", now).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("deleting retained tasks: %w", err)
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

func (r *BunTaskRepo) List(ctx context.Context, filter TaskListFilter) (TaskPage, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var tasks []model.Task
	query := withTaskPayload(r.db.NewSelect().Model(&tasks)).
		OrderExpr("task.id DESC").
		Limit(limit + 1)
	if filter.Type != "" {
		query = query.Where("task.type = ?", filter.Type)
	}
	if filter.Status != "" {
		query = query.Where("task.status = ?", filter.Status)
	}
	if filter.Acknowledged != nil {
		if *filter.Acknowledged {
			query = query.Where("task.acknowledged_at IS NOT NULL")
		} else {
			query = query.Where("task.acknowledged_at IS NULL")
		}
	}
	if filter.HideHealthyRecurringSystem {
		query = query.Where("(task.type NOT IN (?) OR task.status = ?)", bun.List(model.RecurringSystemTaskTypes()), model.TaskStatusFailed)
	}
	if filter.BeforeID > 0 {
		query = query.Where("task.id < ?", filter.BeforeID)
	}
	if err := query.Scan(ctx); err != nil {
		return TaskPage{}, fmt.Errorf("listing tasks: %w", err)
	}
	page := TaskPage{Tasks: tasks}
	if len(page.Tasks) > limit {
		page.Tasks = page.Tasks[:limit]
		page.NextBeforeID = page.Tasks[len(page.Tasks)-1].ID
	}
	return page, nil
}

func (r *BunTaskRepo) CountByStatus(ctx context.Context) ([]TaskStatusCount, error) {
	var counts []TaskStatusCount
	err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		Column("type", "status").
		ColumnExpr("COUNT(*) AS count").
		Group("type", "status").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting tasks by status: %w", err)
	}
	return counts, nil
}

func (r *BunTaskRepo) CountByPresentationStatus(ctx context.Context) ([]TaskStatusCount, error) {
	const presentationStatus = "CASE WHEN status = 'failed' AND acknowledged_at IS NOT NULL THEN 'dismissed' ELSE status END"
	var counts []TaskStatusCount
	err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		Column("type").
		ColumnExpr(presentationStatus+" AS status").
		ColumnExpr("COUNT(*) AS count").
		GroupExpr("type, "+presentationStatus).
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting tasks by presentation status: %w", err)
	}
	return counts, nil
}

func (r *BunTaskRepo) CountUnacknowledgedFailed(ctx context.Context) (int64, error) {
	count, err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("status = ?", model.TaskStatusFailed).
		Where("acknowledged_at IS NULL").
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting unacknowledged failed tasks: %w", err)
	}
	return int64(count), nil
}

func (r *BunTaskRepo) CountOverviewActivePipeline(ctx context.Context) ([]TaskPipelineCount, error) {
	var counts []TaskPipelineCount
	err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		ColumnExpr("type AS pipeline").
		Column("status").
		ColumnExpr("COUNT(*) AS count").
		Where("status IN (?, ?)", model.TaskStatusPending, model.TaskStatusRunning).
		Where("type NOT IN (?)", bun.List(model.RecurringSystemTaskTypes())).
		Group("type", "status").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting active task pipeline: %w", err)
	}
	return counts, nil
}

func (r *BunTaskRepo) CountActiveObjectTasksByBucket(ctx context.Context, bucketID int64) (int64, error) {
	var count int64
	err := r.db.NewRaw(`SELECT COUNT(*)
		FROM tasks AS t
		JOIN object_versions AS ov ON ov.version_id = t.subject_key
		WHERE t.subject_type = 'object_version'
		  AND t.status IN ('pending', 'running')
		  AND ov.bucket_id = ?`, bucketID).Scan(ctx, &count)
	if err != nil {
		return 0, fmt.Errorf("counting active object tasks by bucket: %w", err)
	}
	return count, nil
}

func (r *BunTaskRepo) CountActiveBucketTasksByBucketID(ctx context.Context, bucketID int64) (int64, error) {
	count, err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("subject_type = 'bucket' AND subject_key = ?", fmt.Sprint(bucketID)).
		Where("status IN (?, ?)", model.TaskStatusPending, model.TaskStatusRunning).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting active bucket tasks: %w", err)
	}
	return int64(count), nil
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}
