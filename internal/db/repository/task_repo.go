package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
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
    wait_reason = NULL,
    status_message = NULL,
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
    wait_reason = NULL,
    status_message = NULL,
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
    wait_reason = NULL,
    status_message = NULL,
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
    wait_reason = NULL,
    status_message = NULL,
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

// MarkWorkStarted preserves the first operation time under the live claim fence.
func (r *BunTaskRepo) MarkWorkStarted(ctx context.Context, id, generation int64, startedAt time.Time) error {
	if startedAt.IsZero() {
		return ErrInvalidInput
	}
	now := time.Now()
	result, err := r.db.NewUpdate().Model((*model.Task)(nil)).
		Set("work_started_at = COALESCE(work_started_at, ?)", startedAt).
		Set("updated_at = ?", now).
		Where("id = ? AND claim_generation = ? AND status = ? AND lease_until > ?", id, generation, model.TaskStatusRunning, now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("recording task %d work start: %w", id, err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrTaskLeaseLost
	}
	return nil
}

func (r *BunTaskRepo) Enqueue(ctx context.Context, task *model.Task) (*model.Task, bool, error) {
	if task == nil || task.Type == "" || task.IdempotencyKey == "" || task.InputVersion < 1 || len(task.Input) == 0 || task.InputHash == "" || task.RetryGroupKey == "" || len(task.Policy) == 0 || !json.Valid(task.Policy) || task.RetryLimit == nil || *task.RetryLimit < 0 {
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

	if len(task.Runtime) == 0 {
		task.Runtime = json.RawMessage(`{}`)
	}
	if !json.Valid(task.Runtime) {
		return nil, false, ErrInvalidInput
	}
	inserted := false
	if err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		result, err := db.NewInsert().
			Model(task).
			On("CONFLICT (type, idempotency_key) WHERE superseded_at IS NULL DO NOTHING").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("enqueuing task %s/%s: %w", task.Type, task.IdempotencyKey, err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return nil
		}
		inserted = true
		payload := &model.TaskPayload{TaskID: task.ID, Input: task.Input, Checkpoint: task.Checkpoint, Policy: task.Policy, Runtime: task.Runtime}
		if _, err := db.NewInsert().Model(payload).Exec(ctx); err != nil {
			return fmt.Errorf("enqueuing task %s/%s payload: %w", task.Type, task.IdempotencyKey, err)
		}
		return (&BunTaskRepo{db: db}).AppendEvent(ctx, task.ID, "created", json.RawMessage(`{}`))
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
		ColumnExpr("task_payload.policy_json AS policy").
		ColumnExpr("task_payload.runtime_json AS runtime").
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
	task.Policy = payload.Policy
	task.Runtime = payload.Runtime
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
		Where("task.type = ? AND task.idempotency_key = ? AND task.superseded_at IS NULL", taskType, key).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("selecting task %s/%s: %w", taskType, key, err)
	}
	return task, nil
}

func (r *BunTaskRepo) PreviousStoreCheckpoints(ctx context.Context, copyID, taskID int64) ([]model.Task, error) {
	if copyID < 1 || taskID < 1 {
		return nil, ErrInvalidInput
	}
	var tasks []model.Task
	err := withTaskPayload(r.db.NewSelect().Model(&tasks)).
		Where("task.type = ? AND task.status = ?", model.TaskTypeStorageStore, model.TaskStatusFailed).
		Where("task.subject_type = ? AND task.subject_key = ?", model.TaskSubjectStorageCopy, strconv.FormatInt(copyID, 10)).
		Where("task.id <> ? AND task_payload.checkpoint_json IS NOT NULL", taskID).
		OrderExpr("task.id DESC").Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("selecting previous Store checkpoints: %w", err)
	}
	return tasks, nil
}

func (r *BunTaskRepo) ClaimNext(ctx context.Context, leaseDuration time.Duration) (*model.Task, error) {
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive: %w", ErrInvalidInput)
	}
	if r.db.Dialect().Name() != dialect.PG {
		// The claim and its payload read commit together, as on PostgreSQL.
		var claimed *model.Task
		err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
			var err error
			claimed, err = r.claimNextSQLite(ctx, db, leaseDuration)
			return err
		})
		return claimed, err
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

func (r *BunTaskRepo) WriteCheckpoint(ctx context.Context, id, generation int64, checkpoint json.RawMessage) error {
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
		// checkpoint must stay a json.RawMessage: bun renders a plain []byte as
		// a bytea/blob literal, which PostgreSQL jsonb rejects.
		result, err = db.NewUpdate().
			Model((*model.TaskPayload)(nil)).
			Set("checkpoint_json = ?", checkpoint).
			Where("task_id = ?", id).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("writing task %d checkpoint: %w", id, err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return fmt.Errorf("writing task %d checkpoint: payload row not found: %w", id, ErrNotFound)
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

func (r *BunTaskRepo) settle(ctx context.Context, id, generation int64, transition TaskTransition) error {
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
	if transition.ClearWorkStartedAt {
		query = query.Set("work_started_at = NULL")
	} else if transition.WorkStartedAt != nil {
		query = query.Set("work_started_at = COALESCE(work_started_at, ?)", *transition.WorkStartedAt)
	}
	if transition.IncrementRetry {
		query = query.Set("retry_count = retry_count + 1")
	}
	if transition.Status == model.TaskStatusPending {
		availableAt := "CASE WHEN cancellation_requested_at IS NOT NULL AND ? = FALSE THEN ? ELSE ? END"
		if r.db.Dialect().Name() == dialect.PG {
			// PostgreSQL infers a CASE of untyped timestamp literals as text.
			availableAt = "CASE WHEN cancellation_requested_at IS NOT NULL AND ? = FALSE THEN CAST(? AS TIMESTAMPTZ) ELSE CAST(? AS TIMESTAMPTZ) END"
		}
		query = query.
			Set("resume_mode = CASE WHEN cancellation_requested_at IS NOT NULL THEN ? ELSE ? END", model.TaskResumeModeRecover, transition.ResumeMode).
			Set("available_at = "+availableAt, transition.CancellationObserved, now, transition.AvailableAt).
			Set("finished_at = NULL").
			Set("acknowledged_at = NULL")
	} else {
		query = query.
			Set("resume_mode = ?", transition.ResumeMode).
			Set("finished_at = ?", now)
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
		return !transition.AvailableAt.IsZero() && (transition.ResumeMode == model.TaskResumeModeExecute || transition.ResumeMode == model.TaskResumeModeRecover)
	case model.TaskStatusCompleted, model.TaskStatusFailed, model.TaskStatusCancelled:
		return transition.ResumeMode == model.TaskResumeModeExecute || transition.ResumeMode == model.TaskResumeModeRecover
	default:
		return false
	}
}

func (r *BunTaskRepo) ShortenLease(ctx context.Context, id, generation int64, duration time.Duration) error {
	if duration <= 0 {
		return fmt.Errorf("lease duration must be positive: %w", ErrInvalidInput)
	}
	now := time.Now()
	shortened := now.Add(duration)
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		// Shortening never extends a lease that already expires sooner.
		Set("lease_until = CASE WHEN lease_until < ? THEN lease_until ELSE ? END", shortened, shortened).
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

// WakePendingOfTypes applies type and wait-reason authorization in the update.
func (r *BunTaskRepo) WakePendingOfTypes(ctx context.Context, ids []int64, types []model.TaskType, skipWaitReasons []string) (int, error) {
	if len(types) == 0 {
		return 0, fmt.Errorf("task types are required: %w", ErrInvalidInput)
	}
	if slices.Contains(types, "") {
		return 0, fmt.Errorf("task type is required: %w", ErrInvalidInput)
	}
	for _, id := range ids {
		if id < 1 {
			return 0, fmt.Errorf("task IDs must be positive: %w", ErrInvalidInput)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	now := time.Now()
	query := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("available_at = ?", now).
		Set("updated_at = ?", now).
		Where("id IN (?)", bun.List(ids)).
		Where("type IN (?)", bun.List(types)).
		Where("status = ?", model.TaskStatusPending).
		Where("available_at > ?", now)
	if len(skipWaitReasons) != 0 {
		query = query.Where("wait_reason IS NULL OR wait_reason NOT IN (?)", bun.List(skipWaitReasons))
		query = query.Where("failure_reason IS NULL OR failure_reason NOT IN (?)", bun.List(skipWaitReasons))
	}
	result, err := query.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("waking pending tasks of selected types: %w", err)
	}
	rows, err := result.RowsAffected()
	return int(rows), err
}

func (r *BunTaskRepo) RequestCancellation(ctx context.Context, id int64, reason string) error {
	now := time.Now()
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("cancellation_requested_at = COALESCE(cancellation_requested_at, ?)", now).
		Set("cancellation_reason = COALESCE(cancellation_reason, ?)", nullableText(reason)).
		Set("resume_mode = ?", model.TaskResumeModeRecover).
		Set("available_at = CASE WHEN status = ? AND cancellation_requested_at IS NULL THEN ? ELSE available_at END", model.TaskStatusPending, now).
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

const awaitingCommitReviewSQL = `EXISTS (
 SELECT 1 FROM storage_commit_requests AS review_request
 WHERE review_request.task_id = ?TableAlias.id
   AND review_request.status = 'submitted'
   AND review_request.attention_at IS NOT NULL
)`

func (r *BunTaskRepo) AcknowledgeFailedForSubject(ctx context.Context, subjectType, subjectKey string) (int, error) {
	if subjectType == "" || subjectKey == "" {
		return 0, ErrInvalidInput
	}
	now := time.Now()
	result, err := r.db.NewUpdate().Model((*model.Task)(nil)).
		Set("acknowledged_at = ?", now).Set("updated_at = ?", now).
		Where("subject_type = ? AND subject_key = ? AND status = ?", subjectType, subjectKey, model.TaskStatusFailed).
		Where("acknowledged_at IS NULL AND superseded_at IS NULL").Where("NOT " + awaitingCommitReviewSQL).Exec(ctx)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	return int(count), err
}

// AcknowledgeFailed dismisses one failure. A failure whose storage confirmation
// is flagged for attention is refused with ErrConflict until that confirmation
// is resolved.
func (r *BunTaskRepo) AcknowledgeFailed(ctx context.Context, id int64) error {
	now := time.Now()
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("acknowledged_at = COALESCE(acknowledged_at, ?)", now).
		Set("updated_at = ?", now).
		Where("id = ? AND status = ?", id, model.TaskStatusFailed).
		Where("NOT " + awaitingCommitReviewSQL).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("acknowledging task %d: %w", id, err)
	}
	if rows, _ := result.RowsAffected(); rows == 1 {
		return nil
	}
	held, err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("id = ? AND status = ?", id, model.TaskStatusFailed).
		Where(awaitingCommitReviewSQL).
		Exists(ctx)
	if err != nil {
		return fmt.Errorf("acknowledging task %d: %w", id, err)
	}
	if held {
		return ErrConflict
	}
	return ErrNotFound
}

// acknowledgeFailedMatching selects the failures one bulk dismissal covers. The
// preview and the dismissal itself share it, so the number an operator confirms
// is the number that is dismissed.
func acknowledgeFailedMatching(filter TaskAcknowledgeFilter) func(bun.QueryBuilder) bun.QueryBuilder {
	return func(query bun.QueryBuilder) bun.QueryBuilder {
		query = query.
			Where("status = ? AND acknowledged_at IS NULL AND superseded_at IS NULL", model.TaskStatusFailed).
			Where("finished_at IS NOT NULL AND finished_at <= ?", filter.FailedBefore).
			Where("NOT " + awaitingCommitReviewSQL)
		if filter.Type != "" {
			query = query.Where("type = ?", filter.Type)
		}
		return query
	}
}

func (r *BunTaskRepo) CountFailedMatching(ctx context.Context, filter TaskAcknowledgeFilter) (int, error) {
	if filter.FailedBefore.IsZero() {
		return 0, fmt.Errorf("counting failed tasks: %w", ErrInvalidInput)
	}
	count, err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		ApplyQueryBuilder(acknowledgeFailedMatching(filter)).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting failed tasks: %w", err)
	}
	return count, nil
}

func (r *BunTaskRepo) AcknowledgeFailedMatching(
	ctx context.Context,
	filter TaskAcknowledgeFilter,
) (int, error) {
	if filter.FailedBefore.IsZero() {
		return 0, fmt.Errorf("acknowledging failed tasks: %w", ErrInvalidInput)
	}
	now := time.Now()
	result, err := r.db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("acknowledged_at = ?", now).
		Set("updated_at = ?", now).
		ApplyQueryBuilder(acknowledgeFailedMatching(filter)).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("acknowledging failed tasks: %w", err)
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

func (r *BunTaskRepo) List(ctx context.Context, filter TaskListFilter) (TaskPage, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	selectRows := func(taskType model.TaskType, status model.TaskStatus, excludeFailed bool) ([]model.Task, error) {
		var rows []model.Task
		q := r.db.NewSelect().Model(&rows).Where("task.superseded_at IS NULL").OrderExpr("task.id DESC").Limit(limit + 1)
		if taskType != "" {
			q.Where("task.type = ?", taskType)
		}
		if status != "" {
			q.Where("task.status = ?", status)
		} else if excludeFailed {
			q.Where("task.status <> ?", model.TaskStatusFailed)
		}
		if filter.BeforeID > 0 {
			q.Where("task.id < ?", filter.BeforeID)
		}
		if filter.Acknowledged != nil {
			if *filter.Acknowledged {
				q.Where("task.acknowledged_at IS NOT NULL")
			} else {
				q.Where("task.acknowledged_at IS NULL")
			}
		}
		err := q.Scan(ctx)
		return rows, err
	}
	var rows []model.Task
	if !filter.HideHealthyRecurringSystem || filter.Type != "" || filter.Status == model.TaskStatusFailed {
		status := filter.Status
		if filter.HideHealthyRecurringSystem && filter.Type.IsRecurringSystem() {
			if status != "" && status != model.TaskStatusFailed {
				return TaskPage{}, nil
			}
			status = model.TaskStatusFailed
		}
		selected, err := selectRows(filter.Type, status, false)
		if err != nil {
			return TaskPage{}, err
		}
		rows = selected
	} else {
		// Each branch reads at most one page from its type index. Healthy recurring
		// history therefore cannot force a backwards scan through completed rounds.
		for _, taskType := range []model.TaskType{model.TaskTypeBucketProvision, model.TaskTypeUploadPlan, model.TaskTypeStorageDataSetEnsure, model.TaskTypeStorageTransferPlan, model.TaskTypeStorageStore, model.TaskTypeStoragePull, model.TaskTypeStorageCommit, model.TaskTypeProviderReplacementCoordinate, model.TaskTypeCacheEvict, model.TaskTypeCacheReconcileDurability, model.TaskTypeStorageCleanup, model.TaskTypeStorageDataSetRetire, model.TaskTypeWalletOperation, model.TaskTypeProviderUploadSpeedTest} {
			selected, err := selectRows(taskType, filter.Status, filter.Status == "")
			if err != nil {
				return TaskPage{}, err
			}
			rows = append(rows, selected...)
		}
		if filter.Status == "" {
			selected, err := selectRows("", model.TaskStatusFailed, false)
			if err != nil {
				return TaskPage{}, err
			}
			rows = append(rows, selected...)
		}
		sortTasksDescending(rows)
	}
	page := TaskPage{Tasks: rows}
	if len(page.Tasks) > limit {
		page.Tasks = page.Tasks[:limit]
		page.NextBeforeID = page.Tasks[len(page.Tasks)-1].ID
	}
	// Large JSON is read only for the final page, not every branch candidate.
	for i := range page.Tasks {
		if err := loadTaskPayload(ctx, r.db, &page.Tasks[i]); err != nil {
			return TaskPage{}, err
		}
	}
	return page, nil
}

func (r *BunTaskRepo) CountByStatus(ctx context.Context) ([]TaskStatusCount, error) {
	var counts []TaskStatusCount
	err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		Column("type", "status").
		ColumnExpr("COUNT(*) AS count").
		Where("superseded_at IS NULL").
		Group("type", "status").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting tasks by status: %w", err)
	}
	return counts, nil
}

func (r *BunTaskRepo) CountByPresentationStatus(ctx context.Context) ([]TaskStatusCount, error) {
	return r.CountByStatus(ctx)
}

func (r *BunTaskRepo) CountUnacknowledgedFailed(ctx context.Context) (int64, error) {
	count, err := r.db.NewSelect().
		Model((*model.Task)(nil)).
		Where("status = ?", model.TaskStatusFailed).
		Where("acknowledged_at IS NULL AND superseded_at IS NULL").
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
		Where("superseded_at IS NULL").
		Where("status IN (?, ?)", model.TaskStatusPending, model.TaskStatusRunning).
		Where("type NOT IN (?)", bun.List(model.RecurringSystemTaskTypes())).
		Group("type", "status").
		Scan(ctx, &counts)
	if err != nil {
		return nil, fmt.Errorf("counting active task pipeline: %w", err)
	}
	return counts, nil
}

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}
