package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

const claimExpiredTaskPostgresSQL = `UPDATE tasks
SET status = 'running',
    resume_mode = 'recover',
    claim_generation = claim_generation + 1,
    lease_until = ?,
    finished_at = NULL,
    wait_reason = NULL,
    status_message = NULL,
    updated_at = ?
WHERE id = (
    SELECT id
    FROM tasks
    WHERE status = 'running' AND lease_until <= ?%s
    ORDER BY lease_until, id
    LIMIT 1
    FOR NO KEY UPDATE SKIP LOCKED
)
AND status = 'running' AND lease_until <= ?
RETURNING *`

const claimPendingTaskPostgresSQL = `UPDATE tasks
SET status = 'running',
    claim_generation = claim_generation + 1,
    lease_until = ?,
    finished_at = NULL,
    wait_reason = NULL,
    status_message = NULL,
    updated_at = ?
WHERE id = (
    SELECT id
    FROM tasks
    WHERE status = 'pending' AND available_at <= ?%s
    ORDER BY available_at, id
    LIMIT 1
    FOR NO KEY UPDATE SKIP LOCKED
)
AND status = 'pending' AND available_at <= ?
RETURNING *`

// Type/time bounds keep each head lookup aligned with the type-leading index;
// ordering only by time can repeatedly scan an excluded type's older tasks.
const filteredTaskCandidatePostgresSQL = `WITH RECURSIVE task_types(type) AS (
    (SELECT type FROM tasks WHERE status = '%[1]s' ORDER BY type, %[2]s, id LIMIT 1)
    UNION ALL
    SELECT next_type.type
    FROM task_types AS previous_type
    CROSS JOIN LATERAL (
        SELECT type FROM tasks
        WHERE status = '%[1]s' AND type > previous_type.type
        ORDER BY type, %[2]s, id LIMIT 1
    ) AS next_type
), allowed_types AS MATERIALIZED (
    SELECT type FROM task_types WHERE type NOT IN (?)
)
SELECT candidate.id, candidate.type, candidate.claim_generation,
       candidate.%[2]s AS ordering_time
FROM allowed_types
CROSS JOIN LATERAL (
    SELECT id, type, claim_generation, %[2]s FROM tasks
    WHERE status = '%[1]s'
      AND (type, %[2]s) >= (allowed_types.type, '-infinity'::timestamptz)
      AND (type, %[2]s) <= (allowed_types.type, ?)%[3]s
    ORDER BY type, %[2]s, id LIMIT 1
) AS candidate
ORDER BY candidate.%[2]s, candidate.id LIMIT 1`

const claimTaskCandidatePostgresSQL = `UPDATE tasks
SET status = 'running',
    %[1]sclaim_generation = claim_generation + 1,
    lease_until = ?,
    finished_at = NULL,
    wait_reason = NULL,
    status_message = NULL,
    updated_at = ?
WHERE id = (
    SELECT id FROM tasks
    WHERE id = ? AND type = ? AND status = '%[2]s'
      AND claim_generation = ? AND %[3]s = ? AND %[3]s <= ?
    FOR NO KEY UPDATE SKIP LOCKED
)
RETURNING *`

type taskClaimCandidate struct {
	ID              int64
	Type            model.TaskType
	ClaimGeneration int64
	OrderingTime    time.Time
}

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

func (r *BunTaskRepo) ClaimNext(ctx context.Context, leaseDuration time.Duration, filter TaskClaimFilter) (*model.Task, error) {
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive: %w", ErrInvalidInput)
	}
	db, ok := r.db.(*bun.DB)
	if !ok {
		return r.claimNextPostgres(ctx, r.db, leaseDuration, filter)
	}
	var claimed *model.Task
	err := db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var err error
		claimed, err = r.claimNextPostgres(ctx, tx, leaseDuration, filter)
		return err
	})
	return claimed, err
}

func (r *BunTaskRepo) claimNextPostgres(ctx context.Context, db bun.IDB, leaseDuration time.Duration, filter TaskClaimFilter) (*model.Task, error) {
	if len(filter.ExcludedTypes) > 0 {
		selectionNow := time.Now()
		task, err := claimFilteredTask(ctx, db, leaseDuration, filter, selectionNow, model.TaskStatusRunning, "lease_until")
		if err != nil || task != nil {
			return task, err
		}
		return claimFilteredTask(ctx, db, leaseDuration, filter, selectionNow, model.TaskStatusPending, "available_at")
	}
	return claimNextTask(ctx, db, leaseDuration, filter, claimExpiredTaskPostgresSQL, claimPendingTaskPostgresSQL)
}

func claimFilteredTask(ctx context.Context, db bun.IDB, leaseDuration time.Duration, filter TaskClaimFilter, selectionNow time.Time, status model.TaskStatus, orderingColumn string) (*model.Task, error) {
	excludedIDs := slices.Clone(filter.ExcludedTaskIDs)
	resumeAssignment := ""
	if status == model.TaskStatusRunning {
		resumeAssignment = "resume_mode = 'recover',\n    "
	}
	claimSQL := fmt.Sprintf(claimTaskCandidatePostgresSQL, resumeAssignment, status, orderingColumn)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conditions := ""
		args := []any{bun.List(filter.ExcludedTypes), selectionNow}
		if len(excludedIDs) > 0 {
			conditions = " AND id NOT IN (?)"
			args = append(args, bun.List(excludedIDs))
		}
		var candidate taskClaimCandidate
		err := db.NewRaw(fmt.Sprintf(filteredTaskCandidatePostgresSQL, status, orderingColumn, conditions), args...).Scan(ctx, &candidate)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("selecting next task: %w", err)
		}
		claimNow := time.Now()
		task := new(model.Task)
		err = db.NewRaw(claimSQL, claimNow.Add(leaseDuration), claimNow,
			candidate.ID, candidate.Type, candidate.ClaimGeneration, candidate.OrderingTime, selectionNow).Scan(ctx, task)
		if err == nil {
			return task, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("claiming next task: %w", err)
		}
		// A locked or changed head must not hide its type's next eligible task.
		excludedIDs = append(excludedIDs, candidate.ID)
	}
}

func claimNextTask(
	ctx context.Context,
	db bun.IDB,
	leaseDuration time.Duration,
	filter TaskClaimFilter,
	recoverySQL string,
	pendingSQL string,
) (*model.Task, error) {
	now := time.Now()
	leaseUntil := now.Add(leaseDuration)
	task, err := claimTaskWithSQL(ctx, db, recoverySQL, now, leaseUntil, filter)
	if err != nil || task != nil {
		return task, err
	}
	return claimTaskWithSQL(ctx, db, pendingSQL, now, leaseUntil, filter)
}

func claimTaskWithSQL(ctx context.Context, db bun.IDB, query string, now, leaseUntil time.Time, filter TaskClaimFilter) (*model.Task, error) {
	conditions := ""
	args := []any{leaseUntil, now, now}
	if len(filter.ExcludedTypes) > 0 {
		conditions += " AND type NOT IN (?)"
		args = append(args, bun.List(filter.ExcludedTypes))
	}
	if len(filter.ExcludedTaskIDs) > 0 {
		conditions += " AND id NOT IN (?)"
		args = append(args, bun.List(filter.ExcludedTaskIDs))
	}
	args = append(args, now)
	task := new(model.Task)
	err := db.NewRaw(fmt.Sprintf(query, conditions), args...).Scan(ctx, task)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claiming next task: %w", err)
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
	if !jsonObject(checkpoint) {
		return ErrInvalidInput
	}
	now := time.Now()
	result, err := r.db.NewUpdate().Model((*model.Task)(nil)).Set("checkpoint_json = ?", checkpoint).Set("resume_mode = ?", model.TaskResumeModeRecover).Set("updated_at = ?", now).Where("id = ? AND status = ? AND claim_generation = ? AND lease_until > ?", id, model.TaskStatusRunning, generation, now).Exec(ctx)
	if err != nil {
		return fmt.Errorf("writing task %d checkpoint: %w", id, err)
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return ErrTaskLeaseLost
	}
	return nil
}

func (r *BunTaskRepo) ValidateClaim(ctx context.Context, id, generation int64) error {
	now := time.Now()
	var found int64
	err := r.db.NewRaw(`SELECT id FROM tasks
		WHERE id = ? AND status = 'running' AND claim_generation = ? AND lease_until > ?
		FOR NO KEY UPDATE`, id, generation, now).Scan(ctx, &found)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrTaskLeaseLost
	}
	if err != nil {
		return fmt.Errorf("validating task %d claim: %w", id, err)
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
		Where("wait_reason IS NOT NULL").
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
		Where("wait_reason IS NOT NULL").
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

const awaitingCommitReviewSQL = `EXISTS (
 SELECT 1 FROM storage_commit_requests AS review_request
 WHERE review_request.task_id = ?TableAlias.id
   AND review_request.status = 'submitted'
   AND review_request.attention_at IS NOT NULL
)`

// acknowledgeFailedMatching selects the failures one bulk dismissal covers. The
// preview and the dismissal itself share it, so the number an operator confirms
// is the number that is dismissed.
func acknowledgeFailedMatching(filter TaskAcknowledgeFilter) func(bun.QueryBuilder) bun.QueryBuilder {
	return func(query bun.QueryBuilder) bun.QueryBuilder {
		query = query.
			Where("status = ?", model.TaskStatusFailed).
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
	count, err := taskScopeQuery(r.db, (*model.Task)(nil), TaskScopeWork).
		ApplyQueryBuilder(acknowledgeFailedMatching(filter)).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting failed tasks: %w", err)
	}
	return int(count), nil
}

func (r *BunTaskRepo) CountUnacknowledgedFailed(ctx context.Context) (int64, error) {
	count, err := taskScopeQuery(r.db, (*model.Task)(nil), TaskScopeWork).
		Where("status = ?", model.TaskStatusFailed).
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

func nullableText(value string) any {
	if value == "" {
		return nil
	}
	return value
}
