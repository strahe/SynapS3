package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

// archivedTaskSQL is the complement of the live_tasks view condition.
const archivedTaskSQL = "NOT (task.status IN ('pending','running','failed') AND task.acknowledged_at IS NULL AND task.superseded_at IS NULL)"

func taskScopeQuery(db bun.IDB, dest any, scope TaskScope) *bun.SelectQuery {
	if scope == TaskScopeHistory {
		return db.NewSelect().Model(dest).Where(archivedTaskSQL)
	}
	return db.NewSelect().Model(dest).ModelTableExpr("live_tasks AS task")
}

func (r *BunTaskRepo) findTask(ctx context.Context, lock bool, where string, args ...any) (*model.Task, error) {
	row := new(model.Task)
	q := r.db.NewSelect().Model(row).Where(where, args...)
	if lock {
		q = q.For("NO KEY UPDATE")
	}
	if err := q.Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *BunTaskRepo) GetByID(ctx context.Context, id int64) (*model.Task, error) {
	row, err := r.findTask(ctx, false, "task.id = ?", id)
	if err != nil {
		return nil, fmt.Errorf("selecting task %d: %w", id, err)
	}
	return row, nil
}

func (r *BunTaskRepo) GetForUpdate(ctx context.Context, id int64) (*model.Task, error) {
	return r.findTask(ctx, true, "task.id = ?", id)
}

func (r *BunTaskRepo) GetByIdentity(ctx context.Context, taskType model.TaskType, subjectType, subjectKey string, generation int64) (*model.Task, error) {
	return r.findTask(ctx, false, "task.type = ? AND task.subject_type = ? AND task.subject_key = ? AND task.generation = ? AND task.superseded_at IS NULL", taskType, subjectType, subjectKey, generation)
}

func (r *BunTaskRepo) GetDirectSuccessor(ctx context.Context, id int64) (*model.Task, error) {
	return r.findTask(ctx, false, "task.retry_of_task_id = ?", id)
}

func (r *BunTaskRepo) Enqueue(ctx context.Context, task *model.Task) (*model.Task, bool, error) {
	if task == nil || task.Type == "" || task.SubjectType == "" || task.SubjectKey == "" || task.Generation != nil && *task.Generation < 0 ||
		task.InputVersion < 1 || task.InputHash == "" || !jsonObject(task.Input) || !jsonObject(task.Policy) {
		return nil, false, ErrInvalidInput
	}
	budget, err := taskAttemptBudget(task.Policy)
	if err != nil || task.RetryCount < 0 || task.RetryCount >= budget {
		return nil, false, ErrInvalidInput
	}
	prepared := *task
	prepared.ID = 0
	if prepared.Status == "" {
		prepared.Status = model.TaskStatusPending
	}
	if prepared.Status != model.TaskStatusPending || prepared.SupersededAt != nil || prepared.AcknowledgedAt != nil {
		return nil, false, ErrInvalidInput
	}
	if prepared.ResumeMode == "" {
		prepared.ResumeMode = model.TaskResumeModeExecute
	}
	if prepared.AvailableAt.IsZero() {
		prepared.AvailableAt = time.Now()
	}
	if len(prepared.Runtime) == 0 {
		prepared.Runtime = json.RawMessage(`{}`)
	}
	if !jsonObject(prepared.Runtime) || len(prepared.Checkpoint) != 0 && !jsonObject(prepared.Checkpoint) {
		return nil, false, ErrInvalidInput
	}
	var stored *model.Task
	var created bool
	err = runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		stored, created = nil, false
		row := prepared
		tx := &BunTaskRepo{db: db}
		if row.Generation != nil {
			existing, err := tx.GetByIdentity(ctx, row.Type, row.SubjectType, row.SubjectKey, *row.Generation)
			if err != nil || existing != nil {
				stored = existing
				return err
			}
		}
		if row.RetryOfTaskID != nil {
			// Every successor is created under its parent's lock, so concurrent
			// retries of one round serialize here.
			parent, err := tx.GetForUpdate(ctx, *row.RetryOfTaskID)
			if err != nil {
				return err
			}
			if parent == nil || parent.SupersededAt == nil || !terminalTaskStatus(parent.Status) {
				return ErrConflict
			}
			if child, err := tx.GetDirectSuccessor(ctx, parent.ID); err != nil {
				return err
			} else if child != nil {
				return ErrConflict
			}
		}
		result, err := db.NewInsert().Model(&row).
			On("CONFLICT (type, subject_type, subject_key, generation) WHERE superseded_at IS NULL DO NOTHING").
			Returning("*").Exec(ctx)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n == 0 {
			// A concurrent enqueue committed this identity first. If that round
			// was superseded meanwhile, the whole transaction is retried.
			if stored, err = tx.GetByIdentity(ctx, row.Type, row.SubjectType, row.SubjectKey, *row.Generation); err == nil && stored == nil {
				err = ErrTaskIdentityContended
			}
			return err
		}
		if row.RetryOfTaskID != nil && row.ID <= *row.RetryOfTaskID {
			return ErrConflict
		}
		if err := tx.AppendEvent(ctx, row.ID, "created", json.RawMessage(`{}`)); err != nil {
			return err
		}
		stored, created = &row, true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if created {
		*task = *stored
	}
	return stored, created, nil
}

func jsonObject(raw json.RawMessage) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(raw, &object) == nil && object != nil
}

func taskAttemptBudget(raw json.RawMessage) (int, error) {
	var policy struct {
		Version     int  `json:"version"`
		MaxAttempts *int `json:"max_attempts"`
	}
	if json.Unmarshal(raw, &policy) != nil || policy.Version != 2 || policy.MaxAttempts == nil || *policy.MaxAttempts < 1 {
		return 0, ErrInvalidInput
	}
	return *policy.MaxAttempts, nil
}

func (r *BunTaskRepo) SupersedeTerminal(ctx context.Context, id int64) error {
	now := time.Now()
	result, err := r.db.NewUpdate().Model((*model.Task)(nil)).
		Set("superseded_at = ?", now).Set("updated_at = ?", now).
		Where("id = ? AND superseded_at IS NULL", id).
		Where("status IN (?)", bun.List(terminalTaskStatuses)).
		Exec(ctx)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunTaskRepo) AppendEvent(ctx context.Context, taskID int64, eventType string, details json.RawMessage) error {
	if taskID < 1 || eventType == "" {
		return ErrInvalidInput
	}
	if len(details) == 0 {
		details = json.RawMessage(`{}`)
	}
	if !jsonObject(details) {
		return ErrInvalidInput
	}
	event := &model.TaskEvent{TaskID: taskID, Type: eventType, Details: details, CreatedAt: time.Now()}
	if _, err := r.db.NewInsert().Model(event).Exec(ctx); isForeignKeyViolation(err) {
		return ErrNotFound
	} else if err != nil {
		return fmt.Errorf("appending task %d event: %w", taskID, err)
	}
	return nil
}

func (r *BunTaskRepo) ListEvents(ctx context.Context, taskID, beforeID int64, limit int) ([]model.TaskEvent, error) {
	if limit <= 0 || limit > 128 {
		limit = 128
	}
	events := make([]model.TaskEvent, 0)
	q := r.db.NewSelect().Model(&events).Where("task_id = ?", taskID).OrderExpr("id DESC").Limit(int64(limit))
	if beforeID > 0 {
		q = q.Where("id < ?", beforeID)
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing task %d events: %w", taskID, err)
	}
	return events, nil
}

func (r *BunTaskRepo) LatestForSubject(ctx context.Context, subjectType, subjectKey string, types ...model.TaskType) (*model.Task, error) {
	row := new(model.Task)
	q := r.db.NewSelect().Model(row).Where("task.subject_type = ? AND task.subject_key = ? AND task.superseded_at IS NULL", subjectType, subjectKey).OrderExpr("task.id DESC").Limit(1)
	if len(types) > 0 {
		q = q.Where("task.type IN (?)", bun.List(types))
	}
	if err := q.Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *BunTaskRepo) ListCurrentFailedForSubject(ctx context.Context, subjectType, subjectKey string, beforeID int64, limit int, types ...model.TaskType) (TaskPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	var rows []model.Task
	// superseded_at is NULL here, but ordering by it keeps PostgreSQL on the
	// current-failure index when the primary key looks deceptively cheap.
	q := r.db.NewSelect().Model(&rows).
		Where("task.status = ? AND task.superseded_at IS NULL", model.TaskStatusFailed).
		Where("task.subject_type = ? AND task.subject_key = ?", subjectType, subjectKey).
		OrderExpr("task.superseded_at DESC, task.id DESC").Limit(int64(limit + 1))
	if beforeID > 0 {
		q = q.Where("task.id < ?", beforeID)
	}
	if len(types) > 0 {
		q = q.Where("task.type IN (?)", bun.List(types))
	}
	if err := q.Scan(ctx); err != nil {
		return TaskPage{}, err
	}
	return taskPage(rows, limit), nil
}

func taskPage(rows []model.Task, limit int) TaskPage {
	page := TaskPage{Tasks: rows}
	if len(rows) > limit {
		page.Tasks = rows[:limit]
		page.NextBeforeID = page.Tasks[limit-1].ID
	}
	return page
}

func (r *BunTaskRepo) List(ctx context.Context, filter TaskListFilter) (TaskPage, error) {
	if !filter.Scope.Valid() {
		return TaskPage{}, ErrInvalidInput
	}
	if filter.Scope == "" {
		filter.Scope = TaskScopeWork
	}
	if filter.Status != "" && (filter.Scope == TaskScopeWork && filter.Status != model.TaskStatusPending && filter.Status != model.TaskStatusRunning && filter.Status != model.TaskStatusFailed || filter.Scope == TaskScopeHistory && !terminalTaskStatus(filter.Status)) {
		return TaskPage{}, ErrInvalidInput
	}
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var rows []model.Task
	q := taskScopeQuery(r.db, &rows, filter.Scope).OrderExpr("task.id DESC").Limit(int64(limit + 1))
	if filter.Type != "" {
		q = q.Where("task.type = ?", filter.Type)
	}
	if filter.Status != "" {
		q = q.Where("task.status = ?", filter.Status)
	}
	if filter.BeforeID > 0 {
		q = q.Where("task.id < ?", filter.BeforeID)
	}
	if filter.Acknowledged != nil {
		if *filter.Acknowledged {
			q = q.Where("task.acknowledged_at IS NOT NULL")
		} else {
			q = q.Where("task.acknowledged_at IS NULL")
		}
	}
	if err := q.Scan(ctx); err != nil {
		return TaskPage{}, err
	}
	return taskPage(rows, limit), nil
}

func (r *BunTaskRepo) CountByScope(ctx context.Context, scope TaskScope) ([]TaskStatusCount, error) {
	if !scope.Valid() {
		return nil, ErrInvalidInput
	}
	var counts []TaskStatusCount
	err := taskScopeQuery(r.db, (*model.Task)(nil), scope).
		ColumnExpr("task.type,task.status,COUNT(*) AS count").GroupExpr("task.type,task.status").Scan(ctx, &counts)
	return counts, err
}

func (r *BunTaskRepo) PreviousStoreCheckpoints(ctx context.Context, copyID, taskID int64) ([]model.Task, error) {
	if copyID < 1 || taskID < 1 {
		return nil, ErrInvalidInput
	}
	var rows []model.Task
	err := r.db.NewSelect().Model(&rows).Where("task.type = ? AND task.status = ? AND task.subject_type = ? AND task.subject_key = ? AND task.id <> ? AND task.checkpoint_json IS NOT NULL", model.TaskTypeStorageStore, model.TaskStatusFailed, model.TaskSubjectStorageCopy, fmt.Sprint(copyID), taskID).OrderExpr("task.id DESC").Scan(ctx)
	return rows, err
}

var terminalTaskStatuses = []model.TaskStatus{model.TaskStatusCompleted, model.TaskStatusFailed, model.TaskStatusCancelled}

func terminalTaskStatus(status model.TaskStatus) bool {
	return status == model.TaskStatusCompleted || status == model.TaskStatusFailed || status == model.TaskStatusCancelled
}
