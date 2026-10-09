package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

const (
	taskStoredColumns = "type,idempotency_key,input_version,input_hash,subject_type,subject_key,retry_of_task_id,status,resume_mode,available_at,wait_reason,retry_count,failure_reason,last_error,status_message,cancellation_requested_at,cancellation_reason,claim_generation,claimed_at,lease_until,started_at,finished_at,created_at,updated_at,work_started_at"
	taskJSONColumns   = "input_json,checkpoint_json,policy_json,runtime_json,events_json"
)

func taskProjection(history, includeJSON bool) string {
	id := "id"
	markers := "NULL AS acknowledged_at,NULL AS superseded_at"
	if history {
		id = "task_id AS id"
		markers = "acknowledged_at,superseded_at"
	}
	columns := id + "," + taskStoredColumns + "," + markers
	if includeJSON {
		columns += "," + taskJSONColumns
	}
	return columns
}

func taskRelationSQL(includeJSON bool) string {
	return "SELECT " + taskProjection(false, includeJSON) + " FROM tasks UNION ALL SELECT " + taskProjection(true, includeJSON) + " FROM task_history"
}

func taskRoundsQuery(db bun.IDB, dest any, includeJSON bool) *bun.SelectQuery {
	return db.NewSelect().Model(dest).ModelTableExpr("(?) AS task", bun.Safe(taskRelationSQL(includeJSON))).ColumnExpr("task.*")
}

func taskScopeQuery(db bun.IDB, dest any, scope TaskScope, includeJSON bool) *bun.SelectQuery {
	if scope == TaskScopeHistory {
		return db.NewSelect().Model(dest).ModelTableExpr("(SELECT ? FROM task_history) AS task", bun.Safe(taskProjection(true, includeJSON))).ColumnExpr("task.*")
	}
	return db.NewSelect().Model(dest).ModelTableExpr("(SELECT ? FROM tasks) AS task", bun.Safe(taskProjection(false, includeJSON))).ColumnExpr("task.*")
}

func isHistoryTask(task *model.Task) bool {
	return task.Status == model.TaskStatusCompleted || task.Status == model.TaskStatusCancelled || task.AcknowledgedAt != nil || task.SupersededAt != nil
}

// Creation and supersession change current identity membership. Pure archival does not.
func (r *BunTaskRepo) lockIdentity(ctx context.Context, taskType model.TaskType, key string) error {
	if r.db.Dialect().Name() != dialect.PG {
		return nil
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("synaps3.task.identity:%d:%s:%d:%s", len(taskType), taskType, len(key), key)))
	lockKey := int64(binary.BigEndian.Uint64(sum[:8]))
	var acquired bool
	if err := r.db.NewRaw("SELECT pg_try_advisory_xact_lock(?)", lockKey).Scan(ctx, &acquired); err != nil {
		return err
	}
	if !acquired {
		return ErrTaskIdentityContended
	}
	return nil
}

func (r *BunTaskRepo) GetByID(ctx context.Context, id int64) (*model.Task, error) {
	var rows []model.Task
	if err := taskRoundsQuery(r.db, &rows, true).Where("task.id = ?", id).Scan(ctx); err != nil {
		return nil, fmt.Errorf("selecting task %d: %w", id, err)
	}
	return uniqueTaskRow(rows)
}

func uniqueTaskRow(rows []model.Task) (*model.Task, error) {
	if len(rows) > 1 {
		return nil, fmt.Errorf("multiple task rounds have the same identity: %w", ErrTaskDataCorrupted)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

func (r *BunTaskRepo) GetForUpdate(ctx context.Context, id int64) (*model.Task, error) {
	row := new(model.Task)
	q := taskScopeQuery(r.db, row, TaskScopeWork, true).Where("task.id = ?", id)
	if r.db.Dialect().Name() == dialect.PG {
		q.For("UPDATE")
	}
	if err := q.Scan(ctx); err == nil {
		return row, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	// A hot row can have moved while FOR UPDATE was waiting. Read the new location.
	q = taskScopeQuery(r.db, row, TaskScopeHistory, true).Where("task.id = ?", id)
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

func (r *BunTaskRepo) GetByIdentity(ctx context.Context, taskType model.TaskType, key string) (*model.Task, error) {
	var rows []model.Task
	query := "SELECT " + taskProjection(false, true) + " FROM tasks WHERE type = ? AND idempotency_key = ? UNION ALL SELECT " + taskProjection(true, true) + " FROM task_history WHERE type = ? AND idempotency_key = ? AND superseded_at IS NULL"
	if err := r.db.NewRaw(query, taskType, key, taskType, key).Scan(ctx, &rows); err != nil {
		return nil, err
	}
	return uniqueTaskRow(rows)
}

func (r *BunTaskRepo) GetDirectSuccessor(ctx context.Context, id int64) (*model.Task, error) {
	var rows []model.Task
	if err := taskRoundsQuery(r.db, &rows, true).Where("task.retry_of_task_id = ?", id).Scan(ctx); err != nil {
		return nil, err
	}
	return uniqueTaskRow(rows)
}

func (r *BunTaskRepo) Enqueue(ctx context.Context, task *model.Task) (*model.Task, bool, error) {
	if task == nil || task.Type == "" || task.IdempotencyKey == "" || task.InputVersion < 1 || task.InputHash == "" || !jsonObject(task.Input) || !jsonObject(task.Policy) {
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
	prepared.Events = json.RawMessage(`[]`)
	if !jsonObject(prepared.Runtime) || len(prepared.Checkpoint) != 0 && !jsonObject(prepared.Checkpoint) {
		return nil, false, ErrInvalidInput
	}
	var stored *model.Task
	var created bool
	err = runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		stored, created = nil, false
		row := prepared
		tx := &BunTaskRepo{db: db}
		if err := tx.lockIdentity(ctx, row.Type, row.IdempotencyKey); err != nil {
			return err
		}
		existing, err := tx.GetByIdentity(ctx, row.Type, row.IdempotencyKey)
		if err != nil {
			return err
		}
		if existing != nil {
			stored = existing
			return nil
		}
		if row.RetryOfTaskID != nil {
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
		if _, err := db.NewInsert().Model(&row).Exec(ctx); err != nil {
			return err
		}
		if row.RetryOfTaskID != nil && row.ID <= *row.RetryOfTaskID {
			return ErrConflict
		}
		if err := tx.AppendEvent(ctx, row.ID, "created", json.RawMessage(`{}`)); err != nil {
			return err
		}
		stored, err = tx.GetByID(ctx, row.ID)
		created = true
		return err
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

func (r *BunTaskRepo) archive(ctx context.Context, task *model.Task, generation *int64) error {
	if !terminalTaskStatus(task.Status) || task.Status == model.TaskStatusFailed && task.AcknowledgedAt == nil && task.SupersededAt == nil {
		return ErrInvalidInput
	}
	// Copy only the locked, current row. Any failure rolls the entire move back.
	history := model.TaskHistoryFromTask(task)
	if _, err := r.db.NewInsert().Model(history).Exec(ctx); err != nil {
		return fmt.Errorf("archiving task %d: %w", task.ID, err)
	}
	q := r.db.NewDelete().Model((*model.Task)(nil)).Where("id = ?", task.ID)
	if generation != nil {
		q = q.Where("claim_generation = ? AND status = ? AND lease_until > ?", *generation, model.TaskStatusRunning, time.Now())
	}
	result, err := q.Exec(ctx)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n != 1 {
		return ErrTaskLeaseLost
	}
	return nil
}

func (r *BunTaskRepo) SupersedeTerminal(ctx context.Context, id int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		tx := &BunTaskRepo{db: db}
		row, err := tx.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if row == nil || !terminalTaskStatus(row.Status) || row.SupersededAt != nil {
			return ErrConflict
		}
		if err := tx.lockIdentity(ctx, row.Type, row.IdempotencyKey); err != nil {
			return err
		}
		now := time.Now()
		if isHistoryTask(row) {
			result, err := db.NewUpdate().TableExpr("task_history").Set("superseded_at = ?", now).Set("updated_at = ?", now).Where("task_id = ? AND superseded_at IS NULL", id).Exec(ctx)
			if err != nil {
				return err
			}
			if n, _ := result.RowsAffected(); n != 1 {
				return ErrConflict
			}
			return nil
		}
		row.SupersededAt, row.UpdatedAt = &now, now
		return tx.archive(ctx, row, nil)
	})
}

func decodeTaskEvents(task *model.Task) ([]model.TaskEvent, error) {
	var events []model.TaskEvent
	if len(task.Events) == 0 || json.Unmarshal(task.Events, &events) != nil || events == nil || len(events) > 128 {
		return nil, ErrInvalidInput
	}
	var previous int64
	for i := range events {
		event := &events[i]
		if event.Sequence <= previous || event.Type == "" || event.CreatedAt.IsZero() || !jsonObject(event.Details) {
			return nil, ErrInvalidInput
		}
		event.TaskID = task.ID
		previous = event.Sequence
	}
	return events, nil
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
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		tx := &BunTaskRepo{db: db}
		row, err := tx.GetForUpdate(ctx, taskID)
		if err != nil {
			return err
		}
		if row == nil {
			return ErrNotFound
		}
		if err := appendTaskEvent(row, eventType, details); err != nil {
			return err
		}
		table, key := "tasks", "id"
		if isHistoryTask(row) {
			table, key = "task_history", "task_id"
		}
		_, err = db.NewUpdate().TableExpr(table).Set("events_json = ?", row.Events).Where(key+" = ?", taskID).Exec(ctx)
		return err
	})
}

func appendTaskEvent(task *model.Task, eventType string, details json.RawMessage) error {
	events, err := decodeTaskEvents(task)
	if err != nil {
		return err
	}
	sequence := int64(1)
	if len(events) > 0 {
		sequence = events[len(events)-1].Sequence + 1
	}
	if sequence < 1 {
		return ErrInvalidInput
	}
	events = append(events, model.TaskEvent{TaskID: task.ID, Sequence: sequence, Type: eventType, CreatedAt: time.Now(), Details: details})
	if len(events) > 128 {
		events = events[len(events)-128:]
	}
	task.Events, err = json.Marshal(events)
	return err
}

func (r *BunTaskRepo) ListEvents(ctx context.Context, taskID, beforeSequence int64, limit int) ([]model.TaskEvent, error) {
	if limit <= 0 || limit > 128 {
		limit = 128
	}
	row, err := r.GetByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, ErrNotFound
	}
	events, err := decodeTaskEvents(row)
	if err != nil {
		return nil, err
	}
	result := make([]model.TaskEvent, 0, min(limit, len(events)))
	for i := len(events) - 1; i >= 0 && len(result) < limit; i-- {
		if beforeSequence == 0 || events[i].Sequence < beforeSequence {
			result = append(result, events[i])
		}
	}
	return result, nil
}

func (r *BunTaskRepo) LatestForSubject(ctx context.Context, subjectType, subjectKey string, types ...model.TaskType) (*model.Task, error) {
	var rows []model.Task
	q := taskRoundsQuery(r.db, &rows, true).Where("task.subject_type = ? AND task.subject_key = ? AND task.superseded_at IS NULL", subjectType, subjectKey).OrderExpr("task.id DESC").Limit(1)
	if len(types) > 0 {
		q = q.Where("task.type IN (?)", bun.List(types))
	}
	if err := q.Scan(ctx); err != nil {
		return nil, err
	}
	return uniqueTaskRow(rows)
}

func (r *BunTaskRepo) ListCurrentFailedForSubject(ctx context.Context, subjectType, subjectKey string, beforeID int64, limit int, types ...model.TaskType) (TaskPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	var rows []model.Task
	// These leading order columns are fixed by the filters. Keeping them in
	// the order lets PostgreSQL use the subject index even for NULL subjects.
	q := taskRoundsQuery(r.db, &rows, false).Where("task.status = ? AND task.superseded_at IS NULL", model.TaskStatusFailed).
		OrderExpr("task.subject_type DESC, task.subject_key DESC, task.superseded_at DESC, task.id DESC").Limit(limit + 1)
	if subjectType == "" && subjectKey == "" {
		q.Where("task.subject_type IS NULL AND task.subject_key IS NULL")
	} else {
		q.Where("task.subject_type = ? AND task.subject_key = ?", subjectType, subjectKey)
	}
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

func (r *BunTaskRepo) loadPage(ctx context.Context, page TaskPage) (TaskPage, error) {
	if len(page.Tasks) == 0 {
		return page, nil
	}
	ids := make([]int64, len(page.Tasks))
	for i := range page.Tasks {
		ids[i] = page.Tasks[i].ID
	}
	var rows []model.Task
	if err := taskRoundsQuery(r.db, &rows, true).Where("task.id IN (?)", bun.List(ids)).Scan(ctx); err != nil {
		return TaskPage{}, err
	}
	byID := make(map[int64]model.Task, len(rows))
	for _, row := range rows {
		if _, found := byID[row.ID]; found {
			return TaskPage{}, ErrTaskDataCorrupted
		}
		byID[row.ID] = row
	}
	for i := range page.Tasks {
		row, found := byID[page.Tasks[i].ID]
		if !found {
			return TaskPage{}, ErrNotFound
		}
		page.Tasks[i] = row
	}
	return page, nil
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
	q := taskScopeQuery(r.db, &rows, filter.Scope, false).OrderExpr("task.id DESC").Limit(limit + 1)
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
	return r.loadPage(ctx, taskPage(rows, limit))
}

func (r *BunTaskRepo) CountByScope(ctx context.Context, scope TaskScope) ([]TaskStatusCount, error) {
	if !scope.Valid() {
		return nil, ErrInvalidInput
	}
	return r.countTaskRows(ctx, scope, false)
}

func (r *BunTaskRepo) countTaskRows(ctx context.Context, scope TaskScope, current bool) ([]TaskStatusCount, error) {
	var counts []TaskStatusCount
	var q *bun.SelectQuery
	if current {
		q = r.db.NewSelect().TableExpr("(?) AS task", bun.Safe(taskRelationSQL(false))).Where("task.superseded_at IS NULL")
	} else if scope == TaskScopeHistory {
		q = r.db.NewSelect().TableExpr("task_history AS task")
	} else {
		q = r.db.NewSelect().TableExpr("tasks AS task")
	}
	err := q.ColumnExpr("task.type,task.status,COUNT(*) AS count").GroupExpr("task.type,task.status").Scan(ctx, &counts)
	return counts, err
}

func (r *BunTaskRepo) CountByStatus(ctx context.Context) ([]TaskStatusCount, error) {
	return r.countTaskRows(ctx, "", true)
}

func (r *BunTaskRepo) PreviousStoreCheckpoints(ctx context.Context, copyID, taskID int64) ([]model.Task, error) {
	if copyID < 1 || taskID < 1 {
		return nil, ErrInvalidInput
	}
	var rows []model.Task
	err := taskRoundsQuery(r.db, &rows, true).Where("task.type = ? AND task.status = ? AND task.subject_type = ? AND task.subject_key = ? AND task.id <> ? AND task.checkpoint_json IS NOT NULL", model.TaskTypeStorageStore, model.TaskStatusFailed, model.TaskSubjectStorageCopy, fmt.Sprint(copyID), taskID).OrderExpr("task.id DESC").Scan(ctx)
	return rows, err
}

func terminalTaskStatus(status model.TaskStatus) bool {
	return status == model.TaskStatusCompleted || status == model.TaskStatusFailed || status == model.TaskStatusCancelled
}
