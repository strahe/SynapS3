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
	"github.com/uptrace/bun/dialect"
)

var (
	ErrTaskEffectAlreadyAdmitted = errors.New("task effect already admitted for this attempt")
	ErrTaskOperationUnresolved   = errors.New("previous task operation is unresolved")
)

func (r *BunTaskRepo) CloseLegacy(ctx context.Context, id, generation int64) error {
	now := time.Now()
	q := r.db.NewUpdate().Model((*model.Task)(nil)).Set("status = ?", model.TaskStatusCancelled).Set("finished_at = ?", now).Set("claimed_at = NULL").Set("lease_until = NULL").Set("updated_at = ?", now).Where("id = ? AND superseded_at IS NULL", id)
	if generation == 0 {
		q.Where("status = ?", model.TaskStatusPending)
	} else {
		q.Where("status = ? AND claim_generation = ? AND lease_until > ?", model.TaskStatusRunning, generation, now)
	}
	result, err := q.Exec(ctx)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrTaskLeaseLost
	}
	return nil
}

func (r *BunTaskRepo) GetForUpdate(ctx context.Context, id int64) (*model.Task, error) {
	row := new(model.Task)
	q := withTaskPayload(r.db.NewSelect().Model(row)).Where("task.id = ?", id)
	if r.db.Dialect().Name() == dialect.PG {
		q.For("UPDATE OF task")
	}
	if err := q.Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *BunTaskRepo) GetDirectSuccessor(ctx context.Context, id int64) (*model.Task, error) {
	row := new(model.Task)
	if err := withTaskPayload(r.db.NewSelect().Model(row)).Where("task.retry_of_task_id = ?", id).Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *BunTaskRepo) SupersedeTerminal(ctx context.Context, id int64) error {
	now := time.Now()
	result, err := r.db.NewUpdate().Model((*model.Task)(nil)).Set("superseded_at = ?", now).Set("updated_at = ?", now).Where("id = ? AND superseded_at IS NULL", id).Where("status IN (?, ?, ?)", model.TaskStatusCompleted, model.TaskStatusFailed, model.TaskStatusCancelled).Exec(ctx)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunTaskRepo) LatestForSubject(ctx context.Context, subjectType, subjectKey string, types ...model.TaskType) (*model.Task, error) {
	row := new(model.Task)
	q := withTaskPayload(r.db.NewSelect().Model(row)).Where("task.subject_type = ? AND task.subject_key = ?", subjectType, subjectKey).Where("task.superseded_at IS NULL").OrderExpr("task.id DESC").Limit(1)
	if len(types) > 0 {
		q.Where("task.type IN (?)", bun.List(types))
	}
	if err := q.Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return row, nil
}

func (r *BunTaskRepo) ListHistory(ctx context.Context, anchorID, beforeID int64, limit int) (TaskPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	row, err := r.GetByID(ctx, anchorID)
	if err != nil {
		return TaskPage{}, err
	}
	if row == nil {
		return TaskPage{}, ErrNotFound
	}
	for {
		next, err := r.GetDirectSuccessor(ctx, row.ID)
		if err != nil {
			return TaskPage{}, err
		}
		if next == nil {
			break
		}
		row = next
	}
	if beforeID > 0 {
		for row.ID != beforeID {
			if row.RetryOfTaskID == nil {
				return TaskPage{}, ErrInvalidInput
			}
			row, err = r.GetByID(ctx, *row.RetryOfTaskID)
			if err != nil {
				return TaskPage{}, err
			}
			if row == nil {
				return TaskPage{}, ErrInvalidInput
			}
		}
		if row.RetryOfTaskID == nil {
			return TaskPage{}, nil
		}
		row, err = r.GetByID(ctx, *row.RetryOfTaskID)
		if err != nil {
			return TaskPage{}, err
		}
		if row == nil {
			return TaskPage{}, nil
		}
	}
	page := TaskPage{}
	for len(page.Tasks) < limit {
		page.Tasks = append(page.Tasks, *row)
		if row.RetryOfTaskID == nil {
			return page, nil
		}
		row, err = r.GetByID(ctx, *row.RetryOfTaskID)
		if err != nil {
			return TaskPage{}, err
		}
		if row == nil {
			return page, nil
		}
	}
	page.NextBeforeID = page.Tasks[len(page.Tasks)-1].ID
	return page, nil
}

func (r *BunTaskRepo) AppendEvent(ctx context.Context, taskID int64, eventType string, details json.RawMessage) error {
	if taskID < 1 || eventType == "" {
		return ErrInvalidInput
	}
	if len(details) == 0 {
		details = json.RawMessage(`{}`)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(details, &object); err != nil || object == nil {
		return ErrInvalidInput
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := (&BunTaskRepo{db: db}).GetForUpdate(ctx, taskID)
		if err != nil {
			return err
		}
		if row == nil {
			return ErrNotFound
		}
		var sequence int64
		if err := db.NewRaw("SELECT COALESCE(MAX(sequence), 0) + 1 FROM task_events WHERE task_id = ?", taskID).Scan(ctx, &sequence); err != nil {
			return err
		}
		if _, err := db.NewInsert().Model(&model.TaskEvent{TaskID: taskID, Sequence: sequence, Type: eventType, CreatedAt: time.Now(), Details: details}).Exec(ctx); err != nil {
			return err
		}
		_, err = db.NewDelete().Model((*model.TaskEvent)(nil)).Where("task_id = ? AND sequence <= ?", taskID, sequence-128).Exec(ctx)
		return err
	})
}

func (r *BunTaskRepo) ListEvents(ctx context.Context, taskID, beforeSequence int64, limit int) ([]model.TaskEvent, error) {
	if limit <= 0 || limit > 128 {
		limit = 128
	}
	var events []model.TaskEvent
	q := r.db.NewSelect().Model(&events).Where("task_id = ?", taskID).OrderExpr("sequence DESC").Limit(limit)
	if beforeSequence > 0 {
		q.Where("sequence < ?", beforeSequence)
	}
	err := q.Scan(ctx)
	return events, err
}

// updateOperation preserves additive runtime fields while fencing every write.
func (r *BunTaskRepo) updateOperation(ctx context.Context, id, generation int64, fn func(*BunTaskRepo, *model.Task, *model.TaskRuntime) error) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		tx := &BunTaskRepo{db: db}
		if err := tx.ValidateClaim(ctx, id, generation); err != nil {
			return err
		}
		row, err := tx.GetByID(ctx, id)
		if err != nil {
			return err
		}
		if row == nil {
			return ErrTaskLeaseLost
		}
		var runtime model.TaskRuntime
		if err := json.Unmarshal(row.Runtime, &runtime); err != nil {
			return fmt.Errorf("decoding task runtime: %w", err)
		}
		if err := fn(tx, row, &runtime); err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(row.Runtime, &fields); err != nil || fields == nil {
			return ErrInvalidInput
		}
		encoded, err := json.Marshal(runtime)
		if err != nil {
			return err
		}
		var changed map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &changed); err != nil {
			return err
		}
		for _, key := range []string{"operation_key", "operation_started_at", "last_admitted_attempt"} {
			delete(fields, key)
		}
		for key, value := range changed {
			fields[key] = value
		}
		raw, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		result, err := db.NewUpdate().Model((*model.TaskPayload)(nil)).Set("runtime_json = ?", json.RawMessage(raw)).Where("task_id = ?", id).Exec(ctx)
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count != 1 {
			return ErrNotFound
		}
		return nil
	})
}

func (r *BunTaskRepo) ObserveOperation(ctx context.Context, id, generation int64, key string) (time.Time, error) {
	if key == "" {
		return time.Time{}, ErrInvalidInput
	}
	var started time.Time
	err := r.updateOperation(ctx, id, generation, func(tx *BunTaskRepo, row *model.Task, runtime *model.TaskRuntime) error {
		if runtime.OperationKey != "" && runtime.OperationKey != key {
			return ErrTaskOperationUnresolved
		}
		if runtime.OperationKey == "" {
			now := time.Now()
			runtime.OperationKey = key
			runtime.OperationStartedAt = &now
			runtime.LastAdmittedAttempt = 0
		}
		if runtime.OperationStartedAt == nil {
			return ErrInvalidInput
		}
		started = *runtime.OperationStartedAt
		return tx.MarkWorkStarted(ctx, id, generation, time.Now())
	})
	return started, err
}

func (r *BunTaskRepo) AdmitEffect(ctx context.Context, id, generation int64, key string, checkpoint json.RawMessage) error {
	if key == "" || len(checkpoint) == 0 {
		return ErrInvalidInput
	}
	return r.updateOperation(ctx, id, generation, func(tx *BunTaskRepo, row *model.Task, runtime *model.TaskRuntime) error {
		if runtime.OperationKey != "" && runtime.OperationKey != key {
			return ErrTaskOperationUnresolved
		}
		if runtime.OperationKey == "" {
			now := time.Now()
			runtime.OperationKey = key
			runtime.OperationStartedAt = &now
		}
		if runtime.LastAdmittedAttempt >= row.RetryCount+1 {
			return ErrTaskEffectAlreadyAdmitted
		}
		runtime.LastAdmittedAttempt = row.RetryCount + 1
		if err := tx.WriteCheckpoint(ctx, id, generation, checkpoint); err != nil {
			return err
		}
		if err := tx.MarkWorkStarted(ctx, id, generation, time.Now()); err != nil {
			return err
		}
		return tx.AppendEvent(ctx, id, "effect_admitted", json.RawMessage(fmt.Sprintf(`{"attempt":%d}`, row.RetryCount+1)))
	})
}

func (r *BunTaskRepo) ResolveOperation(ctx context.Context, id, generation int64, key string) error {
	return r.updateOperation(ctx, id, generation, func(_ *BunTaskRepo, _ *model.Task, runtime *model.TaskRuntime) error {
		if runtime.OperationKey != key {
			return ErrConflict
		}
		runtime.OperationKey = ""
		runtime.OperationStartedAt = nil
		runtime.LastAdmittedAttempt = 0
		return nil
	})
}

func (r *BunTaskRepo) Settle(ctx context.Context, id, generation int64, transition TaskTransition) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		tx := &BunTaskRepo{db: db}
		previous, err := tx.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if previous == nil {
			return ErrTaskLeaseLost
		}
		if err := tx.settle(ctx, id, generation, transition); err != nil {
			return err
		}
		eventType := ""
		if transition.IncrementRetry {
			eventType = "retry_scheduled"
		} else if transition.Status != model.TaskStatusPending {
			eventType = string(transition.Status)
		} else if transition.WaitReason != nil && (previous.WaitReason == nil || *previous.WaitReason != *transition.WaitReason) {
			eventType = "waiting"
		}
		if eventType != "" {
			return tx.AppendEvent(ctx, id, eventType, json.RawMessage(`{}`))
		}
		return nil
	})
}

func sortTasksDescending(tasks []model.Task) {
	slices.SortFunc(tasks, func(a, b model.Task) int {
		if a.ID > b.ID {
			return -1
		}
		if a.ID < b.ID {
			return 1
		}
		return 0
	})
}
