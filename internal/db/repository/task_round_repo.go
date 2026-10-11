package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

var (
	ErrTaskEffectAlreadyAdmitted = errors.New("task effect already admitted for this attempt")
	ErrTaskOperationUnresolved   = errors.New("previous task operation is unresolved")
)

func (r *BunTaskRepo) ListHistory(ctx context.Context, anchorID, beforeID int64, limit int) (TaskPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	// A cursor page also reads the cursor round, which proves the cursor
	// belongs to this chain.
	var rows []model.Task
	err := r.db.NewRaw(`WITH RECURSIVE
		ancestors(id, retry_of_task_id) AS (
			SELECT id, retry_of_task_id FROM tasks WHERE id = ?0
			UNION ALL
			SELECT parent.id, parent.retry_of_task_id FROM tasks AS parent JOIN ancestors ON parent.id = ancestors.retry_of_task_id
		),
		descendants(id) AS (
			SELECT id FROM tasks WHERE id = ?0
			UNION ALL
			SELECT child.id FROM tasks AS child JOIN descendants ON child.retry_of_task_id = descendants.id
		)
		SELECT task.* FROM tasks AS task
		WHERE task.id IN (SELECT id FROM ancestors UNION SELECT id FROM descendants)
		  AND (?1 = 0 OR task.id <= ?1)
		ORDER BY task.id DESC LIMIT ?2`, anchorID, beforeID, limit+2).Scan(ctx, &rows)
	if err != nil {
		return TaskPage{}, fmt.Errorf("listing task execution history: %w", err)
	}
	if beforeID > 0 && len(rows) > 0 && rows[0].ID == beforeID {
		rows = rows[1:]
	} else if beforeID > 0 || len(rows) == 0 {
		if anchor, err := r.GetByID(ctx, anchorID); err != nil {
			return TaskPage{}, err
		} else if anchor == nil {
			return TaskPage{}, ErrNotFound
		}
		return TaskPage{}, ErrInvalidInput
	}
	return taskPage(rows, limit), nil
}

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
		maps.Copy(fields, changed)
		raw, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		result, err := db.NewUpdate().Model((*model.Task)(nil)).Set("runtime_json = ?", json.RawMessage(raw)).Where("id = ? AND status = ? AND claim_generation = ? AND lease_until > ?", id, model.TaskStatusRunning, generation, time.Now()).Exec(ctx)
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
