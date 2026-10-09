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

func (r *BunTaskRepo) CloseLegacy(ctx context.Context, id, generation int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		tx := &BunTaskRepo{db: db}
		row, err := tx.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if row == nil || isHistoryTask(row) {
			return ErrTaskLeaseLost
		}
		now := time.Now()
		if generation == 0 {
			if row.Status != model.TaskStatusPending {
				return ErrTaskLeaseLost
			}
		} else if row.Status != model.TaskStatusRunning || row.ClaimGeneration != generation || row.LeaseUntil == nil || !row.LeaseUntil.After(now) {
			return ErrTaskLeaseLost
		}
		row.Status, row.FinishedAt, row.UpdatedAt = model.TaskStatusCancelled, &now, now
		row.ClaimedAt, row.LeaseUntil = nil, nil
		return tx.archive(ctx, row, nil)
	})
}

// FailLegacyPending isolates unverifiable input without changing recovery evidence.
func (r *BunTaskRepo) FailLegacyPending(ctx context.Context, id, generation int64, cause string) error {
	now := time.Now()
	result, err := r.db.NewUpdate().Model((*model.Task)(nil)).
		Set("status = ?", model.TaskStatusFailed).
		Set("failure_reason = ?", "invalid_legacy_task").
		Set("last_error = ?", cause).
		Set("wait_reason = NULL").Set("status_message = NULL").
		Set("finished_at = ?", now).
		Set("claimed_at = NULL").Set("lease_until = NULL").
		Set("updated_at = ?", now).
		Where("id = ? AND status = ? AND claim_generation = ?", id, model.TaskStatusPending, generation).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("failing legacy pending task %d: %w", id, err)
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunTaskRepo) ListHistory(ctx context.Context, anchorID, beforeID int64, limit int) (TaskPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var rows []struct {
		ID          *int64
		AnchorCount int
		Corrupted   int
		CursorFound int
	}
	// Each recursive step reads adjacent rows through primary or successor keys;
	// the entire history relation must not be materialized before traversal.
	err := r.db.NewRaw(`WITH RECURSIVE
		parameters AS (SELECT CAST(? AS BIGINT) AS anchor_id, CAST(? AS BIGINT) AS before_id),
		seed AS (
			SELECT id, retry_of_task_id FROM tasks WHERE id = (SELECT anchor_id FROM parameters)
			UNION ALL
			SELECT task_id AS id, retry_of_task_id FROM task_history WHERE task_id = (SELECT anchor_id FROM parameters)
		),
		chain(id, retry_of_task_id, direction, corrupted) AS (
			SELECT seed.id, seed.retry_of_task_id, directions.direction,
				CASE WHEN (SELECT COUNT(*) FROM seed) > 1 THEN 1 ELSE 0 END
			FROM seed CROSS JOIN (SELECT -1 AS direction UNION ALL SELECT 1) AS directions
			UNION ALL
			SELECT
				CASE WHEN current.direction < 0 THEN COALESCE(parent_work.id, parent_history.task_id, 0)
					ELSE COALESCE(child_work.id, child_history.task_id, 0) END,
				CASE WHEN current.direction < 0 THEN COALESCE(parent_work.retry_of_task_id, parent_history.retry_of_task_id)
					ELSE COALESCE(child_work.retry_of_task_id, child_history.retry_of_task_id) END,
				current.direction,
				CASE WHEN current.direction < 0 THEN
					CASE WHEN (parent_work.id IS NULL AND parent_history.task_id IS NULL)
						OR (parent_work.id IS NOT NULL AND parent_history.task_id IS NOT NULL)
						OR COALESCE(parent_work.id, parent_history.task_id) >= current.id THEN 1 ELSE 0 END
				ELSE
					CASE WHEN (child_work.id IS NOT NULL AND child_history.task_id IS NOT NULL)
						OR COALESCE(child_work.id, child_history.task_id) <= current.id
						OR EXISTS (SELECT 1 FROM task_history AS duplicate WHERE duplicate.task_id = child_work.id)
						OR EXISTS (SELECT 1 FROM tasks AS duplicate WHERE duplicate.id = child_history.task_id)
						THEN 1 ELSE 0 END
				END
			FROM chain AS current
			LEFT JOIN tasks AS parent_work ON current.direction < 0 AND parent_work.id = current.retry_of_task_id
			LEFT JOIN task_history AS parent_history ON current.direction < 0 AND parent_history.task_id = current.retry_of_task_id
			LEFT JOIN tasks AS child_work ON current.direction > 0 AND child_work.retry_of_task_id = current.id
			LEFT JOIN task_history AS child_history ON current.direction > 0 AND child_history.retry_of_task_id = current.id
			WHERE current.corrupted = 0 AND (
				(current.direction < 0 AND current.retry_of_task_id IS NOT NULL)
				OR (current.direction > 0 AND (child_work.id IS NOT NULL OR child_history.task_id IS NOT NULL))
			)
		),
		summary AS (
			SELECT (SELECT COUNT(*) FROM seed) AS anchor_count, COALESCE(MAX(corrupted), 0) AS corrupted,
				CASE WHEN (SELECT before_id FROM parameters) = 0
					OR MAX(CASE WHEN id = (SELECT before_id FROM parameters) THEN 1 ELSE 0 END) = 1
					THEN 1 ELSE 0 END AS cursor_found FROM chain
		),
		page AS (
			SELECT DISTINCT id FROM chain WHERE corrupted = 0 AND id > 0
				AND ((SELECT before_id FROM parameters) = 0 OR id < (SELECT before_id FROM parameters))
			ORDER BY id DESC LIMIT ?
		)
		SELECT page.id, summary.anchor_count, summary.corrupted, summary.cursor_found
		FROM summary LEFT JOIN page ON TRUE ORDER BY page.id DESC`, anchorID, beforeID, limit+1).Scan(ctx, &rows)
	if err != nil {
		return TaskPage{}, fmt.Errorf("listing task execution history: %w", err)
	}
	if len(rows) == 0 || rows[0].AnchorCount == 0 {
		return TaskPage{}, ErrNotFound
	}
	if rows[0].Corrupted != 0 {
		return TaskPage{}, ErrTaskDataCorrupted
	}
	if rows[0].CursorFound == 0 {
		return TaskPage{}, ErrInvalidInput
	}
	tasks := make([]model.Task, 0, len(rows))
	for _, row := range rows {
		if row.ID != nil {
			tasks = append(tasks, model.Task{ID: *row.ID})
		}
	}
	return r.loadPage(ctx, taskPage(tasks, limit))
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
