package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

func (r *BunTaskRepo) Settle(ctx context.Context, id, generation int64, transition TaskTransition) error {
	if !validTaskTransition(transition) {
		return ErrInvalidInput
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		tx := &BunTaskRepo{db: db}
		row, err := tx.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		now := time.Now()
		if row == nil || isHistoryTask(row) || row.Status != model.TaskStatusRunning || row.ClaimGeneration != generation || row.LeaseUntil == nil || !row.LeaseUntil.After(now) {
			return ErrTaskLeaseLost
		}
		row.Status, row.WaitReason, row.FailureReason = transition.Status, transition.WaitReason, transition.FailureReason
		row.LastError, row.StatusMessage, row.UpdatedAt = transition.LastError, transition.StatusMessage, now
		row.ClaimedAt, row.LeaseUntil = nil, nil
		if transition.ClearWorkStartedAt {
			row.WorkStartedAt = nil
		} else if row.WorkStartedAt == nil && transition.WorkStartedAt != nil {
			row.WorkStartedAt = transition.WorkStartedAt
		}
		if transition.IncrementRetry {
			row.RetryCount++
		}
		row.ResumeMode = transition.ResumeMode
		if transition.Status == model.TaskStatusPending {
			row.AvailableAt, row.FinishedAt = transition.AvailableAt, nil
			if row.CancellationRequested() {
				row.ResumeMode = model.TaskResumeModeRecover
				if !transition.CancellationObserved {
					row.AvailableAt = now
				}
			}
		} else {
			row.FinishedAt = &now
		}
		eventType := ""
		if transition.IncrementRetry {
			eventType = "retry_scheduled"
		} else if transition.Status != model.TaskStatusPending {
			eventType = string(transition.Status)
		}
		if eventType != "" {
			details, err := json.Marshal(map[string]any{"attempt": row.RetryCount + 1, "failure_reason": transition.FailureReason})
			if err != nil {
				return err
			}
			if err := appendTaskEvent(row, eventType, details); err != nil {
				return err
			}
		}
		if isHistoryTask(row) {
			return tx.archive(ctx, row, &generation)
		}
		result, err := db.NewUpdate().Model(row).Column("status", "wait_reason", "failure_reason", "last_error", "status_message", "updated_at", "claimed_at", "lease_until", "work_started_at", "retry_count", "resume_mode", "available_at", "finished_at", "events_json").Where("id = ? AND status = ? AND claim_generation = ? AND lease_until > ?", id, model.TaskStatusRunning, generation, now).Exec(ctx)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrTaskLeaseLost
		}
		return nil
	})
}

func (r *BunTaskRepo) AcknowledgeFailed(ctx context.Context, id int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		tx := &BunTaskRepo{db: db}
		row, err := tx.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if row == nil || row.Status != model.TaskStatusFailed {
			return ErrNotFound
		}
		held, err := db.NewSelect().TableExpr("storage_commit_requests").Where("task_id = ? AND status = 'submitted' AND attention_at IS NOT NULL", id).Exists(ctx)
		if err != nil {
			return err
		}
		if held {
			return ErrConflict
		}
		if row.AcknowledgedAt != nil {
			return nil
		}
		now := time.Now()
		cold := isHistoryTask(row)
		row.AcknowledgedAt, row.UpdatedAt = &now, now
		if err := appendTaskEvent(row, "acknowledged", json.RawMessage(`{}`)); err != nil {
			return err
		}
		if cold {
			_, err = db.NewUpdate().TableExpr("task_history").Set("acknowledged_at = ?", now).Set("updated_at = ?", now).Set("events_json = ?", row.Events).Where("task_id = ?", id).Exec(ctx)
			return err
		}
		return tx.archive(ctx, row, nil)
	})
}

func (r *BunTaskRepo) acknowledgeQuery(ctx context.Context, build func(*bun.SelectQuery) *bun.SelectQuery) (int, error) {
	count := 0
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		count = 0
		var rows []model.Task
		q := build(taskScopeQuery(db, &rows, TaskScopeWork, true)).OrderExpr("task.id ASC")
		if db.Dialect().Name() == dialect.PG {
			q.For("UPDATE")
		}
		if err := q.Scan(ctx); err != nil {
			return err
		}
		tx := &BunTaskRepo{db: db}
		for i := range rows {
			now := time.Now()
			rows[i].AcknowledgedAt, rows[i].UpdatedAt = &now, now
			if err := appendTaskEvent(&rows[i], "acknowledged", json.RawMessage(`{}`)); err != nil {
				return err
			}
			if err := tx.archive(ctx, &rows[i], nil); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	return count, err
}

func (r *BunTaskRepo) AcknowledgeFailedForSubject(ctx context.Context, subjectType, subjectKey string) (int, error) {
	if subjectType == "" || subjectKey == "" {
		return 0, ErrInvalidInput
	}
	return r.acknowledgeQuery(ctx, func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.Where("task.status = ? AND task.subject_type = ? AND task.subject_key = ?", model.TaskStatusFailed, subjectType, subjectKey).Where("NOT " + awaitingCommitReviewSQL)
	})
}

func (r *BunTaskRepo) AcknowledgeFailedMatching(ctx context.Context, filter TaskAcknowledgeFilter) (int, error) {
	if filter.FailedBefore.IsZero() {
		return 0, ErrInvalidInput
	}
	return r.acknowledgeQuery(ctx, func(q *bun.SelectQuery) *bun.SelectQuery {
		return q.ApplyQueryBuilder(acknowledgeFailedMatching(filter))
	})
}
