package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/strahe/synaps3/internal/model"

	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/uptrace/bun"
)

type BunProviderUploadSpeedRepo struct{ db bun.IDB }

func (r *BunProviderUploadSpeedRepo) BeginIfDue(ctx context.Context, providerID, hash string, taskID int64, testedBefore time.Time, nonMeasuredCodes []string) error {
	return r.begin(ctx, providerID, hash, taskID, &testedBefore, nonMeasuredCodes)
}

func (r *BunProviderUploadSpeedRepo) Begin(ctx context.Context, providerID, hash string, taskID int64) error {
	return r.begin(ctx, providerID, hash, taskID, nil, nil)
}

func (r *BunProviderUploadSpeedRepo) begin(ctx context.Context, providerID, hash string, taskID int64, testedBefore *time.Time, nonMeasuredCodes []string) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		task, err := taskForBinding(ctx, db, taskID, "provider", providerID, model.TaskTypeProviderUploadSpeedTest)
		if err != nil {
			return err
		}
		var input providerbenchmark.Input
		if json.Unmarshal(task.Input, &input) != nil || input.ProviderID != providerID || input.ServiceURLHash != hash {
			return ErrConflict
		}

		now := time.Now().UTC()
		row := &providerbenchmark.Result{
			ProviderID: providerID, State: providerbenchmark.StateTesting, ServiceURLHash: hash,
			SampleBytes: providerbenchmark.SampleBytes, ActiveTaskID: &taskID, CreatedAt: now, UpdatedAt: now,
		}
		result, err := db.NewInsert().Model(row).On("CONFLICT (provider_id) DO NOTHING").Exec(ctx)
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count == 1 {
			return nil
		}
		query := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
			Set("state = ?", providerbenchmark.StateTesting).
			Set("service_url_hash = ?", hash).
			Set("sample_bytes = CASE WHEN service_url_hash = ? THEN sample_bytes ELSE ? END", hash, providerbenchmark.SampleBytes).
			Set("duration_ms = CASE WHEN service_url_hash = ? THEN duration_ms ELSE NULL END", hash).
			Set("bytes_per_second = CASE WHEN service_url_hash = ? THEN bytes_per_second ELSE NULL END", hash).
			Set("tested_at = CASE WHEN service_url_hash = ? THEN tested_at ELSE NULL END", hash).
			Set("failure_code = CASE WHEN service_url_hash = ? THEN failure_code ELSE NULL END", hash).
			Set("active_task_id = ?", taskID).Set("updated_at = ?", now).
			Where("provider_id = ? AND state <> ?", providerID, providerbenchmark.StateTesting)
		if testedBefore != nil {
			query = query.Where("state IN (?)", bun.List([]providerbenchmark.State{providerbenchmark.StateSucceeded, providerbenchmark.StateFailed})).
				WhereGroup(" AND ", func(q *bun.UpdateQuery) *bun.UpdateQuery {
					q = q.Where("service_url_hash <> ? OR tested_at IS NULL", hash)
					if len(nonMeasuredCodes) > 0 {
						q = q.WhereOr("failure_code IN (?)", bun.List(nonMeasuredCodes))
					}
					if !testedBefore.IsZero() {
						q = q.WhereOr("tested_at <= ?", *testedBefore)
					}
					return q
				})
		}
		result, err = query.Exec(ctx)
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunProviderUploadSpeedRepo) Finish(ctx context.Context, providerID string, taskID int64, state providerbenchmark.State, sampleBytes, durationMS, bytesPerSecond int64, failureCode string) error {
	if state != providerbenchmark.StateSucceeded && state != providerbenchmark.StateFailed {
		return ErrInvalidInput
	}
	now := time.Now().UTC()
	query := r.db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("state = ?", state).Set("active_task_id = NULL").Set("tested_at = ?", now).Set("updated_at = ?", now).
		Where("provider_id = ? AND active_task_id = ? AND state = ?", providerID, taskID, providerbenchmark.StateTesting)
	if state == providerbenchmark.StateSucceeded {
		if sampleBytes < 1 || durationMS < 1 || bytesPerSecond < 1 {
			return ErrInvalidInput
		}
		query = query.Set("sample_bytes = ?", sampleBytes).Set("duration_ms = ?", durationMS).Set("bytes_per_second = ?", bytesPerSecond).Set("failure_code = NULL")
	} else {
		if failureCode == "" {
			return ErrInvalidInput
		}
		query = query.Set("duration_ms = NULL").Set("bytes_per_second = NULL").Set("failure_code = ?", failureCode)
	}
	result, err := query.Exec(ctx)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunProviderUploadSpeedRepo) FailActiveTask(ctx context.Context, taskID int64, failureCode string) error {
	if taskID < 1 || failureCode == "" {
		return ErrInvalidInput
	}
	now := time.Now().UTC()
	_, err := r.db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("state = ?", providerbenchmark.StateFailed).
		Set("active_task_id = NULL").Set("duration_ms = NULL").Set("bytes_per_second = NULL").
		Set("failure_code = ?", failureCode).Set("tested_at = ?", now).Set("updated_at = ?", now).
		Where("active_task_id = ? AND state = ?", taskID, providerbenchmark.StateTesting).Exec(ctx)
	return err
}

func (r *BunProviderUploadSpeedRepo) Get(ctx context.Context, providerID string) (*providerbenchmark.Result, error) {
	var row providerbenchmark.Result
	err := r.db.NewSelect().Model(&row).Where("provider_id = ?", providerID).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func (r *BunProviderUploadSpeedRepo) ListByProviderIDs(ctx context.Context, ids []string) (map[string]providerbenchmark.Result, error) {
	out := make(map[string]providerbenchmark.Result, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	var rows []providerbenchmark.Result
	if err := r.db.NewSelect().Model(&rows).Where("provider_id IN (?)", bun.List(ids)).Scan(ctx); err != nil {
		return nil, err
	}
	for _, row := range rows {
		out[row.ProviderID] = row
	}
	return out, nil
}
