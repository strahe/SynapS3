package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/uptrace/bun"
)

type BunProviderUploadSpeedRepo struct{ db bun.IDB }

func (r *BunProviderUploadSpeedRepo) Begin(ctx context.Context, providerID, hash string, taskID int64) error {
	now := time.Now().UTC()
	row := &providerbenchmark.Result{
		ProviderID: providerID, State: providerbenchmark.StateTesting, ServiceURLHash: hash,
		SampleBytes: providerbenchmark.SampleBytes, ActiveTaskID: &taskID, CreatedAt: now, UpdatedAt: now,
	}
	result, err := r.db.NewInsert().Model(row).On("CONFLICT (provider_id) DO NOTHING").Exec(ctx)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count == 1 {
		return nil
	}
	result, err = r.db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("state = ?", providerbenchmark.StateTesting).
		Set("service_url_hash = ?", hash).
		Set("sample_bytes = ?", providerbenchmark.SampleBytes).
		Set("duration_ms = NULL").Set("bytes_per_second = NULL").Set("tested_at = NULL").Set("failure_code = NULL").
		Set("active_task_id = ?", taskID).Set("updated_at = ?", now).
		Where("provider_id = ? AND state <> ?", providerID, providerbenchmark.StateTesting).Exec(ctx)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrConflict
	}
	return nil
}

func (r *BunProviderUploadSpeedRepo) Finish(ctx context.Context, providerID string, taskID int64, state providerbenchmark.State, durationMS, bytesPerSecond int64, failureCode string) error {
	if state != providerbenchmark.StateSucceeded && state != providerbenchmark.StateFailed {
		return ErrInvalidInput
	}
	now := time.Now().UTC()
	query := r.db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("state = ?", state).Set("active_task_id = NULL").Set("tested_at = ?", now).Set("updated_at = ?", now).
		Where("provider_id = ? AND active_task_id = ? AND state = ?", providerID, taskID, providerbenchmark.StateTesting)
	if state == providerbenchmark.StateSucceeded {
		if durationMS < 1 || bytesPerSecond < 1 {
			return ErrInvalidInput
		}
		query = query.Set("duration_ms = ?", durationMS).Set("bytes_per_second = ?", bytesPerSecond).Set("failure_code = NULL")
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
