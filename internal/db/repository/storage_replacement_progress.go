package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func (r *BunStorageReplacementRepo) ReplacementExecution(
	ctx context.Context,
	replacementID int64,
) (storagereplacement.ExecutionSnapshot, error) {
	if replacementID <= 0 {
		return storagereplacement.ExecutionSnapshot{}, fmt.Errorf("loading replacement execution: %w", ErrInvalidInput)
	}
	var snapshot storagereplacement.ExecutionSnapshot
	err := r.db.NewRaw(`SELECT replacement.id AS replacement_id,
		       replacement.seeding_complete,
		       replacement.items_total,
		       replacement.items_copied,
		       EXISTS (SELECT 1 FROM storage_replacement_items AS item
		               WHERE item.replacement_id = replacement.id AND item.status = 'pending') AS has_pending,
		       FALSE AS has_active,
		       FALSE AS has_retrying,
		       FALSE AS has_waiting_source,
		       EXISTS (SELECT 1 FROM storage_replacement_items AS item
		               WHERE item.replacement_id = replacement.id AND item.status = 'attention') AS has_failed
		FROM storage_replacements AS replacement
		WHERE replacement.id = ?`, replacementID).Scan(ctx, &snapshot)
	if err == sql.ErrNoRows {
		return storagereplacement.ExecutionSnapshot{}, fmt.Errorf("provider replacement %d: %w", replacementID, ErrNotFound)
	}
	if err != nil {
		return storagereplacement.ExecutionSnapshot{}, fmt.Errorf("loading replacement execution: %w", err)
	}
	return snapshot, nil
}

func (r *BunStorageReplacementRepo) ReplacementProgresses(
	ctx context.Context,
	replacementIDs []int64,
) (map[int64]storagereplacement.ProgressSnapshot, error) {
	out := make(map[int64]storagereplacement.ProgressSnapshot, len(replacementIDs))
	if len(replacementIDs) == 0 {
		return out, nil
	}
	type progressRow struct {
		ReplacementID      int64                     `bun:"replacement_id"`
		Status             storagereplacement.Status `bun:"replacement_status"`
		TargetIsCurrent    bool                      `bun:"target_is_current"`
		SeedingComplete    bool                      `bun:"seeding_complete"`
		ItemsTotal         int                       `bun:"items_total"`
		ItemsCopied        int                       `bun:"items_copied"`
		ItemsPending       int                       `bun:"items_pending"`
		ItemsActive        int                       `bun:"items_active"`
		ItemsCancelled     int                       `bun:"items_cancelled"`
		ItemsAttention     int                       `bun:"items_attention"`
		ItemsRetrying      int                       `bun:"items_retrying"`
		ItemsWaitingSource int                       `bun:"items_waiting_source"`
		ItemsFailed        int                       `bun:"items_failed"`
		NextRetryAt        *time.Time                `bun:"next_retry_at"`
	}
	var rows []progressRow
	now := time.Now()
	if err := r.db.NewRaw(`WITH item_progress AS (
		SELECT replacement.id AS replacement_id,
		       replacement.status AS replacement_status,
		       target.is_current AS target_is_current,
		       replacement.seeding_complete,
		       replacement.items_total,
		       replacement.items_copied,
		       item.status AS item_status,
		       CASE
		         WHEN item.id IS NULL THEN NULL
		         WHEN item.status <> 'pending' THEN item.status
		         WHEN target_copy.status = 'failed' OR copy_task.status = 'failed' THEN 'failed'
		         WHEN copy_task.status = 'pending' AND copy_task.wait_reason = 'source' THEN 'waiting_source'
		         WHEN coordinator.status = 'pending' AND coordinator.wait_reason = 'source'
		              AND item.id = (SELECT MIN(waiting_item.id)
		                             FROM storage_replacement_items AS waiting_item
		                             WHERE waiting_item.replacement_id = replacement.id
		                               AND waiting_item.status = 'pending') THEN 'waiting_source'
		         WHEN copy_task.status = 'pending' AND copy_task.retry_count > 0 AND copy_task.available_at > ? THEN 'retrying'
		         WHEN copy_task.status IN ('pending', 'running') THEN 'active'
		         ELSE 'pending'
		       END AS progress_status,
		       CASE WHEN copy_task.status = 'pending' AND copy_task.retry_count > 0 AND copy_task.available_at > ?
		            THEN copy_task.available_at END AS next_retry_at
		FROM storage_replacements AS replacement
		JOIN storage_data_sets AS target ON target.id = replacement.target_data_set_id
		LEFT JOIN storage_replacement_items AS item ON item.replacement_id = replacement.id
		LEFT JOIN storage_copies AS target_copy
		       ON target_copy.content_id = item.content_id
		      AND target_copy.storage_data_set_id = item.target_data_set_id
		LEFT JOIN tasks AS copy_task ON copy_task.id = target_copy.active_task_id
		LEFT JOIN tasks AS coordinator ON coordinator.id = replacement.task_id
		WHERE replacement.id IN (?)
	)
	SELECT replacement_id,
		       replacement_status,
		       target_is_current,
		       seeding_complete,
		       items_total,
		       items_copied,
		       COALESCE(SUM(CASE WHEN progress_status = 'pending' THEN 1 ELSE 0 END), 0) AS items_pending,
		       COALESCE(SUM(CASE WHEN progress_status = 'active' THEN 1 ELSE 0 END), 0) AS items_active,
		       COALESCE(SUM(CASE WHEN item_status = 'cancelled' THEN 1 ELSE 0 END), 0) AS items_cancelled,
		       COALESCE(SUM(CASE WHEN item_status = 'attention' THEN 1 ELSE 0 END), 0) AS items_attention,
		       COALESCE(SUM(CASE WHEN progress_status = 'retrying' THEN 1 ELSE 0 END), 0) AS items_retrying,
		       COALESCE(SUM(CASE WHEN progress_status = 'waiting_source' THEN 1 ELSE 0 END), 0) AS items_waiting_source,
		       COALESCE(SUM(CASE WHEN progress_status = 'failed' THEN 1 ELSE 0 END), 0) AS items_failed,
		       MIN(next_retry_at) AS next_retry_at
	FROM item_progress
	GROUP BY replacement_id, replacement_status, target_is_current, seeding_complete, items_total, items_copied`,
		now, now, bun.List(replacementIDs)).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("loading replacement progress: %w", err)
	}
	for i := range rows {
		row := rows[i]
		processed := row.ItemsCopied + row.ItemsCancelled
		var percent *int
		if row.SeedingComplete {
			value := 100
			if row.ItemsTotal > 0 {
				value = min(100, processed*100/row.ItemsTotal)
			}
			percent = &value
		}
		out[row.ReplacementID] = storagereplacement.ProgressSnapshot{
			ReplacementID:       row.ReplacementID,
			Phase:               replacementProgressPhase(row.Status, row.TargetIsCurrent),
			SeedingComplete:     row.SeedingComplete,
			ItemsTotal:          row.ItemsTotal,
			ItemsProcessed:      processed,
			ItemsCopied:         row.ItemsCopied,
			ItemsNoLongerNeeded: row.ItemsCancelled,
			ItemsPending:        row.ItemsPending,
			ItemsActive:         row.ItemsActive,
			ItemsAttention:      row.ItemsAttention,
			ItemsRetrying:       row.ItemsRetrying,
			ItemsWaitingSource:  row.ItemsWaitingSource,
			ItemsFailed:         row.ItemsFailed,
			Percent:             percent,
			NextRetryAt:         row.NextRetryAt,
		}
	}
	return out, nil
}

func replacementProgressPhase(status storagereplacement.Status, targetIsCurrent bool) storagereplacement.Phase {
	switch status {
	case storagereplacement.StatusPreparingTarget:
		return storagereplacement.PhasePrepare
	case storagereplacement.StatusRetiring, storagereplacement.StatusCleanupAttention, storagereplacement.StatusCompleted:
		return storagereplacement.PhaseRetire
	case storagereplacement.StatusMigrating:
		return storagereplacement.PhaseMigrate
	case storagereplacement.StatusWaiting, storagereplacement.StatusFailed:
		if targetIsCurrent {
			return storagereplacement.PhaseMigrate
		}
		return storagereplacement.PhasePrepare
	default:
		return storagereplacement.PhaseNone
	}
}
