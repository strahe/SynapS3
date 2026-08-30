package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// InitializeReplacementItemRetryBudgets snapshots the current configuration
// only for rows created before item-level retry budgets existed.
func (r *BunStorageReplacementRepo) InitializeReplacementItemRetryBudgets(ctx context.Context, maxRetries int) (int, error) {
	if maxRetries < 0 {
		return 0, fmt.Errorf("initializing replacement item retry budgets: %w", ErrInvalidInput)
	}
	res, err := r.db.NewUpdate().
		Model((*storagereplacement.Item)(nil)).
		Set("max_retries = ?", maxRetries).
		Where("max_retries IS NULL").
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("initializing replacement item retry budgets: %w", err)
	}
	rows, _ := res.RowsAffected()
	return int(rows), nil
}

// ReleaseExpiredItemLeases makes crash-interrupted transfers claimable again.
// Provider-side evidence stays on the target copy row and is deliberately not
// cleared here.
func (r *BunStorageReplacementRepo) ReleaseExpiredItemLeases(ctx context.Context) (int, error) {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*storagereplacement.Item)(nil)).
		Set("status = ?", storagereplacement.ItemStatusPending).
		Set("scheduled_at = ?", now).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("updated_at = ?", now).
		Where("status = ?", storagereplacement.ItemStatusRunning).
		Where("lease_until IS NULL OR lease_until <= ?", now).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("releasing expired replacement item leases: %w", err)
	}
	rows, _ := res.RowsAffected()
	return int(rows), nil
}

// ClaimReadyReplacementItem leases one item across every active replacement.
// Fairness is replacement-level first, then ready time and item id. Candidate
// selection stays bounded within the chosen replacement. PostgreSQL uses SKIP
// LOCKED; SQLite's write transaction serializes the selection.
func (r *BunStorageReplacementRepo) ClaimReadyReplacementItem(ctx context.Context, leaseTTL time.Duration) (*storagereplacement.Item, error) {
	if leaseTTL <= 0 {
		return nil, fmt.Errorf("claiming replacement item: %w", ErrInvalidInput)
	}
	var claimed *storagereplacement.Item
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		now := time.Now()
		replacementID, err := selectReadyReplacementID(ctx, db, now)
		if err != nil {
			return err
		}
		if replacementID == 0 {
			return nil
		}
		var replacementStatus storagereplacement.Status
		if err := db.NewSelect().
			Model((*storagereplacement.Replacement)(nil)).
			Column("status").
			Where("id = ?", replacementID).
			Scan(ctx, &replacementStatus); err != nil {
			return fmt.Errorf("loading ready replacement status: %w", err)
		}
		confirmationOnly := replacementStatus == storagereplacement.StatusFailed ||
			replacementStatus == storagereplacement.StatusSuperseded
		itemID, err := selectReadyReplacementItemID(ctx, db, replacementID, now, confirmationOnly)
		if err != nil {
			return err
		}
		if itemID == 0 {
			return nil
		}
		item := new(storagereplacement.Item)
		err = db.NewRaw(`UPDATE storage_replacement_items
			SET status = ?, attempts = attempts + 1, claimed_at = ?, lease_until = ?, updated_at = ?
			WHERE id = ?
			  AND max_retries IS NOT NULL
			  AND ((status IN (?, ?, ?) AND scheduled_at <= ? AND claimed_at IS NULL)
			       OR (status = ? AND lease_until <= ?))
			RETURNING *`,
			storagereplacement.ItemStatusRunning, now, now.Add(leaseTTL), now,
			itemID,
			storagereplacement.ItemStatusPending,
			storagereplacement.ItemStatusRetrying,
			storagereplacement.ItemStatusWaitingSource,
			now,
			storagereplacement.ItemStatusRunning,
			now,
		).Scan(ctx, item)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return fmt.Errorf("claiming replacement item: %w", err)
		}
		if _, err := db.NewUpdate().
			Model((*storagereplacement.Replacement)(nil)).
			Set("last_dispatched_at = ?", now).
			Set("updated_at = ?", now).
			Where("id = ?", item.ReplacementID).
			Where("status = ? OR (status = ? AND wait_reason = ?)",
				storagereplacement.StatusMigrating,
				storagereplacement.StatusWaiting,
				storagereplacement.WaitReasonReadableSource).
			Exec(ctx); err != nil {
			return fmt.Errorf("recording replacement dispatch: %w", err)
		}
		claimed = item
		return nil
	})
	return claimed, err
}

func selectReadyReplacementID(ctx context.Context, db bun.IDB, now time.Time) (int64, error) {
	var id int64
	err := db.NewRaw(readyReplacementSelectionSQL(db.Dialect().Name()),
		storagereplacement.StatusMigrating,
		storagereplacement.StatusWaiting,
		storagereplacement.WaitReasonReadableSource,
		now,
		now,
		storagereplacement.StatusFailed,
		storagereplacement.StatusSuperseded,
		now,
		now,
	).Scan(ctx, &id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("selecting ready replacement: %w", err)
	}
	return id, nil
}

func readyReplacementSelectionSQL(dialectName dialect.Name) string {
	query := `SELECT replacement.id
		FROM storage_replacements AS replacement
		WHERE (((replacement.status = ? OR (replacement.status = ? AND replacement.wait_reason = ?))
		  AND (EXISTS (
		         SELECT 1
		         FROM storage_replacement_items AS due_item
		         WHERE due_item.replacement_id = replacement.id
		           AND due_item.status IN ('pending', 'retrying', 'waiting_source')
		           AND due_item.scheduled_at <= ?
		           AND due_item.claimed_at IS NULL
		           AND due_item.max_retries IS NOT NULL
		       ) OR EXISTS (
		         SELECT 1
		         FROM storage_replacement_items AS expired_item
		         WHERE expired_item.replacement_id = replacement.id
		           AND expired_item.status = 'running'
		           AND expired_item.lease_until <= ?
		           AND expired_item.max_retries IS NOT NULL
		       )))
		  OR (replacement.status IN (?, ?)
		      AND (EXISTS (
		        SELECT 1
		        FROM storage_replacement_items AS due_item
		        JOIN storage_upload_copies AS due_copy ON due_copy.id = due_item.target_copy_id
		        WHERE due_item.replacement_id = replacement.id
		          AND due_item.status IN ('pending', 'retrying', 'waiting_source')
		          AND due_item.scheduled_at <= ?
		          AND due_item.claimed_at IS NULL
		          AND due_item.max_retries IS NOT NULL
		          AND due_copy.commit_attempt_id IS NOT NULL
		          AND due_copy.commit_attempt_id <> ''
		      ) OR EXISTS (
		        SELECT 1
		        FROM storage_replacement_items AS expired_item
		        JOIN storage_upload_copies AS expired_copy ON expired_copy.id = expired_item.target_copy_id
		        WHERE expired_item.replacement_id = replacement.id
		          AND expired_item.status = 'running'
		          AND expired_item.lease_until <= ?
		          AND expired_item.max_retries IS NOT NULL
		          AND expired_copy.commit_attempt_id IS NOT NULL
		          AND expired_copy.commit_attempt_id <> ''
		      ))))
		ORDER BY CASE WHEN replacement.last_dispatched_at IS NULL THEN 0 ELSE 1 END,
		         replacement.last_dispatched_at ASC,
		         replacement.id ASC
		LIMIT 1`
	if dialectName == dialect.PG {
		// Serializing on the owning replacement keeps concurrent claimers from
		// observing the same fairness timestamp. The item update below is fenced
		// by its ready state inside the same transaction.
		query += " FOR UPDATE OF replacement SKIP LOCKED"
	}
	return query
}

type readyReplacementItemCandidate struct {
	ID      int64     `bun:"id"`
	ReadyAt time.Time `bun:"ready_at"`
}

func selectReadyReplacementItemID(
	ctx context.Context,
	db bun.IDB,
	replacementID int64,
	now time.Time,
	confirmationOnly bool,
) (int64, error) {
	due, err := selectReadyReplacementItemCandidate(ctx, db, readyReplacementDueItemSQL(confirmationOnly),
		replacementID,
		now,
	)
	if err != nil {
		return 0, err
	}
	expired, err := selectReadyReplacementItemCandidate(ctx, db, readyReplacementExpiredItemSQL(confirmationOnly),
		replacementID,
		now,
	)
	if err != nil {
		return 0, err
	}
	if due == nil {
		if expired == nil {
			return 0, nil
		}
		return expired.ID, nil
	}
	if expired == nil || due.ReadyAt.Before(expired.ReadyAt) || (due.ReadyAt.Equal(expired.ReadyAt) && due.ID < expired.ID) {
		return due.ID, nil
	}
	return expired.ID, nil
}

func readyReplacementDueItemSQL(confirmationOnly bool) string {
	if confirmationOnly {
		return `SELECT item.id, item.scheduled_at AS ready_at
			FROM storage_replacement_items AS item
			JOIN storage_upload_copies AS storage_copy ON storage_copy.id = item.target_copy_id
			WHERE item.replacement_id = ?
			  AND item.status IN ('pending', 'retrying', 'waiting_source')
			  AND item.scheduled_at <= ?
			  AND item.claimed_at IS NULL
			  AND item.max_retries IS NOT NULL
			  AND storage_copy.commit_attempt_id IS NOT NULL
			  AND storage_copy.commit_attempt_id <> ''
			ORDER BY item.scheduled_at ASC, item.id ASC
			LIMIT 1`
	}
	return `SELECT item.id, item.scheduled_at AS ready_at
		FROM storage_replacement_items AS item
		WHERE item.replacement_id = ?
		  AND item.status IN ('pending', 'retrying', 'waiting_source')
		  AND item.scheduled_at <= ?
		  AND item.claimed_at IS NULL
		  AND item.max_retries IS NOT NULL
		ORDER BY item.scheduled_at ASC, item.id ASC
		LIMIT 1`
}

func readyReplacementExpiredItemSQL(confirmationOnly bool) string {
	if confirmationOnly {
		return `SELECT item.id, item.lease_until AS ready_at
			FROM storage_replacement_items AS item
			JOIN storage_upload_copies AS storage_copy ON storage_copy.id = item.target_copy_id
			WHERE item.replacement_id = ?
			  AND item.status = 'running'
			  AND item.lease_until <= ?
			  AND item.max_retries IS NOT NULL
			  AND storage_copy.commit_attempt_id IS NOT NULL
			  AND storage_copy.commit_attempt_id <> ''
			ORDER BY item.lease_until ASC, item.id ASC
			LIMIT 1`
	}
	return `SELECT item.id, item.lease_until AS ready_at
		FROM storage_replacement_items AS item
		WHERE item.replacement_id = ?
		  AND item.status = 'running'
		  AND item.lease_until <= ?
		  AND item.max_retries IS NOT NULL
		ORDER BY item.lease_until ASC, item.id ASC
		LIMIT 1`
}

func selectReadyReplacementItemCandidate(
	ctx context.Context,
	db bun.IDB,
	query string,
	args ...any,
) (*readyReplacementItemCandidate, error) {
	candidate := new(readyReplacementItemCandidate)
	if err := db.NewRaw(query, args...).Scan(ctx, candidate); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting ready replacement item: %w", err)
	}
	return candidate, nil
}

func (r *BunStorageReplacementRepo) RenewReplacementItemLease(
	ctx context.Context,
	token storagereplacement.ClaimToken,
	leaseTTL time.Duration,
) error {
	if token.ItemID <= 0 || token.ClaimedAt.IsZero() || leaseTTL <= 0 {
		return fmt.Errorf("renewing replacement item lease: %w", ErrInvalidInput)
	}
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*storagereplacement.Item)(nil)).
		Set("lease_until = ?", now.Add(leaseTTL)).
		Where("id = ? AND status = ?", token.ItemID, storagereplacement.ItemStatusRunning).
		Where("claimed_at = ?", token.ClaimedAt).
		Where("lease_until > ?", now).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("renewing replacement item lease: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return ErrItemClaimLost
	}
	return nil
}

func (r *BunStorageReplacementRepo) ReleaseReplacementItemClaim(ctx context.Context, token storagereplacement.ClaimToken) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*storagereplacement.Item)(nil)).
		Set("status = ?", storagereplacement.ItemStatusPending).
		Set("scheduled_at = ?", now).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("updated_at = ?", now).
		Where("id = ? AND status = ?", token.ItemID, storagereplacement.ItemStatusRunning).
		Where("claimed_at = ?", token.ClaimedAt).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("releasing replacement item claim: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return ErrItemClaimLost
	}
	return nil
}

func (r *BunStorageReplacementRepo) CancelReplacementItemClaim(ctx context.Context, token storagereplacement.ClaimToken) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		item := new(storagereplacement.Item)
		if err := db.NewSelect().Model(item).
			Where("id = ? AND status = ? AND claimed_at = ?", token.ItemID, storagereplacement.ItemStatusRunning, token.ClaimedAt).
			Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrItemClaimLost
			}
			return fmt.Errorf("loading replacement item for cancellation: %w", err)
		}
		return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled)
	})
}

func (r *BunStorageReplacementRepo) CompleteReplacementItemClaim(ctx context.Context, token storagereplacement.ClaimToken) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		now := time.Now()
		item := new(storagereplacement.Item)
		err := db.NewRaw(`UPDATE storage_replacement_items
			SET status = ?, last_error = NULL, claimed_at = NULL, lease_until = NULL, updated_at = ?
			WHERE id = ? AND status = ? AND claimed_at = ? AND lease_until > ?
			RETURNING *`, storagereplacement.ItemStatusCopied, now, token.ItemID,
			storagereplacement.ItemStatusRunning, token.ClaimedAt, now).Scan(ctx, item)
		if err != nil {
			if err == sql.ErrNoRows {
				return ErrItemClaimLost
			}
			return fmt.Errorf("settling replacement item claim: %w", err)
		}
		if _, err := db.NewUpdate().
			Model((*storagereplacement.Replacement)(nil)).
			Set("items_copied = items_copied + 1").
			Set("updated_at = ?", now).
			Where("id = ? AND items_copied < items_total", item.ReplacementID).
			Exec(ctx); err != nil {
			return fmt.Errorf("recording replacement progress: %w", err)
		}
		if err := resumeReadableSourceMigration(ctx, db, item.ReplacementID, now); err != nil {
			return err
		}
		return nil
	})
}

// WaitReplacementItemClaim parks an item without consuming its retry budget.
func (r *BunStorageReplacementRepo) WaitReplacementItemClaim(
	ctx context.Context,
	token storagereplacement.ClaimToken,
	nextCheck time.Time,
	lastError string,
) error {
	return r.transitionReplacementItemClaim(ctx, token, storagereplacement.ItemStatusWaitingSource, nextCheck, lastError)
}

// DeferReplacementItemClaim yields to an earlier upload without consuming a retry.
func (r *BunStorageReplacementRepo) DeferReplacementItemClaim(
	ctx context.Context,
	token storagereplacement.ClaimToken,
	nextAttempt time.Time,
) error {
	return r.transitionReplacementItemClaim(ctx, token, storagereplacement.ItemStatusPending, nextAttempt, "")
}

// RetryReplacementItemClaim persists one transient failure. The item fails
// only after its fixed retry budget is exhausted.
func (r *BunStorageReplacementRepo) RetryReplacementItemClaim(
	ctx context.Context,
	token storagereplacement.ClaimToken,
	nextAttempt time.Time,
	lastError string,
) (storagereplacement.ItemStatus, error) {
	status := storagereplacement.ItemStatusRetrying
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		item := new(storagereplacement.Item)
		if err := db.NewSelect().Model(item).
			Where("id = ? AND status = ? AND claimed_at = ?", token.ItemID, storagereplacement.ItemStatusRunning, token.ClaimedAt).
			Scan(ctx); err != nil {
			if err == sql.ErrNoRows {
				return ErrItemClaimLost
			}
			return fmt.Errorf("loading replacement item retry budget: %w", err)
		}
		if item.LeaseUntil == nil || !item.LeaseUntil.After(time.Now()) {
			return ErrItemClaimLost
		}
		if item.MaxRetries == nil {
			return fmt.Errorf("replacement item retry budget is not initialized: %w", ErrConflict)
		}
		if item.RetryCount >= *item.MaxRetries {
			status = storagereplacement.ItemStatusFailed
		}
		now := time.Now()
		res, err := db.NewUpdate().Model((*storagereplacement.Item)(nil)).
			Set("status = ?", status).
			Set("retry_count = retry_count + 1").
			Set("scheduled_at = ?", nextAttempt).
			Set("last_error = ?", nullableString(lastError)).
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("updated_at = ?", now).
			Where("id = ? AND status = ? AND claimed_at = ?", token.ItemID, storagereplacement.ItemStatusRunning, token.ClaimedAt).
			Where("lease_until > ?", now).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("scheduling replacement item retry: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrItemClaimLost
		}
		return nil
	})
	return status, err
}

func (r *BunStorageReplacementRepo) transitionReplacementItemClaim(
	ctx context.Context,
	token storagereplacement.ClaimToken,
	status storagereplacement.ItemStatus,
	scheduledAt time.Time,
	lastError string,
) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		now := time.Now()
		query := db.NewUpdate().Model((*storagereplacement.Item)(nil)).
			Set("status = ?", status).
			Set("scheduled_at = ?", scheduledAt).
			Set("last_error = ?", nullableString(lastError)).
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("updated_at = ?", now).
			Where("id = ? AND status = ?", token.ItemID, storagereplacement.ItemStatusRunning).
			Where("claimed_at = ?", token.ClaimedAt).
			Where("lease_until > ?", now)
		res, err := query.Exec(ctx)
		if err != nil {
			return fmt.Errorf("transitioning replacement item claim: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrItemClaimLost
		}
		return nil
	})
}

// PauseMigration rejects requests from an older replacement state version.
func (r *BunStorageReplacementRepo) PauseMigration(
	ctx context.Context,
	replacementID, expectedStateVersion int64,
	reason storagereplacement.WaitReason,
) error {
	if !reason.Valid() {
		return fmt.Errorf("pausing replacement migration: %w", ErrInvalidInput)
	}
	now := time.Now()
	res, err := r.db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", storagereplacement.StatusWaiting).
		Set("wait_reason = ?", reason).
		Set("updated_at = ?", now).
		Where("id = ? AND state_version = ?", replacementID, expectedStateVersion).
		Where("status = ? OR (status = ? AND wait_reason = ?)",
			storagereplacement.StatusMigrating,
			storagereplacement.StatusWaiting,
			storagereplacement.WaitReasonReadableSource).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("pausing replacement migration: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return ErrConflict
	}
	return nil
}

func resumeReadableSourceMigration(ctx context.Context, db bun.IDB, replacementID int64, now time.Time) error {
	_, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", storagereplacement.StatusMigrating).
		Set("wait_reason = NULL").
		Set("state_version = state_version + 1").
		Set("updated_at = ?", now).
		Where("id = ? AND status = ? AND wait_reason = ?",
			replacementID,
			storagereplacement.StatusWaiting,
			storagereplacement.WaitReasonReadableSource,
		).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("resuming readable-source replacement migration: %w", err)
	}
	return nil
}

// ReplacementExecution returns the bounded state the coordinator needs. The
// replacement-state index makes each item-state probe stop at its first match.
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
		       EXISTS (SELECT 1 FROM storage_replacement_items AS item
		               WHERE item.replacement_id = replacement.id AND item.status = 'running') AS has_active,
		       EXISTS (SELECT 1 FROM storage_replacement_items AS item
		               WHERE item.replacement_id = replacement.id AND item.status = 'retrying') AS has_retrying,
		       EXISTS (SELECT 1 FROM storage_replacement_items AS item
		               WHERE item.replacement_id = replacement.id AND item.status = 'waiting_source') AS has_waiting_source,
		       EXISTS (SELECT 1 FROM storage_replacement_items AS item
		               WHERE item.replacement_id = replacement.id AND item.status = 'failed') AS has_failed
		FROM storage_replacements AS replacement
		WHERE replacement.id = ?`, replacementID).Scan(ctx, &snapshot)
	if err != nil {
		if err == sql.ErrNoRows {
			return storagereplacement.ExecutionSnapshot{}, fmt.Errorf("provider replacement %d: %w", replacementID, ErrNotFound)
		}
		return storagereplacement.ExecutionSnapshot{}, fmt.Errorf("loading replacement execution: %w", err)
	}
	return snapshot, nil
}

// ReplacementProgresses returns a batch of UI-neutral progress snapshots.
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
		ItemsAttention     int                       `bun:"items_attention"`
		ItemsRetrying      int                       `bun:"items_retrying"`
		ItemsWaitingSource int                       `bun:"items_waiting_source"`
		ItemsFailed        int                       `bun:"items_failed"`
		NextRetryAt        *time.Time                `bun:"next_retry_at"`
	}
	var rows []progressRow
	now := time.Now()
	query := `SELECT replacement.id AS replacement_id,
		       replacement.status AS replacement_status,
		       target.is_current AS target_is_current,
		       replacement.seeding_complete,
		       replacement.items_total,
		       replacement.items_copied,
		       COALESCE(SUM(CASE WHEN target_copy.commit_attention_at IS NULL AND item.status = 'pending'
		         AND target_copy.commit_attempt_id IS NULL THEN 1 ELSE 0 END), 0) AS items_pending,
		       COALESCE(SUM(CASE WHEN target_copy.commit_attention_at IS NULL
		         AND (item.status = 'running' OR target_copy.commit_attempt_id IS NOT NULL) THEN 1 ELSE 0 END), 0) AS items_active,
		       COALESCE(SUM(CASE WHEN target_copy.commit_attention_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS items_attention,
		       COALESCE(SUM(CASE WHEN target_copy.commit_attention_at IS NULL AND item.status = 'retrying'
		         AND target_copy.commit_attempt_id IS NULL THEN 1 ELSE 0 END), 0) AS items_retrying,
		       COALESCE(SUM(CASE WHEN target_copy.commit_attention_at IS NULL AND item.status = 'waiting_source'
		         AND target_copy.commit_attempt_id IS NULL THEN 1 ELSE 0 END), 0) AS items_waiting_source,
		       COALESCE(SUM(CASE WHEN target_copy.commit_attention_at IS NULL AND item.status = 'failed'
		         AND target_copy.commit_attempt_id IS NULL THEN 1 ELSE 0 END), 0) AS items_failed,
		       MIN(CASE WHEN target_copy.commit_attention_at IS NULL AND target_copy.commit_attempt_id IS NULL
		         AND item.status = 'retrying' AND item.scheduled_at > ? THEN item.scheduled_at END) AS next_retry_at
		FROM storage_replacements AS replacement
		JOIN storage_data_sets AS target ON target.id = replacement.target_data_set_id
		LEFT JOIN storage_replacement_items AS item ON item.replacement_id = replacement.id
		LEFT JOIN storage_upload_copies AS target_copy ON target_copy.id = item.target_copy_id
		WHERE replacement.id IN (?)
		GROUP BY replacement.id, replacement.status, target.is_current, replacement.seeding_complete,
		         replacement.items_total, replacement.items_copied`
	if err := r.db.NewRaw(query, now, bun.List(replacementIDs)).Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("loading replacement progress: %w", err)
	}
	for i := range rows {
		row := &rows[i]
		outstanding := row.ItemsPending + row.ItemsActive + row.ItemsAttention + row.ItemsRetrying + row.ItemsWaitingSource + row.ItemsFailed
		noLongerNeeded := max(0, row.ItemsTotal-row.ItemsCopied-outstanding)
		processed := row.ItemsCopied + noLongerNeeded
		var percent *int
		if row.SeedingComplete {
			value := 100
			if row.ItemsTotal > 0 {
				value = processed * 100 / row.ItemsTotal
				if value > 100 {
					value = 100
				}
			}
			percent = &value
		}
		out[row.ReplacementID] = storagereplacement.ProgressSnapshot{
			ReplacementID:       row.ReplacementID,
			Phase:               progressPhase(row.Status, row.TargetIsCurrent),
			SeedingComplete:     row.SeedingComplete,
			ItemsTotal:          row.ItemsTotal,
			ItemsProcessed:      processed,
			ItemsCopied:         row.ItemsCopied,
			ItemsNoLongerNeeded: noLongerNeeded,
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

func progressPhase(status storagereplacement.Status, targetIsCurrent bool) storagereplacement.Phase {
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

// RunningReplacementItemClaimForUpload exposes only the fencing identity needed
// for ordinary-upload mutual exclusion, including before a target copy is attached.
func (r *BunStorageReplacementRepo) RunningReplacementItemClaimForUpload(
	ctx context.Context,
	replacementID, uploadID int64,
) (*storagereplacement.ClaimToken, error) {
	item := new(storagereplacement.Item)
	err := r.db.NewSelect().Model(item).
		Column("id", "claimed_at").
		Where("replacement_id = ? AND upload_id = ?", replacementID, uploadID).
		Where("status = ?", storagereplacement.ItemStatusRunning).
		Where("claimed_at IS NOT NULL AND lease_until > ?", time.Now()).
		Limit(1).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loading replacement item upload claim: %w", err)
	}
	if item.ClaimedAt == nil {
		return nil, nil
	}
	return &storagereplacement.ClaimToken{ItemID: item.ID, ClaimedAt: *item.ClaimedAt}, nil
}
