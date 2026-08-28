package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

var _ StorageReplacementRepository = (*BunStorageReplacementRepo)(nil)

// BunStorageReplacementRepo persists operator-approved provider replacements.
type BunStorageReplacementRepo struct {
	db bun.IDB
}

// Authorize is the only way a replacement comes into existence. It runs as one
// transaction so the superseded predecessor, the new target generation, the
// replacement record, and its coordinator either all exist or none do.
func (r *BunStorageReplacementRepo) Authorize(ctx context.Context, input AuthorizeReplacementInput) (*storagereplacement.Replacement, bool, error) {
	input.ClientRequestID = strings.TrimSpace(input.ClientRequestID)
	if input.BucketID <= 0 || input.SourceDataSetID <= 0 || input.TargetProviderID.IsZero() ||
		input.ClientRequestID == "" || len(input.ClientRequestID) > 128 {
		return nil, false, fmt.Errorf("authorizing provider replacement: %w", ErrInvalidInput)
	}
	if !input.SelectionMode.Valid() {
		return nil, false, fmt.Errorf("authorizing provider replacement: unknown selection mode: %w", ErrInvalidInput)
	}
	var result *storagereplacement.Replacement
	created := false
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		bucket, err := lockBucketByID(ctx, db, input.BucketID)
		if err != nil {
			return err
		}
		if bucket == nil {
			return fmt.Errorf("authorizing provider replacement: bucket %d: %w", input.BucketID, ErrNotFound)
		}
		existing, err := getReplacementByClientRequestID(ctx, db, input.BucketID, input.ClientRequestID)
		if err != nil {
			return err
		}
		if existing != nil {
			if !replacementRequestMatches(existing, input) {
				return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrIdempotencyConflict)
			}
			result = existing
			return nil
		}
		source, err := (&BunStorageUploadRepo{db: db}).GetDataSetBindingByID(ctx, input.SourceDataSetID)
		if err != nil {
			return err
		}
		if source == nil || source.BucketID != input.BucketID {
			return fmt.Errorf("authorizing provider replacement: data set %d: %w", input.SourceDataSetID, ErrNotFound)
		}
		// Replacing a generation that no longer owns the slot would not move any
		// writes, so it is refused rather than silently accepted.
		if !source.IsCurrent {
			return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrSourceNotCurrent)
		}
		if source.ProviderID.Equal(input.TargetProviderID) {
			return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrInvalidTarget)
		}
		// A generation that some unfinished replacement is still migrating into
		// cannot become a source of its own. That replacement's safety gate
		// requires this generation to keep owning the slot, so handing the slot
		// to a third generation would strand it and leave the original source
		// unable to retire.
		pending, err := db.NewSelect().
			Model((*storagereplacement.Replacement)(nil)).
			Where("target_data_set_id = ?", source.ID).
			Where("status NOT IN (?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
			Count(ctx)
		if err != nil {
			return fmt.Errorf("checking replacements targeting this data set: %w", err)
		}
		if pending > 0 {
			return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrActiveReplacement)
		}
		// Any generation that has not been retired still holds this provider's
		// data set for the bucket. Preparing a second one would make the SDK
		// hand back the same data set and collide on the provider/data set
		// uniqueness, so the choice is refused up front with a typed error
		// rather than failing later inside the worker.
		inUse, err := db.NewSelect().
			Model((*model.StorageDataSet)(nil)).
			Where("bucket_id = ? AND provider_id = ?", input.BucketID, input.TargetProviderID).
			Where("status <> ?", model.StorageDataSetStatusRetired).
			Count(ctx)
		if err != nil {
			return fmt.Errorf("checking replacement target provider: %w", err)
		}
		if inUse > 0 {
			return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrTargetInUse)
		}

		now := time.Now()
		// The single-active-replacement index rejects a second live row for one
		// source, so the predecessor must step down before the successor exists.
		superseded, err := supersedeEarlierReplacements(ctx, db, source.ID, now)
		if err != nil {
			return err
		}
		generation, err := nextDataSetGeneration(ctx, db, input.BucketID, source.CopyIndex)
		if err != nil {
			return err
		}
		target := &model.StorageDataSet{
			BucketID:   input.BucketID,
			ProviderID: input.TargetProviderID,
			CopyIndex:  source.CopyIndex,
			Generation: generation,
			// The target only takes the slot once it is writable, so preparing it
			// does not change where uploads go.
			IsCurrent: false,
			Status:    model.StorageDataSetStatusPending,
			CreatedAt: now,
			UpdatedAt: now,
		}
		if _, err := db.NewInsert().Model(target).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrTargetInUse)
			}
			return fmt.Errorf("creating replacement target data set: %w", err)
		}

		replacement := &storagereplacement.Replacement{
			BucketID:        input.BucketID,
			CopyIndex:       source.CopyIndex,
			SourceDataSetID: source.ID,
			TargetDataSetID: target.ID,
			SelectionMode:   input.SelectionMode,
			ClientRequestID: input.ClientRequestID,
			Status:          storagereplacement.StatusPreparingTarget,
			ConfirmedAt:     now,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		if input.SelectionMode == storagereplacement.SelectionModeManual {
			providerID := input.TargetProviderID
			replacement.RequestedProviderID = &providerID
		}
		if _, err := db.NewInsert().Model(replacement).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrActiveReplacement)
			}
			return fmt.Errorf("creating provider replacement: %w", err)
		}
		if err := linkSupersededReplacements(ctx, db, superseded, replacement.ID, now); err != nil {
			return err
		}
		tasks := &BunTaskRepo{db: db}
		for _, id := range superseded {
			// Leftover targets keep costing money until their own coordinator
			// ends them. Queue that work in this transaction so cleanup does
			// not wait for the next process start.
			if _, err := tasks.EnsureRecurring(ctx, storagereplacement.NewAbandonedTargetTask(
				id, input.BucketID, input.MaxRetries, now,
			)); err != nil {
				return fmt.Errorf("queueing abandoned replacement cleanup: %w", err)
			}
		}
		if _, err := tasks.EnsureRecurring(ctx, storagereplacement.NewMigrateTask(
			replacement.ID, input.BucketID, "", input.MaxRetries, now,
		)); err != nil {
			return fmt.Errorf("queueing replacement migration coordinator: %w", err)
		}
		result = replacement
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return result, created, nil
}

func replacementRequestMatches(row *storagereplacement.Replacement, input AuthorizeReplacementInput) bool {
	if row.BucketID != input.BucketID || row.SourceDataSetID != input.SourceDataSetID || row.SelectionMode != input.SelectionMode {
		return false
	}
	if input.SelectionMode != storagereplacement.SelectionModeManual {
		return true
	}
	return row.RequestedProviderID != nil && row.RequestedProviderID.Equal(input.TargetProviderID)
}

func getReplacementByClientRequestID(
	ctx context.Context,
	db bun.IDB,
	bucketID int64,
	clientRequestID string,
) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := db.NewSelect().
		Model(row).
		Where("bucket_id = ? AND client_request_id = ?", bucketID, clientRequestID).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("selecting provider replacement by client request id: %w", err)
	}
	return row, nil
}

// A later confirmation takes over from an earlier one in the same transaction,
// which is what the single-active-replacement index relies on.
func supersedeEarlierReplacements(ctx context.Context, db bun.IDB, sourceDataSetID int64, now time.Time) ([]int64, error) {
	var ids []int64
	if err := db.NewSelect().
		Model((*storagereplacement.Replacement)(nil)).
		Column("id").
		Where("source_data_set_id = ?", sourceDataSetID).
		Where("status NOT IN (?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
		Scan(ctx, &ids); err != nil {
		return nil, fmt.Errorf("selecting earlier provider replacements: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	if _, err := db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", storagereplacement.StatusSuperseded).
		Set("wait_reason = NULL").
		Set("updated_at = ?", now).
		Where("id IN (?)", bun.List(ids)).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("superseding earlier provider replacement: %w", err)
	}
	return ids, nil
}

// The successor id is recorded once it exists, so an operator can follow the
// chain from an abandoned confirmation to the one that replaced it.
func linkSupersededReplacements(ctx context.Context, db bun.IDB, ids []int64, successorID int64, now time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("superseded_by_id = ?", successorID).
		Set("updated_at = ?", now).
		Where("id IN (?)", bun.List(ids)).
		Exec(ctx); err != nil {
		return fmt.Errorf("linking superseded provider replacement: %w", err)
	}
	return nil
}

// Retry resumes the same approved target. The phase is re-derived from the
// data rather than remembered, so a retry always restarts at the stage the
// replacement actually reached.
func (r *BunStorageReplacementRepo) Retry(ctx context.Context, input RetryReplacementInput) (*storagereplacement.Replacement, error) {
	if input.ReplacementID <= 0 || input.MaxRetries < 0 || input.ItemMaxRetries < 0 {
		return nil, fmt.Errorf("retrying provider replacement: %w", ErrInvalidInput)
	}
	var resumed *storagereplacement.Replacement
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := lockReplacementByID(ctx, db, input.ReplacementID)
		if err != nil {
			return err
		}
		if row.Status == storagereplacement.StatusSuperseded {
			return fmt.Errorf("retrying provider replacement: %w", storagereplacement.ErrSuperseded)
		}
		if row.FailureReason != nil && *row.FailureReason == storagereplacement.FailureReasonTargetInUse {
			return fmt.Errorf("retrying provider replacement: %w", storagereplacement.ErrTargetInUse)
		}
		if !row.Status.Retryable() {
			return fmt.Errorf("retrying provider replacement: %w", storagereplacement.ErrNotRetryable)
		}
		running, err := db.NewSelect().
			Model((*model.Task)(nil)).
			Where("idempotency_key IN (?, ?)",
				storagereplacement.MigrateTaskKey(row.ID),
				storagereplacement.RetireTaskKey(row.ID)).
			Where("status = ?", model.TaskStatusRunning).
			Count(ctx)
		if err != nil {
			return fmt.Errorf("checking replacement coordinator tasks: %w", err)
		}
		if running > 0 {
			return fmt.Errorf("retrying provider replacement: %w", storagereplacement.ErrTaskRunning)
		}

		target, err := (&BunStorageUploadRepo{db: db}).GetDataSetBindingByID(ctx, row.TargetDataSetID)
		if err != nil {
			return err
		}
		if target == nil {
			return fmt.Errorf("retrying provider replacement: target data set %d: %w", row.TargetDataSetID, ErrNotFound)
		}
		next := storagereplacement.StatusPreparingTarget
		switch {
		case row.Status == storagereplacement.StatusCleanupAttention:
			next = storagereplacement.StatusRetiring
		case target.IsCurrent:
			next = storagereplacement.StatusMigrating
		}
		now := time.Now()
		if _, err := db.NewUpdate().Model((*storagereplacement.Item)(nil)).
			Set("status = ?", storagereplacement.ItemStatusPending).
			Set("retry_count = 0").
			Set("max_retries = ?", input.ItemMaxRetries).
			Set("scheduled_at = ?", now).
			Set("last_error = NULL").
			Set("claimed_at = NULL").
			Set("lease_until = NULL").
			Set("updated_at = ?", now).
			Where("replacement_id = ? AND status = ?", row.ID, storagereplacement.ItemStatusFailed).
			Exec(ctx); err != nil {
			return fmt.Errorf("resetting failed replacement items: %w", err)
		}
		if err := transitionReplacement(ctx, db, row.ID, []storagereplacement.Status{row.Status}, next, func(q *bun.UpdateQuery) *bun.UpdateQuery {
			return q.Set("last_error = NULL").Set("wait_reason = NULL").Set("failure_reason = NULL")
		}, now); err != nil {
			return err
		}
		task := storagereplacement.NewMigrateTask(row.ID, row.BucketID, "", input.MaxRetries, now)
		if next == storagereplacement.StatusRetiring {
			task = storagereplacement.NewRetireTask(row.ID, row.BucketID, input.MaxRetries, now)
		}
		// The worker marks the coordinator exhausted or failed on its way into a
		// retryable state, and automatic recurrence deliberately leaves those
		// alone. Resuming is the operator's explicit request to undo that.
		if _, err := (&BunTaskRepo{db: db}).ResumeCoordinator(ctx, task); err != nil {
			return fmt.Errorf("requeueing replacement coordinator: %w", err)
		}
		row.Status = next
		row.LastError = nil
		row.WaitReason = nil
		row.FailureReason = nil
		row.UpdatedAt = now
		resumed = row
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resumed, nil
}

// Activate is the single atomic switch: the target starts receiving writes and
// the source starts draining. It touches three rows whatever the bucket holds.
func (r *BunStorageReplacementRepo) Activate(ctx context.Context, replacementID int64) error {
	if replacementID <= 0 {
		return fmt.Errorf("activating provider replacement: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		var bucketID int64
		if err := db.NewSelect().
			Model((*storagereplacement.Replacement)(nil)).
			Column("bucket_id").
			Where("id = ?", replacementID).
			Scan(ctx, &bucketID); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("provider replacement %d: %w", replacementID, ErrNotFound)
			}
			return fmt.Errorf("selecting replacement bucket: %w", err)
		}
		if _, err := lockBucketByID(ctx, db, bucketID); err != nil {
			return err
		}
		row, err := lockReplacementByID(ctx, db, replacementID)
		if err != nil {
			return err
		}
		if row.Status != storagereplacement.StatusPreparingTarget && row.Status != storagereplacement.StatusWaiting {
			return fmt.Errorf("activating provider replacement %d from %s: %w", replacementID, row.Status, ErrConflict)
		}
		if row.BucketID != bucketID {
			return fmt.Errorf("activating provider replacement: bucket changed: %w", ErrConflict)
		}
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.StorageDataSet)(nil)).
			Set("is_current = ?", false).
			Set("status = ?", model.StorageDataSetStatusDraining).
			Set("updated_at = ?", now).
			Where("id = ? AND is_current = ?", row.SourceDataSetID, true).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("draining replacement source: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("draining replacement source: %w", ErrConflict)
		}
		res, err = db.NewUpdate().
			Model((*model.StorageDataSet)(nil)).
			Set("is_current = ?", true).
			Set("updated_at = ?", now).
			Where("id = ? AND is_current = ?", row.TargetDataSetID, false).
			Where("status = ?", model.StorageDataSetStatusReady).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("activating replacement target: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("activating replacement target: %w", ErrConflict)
		}
		return transitionReplacement(ctx, db, replacementID,
			[]storagereplacement.Status{row.Status}, storagereplacement.StatusMigrating,
			func(q *bun.UpdateQuery) *bun.UpdateQuery { return q.Set("wait_reason = NULL") }, now)
	})
}

func (r *BunStorageReplacementRepo) MarkMigrating(ctx context.Context, replacementID int64) error {
	return r.transition(ctx, replacementID,
		[]storagereplacement.Status{storagereplacement.StatusWaiting, storagereplacement.StatusMigrating},
		storagereplacement.StatusMigrating,
		func(q *bun.UpdateQuery) *bun.UpdateQuery { return q.Set("wait_reason = NULL") })
}

// MarkWaiting records a recoverable pause. It never consumes retry budget and
// is never reported as a failure.
func (r *BunStorageReplacementRepo) MarkWaiting(ctx context.Context, replacementID int64, reason storagereplacement.WaitReason) error {
	if !reason.Valid() {
		return fmt.Errorf("marking replacement waiting: unknown reason: %w", ErrInvalidInput)
	}
	return r.transition(ctx, replacementID,
		[]storagereplacement.Status{
			storagereplacement.StatusPreparingTarget,
			storagereplacement.StatusMigrating,
			storagereplacement.StatusWaiting,
			storagereplacement.StatusRetiring,
		},
		storagereplacement.StatusWaiting,
		func(q *bun.UpdateQuery) *bun.UpdateQuery { return q.Set("wait_reason = ?", reason) })
}

func (r *BunStorageReplacementRepo) MarkFailed(
	ctx context.Context,
	replacementID int64,
	reason *storagereplacement.FailureReason,
	lastError string,
) error {
	if reason != nil && !reason.Valid() {
		return fmt.Errorf("marking replacement failed: unknown reason: %w", ErrInvalidInput)
	}
	return r.transition(ctx, replacementID,
		[]storagereplacement.Status{
			storagereplacement.StatusPreparingTarget,
			storagereplacement.StatusMigrating,
			storagereplacement.StatusWaiting,
		},
		storagereplacement.StatusFailed,
		func(q *bun.UpdateQuery) *bun.UpdateQuery {
			return q.Set("last_error = ?", lastError).
				Set("wait_reason = NULL").
				Set("failure_reason = ?", reason)
		})
}

func (r *BunStorageReplacementRepo) FailCoordinator(
	ctx context.Context,
	input ReplacementCoordinatorFailureInput,
) error {
	if input.ReplacementID <= 0 || input.Task == nil ||
		(input.FailureReason != nil && !input.FailureReason.Valid()) {
		return fmt.Errorf("failing provider replacement coordinator: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		replacements := &BunStorageReplacementRepo{db: db}
		if err := replacements.MarkFailed(ctx, input.ReplacementID, input.FailureReason, input.LastError); err != nil {
			return err
		}
		if err := (&BunTaskRepo{db: db}).FailRunning(ctx, input.Task, input.LastError); err != nil {
			return fmt.Errorf("stopping provider replacement coordinator: %w", err)
		}
		return nil
	})
}

func (r *BunStorageReplacementRepo) ScheduleCoordinatorRetry(
	ctx context.Context,
	input ReplacementCoordinatorRetryInput,
) (model.TaskStatus, error) {
	if input.ReplacementID <= 0 || input.Task == nil {
		return "", fmt.Errorf("scheduling provider replacement coordinator retry: %w", ErrInvalidInput)
	}
	var taskStatus model.TaskStatus
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		replacement, err := lockReplacementByID(ctx, db, input.ReplacementID)
		if err != nil {
			return err
		}
		taskStatus, err = (&BunTaskRepo{db: db}).ScheduleRetryRunning(ctx, input.Task, input.LastError, input.Backoff)
		if err != nil || taskStatus != model.TaskStatusExhausted {
			return err
		}

		next, changed := storagereplacement.OnTaskExhausted(replacement.Status)
		if !changed {
			return nil
		}
		replacementError := input.LastError + " (max retries reached)"
		return transitionReplacement(ctx, db, replacement.ID, []storagereplacement.Status{replacement.Status}, next,
			func(q *bun.UpdateQuery) *bun.UpdateQuery {
				return q.Set("last_error = ?", replacementError).
					Set("wait_reason = NULL").
					Set("failure_reason = NULL")
			}, time.Now())
	})
	if err != nil {
		return "", err
	}
	return taskStatus, nil
}

// MarkCleanupAttention is committed in the same transaction that stops the
// coordinator task, so automatic retry can never resume suppressed cleanup.
func (r *BunStorageReplacementRepo) MarkCleanupAttention(ctx context.Context, replacementID int64, lastError string) error {
	return r.transition(ctx, replacementID,
		[]storagereplacement.Status{storagereplacement.StatusRetiring, storagereplacement.StatusWaiting},
		storagereplacement.StatusCleanupAttention,
		func(q *bun.UpdateQuery) *bun.UpdateQuery {
			return q.Set("last_error = ?", lastError).Set("wait_reason = NULL")
		})
}

func (r *BunStorageReplacementRepo) BeginRetirement(ctx context.Context, replacementID int64) error {
	return r.transition(ctx, replacementID,
		[]storagereplacement.Status{
			storagereplacement.StatusMigrating,
			storagereplacement.StatusWaiting,
			storagereplacement.StatusCleanupAttention,
		},
		storagereplacement.StatusRetiring,
		func(q *bun.UpdateQuery) *bun.UpdateQuery { return q.Set("wait_reason = NULL") })
}

// RecordTerminationEpoch persists the end of term before the remote service is
// treated as terminated, so a crash in between re-reads it instead of
// terminating a second time.
func (r *BunStorageReplacementRepo) RecordTerminationEpoch(ctx context.Context, input RecordTerminationEpochInput) error {
	if input.ReplacementID <= 0 {
		return fmt.Errorf("recording replacement termination epoch: %w", ErrInvalidInput)
	}
	res, err := r.db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("termination_epoch = ?", input.Epoch).
		Set("termination_tx_hash = ?", nullableString(input.TxHash)).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", input.ReplacementID).
		Where("status IN (?, ?, ?)",
			storagereplacement.StatusRetiring,
			storagereplacement.StatusWaiting,
			storagereplacement.StatusCleanupAttention).
		Where("termination_epoch IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("recording replacement termination epoch: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return fmt.Errorf("recording replacement termination epoch: %w", ErrConflict)
	}
	return nil
}

// RecordAbandonedTerminationEpoch persists termination of a superseded target
// before waiting for its end epoch. A restarted worker observes this value
// instead of paying for another termination transaction.
func (r *BunStorageReplacementRepo) RecordAbandonedTerminationEpoch(
	ctx context.Context,
	input RecordTerminationEpochInput,
) error {
	if input.ReplacementID <= 0 || input.Epoch < 0 {
		return fmt.Errorf("recording abandoned target termination epoch: %w", ErrInvalidInput)
	}
	res, err := r.db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("abandoned_termination_epoch = ?", input.Epoch).
		Set("abandoned_termination_tx_hash = ?", nullableString(input.TxHash)).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", input.ReplacementID).
		Where("status = ?", storagereplacement.StatusSuperseded).
		Where("abandoned_termination_epoch IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("recording abandoned target termination epoch: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	row, getErr := r.GetByID(ctx, input.ReplacementID)
	if getErr != nil {
		return getErr
	}
	if row != nil && row.Status == storagereplacement.StatusSuperseded &&
		row.AbandonedTerminationEpoch != nil && *row.AbandonedTerminationEpoch == input.Epoch {
		return nil
	}
	return fmt.Errorf("recording abandoned target termination epoch: %w", ErrConflict)
}

func (r *BunStorageReplacementRepo) GetByID(ctx context.Context, id int64) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := r.db.NewSelect().Model(row).Where("id = ?", id).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting provider replacement: %w", err)
	}
	return row, nil
}

func (r *BunStorageReplacementRepo) GetByClientRequestID(
	ctx context.Context,
	bucketID int64,
	clientRequestID string,
) (*storagereplacement.Replacement, error) {
	clientRequestID = strings.TrimSpace(clientRequestID)
	if bucketID <= 0 || clientRequestID == "" || len(clientRequestID) > 128 {
		return nil, fmt.Errorf("selecting provider replacement by client request id: %w", ErrInvalidInput)
	}
	return getReplacementByClientRequestID(ctx, r.db, bucketID, clientRequestID)
}

// ListForBucket returns the whole replacement history, newest first, so the API
// can present a stable order.
func (r *BunStorageReplacementRepo) ListForBucket(ctx context.Context, bucketID int64, limit int) ([]storagereplacement.Replacement, error) {
	var rows []storagereplacement.Replacement
	q := r.db.NewSelect().
		Model(&rows).
		Where("bucket_id = ?", bucketID).
		OrderExpr("id DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing bucket provider replacements: %w", err)
	}
	return rows, nil
}

func (r *BunStorageReplacementRepo) GetActiveForDataSet(ctx context.Context, dataSetID int64) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := r.db.NewSelect().
		Model(row).
		// Either generation is owned by the replacement. After activation the
		// coordinator writes the target, so a caller asking about the target has
		// to see the replacement too.
		Where("source_data_set_id = ? OR target_data_set_id = ?", dataSetID, dataSetID).
		Where("status NOT IN (?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting active provider replacement: %w", err)
	}
	return row, nil
}

// HasInProgressForDataSet covers both generations a replacement owns. The
// target matters as much as the source: in-place recovery would otherwise queue
// repair work on the generation the coordinator is actively writing, and the
// two would race over the same copy row. It excludes terminally failed and
// attention states so a generation whose replacement gave up can still recover
// and repair in place.
func (r *BunStorageReplacementRepo) HasInProgressForDataSet(ctx context.Context, dataSetID int64) (bool, error) {
	exists, err := r.db.NewSelect().
		Model((*storagereplacement.Replacement)(nil)).
		Where("source_data_set_id = ? OR target_data_set_id = ?", dataSetID, dataSetID).
		Where("status IN (?, ?, ?, ?)",
			storagereplacement.StatusPreparingTarget,
			storagereplacement.StatusMigrating,
			storagereplacement.StatusWaiting,
			storagereplacement.StatusRetiring).
		Exists(ctx)
	if err != nil {
		return false, fmt.Errorf("checking in-progress provider replacement: %w", err)
	}
	return exists, nil
}

func (r *BunStorageReplacementRepo) ListActive(ctx context.Context, afterID int64, limit int) ([]storagereplacement.Replacement, error) {
	var rows []storagereplacement.Replacement
	if err := r.db.NewSelect().
		Model(&rows).
		Where("id > ?", afterID).
		Where("status IN (?, ?, ?, ?)",
			storagereplacement.StatusPreparingTarget,
			storagereplacement.StatusMigrating,
			storagereplacement.StatusWaiting,
			storagereplacement.StatusRetiring).
		OrderExpr("id ASC").
		Limit(limit).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing active provider replacements: %w", err)
	}
	return rows, nil
}

// A superseded replacement leaves behind a target generation that holds partly
// migrated data and no coverage obligation. It still needs its own retirement
// so the abandoned service does not keep costing money.
func (r *BunStorageReplacementRepo) ListSupersededCleanupCandidates(ctx context.Context, afterID int64, limit int) ([]storagereplacement.Replacement, error) {
	var rows []storagereplacement.Replacement
	if err := r.db.NewSelect().
		Model(&rows).
		Where("id > ?", afterID).
		Where("status = ?", storagereplacement.StatusSuperseded).
		Where(`EXISTS (
			SELECT 1 FROM storage_data_sets AS abandoned_target
			WHERE abandoned_target.id = storage_replacement.target_data_set_id
			  AND abandoned_target.is_current = ?
			  AND abandoned_target.status <> ?
		)`, false, model.StorageDataSetStatusRetired).
		OrderExpr("id ASC").
		Limit(limit).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing superseded replacement cleanup candidates: %w", err)
	}
	return rows, nil
}

func (r *BunStorageReplacementRepo) transition(
	ctx context.Context,
	replacementID int64,
	from []storagereplacement.Status,
	to storagereplacement.Status,
	mutate func(*bun.UpdateQuery) *bun.UpdateQuery,
) error {
	if replacementID <= 0 {
		return fmt.Errorf("updating provider replacement: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		return transitionReplacement(ctx, db, replacementID, from, to, mutate, time.Now())
	})
}

// transitionReplacement refuses any move the state machine does not define, so
// an illegal combination fails loudly instead of corrupting the record.
func transitionReplacement(
	ctx context.Context,
	db bun.IDB,
	replacementID int64,
	from []storagereplacement.Status,
	to storagereplacement.Status,
	mutate func(*bun.UpdateQuery) *bun.UpdateQuery,
	now time.Time,
) error {
	allowed := make([]storagereplacement.Status, 0, len(from))
	for _, candidate := range from {
		if candidate == to || storagereplacement.Allowed(candidate, to) {
			allowed = append(allowed, candidate)
		}
	}
	if len(allowed) == 0 {
		return fmt.Errorf("replacement %d to %s: %w", replacementID, to, storagereplacement.ErrIllegalTransition)
	}
	q := db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("status = ?", to).
		Set("state_version = state_version + 1").
		Set("updated_at = ?", now).
		Where("id = ?", replacementID).
		Where("status IN (?)", bun.List(allowed))
	if mutate != nil {
		q = mutate(q)
	}
	res, err := q.Exec(ctx)
	if err != nil {
		return fmt.Errorf("updating provider replacement: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return nil
	}
	count, countErr := db.NewSelect().
		Model((*storagereplacement.Replacement)(nil)).
		Where("id = ?", replacementID).
		Count(ctx)
	if countErr != nil {
		return fmt.Errorf("updating provider replacement: %w", countErr)
	}
	if count == 0 {
		return fmt.Errorf("provider replacement %d: %w", replacementID, ErrNotFound)
	}
	return fmt.Errorf("updating provider replacement %d to %s: %w", replacementID, to, ErrConflict)
}

func lockReplacementByID(ctx context.Context, db bun.IDB, replacementID int64) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := db.NewRaw(
		`UPDATE storage_replacements SET updated_at = updated_at WHERE id = ? RETURNING *`,
		replacementID,
	).Scan(ctx, row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("provider replacement %d: %w", replacementID, ErrNotFound)
		}
		return nil, fmt.Errorf("locking provider replacement: %w", err)
	}
	return row, nil
}
