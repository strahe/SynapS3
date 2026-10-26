package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

var _ StorageReplacementRepository = (*BunStorageReplacementRepo)(nil)

// BunStorageReplacementRepo persists operator-approved provider replacements.
type BunStorageReplacementRepo struct {
	db bun.IDB
}

// Authorize is the only way a replacement comes into existence. It records
// domain authorization and topology state only; TaskService binds the
// coordinator in the caller's transaction.
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
		source := new(model.StorageDataSet)
		query := db.NewSelect().Model(source).Where("id = ?", input.SourceDataSetID)
		query = query.For("UPDATE")
		if err := query.Scan(ctx); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if source == nil || source.BucketID != input.BucketID {
			return fmt.Errorf("authorizing provider replacement: data set %d: %w", input.SourceDataSetID, ErrNotFound)
		}
		if input.Preflight != nil {
			if input.Preflight.Source.ID != source.ID || input.Preflight.Source.BucketID != bucket.ID {
				return ErrInvalidInput
			}
			if err := validateReplacementPreflight(ctx, db, *input.Preflight); err != nil {
				return err
			}
		}
		local, err := replacementSourceEligibility(ctx, db, source)
		if err != nil {
			return err
		}
		if source.ProviderID.Equal(input.TargetProviderID) {
			return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrInvalidTarget)
		}
		// Unretired generations reserve the provider across all replica slots.
		var total, inUse int
		err = db.NewSelect().
			Model((*model.StorageDataSet)(nil)).
			ColumnExpr("COUNT(*), COUNT(*) FILTER (WHERE status <> ?)", model.StorageDataSetStatusRetired).
			Where("bucket_id = ? AND provider_id = ?", input.BucketID, input.TargetProviderID).
			Scan(ctx, &total, &inUse)
		if err != nil {
			return fmt.Errorf("checking replacement target provider: %w", err)
		}
		if inUse > 0 {
			return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrTargetInUse)
		}
		if input.SelectionMode == storagereplacement.SelectionModeAutomatic && total > 0 {
			return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrNoEligibleProvider)
		}
		if err := abandonUnsentReplacementTargets(ctx, db, source.ID); err != nil {
			return err
		}

		now := time.Now()
		// The single-active-replacement index rejects a second live row for one
		// source, so the predecessor must step down before the successor exists.
		superseded, err := supersedeEarlierReplacements(ctx, db, source.ID, local, source.Status != model.StorageDataSetStatusReady, now)
		if err != nil {
			return err
		}
		if source.Status != model.StorageDataSetStatusReady {
			result, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
				Set("ensure_task_id = NULL").Set("creation_rejection = ?", source.CreationRejection).
				Set("updated_at = ?", now).Where("id = ? AND is_current = ?", source.ID, true).Exec(ctx)
			if err := requireTaskFenceRows(result, err, "revoking replacement source creation"); err != nil {
				return err
			}
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
			IsCurrent:  false,
			Status:     model.StorageDataSetStatusPending,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if _, err := db.NewInsert().Model(target).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrTargetInUse)
			}
			return fmt.Errorf("creating replacement target data set: %w", err)
		}

		replacement := &storagereplacement.Replacement{
			BucketID:             input.BucketID,
			CopyIndex:            source.CopyIndex,
			SourceDataSetID:      source.ID,
			TargetDataSetID:      target.ID,
			SelectionMode:        input.SelectionMode,
			PriceListFingerprint: input.PriceListFingerprint,
			ClientRequestID:      input.ClientRequestID,
			Status:               storagereplacement.StatusPreparingTarget,
			TaskGeneration:       1,
			CreatedAt:            now,
			UpdatedAt:            now,
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
		result = replacement
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return result, created, nil
}

func abandonUnsentReplacementTargets(ctx context.Context, db bun.IDB, sourceDataSetID int64) error {
	var ids []int64
	if err := db.NewSelect().Model((*storagereplacement.Replacement)(nil)).Column("target_data_set_id").
		Where("source_data_set_id = ? AND status NOT IN (?, ?)", sourceDataSetID, storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).Scan(ctx, &ids); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if err := lockReplacementDataSets(ctx, db, ids...); err != nil {
		return err
	}
	targets, err := replaceableEarlierTargets(ctx, db, sourceDataSetID)
	if err != nil {
		return err
	}
	for _, target := range targets {
		status := model.StorageDataSetStatusRetired
		if target.DataSetID != nil {
			status = model.StorageDataSetStatusDraining
		}
		if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
			Set("status = ?", status).Set("is_current = ?", false).Set("ensure_task_id = NULL").
			Set("creation_rejection = ?", target.CreationRejection).Set("updated_at = ?", time.Now()).Where("id = ?", target.ID).Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

func replacementRequestMatches(row *storagereplacement.Replacement, input AuthorizeReplacementInput) bool {
	if row.BucketID != input.BucketID || row.SourceDataSetID != input.SourceDataSetID || row.SelectionMode != input.SelectionMode || row.PriceListFingerprint != input.PriceListFingerprint {
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
	err := withReplacementTerminations(db.NewSelect().Model(row)).
		Where("storage_replacement.bucket_id = ? AND storage_replacement.client_request_id = ?", bucketID, clientRequestID).
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
func supersedeEarlierReplacements(ctx context.Context, db bun.IDB, sourceDataSetID int64, includeIncoming, initializing bool, now time.Time) ([]int64, error) {
	var ids []int64
	if err := db.NewSelect().
		Model((*storagereplacement.Replacement)(nil)).
		Column("id").
		Where("(source_data_set_id = ? OR (? AND target_data_set_id = ?))", sourceDataSetID, includeIncoming, sourceDataSetID).
		Where("status NOT IN (?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
		Scan(ctx, &ids); err != nil {
		return nil, fmt.Errorf("selecting earlier provider replacements: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Stopped coordinators cannot observe supersession. Fence their old claim
	// and reserve a fresh task identity for the caller to enqueue atomically.
	if _, err := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
		Set("task_generation = task_generation + 1").Set("task_id = NULL").
		Where("id IN (?)", bun.List(ids)).
		Where("? OR task_id IS NULL OR task_id IN (SELECT id FROM tasks WHERE status IN (?, ?, ?)) OR target_data_set_id IN (SELECT id FROM storage_data_sets WHERE status <> ?)",
			initializing, model.TaskStatusFailed, model.TaskStatusCancelled, model.TaskStatusCompleted, model.StorageDataSetStatusReady).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("fencing stopped superseded coordinator: %w", err)
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
	if input.ReplacementID <= 0 {
		return nil, fmt.Errorf("retrying provider replacement: %w", ErrInvalidInput)
	}
	var resumed *storagereplacement.Replacement
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := lockReplacementByID(ctx, db, input.ReplacementID)
		if err != nil {
			return err
		}
		if err := replacementRetryEligibility(ctx, db, row); err != nil {
			return err
		}
		target, err := (&BunStorageContentRepo{db: db}).GetDataSetBindingByID(ctx, row.TargetDataSetID)
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
		case target.IsCurrent && target.Status == model.StorageDataSetStatusReady && target.DataSetID != nil && !target.DataSetID.IsZero():
			next = storagereplacement.StatusMigrating
		}
		now := time.Now()
		if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
			Set("retirement_task_id = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", row.SourceDataSetID).
			Where("retirement_task_id IN (SELECT id FROM live_tasks WHERE status = ?)", model.TaskStatusFailed).
			Exec(ctx); err != nil {
			return fmt.Errorf("releasing failed retirement task: %w", err)
		}
		if _, err := db.NewUpdate().Model((*storagereplacement.Item)(nil)).
			Set("status = ?", storagereplacement.ItemStatusPending).
			Set("last_error = NULL").
			Set("updated_at = ?", now).
			Where("replacement_id = ? AND status = ?", row.ID, storagereplacement.ItemStatusAttention).
			Exec(ctx); err != nil {
			return fmt.Errorf("resetting replacement items needing attention: %w", err)
		}
		if err := transitionReplacement(ctx, db, row.ID, []storagereplacement.Status{row.Status}, next, func(q *bun.UpdateQuery) *bun.UpdateQuery {
			return q.Set("last_error = NULL").Set("wait_reason = NULL").Set("failure_reason = NULL").
				Set("task_generation = task_generation + 1").Set("task_id = NULL")
		}, now); err != nil {
			return err
		}
		row.Status = next
		row.LastError = nil
		row.WaitReason = nil
		row.FailureReason = nil
		row.TaskGeneration++
		row.TaskID = nil
		row.UpdatedAt = now
		resumed = row
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resumed, nil
}

func (r *BunStorageReplacementRepo) BindTask(ctx context.Context, replacementID, generation, taskID int64) error {
	if replacementID < 1 || generation < 1 || taskID < 1 {
		return ErrInvalidInput
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		task, err := taskForBinding(ctx, db, taskID, "storage_replacement", strconv.FormatInt(replacementID, 10), model.TaskTypeProviderReplacementCoordinate)
		if err != nil {
			return err
		}
		input, err := storagereplacement.ParseCoordinateInput(task)
		if err != nil || input.ReplacementID != replacementID || input.Generation != generation {
			return ErrConflict
		}

		result, err := db.NewUpdate().
			Model((*storagereplacement.Replacement)(nil)).
			Set("task_id = ?", taskID).
			Set("updated_at = ?", time.Now()).
			Where("id = ? AND task_generation = ? AND task_id IS NULL", replacementID, generation).
			Exec(ctx)
		return requireTaskFenceRows(result, err, "binding provider replacement task")
	})
}

func (r *BunStorageReplacementRepo) AuthorizeTask(ctx context.Context, replacementID, generation, taskID int64) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := withReplacementTerminations(r.db.NewSelect().Model(row)).
		Where("storage_replacement.id = ? AND storage_replacement.task_generation = ? AND storage_replacement.task_id = ?", replacementID, generation, taskID).
		Scan(ctx)
	if err == sql.ErrNoRows {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, fmt.Errorf("authorizing provider replacement task: %w", err)
	}
	return row, nil
}

func (r *BunStorageReplacementRepo) CompleteTask(ctx context.Context, replacementID, generation, taskID int64) error {
	result, err := r.db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("task_id = NULL").
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND task_generation = ? AND task_id = ?", replacementID, generation, taskID).
		Exec(ctx)
	return requireTaskFenceRows(result, err, "completing provider replacement task")
}

// Activate is the single atomic switch: the target starts receiving writes and
// the source retires locally or starts draining its remote service.
func (r *BunStorageReplacementRepo) Activate(ctx context.Context, replacementID, generation, taskID int64) error {
	if replacementID <= 0 || generation <= 0 || taskID <= 0 {
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
		if row.TaskGeneration != generation || row.TaskID == nil || *row.TaskID != taskID {
			return ErrConflict
		}
		if err := lockReplacementDataSets(ctx, db, row.SourceDataSetID, row.TargetDataSetID); err != nil {
			return err
		}
		contents := &BunStorageContentRepo{db: db}
		source, err := contents.GetDataSetBindingByID(ctx, row.SourceDataSetID)
		if err != nil {
			return err
		}
		target, err := contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
		if err != nil {
			return err
		}
		if source == nil || target == nil || !source.IsCurrent || target.IsCurrent ||
			target.Status != model.StorageDataSetStatusReady || target.DataSetID == nil || target.DataSetID.IsZero() ||
			target.BucketID != source.BucketID || target.CopyIndex != source.CopyIndex || target.Generation <= source.Generation {
			return ErrConflict
		}
		sourceStatus := model.StorageDataSetStatusDraining
		if source.DataSetID == nil {
			if source.EnsureTaskID != nil {
				return ErrConflict
			}
			sourceStatus = model.StorageDataSetStatusRetired
		}
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.StorageDataSet)(nil)).
			Set("is_current = ?", false).
			Set("status = ?", sourceStatus).
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
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := lockReplacementByID(ctx, db, replacementID)
		if err != nil {
			return err
		}
		ready, err := db.NewSelect().Model((*model.StorageDataSet)(nil)).Where("id = ?", row.TargetDataSetID).
			Where("is_current = ? AND status = ? AND data_set_id IS NOT NULL AND data_set_id <> ''", true, model.StorageDataSetStatusReady).Exists(ctx)
		if err != nil {
			return err
		}
		if !ready {
			return ErrConflict
		}
		return transitionReplacement(ctx, db, row.ID,
			[]storagereplacement.Status{storagereplacement.StatusPreparingTarget, storagereplacement.StatusWaiting, storagereplacement.StatusMigrating},
			storagereplacement.StatusMigrating, func(q *bun.UpdateQuery) *bun.UpdateQuery { return q.Set("wait_reason = NULL") }, time.Now())
	})
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
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		return transitionReplacement(ctx, db, replacementID,
			[]storagereplacement.Status{
				storagereplacement.StatusPreparingTarget,
				storagereplacement.StatusMigrating,
				storagereplacement.StatusWaiting,
			},
			storagereplacement.StatusFailed,
			func(q *bun.UpdateQuery) *bun.UpdateQuery {
				return q.Set("last_error = ?", errtext.Redact(lastError)).
					Set("wait_reason = NULL").
					Set("failure_reason = ?", reason)
			}, time.Now())
	})
}

// MarkCleanupAttention is committed in the same transaction that stops the
// coordinator task, so automatic retry can never resume suppressed cleanup.
// Marking a replacement that already waits for attention only refreshes its
// error, so a retried task that stops again for the same reason can still
// settle.
func (r *BunStorageReplacementRepo) MarkCleanupAttention(ctx context.Context, replacementID int64, lastError string) error {
	return r.transition(ctx, replacementID,
		[]storagereplacement.Status{storagereplacement.StatusRetiring, storagereplacement.StatusWaiting, storagereplacement.StatusCleanupAttention},
		storagereplacement.StatusCleanupAttention,
		func(q *bun.UpdateQuery) *bun.UpdateQuery {
			return q.Set("last_error = ?", errtext.Redact(lastError)).Set("wait_reason = NULL")
		})
}

// FailForEngineTask records on the replacement that the Engine failed its
// current coordinator claim itself. A replacement resumes only through the Data
// Sets retry, which accepts a failed or cleanup-attention replacement, so the
// failure lands there. A coordinator that no longer owns the replacement
// changes nothing.
func (r *BunStorageReplacementRepo) FailForEngineTask(ctx context.Context, taskID int64, lastError string) error {
	if taskID <= 0 || lastError == "" {
		return fmt.Errorf("failing provider replacement for its task: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row := new(storagereplacement.Replacement)
		err := db.NewSelect().Model(row).Where("task_id = ?", taskID).For("NO KEY UPDATE").Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("locking provider replacement for its task: %w", err)
		}
		replacements := &BunStorageReplacementRepo{db: db}
		switch row.Status {
		case storagereplacement.StatusRetiring:
			return replacements.MarkCleanupAttention(ctx, row.ID, lastError)
		case storagereplacement.StatusPreparingTarget, storagereplacement.StatusMigrating, storagereplacement.StatusWaiting:
			return replacements.MarkFailed(ctx, row.ID, nil, lastError)
		default:
			return nil
		}
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

// BeginTerminationAttempt admits one request for the current retirement owner.
// A new attempt requires a reverted predecessor; repeating its admission is inert.
// A released attempt is admitted again in place, by its task or a successor, and
// so is an unresolved one by a successor when ResumeUnresolved.
func (r *BunStorageReplacementRepo) BeginTerminationAttempt(ctx context.Context, input BeginTerminationAttemptInput) error {
	if input.AttemptNo <= 0 || input.PreviousAttemptNo < 0 || input.AttemptedAt.IsZero() {
		return fmt.Errorf("admitting termination attempt: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		termination, err := authorizeTerminationEvidence(ctx, db, RecordTerminationEvidenceInput{
			ReplacementID: input.ReplacementID, DataSetID: input.DataSetID, Generation: input.Generation,
			TaskID: input.TaskID, Role: input.Role,
		})
		if err != nil {
			return err
		}
		if termination.Epoch != nil {
			return ErrConflict
		}
		previous, err := latestTerminationAttempt(ctx, db, termination.ID)
		if err != nil {
			return err
		}
		if previous != nil && previous.AttemptNo == input.AttemptNo {
			owned := previous.TaskID != nil && *previous.TaskID == input.TaskID
			unresolved := input.ResumeUnresolved && !owned && previous.TxHash == nil && previous.ConfirmedTxHash == nil && previous.ReceiptStatus == nil
			if previous.UnsentAt != nil || unresolved {
				_, err := db.NewUpdate().Model((*storagereplacement.TerminationAttempt)(nil)).
					Set("task_id = ?", input.TaskID).Set("attempted_at = ?", input.AttemptedAt).
					Set("unsent_at = NULL").Set("updated_at = ?", time.Now().UTC()).Where("id = ?", previous.ID).Exec(ctx)
				return err
			}
			if !owned {
				return ErrConflict
			}
			return nil
		}
		if previous == nil {
			if input.AttemptNo != 1 || input.PreviousAttemptNo != 0 {
				return ErrConflict
			}
		} else if input.PreviousAttemptNo != previous.AttemptNo || input.AttemptNo != previous.AttemptNo+1 ||
			previous.ReceiptStatus == nil || *previous.ReceiptStatus != 0 ||
			(previous.TaskID != nil && *previous.TaskID == input.TaskID) {
			return ErrConflict
		}
		attempt := &storagereplacement.TerminationAttempt{
			TerminationID: termination.ID, AttemptNo: input.AttemptNo,
			TaskID: &input.TaskID, AttemptedAt: &input.AttemptedAt,
		}
		_, err = db.NewInsert().Model(attempt).Exec(ctx)
		return err
	})
}

// RecordTerminationEvidence preserves each attempt's immutable transaction pair
// and receipt outcome. The service epoch is independent of any failed attempt.
func (r *BunStorageReplacementRepo) RecordTerminationEvidence(ctx context.Context, input RecordTerminationEvidenceInput) error {
	if input.AttemptNo < 0 || (input.AttemptNo == 0 && input.TxHash == "" && input.ConfirmedTxHash == "" && input.ReceiptStatus == nil && input.Epoch == nil) ||
		(input.Epoch != nil && *input.Epoch < 0) || (input.ReceiptStatus != nil && *input.ReceiptStatus > 1) {
		return fmt.Errorf("recording termination evidence: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		termination, err := authorizeTerminationEvidence(ctx, db, input)
		if err != nil {
			return err
		}
		if termination.Epoch != nil && input.Epoch != nil && *termination.Epoch != *input.Epoch {
			return fmt.Errorf("termination epoch conflicts with the recorded result: %w", ErrConflict)
		}
		attempt, err := latestTerminationAttempt(ctx, db, termination.ID)
		if err != nil {
			return err
		}
		hasTransaction := input.TxHash != "" || input.ConfirmedTxHash != "" || input.ReceiptStatus != nil
		if input.AttemptNo > 0 && (attempt == nil || input.AttemptNo != attempt.AttemptNo) {
			return ErrConflict
		}
		if hasTransaction {
			if attempt == nil {
				// An observed transaction without admission metadata is retained as
				// evidence without attributing its submission to the observing task.
				attempt = &storagereplacement.TerminationAttempt{TerminationID: termination.ID, AttemptNo: 1}
			}
			if err := mergeTerminationAttempt(ctx, db, attempt, input); err != nil {
				return err
			}
		}
		if input.Epoch == nil || termination.Epoch != nil {
			return nil
		}
		_, err = db.NewUpdate().Model((*storagereplacement.Termination)(nil)).
			Set("epoch = ?", input.Epoch).Set("updated_at = ?", time.Now().UTC()).Where("id = ?", termination.ID).Exec(ctx)
		return err
	})
}

// RecordTerminationUnsent releases the latest admission after the current
// retirement owner proved that no transaction from it can land. Releasing again
// is inert; an attempt with any transaction evidence cannot be released.
func (r *BunStorageReplacementRepo) RecordTerminationUnsent(ctx context.Context, input RecordTerminationEvidenceInput) error {
	if input.AttemptNo <= 0 || input.TxHash != "" || input.ConfirmedTxHash != "" || input.ReceiptStatus != nil || input.Epoch != nil {
		return fmt.Errorf("releasing termination attempt: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		termination, err := authorizeTerminationEvidence(ctx, db, input)
		if err != nil {
			return err
		}
		attempt, err := latestTerminationAttempt(ctx, db, termination.ID)
		if err != nil {
			return err
		}
		if termination.Epoch != nil || attempt == nil || attempt.AttemptNo != input.AttemptNo || attempt.TaskID == nil ||
			attempt.TxHash != nil || attempt.ConfirmedTxHash != nil || attempt.ReceiptStatus != nil {
			return ErrConflict
		}
		if attempt.UnsentAt != nil {
			return nil
		}
		now := time.Now().UTC()
		_, err = db.NewUpdate().Model((*storagereplacement.TerminationAttempt)(nil)).
			Set("unsent_at = ?", now).Set("updated_at = ?", now).Where("id = ?", attempt.ID).Exec(ctx)
		return err
	})
}

func authorizeTerminationEvidence(ctx context.Context, db bun.IDB, input RecordTerminationEvidenceInput) (*storagereplacement.Termination, error) {
	if input.ReplacementID <= 0 || input.DataSetID <= 0 || input.Generation <= 0 || input.TaskID <= 0 {
		return nil, fmt.Errorf("authorizing termination evidence: %w", ErrInvalidInput)
	}
	row, err := lockReplacementByID(ctx, db, input.ReplacementID)
	if err != nil {
		return nil, err
	}
	var sourceID, abandonedID *int64
	switch input.Role {
	case storagereplacement.TerminationRoleSource:
		if input.DataSetID != row.SourceDataSetID || (row.Status != storagereplacement.StatusRetiring && row.Status != storagereplacement.StatusWaiting && row.Status != storagereplacement.StatusCleanupAttention) {
			return nil, ErrConflict
		}
		sourceID = &row.SourceDataSetID
	case storagereplacement.TerminationRoleAbandonedTarget:
		if input.DataSetID != row.TargetDataSetID || row.Status != storagereplacement.StatusSuperseded {
			return nil, ErrConflict
		}
		abandonedID = &row.TargetDataSetID
	default:
		return nil, ErrInvalidInput
	}
	if err := lockReplacementDataSets(ctx, db, input.DataSetID); err != nil {
		return nil, err
	}
	if _, err := (&BunStorageContentRepo{db: db}).AuthorizeDataSetRetirementTask(ctx, input.DataSetID, input.Generation, input.TaskID); err != nil {
		return nil, err
	}
	termination := new(storagereplacement.Termination)
	err = db.NewSelect().Model(termination).Where("replacement_id = ? AND role = ?", input.ReplacementID, input.Role).For("NO KEY UPDATE").Scan(ctx)
	if err == nil {
		return termination, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	termination.ReplacementID, termination.Role = input.ReplacementID, input.Role
	termination.SourceDataSetID, termination.AbandonedTargetDataSetID = sourceID, abandonedID
	_, err = db.NewInsert().Model(termination).Returning("id").Exec(ctx)
	return termination, err
}

func latestTerminationAttempt(ctx context.Context, db bun.IDB, terminationID int64) (*storagereplacement.TerminationAttempt, error) {
	attempt := new(storagereplacement.TerminationAttempt)
	err := db.NewSelect().Model(attempt).ColumnExpr("storage_data_set_termination_attempt.*").
		Where("termination_id = ?", terminationID).OrderExpr("attempt_no DESC").Limit(1).For("NO KEY UPDATE").Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return attempt, err
}

func mergeTerminationAttempt(ctx context.Context, db bun.IDB, attempt *storagereplacement.TerminationAttempt, input RecordTerminationEvidenceInput) error {
	if (attempt.TxHash != nil && input.TxHash != "" && *attempt.TxHash != input.TxHash) ||
		(attempt.ConfirmedTxHash != nil && input.ConfirmedTxHash != "" && *attempt.ConfirmedTxHash != input.ConfirmedTxHash) ||
		(attempt.ReceiptStatus != nil && input.ReceiptStatus != nil && *attempt.ReceiptStatus != *input.ReceiptStatus) {
		return fmt.Errorf("termination attempt evidence conflicts with the recorded result: %w", ErrConflict)
	}
	var hashes []string
	if input.TxHash != "" {
		hashes = append(hashes, input.TxHash)
	}
	if input.ConfirmedTxHash != "" {
		hashes = append(hashes, input.ConfirmedTxHash)
	}
	if len(hashes) > 0 {
		reused, err := db.NewSelect().Model((*storagereplacement.TerminationAttempt)(nil)).
			Where("termination_id = ? AND id <> ?", attempt.TerminationID, attempt.ID).
			Where("(tx_hash IN (?) OR confirmed_tx_hash IN (?))", bun.List(hashes), bun.List(hashes)).Exists(ctx)
		if err != nil {
			return err
		}
		if reused {
			return fmt.Errorf("termination transaction belongs to another attempt: %w", ErrConflict)
		}
	}
	changed := false
	if attempt.TxHash == nil && input.TxHash != "" {
		attempt.TxHash, changed = &input.TxHash, true
	}
	if attempt.ConfirmedTxHash == nil && input.ConfirmedTxHash != "" {
		attempt.ConfirmedTxHash, changed = &input.ConfirmedTxHash, true
	}
	if input.ReceiptStatus != nil && attempt.ConfirmedTxHash == nil {
		return fmt.Errorf("termination receipt has no confirmed transaction hash: %w", ErrInvalidInput)
	}
	if attempt.ReceiptStatus == nil && input.ReceiptStatus != nil {
		attempt.ReceiptStatus, changed = input.ReceiptStatus, true
	}
	if attempt.ID == 0 {
		_, err := db.NewInsert().Model(attempt).Exec(ctx)
		return err
	}
	if !changed {
		return nil
	}
	_, err := db.NewUpdate().Model((*storagereplacement.TerminationAttempt)(nil)).
		Set("tx_hash = ?", attempt.TxHash).Set("confirmed_tx_hash = ?", attempt.ConfirmedTxHash).
		Set("receipt_status = ?", attempt.ReceiptStatus).Set("unsent_at = NULL").Set("updated_at = ?", time.Now().UTC()).Where("id = ?", attempt.ID).Exec(ctx)
	return err
}

func (r *BunStorageReplacementRepo) ListTerminationAttempts(ctx context.Context, replacementIDs []int64) ([]storagereplacement.TerminationAttempt, error) {
	attempts := make([]storagereplacement.TerminationAttempt, 0)
	if len(replacementIDs) == 0 {
		return attempts, nil
	}
	err := r.db.NewSelect().Model(&attempts).ColumnExpr("storage_data_set_termination_attempt.*").
		ColumnExpr("termination.replacement_id").ColumnExpr("termination.role").
		Join("JOIN storage_data_set_terminations AS termination ON termination.id = storage_data_set_termination_attempt.termination_id").
		Where("termination.replacement_id IN (?)", bun.List(replacementIDs)).
		OrderExpr("termination.replacement_id, termination.role, storage_data_set_termination_attempt.attempt_no").Scan(ctx)
	return attempts, err
}

// withReplacementTerminations projects the service epoch and latest attempt.
func withReplacementTerminations(q *bun.SelectQuery) *bun.SelectQuery {
	return q.
		ColumnExpr("storage_replacement.*").
		ColumnExpr("COALESCE(source_attempt.attempt_no, 0) AS termination_attempt_no").
		ColumnExpr("source_attempt.tx_hash AS termination_tx_hash").
		ColumnExpr("source_attempt.confirmed_tx_hash AS termination_confirmed_tx_hash").
		ColumnExpr("source_attempt.receipt_status AS termination_receipt_status").
		ColumnExpr("source_termination.epoch AS termination_epoch").
		ColumnExpr("COALESCE(abandoned_attempt.attempt_no, 0) AS abandoned_termination_attempt_no").
		ColumnExpr("abandoned_attempt.tx_hash AS abandoned_termination_tx_hash").
		ColumnExpr("abandoned_attempt.confirmed_tx_hash AS abandoned_termination_confirmed_tx_hash").
		ColumnExpr("abandoned_attempt.receipt_status AS abandoned_termination_receipt_status").
		ColumnExpr("abandoned_termination.epoch AS abandoned_termination_epoch").
		Join("LEFT JOIN storage_data_set_terminations AS source_termination"+
			" ON source_termination.replacement_id = storage_replacement.id AND source_termination.role = ?",
			storagereplacement.TerminationRoleSource).
		Join("LEFT JOIN LATERAL (SELECT attempt_no, tx_hash, confirmed_tx_hash, receipt_status"+
			" FROM storage_data_set_termination_attempts WHERE termination_id = source_termination.id"+
			" ORDER BY attempt_no DESC LIMIT 1) AS source_attempt ON TRUE").
		Join("LEFT JOIN storage_data_set_terminations AS abandoned_termination"+
			" ON abandoned_termination.replacement_id = storage_replacement.id AND abandoned_termination.role = ?",
			storagereplacement.TerminationRoleAbandonedTarget).
		Join("LEFT JOIN LATERAL (SELECT attempt_no, tx_hash, confirmed_tx_hash, receipt_status" +
			" FROM storage_data_set_termination_attempts WHERE termination_id = abandoned_termination.id" +
			" ORDER BY attempt_no DESC LIMIT 1) AS abandoned_attempt ON TRUE")
}

func (r *BunStorageReplacementRepo) GetByID(ctx context.Context, id int64) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := withReplacementTerminations(r.db.NewSelect().Model(row)).Where("storage_replacement.id = ?", id).Scan(ctx)
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
	q := withReplacementTerminations(r.db.NewSelect().Model(&rows)).
		Where("storage_replacement.bucket_id = ?", bucketID).
		OrderExpr("storage_replacement.id DESC")
	if limit > 0 {
		q = q.Limit(int64(limit))
	}
	if err := q.Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing bucket provider replacements: %w", err)
	}
	return rows, nil
}

func (r *BunStorageReplacementRepo) ListUnscheduledSuperseded(ctx context.Context, successorID int64) ([]storagereplacement.Replacement, error) {
	var rows []storagereplacement.Replacement
	if err := r.db.NewSelect().Model(&rows).
		Where("superseded_by_id = ?", successorID).
		Where("status = ? AND task_id IS NULL", storagereplacement.StatusSuperseded).
		OrderExpr("id ASC").Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing superseded coordinators to schedule: %w", err)
	}
	return rows, nil
}

func (r *BunStorageReplacementRepo) GetActiveForDataSet(ctx context.Context, dataSetID int64) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := withReplacementTerminations(r.db.NewSelect().Model(row)).
		// Either generation is owned by the replacement. After activation the
		// coordinator writes the target, so a caller asking about the target has
		// to see the replacement too.
		Where("storage_replacement.source_data_set_id = ? OR storage_replacement.target_data_set_id = ?", dataSetID, dataSetID).
		Where("storage_replacement.status NOT IN (?, ?)", storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
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
// two would race over the same copy row. It excludes operator-paused failed and
// attention states so that generation can still recover and repair in place;
// the replacement nevertheless retains its source, target, and slot identity.
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
	if err := withReplacementTerminations(r.db.NewSelect().Model(&rows)).
		Where("storage_replacement.id > ?", afterID).
		Where("storage_replacement.status IN (?, ?, ?, ?)",
			storagereplacement.StatusPreparingTarget,
			storagereplacement.StatusMigrating,
			storagereplacement.StatusWaiting,
			storagereplacement.StatusRetiring).
		OrderExpr("storage_replacement.id ASC").
		Limit(int64(limit)).
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing active provider replacements: %w", err)
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
	err := db.NewSelect().Model(row).Where("id = ?", replacementID).For("NO KEY UPDATE").Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("provider replacement %d: %w", replacementID, ErrNotFound)
		}
		return nil, fmt.Errorf("locking provider replacement: %w", err)
	}
	// The terminations the replacement records live beside it and callers of
	// this lock read them.
	if err := loadReplacementTerminations(ctx, db, row); err != nil {
		return nil, err
	}
	return row, nil
}

// loadReplacementTerminations fills the same projection for locked replacements.
func loadReplacementTerminations(ctx context.Context, db bun.IDB, row *storagereplacement.Replacement) error {
	var terminations []struct {
		Role            storagereplacement.TerminationRole
		Epoch           *int64
		AttemptNo       int64
		TxHash          *string
		ConfirmedTxHash *string
		ReceiptStatus   *uint64
	}
	if err := db.NewSelect().TableExpr("storage_data_set_terminations AS termination").
		ColumnExpr("termination.role, termination.epoch, COALESCE(attempt.attempt_no, 0) AS attempt_no").
		ColumnExpr("attempt.tx_hash, attempt.confirmed_tx_hash, attempt.receipt_status").
		Join("LEFT JOIN LATERAL (SELECT attempt_no, tx_hash, confirmed_tx_hash, receipt_status"+
			" FROM storage_data_set_termination_attempts WHERE termination_id = termination.id"+
			" ORDER BY attempt_no DESC LIMIT 1) AS attempt ON TRUE").
		Where("termination.replacement_id = ?", row.ID).Scan(ctx, &terminations); err != nil {
		return fmt.Errorf("loading provider replacement terminations: %w", err)
	}
	for _, termination := range terminations {
		switch termination.Role {
		case storagereplacement.TerminationRoleSource:
			row.TerminationAttemptNo, row.TerminationReceiptStatus = termination.AttemptNo, termination.ReceiptStatus
			row.TerminationTxHash, row.TerminationConfirmedTxHash, row.TerminationEpoch = termination.TxHash, termination.ConfirmedTxHash, termination.Epoch
		case storagereplacement.TerminationRoleAbandonedTarget:
			row.AbandonedTerminationAttemptNo, row.AbandonedTerminationReceiptStatus = termination.AttemptNo, termination.ReceiptStatus
			row.AbandonedTerminationTxHash, row.AbandonedTerminationConfirmedTxHash, row.AbandonedTerminationEpoch = termination.TxHash, termination.ConfirmedTxHash, termination.Epoch
		}
	}
	return nil
}
