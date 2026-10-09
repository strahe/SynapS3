package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
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
		if db.Dialect().Name() == dialect.PG {
			query = query.For("UPDATE")
		}
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
		if local && len(source.CreationRejection) != 0 {
			if err := verifyCreationRejection(source, input.VerifiedCreationRejection); err != nil {
				return err
			}
		}
		if source.ProviderID.Equal(input.TargetProviderID) {
			return fmt.Errorf("authorizing provider replacement: %w", storagereplacement.ErrInvalidTarget)
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
		if err := abandonUnsentReplacementTargets(ctx, db, source.ID, input.Preflight); err != nil {
			return err
		}

		now := time.Now()
		// The single-active-replacement index rejects a second live row for one
		// source, so the predecessor must step down before the successor exists.
		superseded, err := supersedeEarlierReplacements(ctx, db, source.ID, local, now)
		if err != nil {
			return err
		}
		if local {
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

// Earlier targets can be abandoned only after creation stopped without a
// submission, with either no request or persisted provider refusal evidence.
func abandonUnsentReplacementTargets(ctx context.Context, db bun.IDB, sourceDataSetID int64, preflight *ReplacementPreflight) error {
	var ids []int64
	if err := db.NewSelect().Model((*storagereplacement.Replacement)(nil)).Column("target_data_set_id").
		Where("source_data_set_id = ? AND status NOT IN (?, ?)", sourceDataSetID, storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).Scan(ctx, &ids); err != nil {
		return err
	}
	if err := lockReplacementDataSets(ctx, db, ids...); err != nil {
		return err
	}
	targets, err := replaceableEarlierTargets(ctx, db, sourceDataSetID)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if len(target.CreationRejection) != 0 {
			var checked *model.DataSetCreationRejection
			if preflight != nil {
				for _, entry := range preflight.Targets {
					if entry.DataSet.ID == target.ID {
						checked = entry.VerifiedCreationRejection
					}
				}
			}
			if err := verifyCreationRejection(&target, checked); err != nil {
				return err
			}
		}
		if _, err := db.NewUpdate().Model((*model.StorageDataSet)(nil)).
			Set("status = ?", model.StorageDataSetStatusRetired).Set("is_current = ?", false).
			Set("ensure_task_id = NULL").Set("creation_rejection = ?", target.CreationRejection).Set("updated_at = ?", time.Now()).Where("id = ?", target.ID).Exec(ctx); err != nil {
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
func supersedeEarlierReplacements(ctx context.Context, db bun.IDB, sourceDataSetID int64, includeIncoming bool, now time.Time) ([]int64, error) {
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
		Where("task_id IS NULL OR task_id IN (SELECT id FROM tasks WHERE status IN (?, ?, ?) UNION ALL SELECT task_id FROM task_history WHERE status IN (?, ?, ?))",
			model.TaskStatusFailed, model.TaskStatusCancelled, model.TaskStatusCompleted,
			model.TaskStatusFailed, model.TaskStatusCancelled, model.TaskStatusCompleted).
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
			Where("retirement_task_id IN (SELECT id FROM tasks WHERE status = ?)", model.TaskStatusFailed).
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
			if err := uncreatedDataSetEligibility(ctx, db, source); err != nil {
				return err
			}
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
		if reason != nil && *reason == storagereplacement.FailureReasonTargetRejected {
			// Ready binding locks this same row before changing the target. A
			// discarded failure settlement is retried by the engine as recovery.
			row, err := lockReplacementByID(ctx, db, replacementID)
			if err != nil {
				return err
			}
			if row == nil {
				return ErrNotFound
			}
			target, err := (&BunStorageContentRepo{db: db}).GetDataSetBindingByID(ctx, row.TargetDataSetID)
			if err != nil {
				return err
			}
			if target == nil || target.DataSetID != nil ||
				(target.Status != model.StorageDataSetStatusPending && target.Status != model.StorageDataSetStatusCreating) {
				return ErrConflict
			}
			if evidence, err := target.CreationRejectionEvidence(); err != nil || evidence == nil {
				return ErrConflict
			}
		}
		return transitionReplacement(ctx, db, replacementID,
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
			return q.Set("last_error = ?", lastError).Set("wait_reason = NULL")
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
		err := db.NewRaw(
			`UPDATE storage_replacements SET updated_at = updated_at WHERE task_id = ? RETURNING *`,
			taskID,
		).Scan(ctx, row)
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

// RecordTerminationEpoch persists the end of term before the remote service is
// treated as terminated, so a crash in between re-reads it instead of
// terminating a second time.
func (r *BunStorageReplacementRepo) RecordTerminationEpoch(ctx context.Context, input RecordTerminationEpochInput) error {
	if input.ReplacementID <= 0 {
		return fmt.Errorf("recording replacement termination epoch: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := selectReplacementForTermination(ctx, db, input.ReplacementID,
			storagereplacement.StatusRetiring,
			storagereplacement.StatusWaiting,
			storagereplacement.StatusCleanupAttention)
		if err != nil {
			return fmt.Errorf("recording replacement termination epoch: %w", err)
		}
		termination := &storagereplacement.Termination{
			ReplacementID:   row.ID,
			Role:            storagereplacement.TerminationRoleSource,
			SourceDataSetID: &row.SourceDataSetID,
			TxHash:          nullableString(input.TxHash),
			Epoch:           input.Epoch,
		}
		if _, err := db.NewInsert().Model(termination).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("recording replacement termination epoch: %w", ErrConflict)
			}
			return fmt.Errorf("recording replacement termination epoch: %w", err)
		}
		return nil
	})
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
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := selectReplacementForTermination(ctx, db, input.ReplacementID, storagereplacement.StatusSuperseded)
		if err != nil {
			return fmt.Errorf("recording abandoned target termination epoch: %w", err)
		}
		termination := &storagereplacement.Termination{
			ReplacementID:            row.ID,
			Role:                     storagereplacement.TerminationRoleAbandonedTarget,
			AbandonedTargetDataSetID: &row.TargetDataSetID,
			TxHash:                   nullableString(input.TxHash),
			Epoch:                    input.Epoch,
		}
		if _, err := db.NewInsert().Model(termination).Exec(ctx); err == nil {
			return nil
		} else if !isUniqueViolation(err) {
			return fmt.Errorf("recording abandoned target termination epoch: %w", err)
		}
		// A crash after the transaction was paid for replays with the same
		// epoch; anything else is a genuine conflict.
		existing := new(storagereplacement.Termination)
		if err := db.NewSelect().Model(existing).
			Where("replacement_id = ? AND role = ?", row.ID, storagereplacement.TerminationRoleAbandonedTarget).
			Scan(ctx); err != nil {
			return fmt.Errorf("recording abandoned target termination epoch: %w", err)
		}
		if existing.Epoch == input.Epoch {
			return nil
		}
		return fmt.Errorf("recording abandoned target termination epoch: %w", ErrConflict)
	})
}

// selectReplacementForTermination reads the replacement a termination is about
// to be recorded against, refusing any status that has no term to end.
func selectReplacementForTermination(
	ctx context.Context,
	db bun.IDB,
	replacementID int64,
	statuses ...storagereplacement.Status,
) (*storagereplacement.Replacement, error) {
	row := new(storagereplacement.Replacement)
	err := db.NewSelect().
		Model(row).
		Where("id = ?", replacementID).
		Where("status IN (?)", bun.List(statuses)).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

// withReplacementTerminations projects the end of term recorded for each of a
// replacement's two data sets. The rows live in storage_data_set_terminations,
// so a replacement carries no repeated column group of its own.
func withReplacementTerminations(q *bun.SelectQuery) *bun.SelectQuery {
	return q.
		ColumnExpr("storage_replacement.*").
		ColumnExpr("source_termination.tx_hash AS termination_tx_hash").
		ColumnExpr("source_termination.epoch AS termination_epoch").
		ColumnExpr("abandoned_termination.tx_hash AS abandoned_termination_tx_hash").
		ColumnExpr("abandoned_termination.epoch AS abandoned_termination_epoch").
		Join("LEFT JOIN storage_data_set_terminations AS source_termination"+
			" ON source_termination.replacement_id = storage_replacement.id AND source_termination.role = ?",
			storagereplacement.TerminationRoleSource).
		Join("LEFT JOIN storage_data_set_terminations AS abandoned_termination"+
			" ON abandoned_termination.replacement_id = storage_replacement.id AND abandoned_termination.role = ?",
			storagereplacement.TerminationRoleAbandonedTarget)
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
		q = q.Limit(limit)
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
		Limit(limit).
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
	// The lock statement returns the replacement's own columns; the terminations
	// it records live beside it and callers of this lock read them.
	if err := loadReplacementTerminations(ctx, db, row); err != nil {
		return nil, err
	}
	return row, nil
}

// loadReplacementTerminations fills in the termination projection for a
// replacement read without the join.
func loadReplacementTerminations(ctx context.Context, db bun.IDB, row *storagereplacement.Replacement) error {
	var terminations []storagereplacement.Termination
	if err := db.NewSelect().
		Model(&terminations).
		Where("replacement_id = ?", row.ID).
		Scan(ctx); err != nil {
		return fmt.Errorf("loading provider replacement terminations: %w", err)
	}
	for i := range terminations {
		termination := &terminations[i]
		epoch := termination.Epoch
		switch termination.Role {
		case storagereplacement.TerminationRoleSource:
			row.TerminationTxHash, row.TerminationEpoch = termination.TxHash, &epoch
		case storagereplacement.TerminationRoleAbandonedTarget:
			row.AbandonedTerminationTxHash, row.AbandonedTerminationEpoch = termination.TxHash, &epoch
		}
	}
	return nil
}
