package repository

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

// Blocker names are stable so the API and UI can explain a held source without
// inventing their own vocabulary.
const (
	retirementBlockerCoverage      = "coverage"
	retirementBlockerSourceWrites  = "source_writes"
	retirementBlockerAttempts      = "confirmation_attempts"
	retirementBlockerWaitingItems  = "waiting_items"
	retirementBlockerSlotOwnership = "slot_ownership"
	retirementBlockerEpoch         = "termination_epoch"
)

// EvaluateRetirementGate answers one question: can this source stop existing
// without losing anything? Pass a nil observedEpoch to evaluate everything
// except the epoch, which must be read outside a transaction.
func (r *BunStorageReplacementRepo) EvaluateRetirementGate(ctx context.Context, replacementID int64, observedEpoch *int64) (RetirementGate, error) {
	return evaluateRetirementGate(ctx, r.db, replacementID, observedEpoch)
}

func evaluateRetirementGate(ctx context.Context, db bun.IDB, replacementID int64, observedEpoch *int64) (RetirementGate, error) {
	gate := RetirementGate{}
	row := new(storagereplacement.Replacement)
	if err := withReplacementTerminations(db.NewSelect().Model(row)).
		Where("storage_replacement.id = ?", replacementID).
		Scan(ctx); err != nil {
		return gate, fmt.Errorf("loading provider replacement for retirement: %w", err)
	}
	gate.TerminationEpoch = row.TerminationEpoch

	uploads := &BunStorageContentRepo{db: db}
	source, err := uploads.GetDataSetBindingByID(ctx, row.SourceDataSetID)
	if err != nil {
		return gate, err
	}
	target, err := uploads.GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil {
		return gate, err
	}
	if source == nil || target == nil {
		return gate, fmt.Errorf("loading retirement data sets: %w", ErrNotFound)
	}

	// G1: every still-accessible version stored on the source must be readable
	// on the generation that replaced it.
	gaps, err := countRetirementCoverageGaps(ctx, db, source.ID, target.ID)
	if err != nil {
		return gate, err
	}
	gate.CoverageGaps = gaps
	if gaps > 0 {
		gate.Blockers = append(gate.Blockers, retirementBlockerCoverage)
	}

	// G2: nothing may still be writing to the source.
	writes, err := countRetirementSourceWrites(ctx, db, row.BucketID, source.ID)
	if err != nil {
		return gate, err
	}
	gate.SourceWrites = writes
	if writes > 0 {
		gate.Blockers = append(gate.Blockers, retirementBlockerSourceWrites)
	}
	attempts, err := countReplacementActiveCommitAttempts(ctx, db, row.ID)
	if err != nil {
		return gate, err
	}
	gate.ActiveAttempts = attempts
	if attempts > 0 {
		gate.Blockers = append(gate.Blockers, retirementBlockerAttempts)
	}

	// G3: no migration item may still be owed.
	waiting, err := db.NewSelect().
		Model((*storagereplacement.Item)(nil)).
		Where("replacement_id = ?", row.ID).
		Where("status IN (?, ?)",
			storagereplacement.ItemStatusPending,
			storagereplacement.ItemStatusAttention).
		Count(ctx)
	if err != nil {
		return gate, fmt.Errorf("counting outstanding replacement items: %w", err)
	}
	gate.WaitingItems = waiting
	if waiting > 0 {
		gate.Blockers = append(gate.Blockers, retirementBlockerWaitingItems)
	}

	// G4: the slot must have genuinely moved on. This one is structural: if it
	// fails, something reordered the generations and retrying will not help.
	gate.SlotOwned = !source.IsCurrent &&
		target.IsCurrent &&
		target.Status == model.StorageDataSetStatusReady &&
		target.Generation > source.Generation
	if !gate.SlotOwned {
		gate.Blockers = append(gate.Blockers, retirementBlockerSlotOwnership)
	}

	// G5: the chain must have reached the recorded end of term.
	gate.EpochReached = row.TerminationEpoch != nil && observedEpoch != nil && *observedEpoch >= *row.TerminationEpoch
	if observedEpoch != nil && !gate.EpochReached {
		gate.Blockers = append(gate.Blockers, retirementBlockerEpoch)
	}
	return gate, nil
}

// A gap is an upload the source still holds, whose content some live version
// still needs, and which the target cannot serve.
func countRetirementCoverageGaps(ctx context.Context, db bun.IDB, sourceDataSetID, targetDataSetID int64) (int, error) {
	var count int
	if err := db.NewRaw(retirementCoverageGapsSQL(), sourceDataSetID, false, targetDataSetID).Scan(ctx, &count); err != nil {
		return 0, fmt.Errorf("counting retirement coverage gaps: %w", err)
	}
	return count, nil
}

// retirementCoverageGapsSQL is shared with the query-plan regression test so
// the tested plan cannot drift away from the production retirement gate.
func retirementCoverageGapsSQL() string {
	return fmt.Sprintf(`SELECT COUNT(*) FROM storage_contents AS retiring_upload
		WHERE EXISTS (
			SELECT 1 FROM storage_copies AS source_copy
			WHERE source_copy.content_id = retiring_upload.id
			  AND source_copy.storage_data_set_id = ?
			  AND source_copy.status = %[1]s
		)
		AND EXISTS (
			SELECT 1 FROM object_versions AS live_version
			WHERE %[2]s
			  AND live_version.is_delete_marker = ?
		)
		AND NOT EXISTS (
			SELECT 1 FROM storage_copies AS target_copy
			JOIN storage_data_sets AS target_data_set ON target_data_set.id = target_copy.storage_data_set_id
			WHERE target_copy.content_id = retiring_upload.id
			  AND target_copy.storage_data_set_id = ?
			  AND %[3]s
		)`,
		storageHealthCommittedCopyStatusSQL(),
		objectVersionReferencesStorageContentSQL("live_version", "retiring_upload"),
		readableCommittedCopyPredicateSQL("target_copy", "target_data_set"),
	)
}

// A common storage task is bound to its copy before it can perform an external
// effect. Therefore the copy lifecycle is the complete source-write fence; the
// retirement gate never needs to inspect task input.
func countRetirementSourceWrites(ctx context.Context, db bun.IDB, _ int64, sourceDataSetID int64) (int, error) {
	inFlight, err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Where("storage_data_set_id = ?", sourceDataSetID).
		Where("status IN (?, ?, ?)",
			model.StorageCopyStatusPending,
			model.StorageCopyStatusPieceReady,
			model.StorageCopyStatusCommitting).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting in-flight source copies: %w", err)
	}

	return inFlight, nil
}

// CountAbandonedTargetSoleCopies reports how many uploads would lose their only
// readable copy if this generation's service ended. An abandoned target holds
// partially migrated data that the retiring source should still have, so the
// answer is normally zero; anything else means terminating it would destroy the
// last copy of something.
func (r *BunStorageReplacementRepo) CountAbandonedTargetSoleCopies(ctx context.Context, targetDataSetID int64) (int, error) {
	predicate := readableCommittedCopyPredicateSQL("abandoned_copy", "abandoned_data_set")
	elsewhere := readableCommittedCopyPredicateSQL("other_copy", "other_data_set")
	query := fmt.Sprintf(`SELECT COUNT(*)
		FROM storage_copies AS abandoned_copy
		JOIN storage_data_sets AS abandoned_data_set ON abandoned_data_set.id = abandoned_copy.storage_data_set_id
		JOIN storage_contents AS abandoned_upload ON abandoned_upload.id = abandoned_copy.content_id
		WHERE abandoned_copy.storage_data_set_id = ?
		  AND %[1]s
		  AND EXISTS (
			SELECT 1 FROM object_versions AS live_version
			WHERE %[2]s
			  AND live_version.is_delete_marker = ?
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM storage_copies AS other_copy
			JOIN storage_data_sets AS other_data_set ON other_data_set.id = other_copy.storage_data_set_id
			WHERE other_copy.content_id = abandoned_copy.content_id
			  AND other_copy.storage_data_set_id <> ?
			  AND %[3]s
		  )`,
		predicate,
		objectVersionReferencesStorageContentSQL("live_version", "abandoned_upload"),
		elsewhere,
	)
	var count int
	if err := r.db.NewRaw(query, targetDataSetID, false, targetDataSetID).Scan(ctx, &count); err != nil {
		return 0, fmt.Errorf("counting abandoned target sole copies: %w", err)
	}
	return count, nil
}

// RetireAbandonedTarget marks an abandoned generation retired. It never touches
// the replacement record, which stays superseded, and it refuses a generation
// that still owns its slot.
func (r *BunStorageReplacementRepo) RetireAbandonedTarget(ctx context.Context, replacementID int64) error {
	return r.retireAbandonedTarget(ctx, replacementID, false)
}

// CompleteAbandonedTargetTermination retires the superseded target once its end
// of term has been recorded, refusing to retire one that has none.
func (r *BunStorageReplacementRepo) CompleteAbandonedTargetTermination(ctx context.Context, replacementID int64) error {
	return r.retireAbandonedTarget(ctx, replacementID, true)
}

func (r *BunStorageReplacementRepo) retireAbandonedTarget(
	ctx context.Context,
	replacementID int64,
	requireRecordedTermination bool,
) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := lockReplacementByID(ctx, db, replacementID)
		if err != nil {
			return err
		}
		if err := lockReplacementDataSets(ctx, db, row.TargetDataSetID); err != nil {
			return err
		}
		attempts, err := (&BunStorageContentRepo{db: db}).CountActiveCommitAttemptsForDataSet(ctx, row.TargetDataSetID)
		if err != nil {
			return err
		}
		if attempts > 0 {
			return fmt.Errorf("retiring abandoned target of replacement %d has %d active confirmation attempts: %w",
				replacementID, attempts, storagereplacement.ErrPrematureComplete)
		}
		sole, err := (&BunStorageReplacementRepo{db: db}).CountAbandonedTargetSoleCopies(ctx, row.TargetDataSetID)
		if err != nil {
			return err
		}
		if sole > 0 {
			return fmt.Errorf("retiring abandoned target of replacement %d holds %d sole copies: %w",
				replacementID, sole, storagereplacement.ErrPrematureComplete)
		}
		if requireRecordedTermination &&
			(row.Status != storagereplacement.StatusSuperseded || row.AbandonedTerminationEpoch == nil) {
			return fmt.Errorf("completing abandoned target termination: %w", ErrConflict)
		}
		res, err := db.NewUpdate().
			Model((*model.StorageDataSet)(nil)).
			Set("status = ?", model.StorageDataSetStatusRetired).
			Set("updated_at = ?", time.Now()).
			Where("id = ? AND is_current = ?", row.TargetDataSetID, false).
			Where("status <> ?", model.StorageDataSetStatusRetired).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("retiring abandoned target: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("retiring abandoned target: %w", ErrConflict)
		}
		return nil
	})
}

// CompleteRetirement re-runs every predicate inside its own transaction, so a
// caller outside the worker cannot retire a source that is still needed.
func (r *BunStorageReplacementRepo) CompleteRetirement(ctx context.Context, replacementID int64, observedEpoch int64) error {
	if replacementID <= 0 {
		return fmt.Errorf("completing provider replacement: %w", ErrInvalidInput)
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := lockReplacementByID(ctx, db, replacementID)
		if err != nil {
			return err
		}
		if row.Status == storagereplacement.StatusCompleted {
			return nil
		}
		if err := lockReplacementDataSets(ctx, db, row.SourceDataSetID, row.TargetDataSetID); err != nil {
			return err
		}
		gate, err := evaluateRetirementGate(ctx, db, replacementID, &observedEpoch)
		if err != nil {
			return err
		}
		if !gate.Passed() {
			return fmt.Errorf("completing provider replacement %d blocked by %v: %w",
				replacementID, gate.Blockers, storagereplacement.ErrPrematureComplete)
		}
		now := time.Now()
		res, err := db.NewUpdate().
			Model((*model.StorageDataSet)(nil)).
			Set("status = ?", model.StorageDataSetStatusRetired).
			Set("updated_at = ?", now).
			Where("id = ? AND is_current = ?", row.SourceDataSetID, false).
			Where("status <> ?", model.StorageDataSetStatusRetired).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("retiring replacement source: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("retiring replacement source: %w", ErrConflict)
		}
		return transitionReplacement(ctx, db, replacementID,
			[]storagereplacement.Status{storagereplacement.StatusRetiring},
			storagereplacement.StatusCompleted,
			func(q *bun.UpdateQuery) *bun.UpdateQuery {
				return q.Set("wait_reason = NULL").
					Set("last_error = NULL")
			}, now)
	})
}

func countReplacementActiveCommitAttempts(ctx context.Context, db bun.IDB, replacementID int64) (int, error) {
	count, err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Join("JOIN storage_replacement_items AS replacement_item ON replacement_item.content_id = storage_copy.content_id AND replacement_item.target_data_set_id = storage_copy.storage_data_set_id").
		Where("replacement_item.replacement_id = ?", replacementID).
		Where(attemptedStorageCommitSQL("storage_copy")).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting replacement confirmation attempts: %w", err)
	}
	return count, nil
}

func lockReplacementDataSets(ctx context.Context, db bun.IDB, ids ...int64) error {
	ids = append([]int64(nil), ids...)
	slices.Sort(ids)
	for i, id := range ids {
		if id <= 0 || (i > 0 && id == ids[i-1]) {
			continue
		}
		res, err := db.NewUpdate().
			Model((*model.StorageDataSet)(nil)).
			Set("updated_at = updated_at").
			Where("id = ?", id).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("locking replacement data set %d: %w", id, err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("locking replacement data set %d: %w", id, ErrNotFound)
		}
	}
	return nil
}
