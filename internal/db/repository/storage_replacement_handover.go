package repository

import (
	"context"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

// HasUnsignedSourceWrites reports whether a source has writes that can be handed over.
func (r *BunStorageReplacementRepo) HasUnsignedSourceWrites(ctx context.Context, sourceDataSetID int64) (bool, error) {
	if sourceDataSetID < 1 {
		return false, ErrInvalidInput
	}
	return unsignedSourceWrites(r.db, sourceDataSetID).Exists(ctx)
}

// HandOverSourceWritesBatch moves unsigned writes still bound to an activated
// remote source to its target. A sealed registration member or a copy with an
// unresolved pull attempt carries a signed authorization and stays until its
// outcome is known. The caller fences this transaction with the live engine
// claim.
func (r *BunStorageReplacementRepo) HandOverSourceWritesBatch(ctx context.Context, replacementID, generation, taskID int64, limit int) ([]model.StorageCopy, bool, error) {
	if limit <= 0 || limit > 128 {
		return nil, false, ErrInvalidInput
	}
	row, err := r.GetByID(ctx, replacementID)
	if err != nil || row == nil {
		if err == nil {
			err = ErrNotFound
		}
		return nil, false, err
	}
	var targets []model.StorageCopy
	done := false
	err = runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		targets = nil
		done = false
		if _, err := lockBucketByID(ctx, db, row.BucketID); err != nil {
			return err
		}
		locked, source, target, err := lockFencedReplacement(ctx, db, replacementID, generation, taskID)
		if err != nil {
			return err
		}
		if source.DataSetID == nil || source.IsCurrent || source.Status != model.StorageDataSetStatusDraining ||
			!target.IsCurrent || target.Status != model.StorageDataSetStatusReady || target.DataSetID == nil || target.DataSetID.IsZero() {
			return ErrConflict
		}
		var copies []model.StorageCopy
		if err := unsignedSourceWrites(db, source.ID).ColumnExpr("storage_copy.*").
			OrderExpr("storage_copy.content_id ASC, storage_copy.id ASC").Limit(int64(limit)).Scan(ctx, &copies); err != nil {
			return err
		}
		contentIDs := make([]int64, 0, len(copies))
		for _, copyRow := range copies {
			contentIDs = append(contentIDs, copyRow.ContentID)
		}
		if _, err := lockStorageContentsByID(ctx, db, contentIDs); err != nil {
			return err
		}
		for _, copyRow := range copies {
			// Sealing and pull reservation sign a write under the same content
			// lock, so this check sees any signature that won the lock.
			unsigned, err := unsignedSourceWrites(db, source.ID).Where("storage_copy.id = ?", copyRow.ID).Exists(ctx)
			if err != nil {
				return err
			}
			if !unsigned {
				continue
			}
			targetCopy, err := releaseReplacedCopy(ctx, db, locked, target, copyRow, "Moved to the replacement provider")
			if err != nil {
				return err
			}
			if targetCopy != nil {
				now := time.Now()
				item := &storagereplacement.Item{
					ReplacementID: locked.ID, ContentID: targetCopy.ContentID, TargetDataSetID: target.ID,
					Status: storagereplacement.ItemStatusPending, CreatedAt: now, UpdatedAt: now,
				}
				if _, err := db.NewInsert().Model(item).On("CONFLICT (replacement_id, content_id) DO NOTHING").Exec(ctx); err != nil {
					return err
				}
				targets = append(targets, *targetCopy)
			}
		}
		done = len(copies) < limit
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return targets, done, nil
}

func unsignedSourceWrites(db bun.IDB, sourceDataSetID int64) *bun.SelectQuery {
	return inFlightDataSetCopies(db.NewSelect().Model((*model.StorageCopy)(nil)), sourceDataSetID).
		Where("NOT " + pinnedCommitMemberSQL("storage_copy")).
		Where(`NOT EXISTS (SELECT 1 FROM storage_pull_attempts AS pull_attempt
			WHERE pull_attempt.content_id = storage_copy.content_id
			  AND pull_attempt.storage_data_set_id = storage_copy.storage_data_set_id
			  AND pull_attempt.resolved_at IS NULL)`)
}
