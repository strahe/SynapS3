package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func uncreatedSlotCopies(db bun.IDB, row *storagereplacement.Replacement, target *model.StorageDataSet) *bun.SelectQuery {
	return db.NewSelect().Model((*model.StorageCopy)(nil)).
		Join("JOIN storage_data_sets AS ended ON ended.id = storage_copy.storage_data_set_id").
		Where("ended.bucket_id = ? AND ended.copy_index = ? AND ended.generation < ?", row.BucketID, row.CopyIndex, target.Generation).
		Where("ended.status = ? AND ended.is_current = ? AND ended.ensure_task_id IS NULL", model.StorageDataSetStatusRetired, false).
		Where("ended.data_set_id IS NULL AND ended.create_transaction_id IS NULL AND ended.create_status_url IS NULL").
		Where("(ended.client_data_set_id IS NULL OR ended.creation_rejection IS NOT NULL)").
		Where("(storage_copy.status <> ? OR storage_copy.active_task_id IS NOT NULL)", model.StorageCopyStatusFailed)
}

// AbandonUncreatedCopiesBatch transfers local obligations before releasing old
// copy fences. The caller fences this transaction with the live engine claim.
func (r *BunStorageReplacementRepo) AbandonUncreatedCopiesBatch(ctx context.Context, replacementID, generation, taskID int64, limit int) (bool, error) {
	if limit <= 0 || limit > 128 {
		return false, ErrInvalidInput
	}
	row, err := r.GetByID(ctx, replacementID)
	if err != nil || row == nil {
		if err == nil {
			err = ErrNotFound
		}
		return false, err
	}
	done := false
	err = runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if _, err := lockBucketByID(ctx, db, row.BucketID); err != nil {
			return err
		}
		locked, source, target, err := lockLocalReplacement(ctx, db, replacementID, generation, taskID)
		if err != nil {
			return err
		}
		if !locallyEndedDataSet(source) || !target.IsCurrent || target.Status != model.StorageDataSetStatusReady || target.DataSetID == nil || target.DataSetID.IsZero() {
			return ErrConflict
		}
		var copies []model.StorageCopy
		if err := uncreatedSlotCopies(db, locked, target).ColumnExpr("storage_copy.*").
			OrderExpr("storage_copy.content_id ASC, storage_copy.id ASC").Limit(limit).Scan(ctx, &copies); err != nil {
			return err
		}
		contents := &BunStorageContentRepo{db: db}
		checked := make(map[int64]bool)
		for _, copyRow := range copies {
			if !checked[copyRow.StorageDataSetID] {
				ended, err := contents.GetDataSetBindingByID(ctx, copyRow.StorageDataSetID)
				if err != nil {
					return err
				}
				if ended == nil || !locallyEndedDataSet(ended) {
					return storagereplacement.ErrSourceOutcomeUnknown
				}
				effects, err := dataSetHasExternalEffects(ctx, db, ended.ID)
				if err != nil {
					return err
				}
				if effects {
					return storagereplacement.ErrSourceOutcomeUnknown
				}
				checked[ended.ID] = true
			}
			if err := lockStorageContentForCopyMutation(ctx, db, copyRow.ContentID); err != nil {
				return err
			}
			content, err := contents.GetByID(ctx, copyRow.ContentID)
			if err != nil {
				return err
			}
			version, err := contents.GetLiveVersionForUpload(ctx, copyRow.ContentID)
			if err != nil {
				return err
			}
			if localSlotObligation(content, version, locked.CopyIndex) {
				if err := contents.CreateUploadCopiesForBindings(ctx, content.ID, []UploadCopyBindingInput{{
					StorageDataSetID: target.ID, CopyIndex: target.CopyIndex, ProviderID: target.ProviderID,
					TransferMethod: model.StorageCopyTransferMethodPeerPull,
				}}); err != nil {
					return err
				}
			}
			if err := contents.MarkUploadCopyFailed(ctx, MarkUploadCopyFailedInput{
				StorageCopyID: copyRow.ID, ContentID: copyRow.ContentID, CopyIndex: copyRow.CopyIndex,
				LastError: "Replaced before data set setup completed",
			}); err != nil {
				return err
			}
			if _, err := db.NewUpdate().Model((*model.StorageCopy)(nil)).
				Set("active_task_id = NULL").Set("work_generation = work_generation + 1").Set("updated_at = ?", time.Now()).
				Where("id = ? AND status = ?", copyRow.ID, model.StorageCopyStatusFailed).Exec(ctx); err != nil {
				return err
			}
		}
		done = len(copies) < limit
		return nil
	})
	return done, err
}

func localSlotObligation(content *model.StorageContent, version *model.ObjectVersion, copyIndex int) bool {
	return content != nil && version != nil && content.RequestedCopies > copyIndex
}

func localSlotObligationSQL(contentAlias, versionAlias string) string {
	return fmt.Sprintf(`%s.requested_copies > ? AND EXISTS (
		SELECT 1 FROM object_versions AS %s WHERE %s AND %s.is_delete_marker = FALSE)`,
		contentAlias, versionAlias, objectVersionReferencesStorageContentSQL(versionAlias, contentAlias), versionAlias)
}

func lockLocalReplacement(ctx context.Context, db bun.IDB, id, generation, taskID int64) (*storagereplacement.Replacement, *model.StorageDataSet, *model.StorageDataSet, error) {
	row, err := lockReplacementByID(ctx, db, id)
	if err != nil {
		return nil, nil, nil, err
	}
	if row.TaskID == nil || *row.TaskID != taskID || row.TaskGeneration != generation ||
		(row.Status != storagereplacement.StatusPreparingTarget && row.Status != storagereplacement.StatusWaiting && row.Status != storagereplacement.StatusMigrating) {
		return nil, nil, nil, ErrConflict
	}
	contents := &BunStorageContentRepo{db: db}
	source, err := contents.GetDataSetBindingByID(ctx, row.SourceDataSetID)
	if err != nil {
		return nil, nil, nil, err
	}
	target, err := contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil {
		return nil, nil, nil, err
	}
	if source == nil || target == nil {
		return nil, nil, nil, ErrNotFound
	}
	return row, source, target, nil
}

// CompleteWithoutRemoteSource cannot bypass retirement of an existing service.
func (r *BunStorageReplacementRepo) CompleteWithoutRemoteSource(ctx context.Context, replacementID, generation, taskID int64) error {
	row, err := r.GetByID(ctx, replacementID)
	if err != nil || row == nil {
		if err == nil {
			err = ErrNotFound
		}
		return err
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		if _, err := lockBucketByID(ctx, db, row.BucketID); err != nil {
			return err
		}
		locked, source, target, err := lockLocalReplacement(ctx, db, replacementID, generation, taskID)
		if err != nil {
			return err
		}
		if !locallyEndedDataSet(source) || locked.Status != storagereplacement.StatusMigrating || !locked.SeedingComplete ||
			!target.IsCurrent || target.Status != model.StorageDataSetStatusReady || target.DataSetID == nil || target.DataSetID.IsZero() || target.Generation <= source.Generation {
			return storagereplacement.ErrPrematureComplete
		}
		effects, err := dataSetHasExternalEffects(ctx, db, source.ID)
		if err != nil {
			return err
		}
		remaining, err := uncreatedSlotCopies(db, locked, target).Exists(ctx)
		if err != nil {
			return err
		}
		gate, err := evaluateRetirementGate(ctx, db, locked.ID, nil)
		if err != nil {
			return err
		}
		var gaps int
		coverage := fmt.Sprintf(`SELECT COUNT(*) FROM storage_contents AS content
			WHERE content.bucket_id = ? AND content.id <= ? AND %s
			AND NOT EXISTS (SELECT 1 FROM storage_copies AS target_copy
				JOIN storage_data_sets AS target_data_set ON target_data_set.id = target_copy.storage_data_set_id
				WHERE target_copy.content_id = content.id AND target_copy.storage_data_set_id = ? AND %s)`,
			localSlotObligationSQL("content", "live"), readableCommittedCopyPredicateSQL("target_copy", "target_data_set"))
		if err := db.NewRaw(coverage, locked.BucketID, locked.SeedCursorContentID, locked.CopyIndex, target.ID).Scan(ctx, &gaps); err != nil {
			return err
		}
		if effects || remaining || gaps != 0 || len(gate.Blockers) != 0 {
			return storagereplacement.ErrPrematureComplete
		}
		if err := transitionReplacement(ctx, db, locked.ID, []storagereplacement.Status{storagereplacement.StatusMigrating},
			storagereplacement.StatusCompleted, func(q *bun.UpdateQuery) *bun.UpdateQuery {
				return q.Set("wait_reason = NULL").Set("last_error = NULL")
			}, time.Now()); err != nil {
			return err
		}
		return (&BunStorageReplacementRepo{db: db}).CompleteTask(ctx, locked.ID, generation, taskID)
	})
}
