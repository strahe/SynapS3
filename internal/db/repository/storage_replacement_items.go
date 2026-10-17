package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func lockReplacementProgress(ctx context.Context, db bun.IDB, replacementID int64) (*storagereplacement.Replacement, error) {
	var bucketID int64
	if err := db.NewSelect().Model((*storagereplacement.Replacement)(nil)).Column("bucket_id").Where("id = ?", replacementID).Scan(ctx, &bucketID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	bucket, err := lockBucketByID(ctx, db, bucketID)
	if err != nil {
		return nil, err
	}
	if bucket == nil {
		return nil, ErrNotFound
	}
	row, err := lockReplacementByID(ctx, db, replacementID)
	if err != nil {
		return nil, err
	}
	if row.BucketID != bucketID {
		return nil, ErrConflict
	}
	if err := lockReplacementDataSets(ctx, db, row.SourceDataSetID, row.TargetDataSetID); err != nil {
		return nil, err
	}
	return row, nil
}

// LockCoordinatorTask holds the replacement's domain fence for transaction-local writes.
func (r *BunStorageReplacementRepo) LockCoordinatorTask(ctx context.Context, replacementID, generation, taskID int64) (*storagereplacement.Replacement, error) {
	if replacementID <= 0 || generation <= 0 || taskID <= 0 {
		return nil, ErrInvalidInput
	}
	row, err := lockReplacementProgress(ctx, r.db, replacementID)
	if err != nil {
		return nil, err
	}
	if row.TaskGeneration != generation || row.TaskID == nil || *row.TaskID != taskID || row.Status == storagereplacement.StatusCompleted || row.Status == storagereplacement.StatusSuperseded {
		return nil, ErrConflict
	}
	return row, nil
}

func (r *BunStorageReplacementRepo) LockCoordinatorProgress(ctx context.Context, replacementID, generation, taskID int64) (*storagereplacement.Replacement, error) {
	row, err := r.LockCoordinatorTask(ctx, replacementID, generation, taskID)
	if err != nil {
		return nil, err
	}
	if row.Status != storagereplacement.StatusMigrating && row.Status != storagereplacement.StatusWaiting {
		return nil, ErrConflict
	}
	target, err := (&BunStorageContentRepo{db: r.db}).GetDataSetBindingByID(ctx, row.TargetDataSetID)
	if err != nil {
		return nil, err
	}
	if target == nil || !target.IsCurrent || target.Status != model.StorageDataSetStatusReady || target.DataSetID == nil || target.DataSetID.IsZero() {
		return nil, ErrConflict
	}
	return row, nil
}

func (r *BunStorageReplacementRepo) ListPendingReplacementItemsAfter(ctx context.Context, replacementID, afterID int64, limit int) ([]storagereplacement.Item, error) {
	if replacementID <= 0 || afterID < 0 || limit <= 0 {
		return nil, ErrInvalidInput
	}
	var items []storagereplacement.Item
	if err := r.db.NewSelect().Model(&items).
		Where("replacement_id = ? AND status = ? AND id > ?", replacementID, storagereplacement.ItemStatusPending, afterID).
		OrderExpr("id").Limit(int64(limit)).Scan(ctx); err != nil {
		return nil, fmt.Errorf("scanning replacement items: %w", err)
	}
	return items, nil
}

func (r *BunStorageReplacementRepo) AcquireItemsForCoordinator(ctx context.Context, replacementID, generation, taskID, afterID int64, limit int) (ReplacementItemBatch, error) {
	var batch ReplacementItemBatch
	row, err := r.LockCoordinatorProgress(ctx, replacementID, generation, taskID)
	if err != nil {
		return batch, err
	}
	items, err := r.ListPendingReplacementItemsAfter(ctx, replacementID, afterID, limit)
	if err != nil {
		return batch, err
	}
	for _, item := range items {
		snapshot, err := acquireReplacementItemLocked(ctx, r.db, row, item.ID)
		if err != nil && !errors.Is(err, storagereplacement.ErrItemCancelled) {
			return batch, err
		}
		if snapshot != nil {
			batch.Items = append(batch.Items, *snapshot)
		}
		batch.LastItemID = item.ID
		batch.Scanned++
	}
	return batch, nil
}

func (r *BunStorageReplacementRepo) SeedMigrationBatch(ctx context.Context, replacementID int64, limit int) (int, bool, error) {
	if replacementID <= 0 || limit <= 0 {
		return 0, false, ErrInvalidInput
	}
	var inserted int
	var done bool
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		row, err := lockReplacementProgress(ctx, db, replacementID)
		if err != nil {
			return err
		}
		if row.Status != storagereplacement.StatusPreparingTarget && row.Status != storagereplacement.StatusWaiting && row.Status != storagereplacement.StatusMigrating {
			return ErrConflict
		}
		if row.SeedingComplete {
			done = true
			return nil
		}
		eligible, cursor, scanned, err := (&BunStorageReplacementRepo{db: db}).scanMigrationCandidates(ctx, row, limit)
		if err != nil {
			return err
		}
		done = scanned < limit
		if len(eligible) > 0 {
			contents := &BunStorageContentRepo{db: db}
			target, err := contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
			if err != nil {
				return err
			}
			if target == nil {
				return ErrNotFound
			}
			items := make([]storagereplacement.Item, 0, len(eligible))
			now := time.Now()
			for _, contentID := range eligible {
				if err := contents.CreateUploadCopiesForBindings(ctx, contentID, []UploadCopyBindingInput{{
					StorageDataSetID: target.ID, CopyIndex: target.CopyIndex,
					TransferMethod: model.StorageCopyTransferMethodPeerPull, ProviderID: target.ProviderID,
				}}); err != nil {
					return err
				}
				items = append(items, storagereplacement.Item{
					ReplacementID: row.ID, ContentID: contentID,
					TargetDataSetID: target.ID, Status: storagereplacement.ItemStatusPending, CreatedAt: now, UpdatedAt: now,
				})
			}
			result, err := db.NewInsert().Model(&items).On("CONFLICT (replacement_id, content_id) DO NOTHING").Exec(ctx)
			if err != nil {
				return fmt.Errorf("seeding replacement items: %w", err)
			}
			rows, _ := result.RowsAffected()
			inserted = int(rows)
		}
		query := db.NewUpdate().Model((*storagereplacement.Replacement)(nil)).
			Set("seed_cursor_content_id = ?", cursor).Set("updated_at = ?", time.Now()).
			Where("id = ? AND seed_cursor_content_id = ?", row.ID, row.SeedCursorContentID)
		if done {
			query = query.Set("seeding_complete = ?", true)
		}
		result, err := query.Exec(ctx)
		if err != nil {
			return fmt.Errorf("advancing replacement cursor: %w", err)
		}
		if rows, _ := result.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
	return inserted, done, err
}

func (r *BunStorageReplacementRepo) scanMigrationCandidates(
	ctx context.Context,
	row *storagereplacement.Replacement,
	limit int,
) (eligible []int64, cursor int64, scanned int, err error) {
	var candidates []int64
	if err := r.db.NewRaw(storageUploadMigrationWindowSQL(), row.BucketID, row.SeedCursorContentID, limit).
		Scan(ctx, &candidates); err != nil {
		return nil, 0, 0, fmt.Errorf("scanning replacement candidates: %w", err)
	}
	if len(candidates) == 0 {
		return nil, row.SeedCursorContentID, 0, nil
	}
	cursor = candidates[len(candidates)-1]
	query := r.db.NewSelect().TableExpr("storage_contents AS candidate").ColumnExpr("candidate.id").
		Where("candidate.id IN (?)", bun.List(candidates)).Where(slotObligationSQL("candidate", "live_version"), row.CopyIndex).
		OrderExpr("candidate.id")
	if err := query.Scan(ctx, &eligible); err != nil {
		return nil, 0, 0, fmt.Errorf("selecting replacement candidates: %w", err)
	}
	return eligible, cursor, len(candidates), nil
}

func storageUploadMigrationWindowSQL() string {
	return `SELECT id FROM storage_contents WHERE bucket_id = ? AND id > ? ORDER BY id LIMIT ?`
}

func (r *BunStorageReplacementRepo) NextPendingReplacementItem(ctx context.Context, replacementID int64) (*storagereplacement.Item, error) {
	item := new(storagereplacement.Item)
	err := r.db.NewSelect().
		Model(item).
		Where("replacement_id = ? AND status = ?", replacementID, storagereplacement.ItemStatusPending).
		OrderExpr("CASE WHEN target_data_set_id IS NULL THEN 0 ELSE 1 END, id").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("selecting pending replacement item: %w", err)
	}
	return item, nil
}

// AcquireItem re-derives the current domain snapshot. Replacement items are
// ledgers, not workers; task claim and lease state never appears here.
func (r *BunStorageReplacementRepo) InspectItem(ctx context.Context, input AcquireReplacementItemInput) (*ReplacementItemSnapshot, error) {
	if input.ReplacementID <= 0 || input.ItemID <= 0 {
		return nil, ErrInvalidInput
	}
	replacement, err := r.GetByID(ctx, input.ReplacementID)
	if err != nil {
		return nil, err
	}
	if replacement == nil {
		return nil, ErrNotFound
	}
	item := new(storagereplacement.Item)
	if err := r.db.NewSelect().Model(item).Where("id = ?", input.ItemID).Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	snapshot, _, err := inspectReplacementItem(ctx, r.db, replacement, item)
	return snapshot, err
}

func (r *BunStorageReplacementRepo) AcquireItem(ctx context.Context, input AcquireReplacementItemInput) (*ReplacementItemSnapshot, error) {
	if input.ReplacementID <= 0 || input.ItemID <= 0 {
		return nil, ErrInvalidInput
	}
	var snapshot *ReplacementItemSnapshot
	var terminalErr error
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		replacement, err := lockReplacementProgress(ctx, db, input.ReplacementID)
		if err != nil {
			return err
		}
		snapshot, terminalErr = acquireReplacementItemLocked(ctx, db, replacement, input.ItemID)
		if errors.Is(terminalErr, storagereplacement.ErrItemCancelled) {
			return nil
		}
		return terminalErr
	})
	if err != nil {
		return nil, err
	}
	return snapshot, terminalErr
}

func acquireReplacementItemLocked(ctx context.Context, db bun.IDB, replacement *storagereplacement.Replacement, itemID int64) (*ReplacementItemSnapshot, error) {
	item, err := lockReplacementItem(ctx, db, replacement.ID, itemID)
	if err != nil {
		return nil, err
	}
	snapshot, status, terminalErr := inspectReplacementItem(ctx, db, replacement, item)
	if errors.Is(terminalErr, storagereplacement.ErrItemCancelled) {
		if item.Status == storagereplacement.ItemStatusPending {
			if err := settleReplacementItem(ctx, db, item, status, ""); err != nil {
				return nil, err
			}
		}
		return nil, terminalErr
	}
	return snapshot, terminalErr
}

func inspectReplacementItem(ctx context.Context, db bun.IDB, replacement *storagereplacement.Replacement, item *storagereplacement.Item) (*ReplacementItemSnapshot, storagereplacement.ItemStatus, error) {
	if item.ReplacementID != replacement.ID {
		return nil, "", ErrConflict
	}
	if item.Status != storagereplacement.ItemStatusPending || replacement.Status.Terminal() {
		return nil, storagereplacement.ItemStatusCancelled, storagereplacement.ErrItemCancelled
	}
	contents := &BunStorageContentRepo{db: db}
	source, err := contents.GetDataSetBindingByID(ctx, replacement.SourceDataSetID)
	if err != nil {
		return nil, "", err
	}
	target, err := contents.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
	if err != nil {
		return nil, "", err
	}
	content, err := contents.GetByID(ctx, item.ContentID)
	if err != nil {
		return nil, "", err
	}
	if content == nil {
		return nil, storagereplacement.ItemStatusCancelled, storagereplacement.ErrItemCancelled
	}
	if source == nil || target == nil || content.BucketID != replacement.BucketID {
		return nil, "", ErrConflict
	}
	version, err := selectLiveObjectVersionForStorageContent(ctx, db, content, nil)
	if err != nil {
		return nil, "", err
	}
	if version == nil {
		return nil, storagereplacement.ItemStatusCancelled, storagereplacement.ErrItemCancelled
	}
	if !slotObligation(content, version, replacement.CopyIndex) {
		return nil, storagereplacement.ItemStatusCancelled, storagereplacement.ErrItemCancelled
	}
	covered, err := targetHoldsReadableCopy(ctx, db, content.ID, target.ID)
	if err != nil {
		return nil, "", err
	}
	if covered {
		return nil, storagereplacement.ItemStatusCopied, storagereplacement.ErrItemCancelled
	}
	return &ReplacementItemSnapshot{Replacement: *replacement, Item: *item, Source: *source, Target: *target, Upload: *content, Version: *version}, storagereplacement.ItemStatusPending, nil
}

func (r *BunStorageReplacementRepo) MarkReplacementItemCopied(ctx context.Context, replacementID, itemID, targetCopyID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		item, err := lockReplacementItem(ctx, db, replacementID, itemID)
		if err != nil {
			return err
		}
		if targetCopyID > 0 {

			count, countErr := db.NewSelect().
				Model((*model.StorageCopy)(nil)).
				Where("id = ? AND content_id = ? AND storage_data_set_id = ?", targetCopyID, item.ContentID, item.TargetDataSetID).
				Count(ctx)
			if countErr != nil {
				return countErr
			}
			if count != 1 {
				return ErrConflict
			}
		}
		return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCopied, "")
	})
}

func (r *BunStorageReplacementRepo) MarkReplacementItemCancelled(ctx context.Context, replacementID, itemID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		item, err := lockReplacementItem(ctx, db, replacementID, itemID)
		if err != nil {
			return err
		}
		return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled, "")
	})
}

func (r *BunStorageReplacementRepo) MarkReplacementItemAttention(ctx context.Context, replacementID, itemID int64, lastError string) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		item, err := lockReplacementItem(ctx, db, replacementID, itemID)
		if err != nil {
			return err
		}
		return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusAttention, lastError)
	})
}

func lockReplacementItem(ctx context.Context, db bun.IDB, replacementID, itemID int64) (*storagereplacement.Item, error) {
	item := new(storagereplacement.Item)
	err := db.NewSelect().Model(item).Where("id = ? AND replacement_id = ?", itemID, replacementID).For("NO KEY UPDATE").Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return item, err
}

func settleReplacementItem(
	ctx context.Context,
	db bun.IDB,
	item *storagereplacement.Item,
	status storagereplacement.ItemStatus,
	lastError string,
) error {
	if item.Status == status {
		return nil
	}
	if item.Status != storagereplacement.ItemStatusPending {
		return ErrConflict
	}
	now := time.Now()
	result, err := db.NewUpdate().
		Model((*storagereplacement.Item)(nil)).
		Set("status = ?", status).
		Set("last_error = ?", nullableText(errtext.Redact(lastError))).
		Set("updated_at = ?", now).
		Where("id = ? AND status = ?", item.ID, storagereplacement.ItemStatusPending).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("settling replacement item: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrConflict
	}
	if status == storagereplacement.ItemStatusCopied {
		// Progress is counted from the items; a copied item still counts as
		// activity on the replacement.
		_, err = db.NewUpdate().
			Model((*storagereplacement.Replacement)(nil)).
			Set("updated_at = ?", now).
			Where("id = ?", item.ReplacementID).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("recording replacement progress: %w", err)
		}
		_, err = db.NewUpdate().
			Model((*storagereplacement.Replacement)(nil)).
			Set("status = ?", storagereplacement.StatusMigrating).
			Set("wait_reason = NULL").
			Set("updated_at = ?", now).
			Where("id = ? AND status = ? AND wait_reason = ?", item.ReplacementID,
				storagereplacement.StatusWaiting, storagereplacement.WaitReasonReadableSource).
			Exec(ctx)
		return err
	}
	return clearUnattemptedReplacementReservation(ctx, db, item.ContentID, item.TargetDataSetID, now)
}

func clearUnattemptedReplacementReservation(
	ctx context.Context,
	db bun.IDB,
	contentID int64,
	targetDataSetID int64,
	now time.Time,
) error {
	if targetDataSetID == 0 {
		return nil
	}
	var copyIDs []int64
	if err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Column("id").
		Where("content_id = ? AND storage_data_set_id = ?", contentID, targetDataSetID).
		Where("status = ?", model.StorageCopyStatusPieceReady).
		Scan(ctx, &copyIDs); err != nil {
		return err
	}
	for _, copyID := range copyIDs {
		if err := releaseCollectingCommitMembership(ctx, db, copyID, now); err != nil {
			return err
		}
	}
	return nil
}

func targetHoldsReadableCopy(ctx context.Context, db bun.IDB, contentID, targetDataSetID int64) (bool, error) {
	query := fmt.Sprintf(`SELECT COUNT(*)
		FROM storage_copies AS target_copy
		JOIN storage_data_sets AS target_data_set ON target_data_set.id = target_copy.storage_data_set_id
		WHERE target_copy.content_id = ?
		  AND target_copy.storage_data_set_id = ?
		  AND %s`, readableCommittedCopyPredicateSQL("target_copy", "target_data_set"))
	var count int
	if err := db.NewRaw(query, contentID, targetDataSetID).Scan(ctx, &count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// AttachTargetCopy resolves the copy an item already names. Seeding created
// both the item and its pending copy, so this only reads back the binding the
// item was born with and refuses an item that belongs to another content.
func (r *BunStorageReplacementRepo) AttachTargetCopy(ctx context.Context, input AttachReplacementTargetCopyInput) (*model.StorageCopy, error) {
	if input.ReplacementID <= 0 || input.ItemID <= 0 || input.ContentID <= 0 {
		return nil, ErrInvalidInput
	}
	var attached *model.StorageCopy
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		item, err := lockReplacementItem(ctx, db, input.ReplacementID, input.ItemID)
		if err != nil {
			return err
		}
		if item.ContentID != input.ContentID || item.Status != storagereplacement.ItemStatusPending {
			return ErrConflict
		}
		uploads := &BunStorageContentRepo{db: db}
		copyRow, err := uploads.GetUploadCopyForDataSet(ctx, item.ContentID, item.TargetDataSetID)
		if err != nil || copyRow == nil {
			return errors.Join(err, ErrNotFound)
		}
		attached = copyRow
		return nil
	})
	return attached, err
}
