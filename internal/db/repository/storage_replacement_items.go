package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

func (r *BunStorageReplacementRepo) SeedMigrationBatch(
	ctx context.Context,
	replacementID int64,
	limit int,
) (int, bool, error) {
	if replacementID <= 0 || limit <= 0 {
		return 0, false, ErrInvalidInput
	}
	row, err := r.GetByID(ctx, replacementID)
	if err != nil {
		return 0, false, err
	}
	if row == nil {
		return 0, false, ErrNotFound
	}
	if row.SeedingComplete {
		return 0, true, nil
	}
	eligible, cursor, scanned, err := r.scanMigrationCandidates(ctx, row, limit)
	if err != nil {
		return 0, false, err
	}
	if scanned == 0 {
		return 0, true, markSeedingComplete(ctx, r.db, row.ID, row.SeedCursorContentID)
	}
	inserted := 0
	done := scanned < limit
	err = runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		locked, err := lockReplacementByID(ctx, db, replacementID)
		if err != nil {
			return err
		}
		if locked.SeedCursorContentID != row.SeedCursorContentID {
			return ErrConflict
		}
		if len(eligible) > 0 {
			// The target copy is created before the item that names it, so the
			// item is born with a target and its composite foreign key to that
			// copy is checked from the first write rather than skipped while the
			// column is still null.
			contents := &BunStorageContentRepo{db: db}
			target, err := contents.GetDataSetBindingByID(ctx, row.TargetDataSetID)
			if err != nil {
				return err
			}
			if target == nil {
				return fmt.Errorf("seeding replacement items: %w", ErrNotFound)
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
					TargetDataSetID: target.ID,
					Status:          storagereplacement.ItemStatusPending,
					CreatedAt:       now, UpdatedAt: now,
				})
			}
			result, err := db.NewInsert().
				Model(&items).
				On("CONFLICT (replacement_id, content_id) DO NOTHING").
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("seeding replacement items: %w", err)
			}
			rows, _ := result.RowsAffected()
			inserted = int(rows)
		}
		query := db.NewUpdate().
			Model((*storagereplacement.Replacement)(nil)).
			Set("seed_cursor_content_id = ?", cursor).
			Set("items_total = items_total + ?", inserted).
			Set("updated_at = ?", time.Now()).
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
	query := fmt.Sprintf(`SELECT candidate.id
		FROM storage_contents AS candidate
		WHERE candidate.id IN (?)
		  AND EXISTS (
			SELECT 1 FROM object_versions AS live_version
			WHERE %s AND live_version.is_delete_marker = ?
		  )
		ORDER BY candidate.id`, objectVersionReferencesStorageContentSQL("live_version", "candidate"))
	if err := r.db.NewRaw(query, bun.List(candidates), false).Scan(ctx, &eligible); err != nil {
		return nil, 0, 0, fmt.Errorf("selecting replacement candidates: %w", err)
	}
	return eligible, cursor, len(candidates), nil
}

func storageUploadMigrationWindowSQL() string {
	return `SELECT id FROM storage_contents WHERE bucket_id = ? AND id > ? ORDER BY id LIMIT ?`
}

func markSeedingComplete(ctx context.Context, db bun.IDB, replacementID, cursor int64) error {
	result, err := db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("seeding_complete = ?", true).
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND seed_cursor_content_id = ?", replacementID, cursor).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing replacement seeding: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return ErrConflict
	}
	return nil
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
func (r *BunStorageReplacementRepo) AcquireItem(ctx context.Context, input AcquireReplacementItemInput) (*ReplacementItemSnapshot, error) {
	if input.ReplacementID <= 0 || input.ItemID <= 0 {
		return nil, ErrInvalidInput
	}
	var snapshot *ReplacementItemSnapshot
	var terminalErr error
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		item := new(storagereplacement.Item)
		err := db.NewRaw(`UPDATE storage_replacement_items
			SET updated_at = updated_at WHERE id = ? RETURNING *`, input.ItemID).Scan(ctx, item)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if item.ReplacementID != input.ReplacementID {
			return ErrConflict
		}
		if item.Status != storagereplacement.ItemStatusPending {
			terminalErr = storagereplacement.ErrItemCancelled
			return nil
		}
		replacement, err := lockReplacementByID(ctx, db, input.ReplacementID)
		if err != nil {
			return err
		}
		if replacement.Status.Terminal() {
			terminalErr = storagereplacement.ErrItemCancelled
			return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled, "")
		}
		uploads := &BunStorageContentRepo{db: db}
		source, err := uploads.GetDataSetBindingByID(ctx, replacement.SourceDataSetID)
		if err != nil {
			return err
		}
		target, err := uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
		if err != nil {
			return err
		}
		upload, err := uploads.GetByID(ctx, item.ContentID)
		if err != nil {
			return err
		}
		if source == nil || target == nil || upload == nil || upload.BucketID != replacement.BucketID {
			return ErrConflict
		}
		version, err := selectLiveObjectVersionForStorageContent(ctx, db, upload, nil)
		if err != nil {
			return err
		}
		if version == nil {
			terminalErr = storagereplacement.ErrItemCancelled
			return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled, "")
		}
		owed, inFlight, err := sourceCopyState(ctx, db, upload.ID, source.ID)
		if err != nil {
			return err
		}
		if !owed {
			if inFlight {
				_, err := db.NewUpdate().
					Model((*storagereplacement.Item)(nil)).
					Set("last_error = ?", "the retiring provider has not finished storing this content").
					Set("updated_at = ?", time.Now()).
					Where("id = ? AND status = ?", item.ID, storagereplacement.ItemStatusPending).
					Exec(ctx)
				terminalErr = storagereplacement.ErrItemDeferred
				return err
			}
			terminalErr = storagereplacement.ErrItemCancelled
			return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled, "")
		}
		covered, err := targetHoldsReadableCopy(ctx, db, upload.ID, target.ID)
		if err != nil {
			return err
		}
		if covered {
			terminalErr = storagereplacement.ErrItemCancelled
			return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCopied, "")
		}
		snapshot = &ReplacementItemSnapshot{
			Replacement: *replacement, Item: *item, Source: *source,
			Target: *target, Upload: *upload, Version: *version,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if terminalErr != nil {
		return nil, terminalErr
	}
	return snapshot, nil
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
	err := db.NewRaw(`UPDATE storage_replacement_items
		SET updated_at = updated_at
		WHERE id = ? AND replacement_id = ?
		RETURNING *`, itemID, replacementID).Scan(ctx, item)
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
		Set("last_error = ?", nullableText(lastError)).
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
		_, err = db.NewUpdate().
			Model((*storagereplacement.Replacement)(nil)).
			Set("items_copied = items_copied + 1").
			Set("updated_at = ?", now).
			Where("id = ? AND items_copied < items_total", item.ReplacementID).
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
	if _, err := db.NewUpdate().
		Model((*storagecommit.Attempt)(nil)).
		Set("status = ?", storagecommit.AttemptStatusReleased).
		Set("release_reason = ?", string(storagecommit.ReleaseOwnerTerminal)).
		Set("resolved_at = ?", now).
		Set("updated_at = ?", now).
		Where("content_id = ? AND storage_data_set_id = ?", contentID, targetDataSetID).
		Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusReserved).
		Exec(ctx); err != nil {
		return err
	}
	_, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("commit_ready_at = NULL").
		Set("commit_extra_data_hex = NULL").
		Set("updated_at = ?", now).
		Where("content_id = ? AND storage_data_set_id = ?", contentID, targetDataSetID).
		Where("status = ?", model.StorageCopyStatusPieceReady).
		Where(`NOT EXISTS (
			SELECT 1 FROM storage_commit_attempts AS unresolved_attempt
			WHERE unresolved_attempt.content_id = storage_copy.content_id
			  AND unresolved_attempt.storage_data_set_id = storage_copy.storage_data_set_id
			  AND unresolved_attempt.resolved_at IS NULL
		)`).
		Exec(ctx)
	if err != nil {
		return err
	}
	return wakeCommitFIFOHead(ctx, db, targetDataSetID)
}

func sourceCopyState(ctx context.Context, db bun.IDB, contentID, sourceDataSetID int64) (bool, bool, error) {
	var copies []model.StorageCopy
	if err := db.NewSelect().Model(&copies).
		Where("content_id = ? AND storage_data_set_id = ?", contentID, sourceDataSetID).
		Scan(ctx); err != nil {
		return false, false, err
	}
	inFlight := false
	for i := range copies {
		switch copies[i].Status {
		case model.StorageCopyStatusCommitted:
			return true, false, nil
		case model.StorageCopyStatusPending, model.StorageCopyStatusPieceReady, model.StorageCopyStatusCommitting:
			inFlight = true
		}
	}
	return false, inFlight, nil
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
