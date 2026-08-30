package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagereplacement"
	"github.com/uptrace/bun"
)

// SeedMigrationBatchWithBudget snapshots the item retry budget at discovery.
func (r *BunStorageReplacementRepo) SeedMigrationBatchWithBudget(
	ctx context.Context,
	replacementID int64,
	limit int,
	maxRetries int,
) (int, bool, error) {
	if maxRetries < 0 {
		return 0, false, fmt.Errorf("seeding replacement migration: %w", ErrInvalidInput)
	}
	return r.seedMigrationBatch(ctx, replacementID, limit, maxRetries)
}

func (r *BunStorageReplacementRepo) seedMigrationBatch(
	ctx context.Context,
	replacementID int64,
	limit int,
	maxRetries int,
) (int, bool, error) {
	if replacementID <= 0 || limit <= 0 {
		return 0, false, fmt.Errorf("seeding replacement migration: %w", ErrInvalidInput)
	}
	row, err := r.GetByID(ctx, replacementID)
	if err != nil {
		return 0, false, err
	}
	if row == nil {
		return 0, false, fmt.Errorf("provider replacement %d: %w", replacementID, ErrNotFound)
	}
	if row.SeedingComplete {
		return 0, true, nil
	}

	// Deciding which uploads need migrating is a read over bucket history. It
	// runs outside the write transaction so a replacement never holds SQLite's
	// single writer while it scans.
	eligible, cursor, scanned, err := r.scanMigrationCandidates(ctx, row, limit)
	if err != nil {
		return 0, false, err
	}
	if scanned == 0 {
		return 0, true, markSeedingComplete(ctx, r.db, row.ID, row.SeedCursorUploadID)
	}

	inserted := 0
	done := scanned < limit
	err = runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		// Re-lock and re-check the cursor: another pass may have advanced it
		// while this one was reading.
		locked, err := lockReplacementByID(ctx, db, replacementID)
		if err != nil {
			return err
		}
		if locked.SeedCursorUploadID != row.SeedCursorUploadID {
			return fmt.Errorf("advancing replacement migration cursor: %w", ErrConflict)
		}
		if len(eligible) > 0 {
			items := make([]storagereplacement.Item, 0, len(eligible))
			now := time.Now()
			for _, uploadID := range eligible {
				items = append(items, storagereplacement.Item{
					ReplacementID: row.ID,
					UploadID:      uploadID,
					Status:        storagereplacement.ItemStatusPending,
					ScheduledAt:   now,
					MaxRetries:    &maxRetries,
					CreatedAt:     now,
					UpdatedAt:     now,
				})
			}
			res, err := db.NewInsert().
				Model(&items).
				On("CONFLICT (replacement_id, upload_id) DO NOTHING").
				Exec(ctx)
			if err != nil {
				return fmt.Errorf("seeding replacement migration items: %w", err)
			}
			affected, _ := res.RowsAffected()
			inserted = int(affected)
		}

		q := db.NewUpdate().
			Model((*storagereplacement.Replacement)(nil)).
			Set("seed_cursor_upload_id = ?", cursor).
			Set("items_total = items_total + ?", inserted).
			Set("updated_at = ?", time.Now()).
			Where("id = ? AND seed_cursor_upload_id = ?", row.ID, row.SeedCursorUploadID)
		if done {
			q = q.Set("seeding_complete = ?", true)
		}
		res, err := q.Exec(ctx)
		if err != nil {
			return fmt.Errorf("advancing replacement migration cursor: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("advancing replacement migration cursor: %w", ErrConflict)
		}
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return inserted, done, nil
}

// scanMigrationCandidates reads one bounded window of upload history and reports
// which uploads still need a copy on the new provider. The cursor advances over
// every upload examined, not only the eligible ones, so a window full of
// ineligible uploads still makes progress.
func (r *BunStorageReplacementRepo) scanMigrationCandidates(
	ctx context.Context,
	row *storagereplacement.Replacement,
	limit int,
) (eligible []int64, cursor int64, scanned int, err error) {
	var candidates []int64
	if err := r.db.NewRaw(storageUploadMigrationWindowSQL(), row.BucketID, row.SeedCursorUploadID, limit).
		Scan(ctx, &candidates); err != nil {
		return nil, 0, 0, fmt.Errorf("scanning replacement migration candidates: %w", err)
	}
	if len(candidates) == 0 {
		return nil, row.SeedCursorUploadID, 0, nil
	}
	cursor = candidates[len(candidates)-1]

	// Migration is keyed by stored content, so content shared by many object
	// versions is copied once.
	//
	// Seeding deliberately does not ask whether the retiring generation already
	// holds a committed copy. An upload still in flight would answer "no" at this
	// instant, commit to the source moments later, and never be revisited once
	// the cursor moved past it, leaving the retirement coverage gate blocked
	// forever. AcquireItem asks that question instead, at a point where it can
	// settle the item either way.
	query := fmt.Sprintf(`SELECT candidate.id
		FROM storage_uploads AS candidate
		WHERE candidate.id IN (?)
		  AND EXISTS (
			SELECT 1 FROM object_versions AS live_version
			WHERE %s
			  AND live_version.is_delete_marker = ?
		  )
		ORDER BY candidate.id ASC`,
		objectVersionReferencesStorageUploadSQL("live_version", "candidate"),
	)
	if err := r.db.NewRaw(query, bun.List(candidates), false).
		Scan(ctx, &eligible); err != nil {
		return nil, 0, 0, fmt.Errorf("selecting replacement migration items: %w", err)
	}
	return eligible, cursor, len(candidates), nil
}

func storageUploadMigrationWindowSQL() string {
	return `SELECT id FROM storage_uploads
		WHERE bucket_id = ? AND id > ?
		ORDER BY id ASC
		LIMIT ?`
}

func markSeedingComplete(ctx context.Context, db bun.IDB, replacementID, cursor int64) error {
	_, err := db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("seeding_complete = ?", true).
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND seed_cursor_upload_id = ?", replacementID, cursor).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("completing replacement migration seeding: %w", err)
	}
	return nil
}

// AcquireItem re-derives every identity the item depends on inside one
// transaction and revalidates the worker's claim, so no provider call can start
// from a stale snapshot.
//
// It also decides, at this moment rather than at seeding time, whether the item
// still needs migrating at all. An item that cannot or need not be migrated is
// settled to a terminal status here; leaving it executable would make the
// coordinator pick it up forever and hold retirement open.
func (r *BunStorageReplacementRepo) AcquireItem(ctx context.Context, input AcquireReplacementItemInput) (*ReplacementItemSnapshot, error) {
	if input.ReplacementID <= 0 || input.ItemID <= 0 || input.ItemClaimedAt.IsZero() {
		return nil, fmt.Errorf("acquiring replacement item: %w", ErrInvalidInput)
	}
	var snapshot *ReplacementItemSnapshot
	// settled and deferred are reported after the transaction commits, so the
	// status this call writes survives; returning an error would roll it back.
	settled := false
	deferred := false
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		item := new(storagereplacement.Item)
		err := db.NewRaw(
			`UPDATE storage_replacement_items SET updated_at = updated_at WHERE id = ? RETURNING *`,
			input.ItemID,
		).Scan(ctx, item)
		if err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("replacement item %d: %w", input.ItemID, ErrNotFound)
			}
			return fmt.Errorf("locking replacement item: %w", err)
		}
		if item.ReplacementID != input.ReplacementID {
			return fmt.Errorf("replacement item %d belongs to another replacement: %w", item.ID, ErrConflict)
		}
		if !item.Status.Executable() {
			settled = true
			return nil
		}
		// The claim is revalidated before any provider call so a lost lease can
		// never race a second worker into the same transfer.
		if item.Status != storagereplacement.ItemStatusRunning || item.ClaimedAt == nil ||
			!item.ClaimedAt.Equal(input.ItemClaimedAt) || item.LeaseUntil == nil || !item.LeaseUntil.After(time.Now()) {
			return ErrItemClaimLost
		}

		replacement, err := lockReplacementByID(ctx, db, input.ReplacementID)
		if err != nil {
			return err
		}
		confirmationRecovery := false
		if !replacement.Status.Active() {
			if (replacement.Status == storagereplacement.StatusFailed || replacement.Status == storagereplacement.StatusSuperseded) &&
				item.TargetCopyID != nil {
				attempts, countErr := db.NewSelect().Model((*model.StorageUploadCopy)(nil)).
					Where("id = ?", *item.TargetCopyID).
					Where("commit_attempt_id IS NOT NULL AND commit_attempt_id <> ''").
					Count(ctx)
				if countErr != nil {
					return fmt.Errorf("checking terminal replacement confirmation: %w", countErr)
				}
				confirmationRecovery = attempts == 1
			}
			if !confirmationRecovery && replacement.Status.Terminal() {
				settled = true
				return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled)
			}
			if !confirmationRecovery {
				deferred = true
				return releaseReplacementItemForRetry(ctx, db, item)
			}
		}
		uploads := &BunStorageUploadRepo{db: db}
		source, err := uploads.GetDataSetBindingByID(ctx, replacement.SourceDataSetID)
		if err != nil {
			return err
		}
		target, err := uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
		if err != nil {
			return err
		}
		if source == nil || target == nil {
			return fmt.Errorf("acquiring replacement item: data set: %w", ErrNotFound)
		}
		if (!confirmationRecovery && !target.IsCurrent) || target.CopyIndex != replacement.CopyIndex || source.CopyIndex != replacement.CopyIndex {
			return fmt.Errorf("acquiring replacement item: replica slot changed: %w", ErrConflict)
		}
		upload, err := uploads.GetByID(ctx, item.UploadID)
		if err != nil {
			return err
		}
		if upload == nil || upload.BucketID != replacement.BucketID {
			return fmt.Errorf("acquiring replacement item: upload %d: %w", item.UploadID, ErrNotFound)
		}
		version, err := selectLiveObjectVersionForStorageUpload(ctx, db, upload, nil)
		if err != nil {
			return err
		}
		if version == nil {
			if confirmationRecovery {
				return fmt.Errorf("acquiring terminal replacement confirmation without a live owner: %w", ErrConflict)
			}
			// Nothing references this content any more, so the new provider does
			// not need it.
			settled = true
			return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled)
		}
		if confirmationRecovery {
			snapshot = &ReplacementItemSnapshot{
				Replacement: *replacement,
				Item:        *item,
				Source:      *source,
				Target:      *target,
				Upload:      *upload,
				Version:     *version,
			}
			return nil
		}
		owed, inFlight, err := sourceCopyState(ctx, db, upload.ID, source.ID)
		if err != nil {
			return err
		}
		if !owed {
			if inFlight {
				// The retiring generation is still writing this content. It is not
				// copyable yet and must not be cancelled: the write will commit,
				// and the coverage gate would then block on content with no item
				// behind it. Park it and revisit.
				deferred = true
				return r.parkItemWaitingSource(ctx, db, item, "the retiring provider has not finished storing this content")
			}
			// The retiring generation never stored this content and never will, so
			// the slot owes the target nothing for it.
			settled = true
			return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCancelled)
		}
		covered, err := targetHoldsReadableCopy(ctx, db, upload.ID, target.ID)
		if err != nil {
			return err
		}
		if covered {
			// Already migrated, most likely by an ordinary upload that landed on
			// the target after activation.
			settled = true
			return settleReplacementItem(ctx, db, item, storagereplacement.ItemStatusCopied)
		}
		snapshot = &ReplacementItemSnapshot{
			Replacement: *replacement,
			Item:        *item,
			Source:      *source,
			Target:      *target,
			Upload:      *upload,
			Version:     *version,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if deferred {
		return nil, storagereplacement.ErrItemDeferred
	}
	if settled {
		return nil, storagereplacement.ErrItemCancelled
	}
	return snapshot, nil
}

func releaseReplacementItemForRetry(ctx context.Context, db bun.IDB, item *storagereplacement.Item) error {
	now := time.Now()
	res, err := db.NewUpdate().
		Model((*storagereplacement.Item)(nil)).
		Set("status = ?", storagereplacement.ItemStatusPending).
		Set("scheduled_at = ?", now).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", item.ID).
		Where("status NOT IN (?, ?)", storagereplacement.ItemStatusCopied, storagereplacement.ItemStatusCancelled).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("releasing inactive replacement item: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return ErrItemClaimLost
	}
	return nil
}

func (r *BunStorageReplacementRepo) parkItemWaitingSource(ctx context.Context, db bun.IDB, item *storagereplacement.Item, reason string) error {
	_, err := db.NewUpdate().
		Model((*storagereplacement.Item)(nil)).
		Set("status = ?", storagereplacement.ItemStatusWaitingSource).
		Set("scheduled_at = ?", time.Now().Add(time.Minute)).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("last_error = ?", reason).
		Set("updated_at = ?", time.Now()).
		Where("id = ?", item.ID).
		Where("status NOT IN (?, ?)", storagereplacement.ItemStatusCopied, storagereplacement.ItemStatusCancelled).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("parking replacement item: %w", err)
	}
	return nil
}

// settleReplacementItem moves an item to a terminal status and keeps the
// replacement's progress counter in step.
func settleReplacementItem(ctx context.Context, db bun.IDB, item *storagereplacement.Item, status storagereplacement.ItemStatus) error {
	res, err := db.NewUpdate().
		Model((*storagereplacement.Item)(nil)).
		Set("status = ?", status).
		Set("claimed_at = NULL").
		Set("lease_until = NULL").
		Set("updated_at = ?", time.Now()).
		Where("id = ?", item.ID).
		Where("status NOT IN (?, ?)", storagereplacement.ItemStatusCopied, storagereplacement.ItemStatusCancelled).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("settling replacement item: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return nil
	}
	if status != storagereplacement.ItemStatusCopied {
		return nil
	}
	if _, err := db.NewUpdate().
		Model((*storagereplacement.Replacement)(nil)).
		Set("items_copied = items_copied + 1").
		Set("updated_at = ?", time.Now()).
		Where("id = ? AND items_copied < items_total", item.ReplacementID).
		Exec(ctx); err != nil {
		return fmt.Errorf("recording replacement progress: %w", err)
	}
	return resumeReadableSourceMigration(ctx, db, item.ReplacementID, time.Now())
}

// sourceCopyState answers two different questions that must not be collapsed:
// whether the retiring generation already stored this content, and whether it is
// still in the middle of storing it. Treating "not committed yet" as "never
// stored" cancels work that the coverage gate will later demand.
func sourceCopyState(ctx context.Context, db bun.IDB, uploadID, sourceDataSetID int64) (owed bool, inFlight bool, err error) {
	var copies []model.StorageUploadCopy
	if err := db.NewSelect().
		Model(&copies).
		Where("upload_id = ? AND storage_data_set_id = ?", uploadID, sourceDataSetID).
		Scan(ctx); err != nil {
		return false, false, fmt.Errorf("checking retiring generation copy: %w", err)
	}
	for i := range copies {
		switch copies[i].Status {
		case model.StorageUploadCopyStatusCommitted:
			return true, false, nil
		case model.StorageUploadCopyStatusPending,
			model.StorageUploadCopyStatusPieceReady,
			model.StorageUploadCopyStatusCommitting:
			inFlight = true
		}
	}
	return false, inFlight, nil
}

// targetHoldsReadableCopy answers the same question the retirement coverage gate
// asks, so an item is never left owing work the gate already considers done.
func targetHoldsReadableCopy(ctx context.Context, db bun.IDB, uploadID, targetDataSetID int64) (bool, error) {
	query := fmt.Sprintf(`SELECT COUNT(*)
		FROM storage_upload_copies AS target_copy
		JOIN storage_data_sets AS target_data_set ON target_data_set.id = target_copy.storage_data_set_id
		WHERE target_copy.upload_id = ?
		  AND target_copy.storage_data_set_id = ?
		  AND %s`, readableCommittedCopyPredicateSQL("target_copy", "target_data_set"))
	var count int
	if err := db.NewRaw(query, uploadID, targetDataSetID).Scan(ctx, &count); err != nil {
		return false, fmt.Errorf("checking replacement target coverage: %w", err)
	}
	return count > 0, nil
}

// AttachTargetCopy creates the copy row on the target generation, or returns
// the existing one so a retried item reuses the same concrete row.
func (r *BunStorageReplacementRepo) AttachTargetCopy(ctx context.Context, input AttachReplacementTargetCopyInput) (*model.StorageUploadCopy, error) {
	if input.ReplacementID <= 0 || input.ItemID <= 0 || input.UploadID <= 0 {
		return nil, fmt.Errorf("attaching replacement target copy: %w", ErrInvalidInput)
	}
	var attached *model.StorageUploadCopy
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		replacement, err := lockReplacementByID(ctx, db, input.ReplacementID)
		if err != nil {
			return err
		}
		uploads := &BunStorageUploadRepo{db: db}
		target, err := uploads.GetDataSetBindingByID(ctx, replacement.TargetDataSetID)
		if err != nil {
			return err
		}
		if target == nil {
			return fmt.Errorf("attaching replacement target copy: data set: %w", ErrNotFound)
		}
		if err := uploads.CreateUploadCopiesForBindings(ctx, input.UploadID, []UploadCopyBindingInput{{
			StorageDataSetID: target.ID,
			CopyIndex:        target.CopyIndex,
			// Migration pulls from a remote replica whenever one is readable.
			TransferMethod: model.StorageCopyTransferMethodPeerPull,
			ProviderID:     target.ProviderID,
		}}); err != nil {
			return err
		}
		copyRow, err := uploads.GetUploadCopyForDataSet(ctx, input.UploadID, target.ID)
		if err != nil {
			return err
		}
		if copyRow == nil {
			return fmt.Errorf("attaching replacement target copy: %w", ErrNotFound)
		}
		query := db.NewUpdate().
			Model((*storagereplacement.Item)(nil)).
			Set("target_copy_id = ?", copyRow.ID).
			Set("updated_at = ?", time.Now()).
			Where("id = ? AND replacement_id = ?", input.ItemID, input.ReplacementID)
		if !input.ItemClaimedAt.IsZero() {
			query = query.Where("status = ? AND claimed_at = ? AND lease_until > ?",
				storagereplacement.ItemStatusRunning, input.ItemClaimedAt, time.Now())
		}
		res, err := query.Exec(ctx)
		if err != nil {
			return fmt.Errorf("recording replacement target copy: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrItemClaimLost
		}
		attached = copyRow
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attached, nil
}
