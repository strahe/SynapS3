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

func (r *BunStorageUploadRepo) ReserveCommitAttempt(
	ctx context.Context,
	input storagecommit.ReserveInput,
) (storagecommit.ReserveResult, error) {
	var out storagecommit.ReserveResult
	if err := validateCommitCopyIdentity(input.Copy); err != nil || input.AttemptID == "" {
		return out, fmt.Errorf("reserving storage commit attempt: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		copyID, dataSet, err := lockCommitCopyFamily(ctx, db, input.Copy)
		if err != nil {
			return err
		}
		currentGeneration := dataSet.IsCurrent &&
			(dataSet.Status == model.StorageDataSetStatusReady || dataSet.Status == model.StorageDataSetStatusUnavailable)
		if !currentGeneration && dataSet.Status != model.StorageDataSetStatusDraining {
			return fmt.Errorf("reserving storage commit attempt on unavailable generation: %w", ErrConflict)
		}
		copyRow, err := loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
		if err != nil {
			return err
		}
		if copyRow.Status != model.StorageUploadCopyStatusPieceReady {
			return fmt.Errorf("reserving storage commit attempt for copy %d in status %s: %w", copyID, copyRow.Status, ErrConflict)
		}
		if _, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("commit_ready_at = COALESCE(commit_ready_at, ?)", now).
			Set("updated_at = ?", now).
			Where("id = ?", copyID).
			Exec(ctx); err != nil {
			return fmt.Errorf("preparing storage commit attempt: %w", err)
		}
		copyRow, err = loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
		if err != nil {
			return err
		}
		if copyRow.CommitAttemptID != nil && *copyRow.CommitAttemptID != "" {
			out.State = storagecommit.ReservationAcquired
			out.Copy = *copyRow
			return nil
		}
		active, err := db.NewSelect().
			Model((*model.StorageUploadCopy)(nil)).
			Where("storage_data_set_id = ?", input.Copy.StorageDataSetID).
			Where("commit_attempt_id IS NOT NULL AND commit_attempt_id <> ''").
			Count(ctx)
		if err != nil {
			return fmt.Errorf("counting active storage commit attempts: %w", err)
		}
		if active >= storagecommit.MaxActiveAttemptsPerDataSet {
			out.State = storagecommit.ReservationWaiting
			out.Copy = *copyRow
			return nil
		}
		var headID int64
		err = db.NewSelect().
			Model((*model.StorageUploadCopy)(nil)).
			Column("id").
			Where("storage_data_set_id = ?", input.Copy.StorageDataSetID).
			Where("status = ?", model.StorageUploadCopyStatusPieceReady).
			Where("commit_attempt_id IS NULL").
			Where("commit_ready_at IS NOT NULL").
			OrderExpr("commit_ready_at ASC").
			OrderExpr("id ASC").
			Limit(1).
			Scan(ctx, &headID)
		if err != nil {
			return fmt.Errorf("selecting next FIFO storage commit: %w", err)
		}
		if headID != copyID {
			out.State = storagecommit.ReservationWaiting
			out.Copy = *copyRow
			return nil
		}
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("commit_attempt_id = ?", input.AttemptID).
			Set("updated_at = ?", now).
			Where("id = ?", copyID).
			Where("status = ?", model.StorageUploadCopyStatusPieceReady).
			Where("commit_attempt_id IS NULL").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("reserving storage commit attempt: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows != 1 {
			return fmt.Errorf("reserving storage commit attempt: %w", ErrConflict)
		}
		copyRow, err = loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
		if err != nil {
			return err
		}
		out.State = storagecommit.ReservationAcquired
		out.Copy = *copyRow
		return nil
	})
	return out, err
}

func (r *BunStorageUploadRepo) MarkCommitAttempted(
	ctx context.Context,
	input storagecommit.AttemptInput,
) (storagecommit.AttemptResult, error) {
	var out storagecommit.AttemptResult
	if err := validateCommitCopyIdentity(input.Copy); err != nil || input.AttemptID == "" || input.ExtraDataHex == "" {
		return out, fmt.Errorf("marking storage commit attempted: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		copyID, _, err := lockCommitCopyFamily(ctx, db, input.Copy)
		if err != nil {
			return err
		}
		copyRow, err := loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
		if err != nil {
			return err
		}
		if copyRow.CommitAttemptID == nil || *copyRow.CommitAttemptID != input.AttemptID {
			return fmt.Errorf("marking storage commit attempted with stale token: %w", ErrConflict)
		}
		if copyRow.CommitAttemptedAt != nil {
			out.Copy = *copyRow
			return nil
		}
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("status = ?", model.StorageUploadCopyStatusCommitting).
			Set("commit_extra_data_hex = COALESCE(commit_extra_data_hex, ?)", input.ExtraDataHex).
			Set("commit_attempted_at = ?", now).
			Set("updated_at = ?", now).
			Where("id = ?", copyID).
			Where("status = ?", model.StorageUploadCopyStatusPieceReady).
			Where("commit_attempt_id = ?", input.AttemptID).
			Where("(commit_extra_data_hex IS NULL OR commit_extra_data_hex = '' OR commit_extra_data_hex = ?)", input.ExtraDataHex).
			Where("commit_attempted_at IS NULL").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage commit attempted: %w", err)
		}
		rows, _ := res.RowsAffected()
		if rows != 1 {
			return fmt.Errorf("marking storage commit attempted: %w", ErrConflict)
		}
		copyRow, err = loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
		if err != nil {
			return err
		}
		out.Entered = true
		out.Copy = *copyRow
		return nil
	})
	return out, err
}

func (r *BunStorageUploadRepo) RecordCommitTransaction(ctx context.Context, input storagecommit.EvidenceInput) error {
	if err := validateCommitEvidenceInput(input, false); err != nil {
		return err
	}
	return r.mutateAttempt(ctx, input.Copy, "recording storage commit transaction", func(db bun.IDB, copyID int64) error {
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("commit_transaction_id = COALESCE(commit_transaction_id, ?)", input.TransactionID).
			Set("updated_at = ?", commitInputTime(input.Now)).
			Where("id = ?", copyID).
			Where("status = ?", model.StorageUploadCopyStatusCommitting).
			Where("commit_attempt_id = ?", input.AttemptID).
			Where("commit_attempted_at IS NOT NULL").
			Where("(commit_transaction_id IS NULL OR commit_transaction_id = '' OR commit_transaction_id = ?)", input.TransactionID).
			Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) RecordCommitSubmission(ctx context.Context, input storagecommit.EvidenceInput) error {
	if err := validateCommitEvidenceInput(input, true); err != nil {
		return err
	}
	return r.mutateAttempt(ctx, input.Copy, "recording storage commit submission", func(db bun.IDB, copyID int64) error {
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("commit_transaction_id = COALESCE(commit_transaction_id, ?)", input.TransactionID).
			Set("commit_submission_json = ?", input.SubmissionJSON).
			Set("updated_at = ?", commitInputTime(input.Now)).
			Where("id = ?", copyID).
			Where("status = ?", model.StorageUploadCopyStatusCommitting).
			Where("commit_attempt_id = ?", input.AttemptID).
			Where("commit_attempted_at IS NOT NULL").
			Where("(commit_transaction_id IS NULL OR commit_transaction_id = '' OR commit_transaction_id = ?)", input.TransactionID).
			Where("(commit_submission_json IS NULL OR commit_submission_json = '' OR commit_submission_json = ?)", input.SubmissionJSON).
			Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) MarkCommitAttention(ctx context.Context, input storagecommit.AttentionInput) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil || input.AttemptID == "" || !input.Code.Valid() {
		return fmt.Errorf("marking storage commit attention: %w", ErrInvalidInput)
	}
	return r.mutateAttempt(ctx, input.Copy, "marking storage commit attention", func(db bun.IDB, copyID int64) error {
		now := commitInputTime(input.Now)
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("commit_attention_code = COALESCE(commit_attention_code, ?)", string(input.Code)).
			Set("commit_attention_at = COALESCE(commit_attention_at, ?)", now).
			Set("updated_at = ?", now).
			Where("id = ?", copyID).
			Where("commit_attempt_id = ?", input.AttemptID).
			Where("commit_attempted_at IS NOT NULL").
			Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) ResetCommitAttempt(ctx context.Context, input storagecommit.ResetInput) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil || input.AttemptID == "" {
		return fmt.Errorf("resetting storage commit attempt: %w", ErrInvalidInput)
	}
	return r.mutateAttempt(ctx, input.Copy, "resetting storage commit attempt", func(db bun.IDB, copyID int64) error {
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("status = ?", model.StorageUploadCopyStatusPieceReady).
			Set("commit_ready_at = NULL").
			Set("commit_attempt_id = NULL").
			Set("commit_attempted_at = NULL").
			Set("commit_submission_json = NULL").
			Set("commit_extra_data_hex = NULL").
			Set("commit_transaction_id = NULL").
			Set("commit_confirmed_transaction_id = NULL").
			Set("commit_attention_code = NULL").
			Set("commit_attention_at = NULL").
			Set("last_error = ?", nullableString(input.LastError)).
			Set("updated_at = ?", commitInputTime(input.Now)).
			Where("id = ?", copyID).
			Where("commit_attempt_id = ?", input.AttemptID).
			Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) ReleaseCommitAttempt(ctx context.Context, input storagecommit.ReleaseInput) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil || input.AttemptID == "" {
		return fmt.Errorf("releasing storage commit attempt: %w", ErrInvalidInput)
	}
	return r.mutateAttempt(ctx, input.Copy, "releasing storage commit attempt", func(db bun.IDB, copyID int64) error {
		q := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("status = CASE WHEN status = ? THEN ? ELSE status END", model.StorageUploadCopyStatusCommitting, model.StorageUploadCopyStatusPieceReady).
			Set("commit_attempt_id = NULL").
			Set("commit_attempted_at = NULL").
			Set("commit_submission_json = NULL").
			Set("commit_transaction_id = NULL").
			Set("commit_confirmed_transaction_id = NULL").
			Set("commit_attention_code = NULL").
			Set("commit_attention_at = NULL").
			Set("last_error = NULL").
			Set("updated_at = ?", commitInputTime(input.Now)).
			Where("id = ?", copyID).
			Where("commit_attempt_id = ?", input.AttemptID)
		if input.ClearReadyAt {
			q = q.Set("commit_ready_at = NULL")
		}
		if input.ClearExtraData {
			q = q.Set("commit_extra_data_hex = NULL")
		}
		if input.KnownNotSubmitted {
			q = q.
				Where("commit_transaction_id IS NULL").
				Where("commit_submission_json IS NULL")
		} else if input.AllowAttempted {
			q = q.Where("commit_attention_at IS NOT NULL")
		} else {
			q = q.Where("commit_attempted_at IS NULL")
		}
		res, err := q.Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) ReleaseCommitReservation(
	ctx context.Context,
	input storagecommit.ReservationReleaseInput,
) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil {
		return fmt.Errorf("releasing storage commit reservation: %w", ErrInvalidInput)
	}
	return r.mutateAttempt(ctx, input.Copy, "releasing storage commit reservation", func(db bun.IDB, copyID int64) error {
		q := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("updated_at = ?", commitInputTime(input.Now)).
			Where("id = ?", copyID).
			Where("commit_attempt_id IS NULL").
			Where("commit_attempted_at IS NULL")
		if input.ClearReadyAt {
			q = q.Set("commit_ready_at = NULL")
		}
		if input.ClearExtraData {
			q = q.Set("commit_extra_data_hex = NULL")
		}
		res, err := q.Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) CountActiveCommitAttemptsForDataSet(ctx context.Context, storageDataSetID int64) (int, error) {
	if storageDataSetID <= 0 {
		return 0, fmt.Errorf("counting active storage commit attempts: %w", ErrInvalidInput)
	}
	count, err := r.db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		Where("storage_data_set_id = ?", storageDataSetID).
		Where("commit_attempt_id IS NOT NULL AND commit_attempt_id <> ''").
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting active storage commit attempts: %w", err)
	}
	return count, nil
}

func (r *BunStorageUploadRepo) ListCommitAttention(ctx context.Context, limit int) ([]storagecommit.AttentionRecord, error) {
	type attentionRow struct {
		CopyID        int64     `bun:"copy_id"`
		UploadID      int64     `bun:"upload_id"`
		CopyIndex     int       `bun:"copy_index"`
		DataSetRowID  int64     `bun:"data_set_row_id"`
		ProviderID    string    `bun:"provider_id"`
		DataSetID     string    `bun:"data_set_id"`
		PieceCID      string    `bun:"piece_cid"`
		AttemptID     string    `bun:"attempt_id"`
		TransactionID string    `bun:"transaction_id"`
		Code          string    `bun:"attention_code"`
		AttemptedAt   time.Time `bun:"attempted_at"`
		AttentionAt   time.Time `bun:"attention_at"`
	}
	var rows []attentionRow
	q := r.db.NewSelect().
		TableExpr("storage_upload_copies AS storage_copy").
		ColumnExpr("storage_copy.id AS copy_id").
		ColumnExpr("storage_copy.upload_id").
		ColumnExpr("storage_copy.copy_index").
		ColumnExpr("storage_copy.storage_data_set_id AS data_set_row_id").
		ColumnExpr("CAST(storage_data_set.provider_id AS TEXT) AS provider_id").
		ColumnExpr("COALESCE(CAST(storage_data_set.data_set_id AS TEXT), '') AS data_set_id").
		ColumnExpr("COALESCE(storage_upload.piece_cid, '') AS piece_cid").
		ColumnExpr("storage_copy.commit_attempt_id AS attempt_id").
		ColumnExpr("COALESCE(storage_copy.commit_transaction_id, '') AS transaction_id").
		ColumnExpr("storage_copy.commit_attention_code AS attention_code").
		ColumnExpr("storage_copy.commit_attempted_at AS attempted_at").
		ColumnExpr("storage_copy.commit_attention_at AS attention_at").
		Join("JOIN storage_uploads AS storage_upload ON storage_upload.id = storage_copy.upload_id").
		Join("JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = storage_copy.storage_data_set_id").
		Where("storage_copy.commit_attention_at IS NOT NULL").
		Where("storage_copy.commit_attempt_id IS NOT NULL AND storage_copy.commit_attempt_id <> ''").
		OrderExpr("storage_copy.commit_attention_at ASC").
		OrderExpr("storage_copy.id ASC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing storage confirmation attention: %w", err)
	}
	out := make([]storagecommit.AttentionRecord, 0, len(rows))
	for _, row := range rows {
		code, err := storagecommit.ParseAttentionCode(row.Code)
		if err != nil {
			code = storagecommit.AttentionCode(row.Code)
		}
		out = append(out, storagecommit.AttentionRecord{
			CopyID: row.CopyID, UploadID: row.UploadID, CopyIndex: row.CopyIndex,
			DataSetRowID: row.DataSetRowID, ProviderID: row.ProviderID, DataSetID: row.DataSetID,
			PieceCID: row.PieceCID, AttemptID: row.AttemptID, TransactionID: row.TransactionID,
			Code: code, AttemptedAt: row.AttemptedAt, AttentionAt: row.AttentionAt,
		})
	}
	return out, nil
}

func (r *BunStorageUploadRepo) ReleaseCommitAttention(ctx context.Context, input storagecommit.ManualReleaseInput) error {
	if input.CopyID <= 0 || input.ExpectedAttemptID == "" || !input.AcknowledgePossibleDuplicate {
		return fmt.Errorf("releasing storage confirmation attention: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		initial := new(model.StorageUploadCopy)
		if err := db.NewSelect().Model(initial).Where("id = ?", input.CopyID).Scan(ctx); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if initial.StorageDataSetID == nil {
			return ErrConflict
		}
		identity := storagecommit.CopyIdentity{
			StorageUploadCopyID: initial.ID, UploadID: initial.UploadID, CopyIndex: initial.CopyIndex,
			StorageDataSetID: *initial.StorageDataSetID,
		}
		copyID, _, err := lockCommitCopyFamily(ctx, db, identity)
		if err != nil {
			return err
		}
		copyRow, err := loadCommitCopy(ctx, db, copyID, *initial.StorageDataSetID)
		if err != nil {
			return err
		}
		if copyRow.CommitAttemptID == nil || *copyRow.CommitAttemptID != input.ExpectedAttemptID || copyRow.CommitAttentionAt == nil {
			return ErrConflict
		}
		attemptID := input.ExpectedAttemptID
		res, err := db.NewUpdate().
			Model((*model.StorageUploadCopy)(nil)).
			Set("status = ?", model.StorageUploadCopyStatusPieceReady).
			Set("commit_ready_at = NULL").
			Set("commit_attempt_id = NULL").
			Set("commit_attempted_at = NULL").
			Set("commit_submission_json = NULL").
			Set("commit_extra_data_hex = NULL").
			Set("commit_transaction_id = NULL").
			Set("commit_confirmed_transaction_id = NULL").
			Set("commit_attention_code = NULL").
			Set("commit_attention_at = NULL").
			Set("last_error = NULL").
			Set("updated_at = ?", now).
			Where("id = ?", copyID).
			Where("commit_attempt_id = ?", attemptID).
			Where("commit_attention_at IS NOT NULL").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("releasing storage confirmation attention: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		var recovered int64
		copyIDExpr := taskPayloadCopyIDSQL(db.Dialect().Name())
		uploadIDExpr, copyIndexExpr := runningUploadCopyTaskPayloadExpressions(db.Dialect().Name())
		copyTaskMatch := fmt.Sprintf(
			"(%[1]s = ? OR ((%[1]s IS NULL OR %[1]s = 0) AND %[2]s = ? AND %[3]s = ?))",
			copyIDExpr("task"), uploadIDExpr, copyIndexExpr,
		)
		res, err = db.NewUpdate().
			Model((*model.Task)(nil)).
			Set("status = ?", model.TaskStatusScheduled).
			Set("scheduled_at = ?", now).
			Set("last_error = NULL").
			Set("status_message = NULL").
			Set("wait_reason = NULL").
			Set("completed_at = NULL").
			Where("type = ?", model.TaskTypeUpload).
			Where("status IN (?, ?, ?)", model.TaskStatusQueued, model.TaskStatusScheduled, model.TaskStatusWaiting).
			Where(copyTaskMatch, copyID, copyRow.UploadID, copyRow.CopyIndex).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("waking storage confirmation task: %w", err)
		}
		rows, _ := res.RowsAffected()
		recovered += rows
		res, err = db.NewUpdate().
			Model((*storagereplacement.Item)(nil)).
			Set("status = ?", storagereplacement.ItemStatusCancelled).
			Set("scheduled_at = ?", now).
			Set("last_error = NULL").
			Where("target_copy_id = ?", copyID).
			Where("status <> ?", storagereplacement.ItemStatusCopied).
			Where("claimed_at IS NULL").
			Where(`EXISTS (
				SELECT 1 FROM storage_replacements AS owner_replacement
				WHERE owner_replacement.id = storage_replacement_item.replacement_id
				  AND owner_replacement.status IN (?, ?)
			)`, storagereplacement.StatusCompleted, storagereplacement.StatusSuperseded).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("settling terminal replacement confirmation item: %w", err)
		}
		rows, _ = res.RowsAffected()
		recovered += rows
		res, err = db.NewUpdate().
			Model((*storagereplacement.Item)(nil)).
			Set("status = ?", storagereplacement.ItemStatusFailed).
			Set("scheduled_at = ?", now).
			Set("last_error = NULL").
			Where("target_copy_id = ?", copyID).
			Where("status <> ?", storagereplacement.ItemStatusCopied).
			Where("claimed_at IS NULL").
			Where(`EXISTS (
				SELECT 1 FROM storage_replacements AS owner_replacement
				WHERE owner_replacement.id = storage_replacement_item.replacement_id
				  AND owner_replacement.status IN (?, ?)
			)`, storagereplacement.StatusFailed, storagereplacement.StatusCleanupAttention).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("holding failed replacement confirmation item: %w", err)
		}
		rows, _ = res.RowsAffected()
		recovered += rows
		res, err = db.NewUpdate().
			Model((*storagereplacement.Item)(nil)).
			Set("status = ?", storagereplacement.ItemStatusPending).
			Set("scheduled_at = ?", now).
			Set("last_error = NULL").
			Where("target_copy_id = ?", copyID).
			Where("status <> ?", storagereplacement.ItemStatusCopied).
			Where("claimed_at IS NULL").
			Where(`EXISTS (
				SELECT 1 FROM storage_replacements AS owner_replacement
				WHERE owner_replacement.id = storage_replacement_item.replacement_id
				  AND owner_replacement.status IN (?, ?, ?, ?)
			)`, storagereplacement.StatusPreparingTarget, storagereplacement.StatusMigrating,
				storagereplacement.StatusWaiting, storagereplacement.StatusRetiring).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("waking replacement confirmation item: %w", err)
		}
		rows, _ = res.RowsAffected()
		recovered += rows
		if recovered == 0 {
			return fmt.Errorf("releasing storage confirmation attention without recoverable work: %w", ErrConflict)
		}
		return nil
	})
}

func (r *BunStorageUploadRepo) mutateAttempt(
	ctx context.Context,
	copyIdentity storagecommit.CopyIdentity,
	op string,
	mutate func(bun.IDB, int64) error,
) error {
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		copyID, _, err := lockCommitCopyFamily(ctx, db, copyIdentity)
		if err != nil {
			return err
		}
		return mutate(db, copyID)
	})
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func lockCommitCopyFamily(ctx context.Context, db bun.IDB, identity storagecommit.CopyIdentity) (int64, *model.StorageDataSet, error) {
	uploads, err := lockStorageUploadsByID(ctx, db, []int64{identity.UploadID})
	if err != nil {
		return 0, nil, fmt.Errorf("locking storage upload for commit: %w", err)
	}
	if uploads[identity.UploadID] == nil {
		return 0, nil, fmt.Errorf("locking storage upload for commit: %w", ErrNotFound)
	}
	dataSet := new(model.StorageDataSet)
	err = db.NewRaw(`UPDATE storage_data_sets
		SET updated_at = updated_at
		WHERE id = ?
		RETURNING *`, identity.StorageDataSetID).Scan(ctx, dataSet)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil, ErrNotFound
		}
		return 0, nil, fmt.Errorf("locking storage data set for commit: %w", err)
	}
	copyID, err := slotCopyTarget(ctx, db, identity.StorageUploadCopyID, identity.UploadID, identity.CopyIndex)
	if err != nil {
		return 0, nil, err
	}
	count, err := db.NewSelect().
		Model((*model.StorageUploadCopy)(nil)).
		Where("id = ?", copyID).
		Where("storage_data_set_id = ?", identity.StorageDataSetID).
		Count(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("validating storage commit copy data set: %w", err)
	}
	if count != 1 {
		return 0, nil, ErrConflict
	}
	if identity.RequireEligibleCopy {
		count, err = db.NewSelect().
			Model((*model.StorageUploadCopy)(nil)).
			Where("id = ?", copyID).
			Where("status <> ?", model.StorageUploadCopyStatusFailed).
			Where(liveObjectVersionExistsForUploadSQL(), identity.UploadID, false).
			Count(ctx)
		if err != nil {
			return 0, nil, fmt.Errorf("checking storage commit copy eligibility: %w", err)
		}
		if count != 1 {
			return 0, nil, ErrConflict
		}
	}
	return copyID, dataSet, nil
}

func loadCommitCopy(ctx context.Context, db bun.IDB, copyID, storageDataSetID int64) (*model.StorageUploadCopy, error) {
	copyRow := new(model.StorageUploadCopy)
	err := db.NewSelect().
		Model(copyRow).
		Where("id = ?", copyID).
		Where("storage_data_set_id = ?", storageDataSetID).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("loading storage commit copy: %w", err)
	}
	return copyRow, nil
}

func validateCommitCopyIdentity(identity storagecommit.CopyIdentity) error {
	if identity.StorageUploadCopyID <= 0 || identity.UploadID <= 0 || identity.CopyIndex < 0 || identity.StorageDataSetID <= 0 {
		return ErrInvalidInput
	}
	return nil
}

func validateCommitEvidenceInput(input storagecommit.EvidenceInput, requireSubmission bool) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil ||
		input.AttemptID == "" ||
		input.TransactionID == "" ||
		(requireSubmission && input.SubmissionJSON == "") {
		return fmt.Errorf("recording storage commit evidence: %w", ErrInvalidInput)
	}
	return nil
}

func commitInputTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now()
	}
	return value
}

func attemptedStorageCommitSQL(alias string) string {
	return fmt.Sprintf(
		"(%[1]s.commit_attempt_id IS NOT NULL AND %[1]s.commit_attempt_id <> '' AND %[1]s.commit_attempted_at IS NOT NULL)",
		alias,
	)
}
