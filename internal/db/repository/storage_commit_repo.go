package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	"github.com/uptrace/bun"
)

func (r *BunStorageContentRepo) ReserveCommitAttempt(
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
		currentGeneration := dataSet.IsCurrent && dataSet.Status == model.StorageDataSetStatusReady
		if !currentGeneration && dataSet.Status != model.StorageDataSetStatusDraining {
			return fmt.Errorf("reserving storage commit attempt on unavailable generation: %w", ErrConflict)
		}
		copyRow, err := loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
		if err != nil {
			return err
		}
		if copyRow.Status != model.StorageCopyStatusPieceReady {
			return fmt.Errorf("reserving storage commit attempt for copy %d in status %s: %w", copyID, copyRow.Status, ErrConflict)
		}
		if copyRow.CommitReadyAt == nil {
			res, updateErr := db.NewUpdate().
				Model((*model.StorageCopy)(nil)).
				Set("commit_ready_at = ?", now).
				Set("updated_at = ?", now).
				Where("id = ?", copyID).
				Where("commit_ready_at IS NULL").
				Exec(ctx)
			if updateErr != nil {
				return fmt.Errorf("preparing storage commit attempt: %w", updateErr)
			}
			if rows, _ := res.RowsAffected(); rows != 1 {
				return fmt.Errorf("preparing storage commit attempt: %w", ErrConflict)
			}
			copyRow, err = loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
			if err != nil {
				return err
			}
		}
		if copyRow.CommitAttemptID != nil {
			out.State = storagecommit.ReservationAcquired
			out.Copy = *copyRow
			return nil
		}
		active, err := countActiveCommitAttemptsForDataSet(ctx, db, input.Copy.StorageDataSetID)
		if err != nil {
			return err
		}
		if active >= storagecommit.MaxActiveAttemptsPerDataSet {
			held, countErr := countCommitAttentionAttemptsForDataSet(ctx, db, input.Copy.StorageDataSetID)
			if countErr != nil {
				return countErr
			}
			out.State = storagecommit.ReservationWaiting
			out.Copy = *copyRow
			out.AttentionHeld = held
			return nil
		}
		var headID int64
		err = db.NewSelect().
			Model((*model.StorageCopy)(nil)).
			Column("id").
			Where("storage_data_set_id = ?", input.Copy.StorageDataSetID).
			Where("status = ?", model.StorageCopyStatusPieceReady).
			Where("commit_ready_at IS NOT NULL").
			Where(`NOT EXISTS (
				SELECT 1 FROM storage_commit_attempts AS unresolved_attempt
				WHERE unresolved_attempt.content_id = storage_copy.content_id
				  AND unresolved_attempt.storage_data_set_id = storage_copy.storage_data_set_id
				  AND unresolved_attempt.resolved_at IS NULL
			)`).
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
		attempt := &storagecommit.Attempt{
			AttemptID:        input.AttemptID,
			ContentID:        copyRow.ContentID,
			StorageDataSetID: copyRow.StorageDataSetID,
			Status:           storagecommit.AttemptStatusReserved,
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		if _, err := db.NewInsert().Model(attempt).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("reserving storage commit attempt: %w", ErrConflict)
			}
			return fmt.Errorf("reserving storage commit attempt: %w", err)
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

func (r *BunStorageContentRepo) MarkCommitAttempted(
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
		attempt, err := loadCommitAttempt(ctx, db, input.Copy, input.AttemptID)
		if err != nil {
			return err
		}
		if attempt.Status == storagecommit.AttemptStatusAttempted {
			copyRow, loadErr := loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
			if loadErr != nil {
				return loadErr
			}
			if attempt.ExtraDataHex == nil || *attempt.ExtraDataHex != input.ExtraDataHex ||
				copyRow.Status != model.StorageCopyStatusCommitting ||
				copyRow.CommitExtraDataHex == nil || *copyRow.CommitExtraDataHex != input.ExtraDataHex {
				return fmt.Errorf("marking storage commit attempted with conflicting evidence: %w", ErrConflict)
			}
			out.Copy = *copyRow
			return nil
		}
		if attempt.Status != storagecommit.AttemptStatusReserved {
			return fmt.Errorf("marking storage commit attempted with resolved token: %w", ErrConflict)
		}
		copyRow, err := loadCommitCopy(ctx, db, copyID, input.Copy.StorageDataSetID)
		if err != nil {
			return err
		}
		if copyRow.Status != model.StorageCopyStatusPieceReady ||
			(copyRow.CommitExtraDataHex != nil && *copyRow.CommitExtraDataHex != input.ExtraDataHex) {
			return fmt.Errorf("marking storage commit attempted for incompatible copy: %w", ErrConflict)
		}
		res, err := db.NewUpdate().
			Model((*storagecommit.Attempt)(nil)).
			Set("status = ?", storagecommit.AttemptStatusAttempted).
			Set("extra_data_hex = ?", input.ExtraDataHex).
			Set("attempted_at = ?", now).
			Set("updated_at = ?", now).
			Where("attempt_id = ?", input.AttemptID).
			Where("content_id = ? AND storage_data_set_id = ?", input.Copy.ContentID, input.Copy.StorageDataSetID).
			Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusReserved).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("marking storage commit attempted: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("marking storage commit attempted: %w", ErrConflict)
		}
		res, err = db.NewUpdate().
			Model((*model.StorageCopy)(nil)).
			Set("status = ?", model.StorageCopyStatusCommitting).
			Set("commit_extra_data_hex = COALESCE(commit_extra_data_hex, ?)", input.ExtraDataHex).
			Set("updated_at = ?", now).
			Where("id = ? AND status = ?", copyID, model.StorageCopyStatusPieceReady).
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("projecting storage commit attempt: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return fmt.Errorf("projecting storage commit attempt: %w", ErrConflict)
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

func (r *BunStorageContentRepo) RecordCommitTransaction(ctx context.Context, input storagecommit.EvidenceInput) error {
	if err := validateCommitEvidenceInput(input, false); err != nil {
		return err
	}
	return r.mutateAttempt(ctx, input.Copy, "recording storage commit transaction", func(db bun.IDB, _ int64) error {
		res, err := db.NewUpdate().
			Model((*storagecommit.Attempt)(nil)).
			Set("transaction_id = COALESCE(transaction_id, ?)", input.TransactionID).
			Set("updated_at = ?", commitInputTime(input.Now)).
			Where("attempt_id = ?", input.AttemptID).
			Where("content_id = ? AND storage_data_set_id = ?", input.Copy.ContentID, input.Copy.StorageDataSetID).
			Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusAttempted).
			Where("(transaction_id IS NULL OR transaction_id = ?)", input.TransactionID).
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

func (r *BunStorageContentRepo) RecordCommitSubmission(ctx context.Context, input storagecommit.EvidenceInput) error {
	if err := validateCommitEvidenceInput(input, true); err != nil {
		return err
	}
	return r.mutateAttempt(ctx, input.Copy, "recording storage commit submission", func(db bun.IDB, _ int64) error {
		res, err := db.NewUpdate().
			Model((*storagecommit.Attempt)(nil)).
			Set("transaction_id = COALESCE(transaction_id, ?)", input.TransactionID).
			Set("submission_json = COALESCE(submission_json, ?)", input.SubmissionJSON).
			Set("updated_at = ?", commitInputTime(input.Now)).
			Where("attempt_id = ?", input.AttemptID).
			Where("content_id = ? AND storage_data_set_id = ?", input.Copy.ContentID, input.Copy.StorageDataSetID).
			Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusAttempted).
			Where("(transaction_id IS NULL OR transaction_id = ?)", input.TransactionID).
			Where("(submission_json IS NULL OR submission_json = ?)", input.SubmissionJSON).
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

func (r *BunStorageContentRepo) MarkCommitAttention(ctx context.Context, input storagecommit.AttentionInput) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil || input.AttemptID == "" || !input.Code.Valid() {
		return fmt.Errorf("marking storage commit attention: %w", ErrInvalidInput)
	}
	return r.mutateAttempt(ctx, input.Copy, "marking storage commit attention", func(db bun.IDB, _ int64) error {
		now := commitInputTime(input.Now)
		res, err := db.NewUpdate().
			Model((*storagecommit.Attempt)(nil)).
			Set("attention_code = COALESCE(attention_code, ?)", string(input.Code)).
			Set("attention_at = COALESCE(attention_at, ?)", now).
			Set("updated_at = ?", now).
			Where("attempt_id = ?", input.AttemptID).
			Where("content_id = ? AND storage_data_set_id = ?", input.Copy.ContentID, input.Copy.StorageDataSetID).
			Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusAttempted).
			Where("(attention_code IS NULL OR attention_code = ?)", string(input.Code)).
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

func (r *BunStorageContentRepo) ResetCommitAttempt(ctx context.Context, input storagecommit.ResetInput) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil || input.AttemptID == "" || input.LastError == "" {
		return fmt.Errorf("resetting storage commit attempt: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	return r.mutateAttempt(ctx, input.Copy, "resetting storage commit attempt", func(db bun.IDB, copyID int64) error {
		res, err := db.NewUpdate().
			Model((*storagecommit.Attempt)(nil)).
			Set("status = ?", storagecommit.AttemptStatusRejected).
			Set("last_error = ?", nullableString(input.LastError)).
			Set("resolved_at = ?", now).
			Set("updated_at = ?", now).
			Where("attempt_id = ?", input.AttemptID).
			Where("content_id = ? AND storage_data_set_id = ?", input.Copy.ContentID, input.Copy.StorageDataSetID).
			Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusAttempted).
			Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return projectResolvedCommitAttempt(ctx, db, copyID, now, true, true, nullableString(input.LastError))
	})
}

func (r *BunStorageContentRepo) ReleaseCommitAttempt(ctx context.Context, input storagecommit.ReleaseInput) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil || input.AttemptID == "" || !input.Reason.Valid() {
		return fmt.Errorf("releasing storage commit attempt: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	return r.mutateAttempt(ctx, input.Copy, "releasing storage commit attempt", func(db bun.IDB, copyID int64) error {
		q := db.NewUpdate().
			Model((*storagecommit.Attempt)(nil)).
			Set("status = ?", storagecommit.AttemptStatusReleased).
			Set("release_reason = ?", string(input.Reason)).
			Set("resolved_at = ?", now).
			Set("updated_at = ?", now).
			Where("attempt_id = ?", input.AttemptID).
			Where("content_id = ? AND storage_data_set_id = ?", input.Copy.ContentID, input.Copy.StorageDataSetID).
			Where("resolved_at IS NULL")
		if input.KnownNotSubmitted {
			q = q.
				Where("status IN (?)", bun.List([]storagecommit.AttemptStatus{
					storagecommit.AttemptStatusReserved,
					storagecommit.AttemptStatusAttempted,
				})).
				Where("transaction_id IS NULL AND submission_json IS NULL")
		} else {
			q = q.Where("status = ?", storagecommit.AttemptStatusReserved)
		}
		res, err := q.Exec(ctx)
		if err != nil {
			return err
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		return projectResolvedCommitAttempt(ctx, db, copyID, now, input.ClearReadyAt, input.ClearExtraData, nil)
	})
}

func (r *BunStorageContentRepo) ReleaseCommitReservation(
	ctx context.Context,
	input storagecommit.ReservationReleaseInput,
) error {
	if err := validateCommitCopyIdentity(input.Copy); err != nil {
		return fmt.Errorf("releasing storage commit reservation: %w", ErrInvalidInput)
	}
	return r.mutateAttempt(ctx, input.Copy, "releasing storage commit reservation", func(db bun.IDB, copyID int64) error {
		q := db.NewUpdate().
			Model((*model.StorageCopy)(nil)).
			Set("updated_at = ?", commitInputTime(input.Now)).
			Where("id = ?", copyID).
			Where("status = ?", model.StorageCopyStatusPieceReady).
			Where(`NOT EXISTS (
				SELECT 1 FROM storage_commit_attempts AS unresolved_attempt
				WHERE unresolved_attempt.content_id = storage_copy.content_id
				  AND unresolved_attempt.storage_data_set_id = storage_copy.storage_data_set_id
				  AND unresolved_attempt.resolved_at IS NULL
			)`)
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

func (r *BunStorageContentRepo) CountActiveCommitAttemptsForDataSet(ctx context.Context, storageDataSetID int64) (int, error) {
	if storageDataSetID <= 0 {
		return 0, fmt.Errorf("counting active storage commit attempts: %w", ErrInvalidInput)
	}
	return countActiveCommitAttemptsForDataSet(ctx, r.db, storageDataSetID)
}

func countActiveCommitAttemptsForDataSet(ctx context.Context, db bun.IDB, storageDataSetID int64) (int, error) {
	count, err := db.NewSelect().
		Model((*storagecommit.Attempt)(nil)).
		Where("storage_data_set_id = ? AND resolved_at IS NULL", storageDataSetID).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting active storage commit attempts: %w", err)
	}
	return count, nil
}

func countCommitAttentionAttemptsForDataSet(ctx context.Context, db bun.IDB, storageDataSetID int64) (int, error) {
	count, err := db.NewSelect().
		Model((*storagecommit.Attempt)(nil)).
		Where("storage_data_set_id = ? AND resolved_at IS NULL", storageDataSetID).
		Where("attention_at IS NOT NULL").
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting storage commit attempts held for attention: %w", err)
	}
	return count, nil
}

func (r *BunStorageContentRepo) ListCommitAttention(ctx context.Context, limit int) ([]storagecommit.AttentionRecord, error) {
	type attentionRow struct {
		CopyID        int64     `bun:"copy_id"`
		ContentID     int64     `bun:"content_id"`
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
		TableExpr("storage_commit_attempts AS commit_attempt").
		ColumnExpr("storage_copy.id AS copy_id").
		ColumnExpr("commit_attempt.content_id").
		ColumnExpr("storage_copy.copy_index").
		ColumnExpr("commit_attempt.storage_data_set_id AS data_set_row_id").
		ColumnExpr("CAST(storage_data_set.provider_id AS TEXT) AS provider_id").
		ColumnExpr("COALESCE(CAST(storage_data_set.data_set_id AS TEXT), '') AS data_set_id").
		ColumnExpr("COALESCE(storage_content.piece_cid, '') AS piece_cid").
		ColumnExpr("commit_attempt.attempt_id").
		ColumnExpr("COALESCE(commit_attempt.transaction_id, '') AS transaction_id").
		ColumnExpr("commit_attempt.attention_code").
		ColumnExpr("commit_attempt.attempted_at").
		ColumnExpr("commit_attempt.attention_at").
		Join("JOIN storage_copies AS storage_copy ON storage_copy.content_id = commit_attempt.content_id AND storage_copy.storage_data_set_id = commit_attempt.storage_data_set_id").
		Join("JOIN storage_contents AS storage_content ON storage_content.id = commit_attempt.content_id").
		Join("JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = commit_attempt.storage_data_set_id").
		Where("commit_attempt.status = ? AND commit_attempt.resolved_at IS NULL", storagecommit.AttemptStatusAttempted).
		Where("commit_attempt.attention_at IS NOT NULL").
		OrderExpr("commit_attempt.attention_at ASC").
		OrderExpr("storage_copy.id ASC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing storage confirmation attention: %w", err)
	}
	out := make([]storagecommit.AttentionRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, storagecommit.AttentionRecord{
			CopyID: row.CopyID, ContentID: row.ContentID, CopyIndex: row.CopyIndex,
			DataSetRowID: row.DataSetRowID, ProviderID: row.ProviderID, DataSetID: row.DataSetID,
			PieceCID: row.PieceCID, AttemptID: row.AttemptID, TransactionID: row.TransactionID,
			Code: storagecommit.AttentionCode(row.Code), AttemptedAt: row.AttemptedAt, AttentionAt: row.AttentionAt,
		})
	}
	return out, nil
}

func (r *BunStorageContentRepo) ReleaseCommitAttention(ctx context.Context, input storagecommit.ManualReleaseInput) error {
	if input.CopyID <= 0 || input.ExpectedAttemptID == "" || !input.AcknowledgePossibleDuplicate {
		return fmt.Errorf("releasing storage confirmation attention: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		initial := new(model.StorageCopy)
		if err := db.NewSelect().Model(initial).Where("id = ?", input.CopyID).Scan(ctx); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		identity := storagecommit.CopyIdentity{
			StorageCopyID:    initial.ID,
			ContentID:        initial.ContentID,
			CopyIndex:        initial.CopyIndex,
			StorageDataSetID: initial.StorageDataSetID,
		}
		copyID, _, err := lockCommitCopyFamily(ctx, db, identity)
		if err != nil {
			return err
		}
		res, err := db.NewUpdate().
			Model((*storagecommit.Attempt)(nil)).
			Set("status = ?", storagecommit.AttemptStatusReleased).
			Set("release_reason = ?", string(storagecommit.ReleaseManualDuplicateAck)).
			Set("resolved_at = ?", now).
			Set("updated_at = ?", now).
			Where("attempt_id = ?", input.ExpectedAttemptID).
			Where("content_id = ? AND storage_data_set_id = ?", initial.ContentID, initial.StorageDataSetID).
			Where("status = ? AND resolved_at IS NULL", storagecommit.AttemptStatusAttempted).
			Where("attention_at IS NOT NULL").
			Exec(ctx)
		if err != nil {
			return fmt.Errorf("releasing storage confirmation attention: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows != 1 {
			return ErrConflict
		}
		if err := projectResolvedCommitAttempt(ctx, db, copyID, now, true, true, nil); err != nil {
			return fmt.Errorf("releasing storage confirmation attention: %w", err)
		}
		return resumeCommitTaskAfterAttentionRelease(ctx, db, initial.ActiveTaskID)
	})
}

func resumeCommitTaskAfterAttentionRelease(ctx context.Context, db bun.IDB, taskID *int64) error {
	if taskID == nil {
		return nil
	}
	tasks := &BunTaskRepo{db: db}
	taskRow, err := tasks.GetByID(ctx, *taskID)
	if err != nil {
		return fmt.Errorf("loading released storage commit task: %w", err)
	}
	if taskRow == nil || taskRow.Type != model.TaskTypeStorageCommit {
		return fmt.Errorf("released storage commit has no matching task: %w", ErrConflict)
	}
	switch taskRow.Status {
	case model.TaskStatusPending, model.TaskStatusRunning:
		return nil
	case model.TaskStatusFailed:
		if err := tasks.RetryFailed(ctx, taskRow.ID); err != nil {
			return fmt.Errorf("resuming released storage commit task: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("released storage commit task is terminal: %w", ErrConflict)
	}
}

func (r *BunStorageContentRepo) mutateAttempt(
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
	uploads, err := lockStorageContentsByID(ctx, db, []int64{identity.ContentID})
	if err != nil {
		return 0, nil, fmt.Errorf("locking storage upload for commit: %w", err)
	}
	if uploads[identity.ContentID] == nil {
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
	copyID, err := slotCopyTarget(ctx, db, identity.StorageCopyID, identity.ContentID, identity.CopyIndex)
	if err != nil {
		return 0, nil, err
	}
	count, err := db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Where("id = ?", copyID).
		Where("content_id = ? AND storage_data_set_id = ?", identity.ContentID, identity.StorageDataSetID).
		Count(ctx)
	if err != nil {
		return 0, nil, fmt.Errorf("validating storage commit copy data set: %w", err)
	}
	if count != 1 {
		return 0, nil, ErrConflict
	}
	if identity.RequireEligibleCopy {
		count, err = db.NewSelect().
			Model((*model.StorageCopy)(nil)).
			Where("id = ?", copyID).
			Where("status <> ?", model.StorageCopyStatusFailed).
			Where(liveObjectVersionExistsForUploadSQL(), identity.ContentID, false).
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

func loadCommitCopy(ctx context.Context, db bun.IDB, copyID, storageDataSetID int64) (*model.StorageCopy, error) {
	copyRow := new(model.StorageCopy)
	q := db.NewSelect().Model(copyRow)
	projectActiveCommitAttempt(q, "storage_copy")
	err := q.
		Where("storage_copy.id = ?", copyID).
		Where("storage_copy.storage_data_set_id = ?", storageDataSetID).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("loading storage commit copy: %w", err)
	}
	return copyRow, nil
}

func projectActiveCommitAttempt(q *bun.SelectQuery, copyAlias string) {
	q.ColumnExpr(copyAlias + ".*").
		ColumnExpr("active_commit_attempt.attempt_id AS commit_attempt_id").
		ColumnExpr("active_commit_attempt.attempted_at AS commit_attempted_at").
		ColumnExpr("active_commit_attempt.transaction_id AS commit_transaction_id").
		ColumnExpr("active_commit_attempt.submission_json AS commit_submission_json").
		ColumnExpr("active_commit_attempt.confirmed_transaction_id AS commit_confirmed_transaction_id").
		ColumnExpr("active_commit_attempt.attention_code AS commit_attention_code").
		ColumnExpr("active_commit_attempt.attention_at AS commit_attention_at").
		Join("LEFT JOIN storage_commit_attempts AS active_commit_attempt ON active_commit_attempt.content_id = " + copyAlias + ".content_id AND active_commit_attempt.storage_data_set_id = " + copyAlias + ".storage_data_set_id AND active_commit_attempt.resolved_at IS NULL")
}

func loadCommitAttempt(
	ctx context.Context,
	db bun.IDB,
	identity storagecommit.CopyIdentity,
	attemptID string,
) (*storagecommit.Attempt, error) {
	attempt := new(storagecommit.Attempt)
	err := db.NewSelect().
		Model(attempt).
		Where("attempt_id = ?", attemptID).
		Where("content_id = ? AND storage_data_set_id = ?", identity.ContentID, identity.StorageDataSetID).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrConflict
		}
		return nil, fmt.Errorf("loading storage commit attempt: %w", err)
	}
	return attempt, nil
}

func projectResolvedCommitAttempt(
	ctx context.Context,
	db bun.IDB,
	copyID int64,
	now time.Time,
	clearReadyAt bool,
	clearExtraData bool,
	lastError *string,
) error {
	q := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("status = CASE WHEN status = ? THEN ? ELSE status END", model.StorageCopyStatusCommitting, model.StorageCopyStatusPieceReady).
		Set("last_error = ?", lastError).
		Set("updated_at = ?", now).
		Where("id = ?", copyID)
	if clearReadyAt {
		q = q.Set("commit_ready_at = NULL")
	}
	if clearExtraData {
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
}

func validateCommitCopyIdentity(identity storagecommit.CopyIdentity) error {
	if identity.StorageCopyID <= 0 || identity.ContentID <= 0 || identity.CopyIndex < 0 || identity.StorageDataSetID <= 0 {
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
	return fmt.Sprintf(`EXISTS (
		SELECT 1 FROM storage_commit_attempts AS attempted_commit
		WHERE attempted_commit.content_id = %[1]s.content_id
		  AND attempted_commit.storage_data_set_id = %[1]s.storage_data_set_id
		  AND attempted_commit.status = 'attempted'
		  AND attempted_commit.resolved_at IS NULL
	)`, alias)
}
