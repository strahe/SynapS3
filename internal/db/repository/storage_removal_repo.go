package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/strahe/synaps3/internal/errtext"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagepipeline"
	"github.com/strahe/synaps3/internal/storageremoval"
	"github.com/uptrace/bun"
)

var ErrNoRemovalRoom = errors.New("data set deletion queue has no room")

type BunStorageRemovalRepo struct{ db bun.IDB }

func (r *BunStorageRemovalRepo) GetRemovalRequest(ctx context.Context, id string) (*storageremoval.Request, error) {
	row := new(storageremoval.Request)
	err := r.db.NewSelect().Model(row).Where("request_id = ?", id).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return row, err
}

func (r *BunStorageRemovalRepo) ListRemovalMembers(ctx context.Context, id string) ([]model.StorageCleanupCopy, error) {
	var rows []model.StorageCleanupCopy
	err := r.db.NewSelect().Model(&rows).Where("removal_request_id = ?", id).OrderExpr("id ASC").Scan(ctx)
	return rows, err
}

func (r *BunStorageRemovalRepo) ListRemovalSendPieces(ctx context.Context, id string, sendNo int) ([]storageremoval.SendPiece, error) {
	var rows []storageremoval.SendPiece
	err := r.db.NewSelect().Model(&rows).Where("request_id = ? AND send_no = ?", id, sendNo).OrderExpr("cleanup_copy_id ASC").Scan(ctx)
	return rows, err
}

func (r *BunStorageRemovalRepo) latestSend(ctx context.Context, id, condition string) (*storageremoval.Send, error) {
	row := new(storageremoval.Send)
	err := r.db.NewSelect().Model(row).Where("request_id = ?", id).Where(condition).OrderExpr("send_no DESC").Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	row.Pieces, err = r.ListRemovalSendPieces(ctx, id, row.SendNo)
	return row, err
}

func (r *BunStorageRemovalRepo) LatestExposedSend(ctx context.Context, id string) (*storageremoval.Send, error) {
	return r.latestSend(ctx, id, "unsent_at IS NULL")
}

func (r *BunStorageRemovalRepo) LatestPossiblyAcceptedSend(ctx context.Context, id string) (*storageremoval.Send, error) {
	return r.latestSend(ctx, id, "unsent_at IS NULL AND refused_at IS NULL")
}

func (r *BunStorageRemovalRepo) ListRemovalSends(ctx context.Context, id string) ([]storageremoval.Send, error) {
	var rows []storageremoval.Send
	if err := r.db.NewSelect().Model(&rows).Where("request_id = ?", id).OrderExpr("send_no ASC").Scan(ctx); err != nil {
		return nil, err
	}
	for i := range rows {
		pieces, err := r.ListRemovalSendPieces(ctx, id, rows[i].SendNo)
		if err != nil {
			return nil, err
		}
		rows[i].Pieces = pieces
	}
	return rows, nil
}

func (r *BunStorageRemovalRepo) ListRemovalBatchesForTasks(ctx context.Context, ids []int64) ([]storageremoval.TaskBatch, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var rows []storageremoval.TaskBatch
	err := r.db.NewRaw(`SELECT request.task_id, request.request_id, request.status,
		request.attention_code, request.attention_at, request.last_error,
		data_set.provider_id, data_set.data_set_id,
		(SELECT COUNT(*) FROM storage_cleanup_copies AS member WHERE member.removal_request_id = request.request_id) AS piece_count,
		(SELECT tx_hash FROM storage_removal_sends AS evidence WHERE evidence.request_id = request.request_id AND tx_hash IS NOT NULL ORDER BY send_no DESC LIMIT 1) AS tx_hash
		FROM storage_removal_requests AS request
		JOIN storage_data_sets AS data_set ON data_set.id = request.storage_data_set_id
		WHERE request.task_id IN (?)`, bun.List(ids)).Scan(ctx, &rows)
	return rows, err
}

func removalTime(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now().UTC().Truncate(time.Microsecond)
	}
	return now.UTC().Truncate(time.Microsecond)
}

func removalIDs(ids []int64) []int64 { return slices.Compact(slices.Sorted(slices.Values(ids))) }

// All member-content locks precede the data set and request locks. A joining
// content discovered after the snapshot makes this transaction retry its read.
func (r *BunStorageRemovalRepo) owned(ctx context.Context, id string, taskID int64, statuses []storageremoval.RequestStatus, fn func(bun.IDB, *storageremoval.Request, []model.StorageCleanupCopy) error) error {
	if id == "" || taskID <= 0 {
		return ErrInvalidInput
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		tx := &BunStorageRemovalRepo{db: db}
		request, err := tx.GetRemovalRequest(ctx, id)
		if err != nil {
			return err
		}
		members, err := tx.ListRemovalMembers(ctx, id)
		if err != nil {
			return err
		}
		contentIDs := make([]int64, 0, len(members))
		for _, row := range members {
			contentIDs = append(contentIDs, row.ContentID)
		}
		contentIDs = removalIDs(contentIDs)
		if len(contentIDs) > 0 {
			var contents []model.StorageContent
			if err := db.NewSelect().Model(&contents).Where("id IN (?)", bun.List(contentIDs)).OrderExpr("id ASC").For("NO KEY UPDATE").Scan(ctx); err != nil {
				return err
			}
		}
		if _, err := lockCommitDataSet(ctx, db, request.StorageDataSetID); err != nil {
			return err
		}
		if err := db.NewSelect().Model(request).Where("request_id = ?", id).For("NO KEY UPDATE").Scan(ctx); err != nil {
			return err
		}
		if request.TaskID == nil || *request.TaskID != taskID || len(statuses) > 0 && !slices.Contains(statuses, request.Status) {
			return ErrConflict
		}
		if err := db.NewSelect().Model(&members).Where("removal_request_id = ?", id).OrderExpr("id ASC").For("NO KEY UPDATE").Scan(ctx); err != nil {
			return err
		}
		for _, member := range members {
			if !slices.Contains(contentIDs, member.ContentID) {
				return ErrConflict
			}
		}
		return fn(db, request, members)
	})
}

func wakeRemovalContents(ctx context.Context, db bun.IDB, members []model.StorageCleanupCopy) error {
	ids := make([]int64, 0, len(members))
	for _, row := range members {
		ids = append(ids, row.ContentID)
	}
	if len(ids) == 0 {
		return nil
	}
	var taskIDs []int64
	if err := db.NewSelect().Model((*model.StorageContent)(nil)).Column("cleanup_task_id").Where("id IN (?)", bun.List(removalIDs(ids))).Where("cleanup_task_id IS NOT NULL").Scan(ctx, &taskIDs); err != nil {
		return err
	}
	if len(taskIDs) == 0 {
		return nil
	}
	_, err := (&BunTaskRepo{db: db}).WakePending(ctx, removalIDs(taskIDs))
	return err
}

func saveRemovalRequest(ctx context.Context, db bun.IDB, request *storageremoval.Request, oldStatus storageremoval.RequestStatus, oldTaskID int64) error {
	request.UpdatedAt = removalTime(time.Time{})
	result, err := db.NewUpdate().Model(request).Column("status", "task_id", "sealed_at", "queued_at", "queued_block", "attention_code", "attention_at", "last_error", "updated_at").Where("request_id = ? AND task_id = ? AND status = ?", request.RequestID, oldTaskID, oldStatus).Exec(ctx)
	return requireTaskFenceRows(result, err, "updating removal request")
}

func (r *BunStorageRemovalRepo) JoinOrCreateCollecting(ctx context.Context, input JoinRemovalInput) (*storageremoval.Request, error) {
	if input.ContentID <= 0 || input.Generation <= 0 || input.ContentTaskID <= 0 || input.StorageDataSetID <= 0 || len(input.CopyIDs) == 0 || input.Enqueue == nil || !input.Identity.Complete() {
		return nil, ErrInvalidInput
	}
	var request *storageremoval.Request
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		contents, err := lockStorageContentsByID(ctx, db, []int64{input.ContentID})
		if err != nil {
			return err
		}
		content := contents[input.ContentID]
		if content.CleanupGeneration != input.Generation || content.CleanupTaskID == nil || *content.CleanupTaskID != input.ContentTaskID {
			return ErrConflict
		}
		if _, err := lockCommitDataSet(ctx, db, input.StorageDataSetID); err != nil {
			return err
		}
		request = new(storageremoval.Request)
		err = db.NewSelect().Model(request).Where("storage_data_set_id = ? AND status = ?", input.StorageDataSetID, storageremoval.RequestStatusCollecting).For("NO KEY UPDATE").Scan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			id, err := storageremoval.NewRequestID()
			if err != nil {
				return err
			}
			taskID, err := input.Enqueue(ctx, NewRepositories(db), storageremoval.Input{ExecutionIdentity: input.Identity, RequestID: id})
			if err != nil {
				return err
			}
			now := removalTime(input.Now)
			request = &storageremoval.Request{RequestID: id, StorageDataSetID: input.StorageDataSetID, Status: storageremoval.RequestStatusCollecting, TaskID: &taskID, CreatedAt: now, UpdatedAt: now}
			if _, err := db.NewInsert().Model(request).Exec(ctx); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		var copies []model.StorageCleanupCopy
		if err := db.NewSelect().Model(&copies).Where("id IN (?)", bun.List(removalIDs(input.CopyIDs))).OrderExpr("id ASC").For("NO KEY UPDATE").Scan(ctx); err != nil {
			return err
		}
		if len(copies) != len(removalIDs(input.CopyIDs)) {
			return ErrConflict
		}
		for _, row := range copies {
			if row.ContentID != input.ContentID || row.StorageDataSetID != input.StorageDataSetID || row.Status != model.StorageCleanupCopyStatusPending || row.DeleteTxHash != nil || row.RemovalRequestID != nil && *row.RemovalRequestID != request.RequestID {
				return ErrConflict
			}
		}
		_, err = db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Set("removal_request_id = ?", request.RequestID).Set("updated_at = ?", removalTime(input.Now)).Where("id IN (?)", bun.List(input.CopyIDs)).Exec(ctx)
		if err != nil {
			return err
		}
		_, err = (&BunTaskRepo{db: db}).WakePending(ctx, []int64{*request.TaskID})
		return err
	})
	return request, err
}

func removalReferences(ctx context.Context, db bun.IDB, contentID int64) (bool, error) {
	referenced, err := uploadHasObjectReferences(ctx, db, contentID)
	if err != nil || referenced {
		return referenced, err
	}
	return cleanupHasObjectReferences(ctx, db, contentID)
}

func (r *BunStorageRemovalRepo) SealRemovalRequest(ctx context.Context, input SealRemovalInput) error {
	if input.MaxPieces <= 0 {
		return ErrInvalidInput
	}
	return r.owned(ctx, input.RequestID, input.TaskID, []storageremoval.RequestStatus{storageremoval.RequestStatusCollecting}, func(db bun.IDB, request *storageremoval.Request, members []model.StorageCleanupCopy) error {
		kept := 0
		var dropped []model.StorageCleanupCopy
		for _, row := range members {
			referenced, err := removalReferences(ctx, db, row.ContentID)
			if err != nil {
				return err
			}
			if referenced || kept >= input.MaxPieces {
				dropped = append(dropped, row)
			} else {
				kept++
			}
		}
		if err := detachRemovalMembers(ctx, db, dropped); err != nil {
			return err
		}
		now := removalTime(input.Now)
		request.SealedAt = &now
		request.Status = storageremoval.RequestStatusReady
		if kept == 0 {
			request.Status = storageremoval.RequestStatusAbandoned
			request.TaskID = nil
			message := "No pieces require removal"
			request.LastError = &message
		}
		return saveRemovalRequest(ctx, db, request, storageremoval.RequestStatusCollecting, input.TaskID)
	})
}

func detachRemovalMembers(ctx context.Context, db bun.IDB, members []model.StorageCleanupCopy) error {
	if len(members) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(members))
	for _, row := range members {
		ids = append(ids, row.ID)
	}
	if _, err := db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Set("removal_request_id = NULL").Set("updated_at = ?", removalTime(time.Time{})).Where("id IN (?)", bun.List(ids)).Exec(ctx); err != nil {
		return err
	}
	return wakeRemovalContents(ctx, db, members)
}

func (r *BunStorageRemovalRepo) changeMembers(ctx context.Context, input RemovalMembersInput, status model.StorageCleanupCopyStatus, detach bool) error {
	return r.owned(ctx, input.RequestID, input.TaskID, nil, func(db bun.IDB, request *storageremoval.Request, members []model.StorageCleanupCopy) error {
		tx := &BunStorageRemovalRepo{db: db}
		accepted, err := tx.LatestPossiblyAcceptedSend(ctx, input.RequestID)
		if err != nil {
			return err
		}
		if accepted != nil && (detach || status == model.StorageCleanupCopyStatusFailed) {
			return ErrConflict
		}
		if request.Status.Terminal() {
			return ErrConflict
		}
		var selected []model.StorageCleanupCopy
		for _, row := range members {
			if slices.Contains(input.CopyIDs, row.ID) {
				selected = append(selected, row)
			}
		}
		if len(selected) != len(removalIDs(input.CopyIDs)) {
			return ErrConflict
		}
		for _, row := range selected {
			q := db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Set("updated_at = ?", removalTime(input.Now)).Where("id = ? AND removal_request_id = ?", row.ID, input.RequestID)
			if status != "" {
				q = q.Set("status = ?", status)
			}
			if status == model.StorageCleanupCopyStatusRemoved {
				q = q.Set("removed_at = COALESCE(removed_at, ?)", removalTime(input.Now))
			}
			if status == model.StorageCleanupCopyStatusFailed {
				q = q.Set("last_error = ?", errtext.Redact(input.Message))
			}
			if detach {
				q = q.Set("removal_request_id = NULL")
			}
			if _, err := q.Exec(ctx); err != nil {
				return err
			}
		}
		if err := wakeRemovalContents(ctx, db, selected); err != nil {
			return err
		}
		if detach && len(selected) == len(members) {
			old := request.Status
			request.Status = storageremoval.RequestStatusAbandoned
			request.TaskID = nil
			message := "No pieces require removal"
			request.LastError = &message
			request.AttentionCode, request.AttentionAt = nil, nil
			return saveRemovalRequest(ctx, db, request, old, input.TaskID)
		}
		return nil
	})
}

func (r *BunStorageRemovalRepo) DetachMembers(ctx context.Context, input RemovalMembersInput) error {
	return r.changeMembers(ctx, input, "", true)
}

func (r *BunStorageRemovalRepo) MarkMembersRemoved(ctx context.Context, input RemovalMembersInput) error {
	return r.changeMembers(ctx, input, model.StorageCleanupCopyStatusRemoved, input.Detach)
}

func (r *BunStorageRemovalRepo) MarkMemberFailed(ctx context.Context, input RemovalMembersInput) error {
	return r.changeMembers(ctx, input, model.StorageCleanupCopyStatusFailed, true)
}

func (r *BunStorageRemovalRepo) AdmitRemovalSend(ctx context.Context, input AdmitRemovalSendInput) error {
	if input.SendNo <= 0 || len(input.CopyIDs) == 0 || input.QueueLength < 0 || input.QueueLength > 2000 || input.SnapshotBlock > math.MaxInt64 {
		return ErrInvalidInput
	}
	return r.owned(ctx, input.RequestID, input.TaskID, []storageremoval.RequestStatus{storageremoval.RequestStatusReady, storageremoval.RequestStatusSubmitted}, func(db bun.IDB, request *storageremoval.Request, members []model.StorageCleanupCopy) error {
		tx := &BunStorageRemovalRepo{db: db}
		var latest int
		if err := db.NewSelect().Model((*storageremoval.Send)(nil)).ColumnExpr("COALESCE(MAX(send_no), 0)").Where("request_id = ?", input.RequestID).Scan(ctx, &latest); err != nil {
			return err
		}
		if input.SendNo != latest+1 {
			return ErrConflict
		}
		exposed, err := tx.LatestExposedSend(ctx, input.RequestID)
		if err != nil {
			return err
		}
		accepted, err := tx.LatestPossiblyAcceptedSend(ctx, input.RequestID)
		if err != nil {
			return err
		}
		selected := make([]model.StorageCleanupCopy, 0, len(input.CopyIDs))
		for _, row := range members {
			if slices.Contains(input.CopyIDs, row.ID) && row.Status != model.StorageCleanupCopyStatusRemoved {
				selected = append(selected, row)
			}
		}
		if len(selected) != len(removalIDs(input.CopyIDs)) {
			return ErrConflict
		}
		if exposed != nil {
			for _, row := range selected {
				if !slices.ContainsFunc(exposed.Pieces, func(piece storageremoval.SendPiece) bool {
					return piece.CleanupCopyID == row.ID && piece.PieceID.Equal(row.PieceID)
				}) {
					return ErrConflict
				}
			}
		}
		if accepted == nil {
			for _, row := range members {
				referenced, err := removalReferences(ctx, db, row.ContentID)
				if err != nil {
					return err
				}
				if referenced {
					return ErrConflict
				}
			}
		}
		var inFlight int
		if err := db.NewRaw(`SELECT COUNT(DISTINCT (piece.request_id, piece.cleanup_copy_id))
			FROM storage_removal_requests AS batch
			JOIN storage_removal_sends AS send ON send.request_id = batch.request_id AND send.refused_at IS NULL AND send.unsent_at IS NULL AND send.receipt_status IS DISTINCT FROM 0
			JOIN storage_removal_send_pieces AS piece ON piece.request_id = batch.request_id AND piece.send_no = send.send_no
			WHERE batch.storage_data_set_id = ? AND batch.request_id <> ?
			AND (batch.status = 'submitted' OR (batch.status = 'queued' AND batch.queued_block > ?))`, request.StorageDataSetID, request.RequestID, int64(input.SnapshotBlock)).Scan(ctx, &inFlight); err != nil {
			return err
		}
		if input.QueueLength+inFlight+len(selected) > 2000 {
			return ErrNoRemovalRoom
		}
		now := removalTime(input.AttemptedAt)
		send := &storageremoval.Send{RequestID: input.RequestID, SendNo: input.SendNo, TaskID: &input.TaskID, AttemptedAt: &now, CreatedAt: now, UpdatedAt: now}
		if err := insertRemovalSend(ctx, db, send, selected); err != nil {
			return err
		}
		old := request.Status
		request.Status = storageremoval.RequestStatusSubmitted
		request.AttentionCode, request.AttentionAt = nil, nil
		return saveRemovalRequest(ctx, db, request, old, input.TaskID)
	})
}

func insertRemovalSend(ctx context.Context, db bun.IDB, send *storageremoval.Send, members []model.StorageCleanupCopy) error {
	if _, err := db.NewInsert().Model(send).Exec(ctx); err != nil {
		return err
	}
	pieces := make([]storageremoval.SendPiece, 0, len(members))
	for _, row := range members {
		pieces = append(pieces, storageremoval.SendPiece{RequestID: send.RequestID, SendNo: send.SendNo, CleanupCopyID: row.ID, PieceID: row.PieceID, CreatedAt: send.CreatedAt})
	}
	if len(pieces) == 0 {
		return ErrInvalidInput
	}
	_, err := db.NewInsert().Model(&pieces).Exec(ctx)
	return err
}

func (r *BunStorageRemovalRepo) sendEvidence(ctx context.Context, input RemovalSendEvidenceInput, kind string) error {
	return r.owned(ctx, input.RequestID, input.TaskID, []storageremoval.RequestStatus{storageremoval.RequestStatusSubmitted, storageremoval.RequestStatusReady}, func(db bun.IDB, request *storageremoval.Request, _ []model.StorageCleanupCopy) error {
		send := new(storageremoval.Send)
		if err := db.NewSelect().Model(send).Where("request_id = ? AND send_no = ?", input.RequestID, input.SendNo).For("NO KEY UPDATE").Scan(ctx); err != nil {
			return err
		}
		now := removalTime(input.Now)
		switch kind {
		case "submission":
			if input.TxHash == "" || send.UnsentAt != nil || send.RefusedAt != nil || send.TxHash != nil && *send.TxHash != input.TxHash {
				return ErrConflict
			}
			send.TxHash = &input.TxHash
		case "refused", "unsent":
			if send.TxHash != nil || send.ReceiptStatus != nil || kind == "refused" && send.UnsentAt != nil || kind == "unsent" && send.RefusedAt != nil {
				return ErrConflict
			}
			if kind == "refused" && send.RefusedAt == nil {
				send.RefusedAt = &now
			}
			if kind == "unsent" && send.UnsentAt == nil {
				send.UnsentAt = &now
			}
		case "receipt":
			if input.ReceiptStatus == nil || (*input.ReceiptStatus != 0 && *input.ReceiptStatus != 1) || input.ConfirmedTxHash == "" || send.RefusedAt != nil || send.UnsentAt != nil {
				return ErrInvalidInput
			}
			if send.ReceiptStatus != nil && *send.ReceiptStatus != *input.ReceiptStatus || send.ConfirmedTxHash != nil && *send.ConfirmedTxHash != input.ConfirmedTxHash {
				return ErrConflict
			}
			send.ReceiptStatus = input.ReceiptStatus
			send.ConfirmedTxHash = &input.ConfirmedTxHash
		default:
			return ErrInvalidInput
		}
		if input.Message != "" {
			text := errtext.Redact(input.Message)
			send.LastError = &text
			request.LastError = &text
		}
		send.UpdatedAt = now
		if _, err := db.NewUpdate().Model(send).Column("tx_hash", "confirmed_tx_hash", "receipt_status", "refused_at", "unsent_at", "last_error", "updated_at").Where("request_id = ? AND send_no = ?", input.RequestID, input.SendNo).Exec(ctx); err != nil {
			return err
		}
		old := request.Status
		if kind == "refused" || kind == "unsent" {
			accepted, err := (&BunStorageRemovalRepo{db: db}).LatestPossiblyAcceptedSend(ctx, input.RequestID)
			if err != nil {
				return err
			}
			if accepted == nil {
				request.Status = storageremoval.RequestStatusReady
			}
		}
		return saveRemovalRequest(ctx, db, request, old, input.TaskID)
	})
}

func (r *BunStorageRemovalRepo) RecordSendSubmission(ctx context.Context, input RemovalSendEvidenceInput) error {
	return r.sendEvidence(ctx, input, "submission")
}

func (r *BunStorageRemovalRepo) RecordSendRefused(ctx context.Context, input RemovalSendEvidenceInput) error {
	return r.sendEvidence(ctx, input, "refused")
}

func (r *BunStorageRemovalRepo) RecordSendUnsent(ctx context.Context, input RemovalSendEvidenceInput) error {
	return r.sendEvidence(ctx, input, "unsent")
}

func (r *BunStorageRemovalRepo) RecordSendReceipt(ctx context.Context, input RemovalSendEvidenceInput) error {
	return r.sendEvidence(ctx, input, "receipt")
}

func (r *BunStorageRemovalRepo) SetRemovalAttention(ctx context.Context, input RemovalAttentionInput) error {
	if !input.Code.Valid() {
		return ErrInvalidInput
	}
	return r.owned(ctx, input.RequestID, input.TaskID, []storageremoval.RequestStatus{storageremoval.RequestStatusReady, storageremoval.RequestStatusSubmitted, storageremoval.RequestStatusQueued}, func(db bun.IDB, request *storageremoval.Request, _ []model.StorageCleanupCopy) error {
		old := request.Status
		code := string(input.Code)
		now := removalTime(input.Now)
		if request.AttentionCode == nil || *request.AttentionCode != code {
			request.AttentionAt = &now
		}
		request.AttentionCode = &code
		if input.Message != "" {
			message := errtext.Redact(input.Message)
			request.LastError = &message
		}
		return saveRemovalRequest(ctx, db, request, old, input.TaskID)
	})
}

func (r *BunStorageRemovalRepo) ClearRemovalAttention(ctx context.Context, input RemovalRequestInput) error {
	return r.owned(ctx, input.RequestID, input.TaskID, nil, func(db bun.IDB, request *storageremoval.Request, _ []model.StorageCleanupCopy) error {
		old := request.Status
		request.AttentionCode, request.AttentionAt = nil, nil
		return saveRemovalRequest(ctx, db, request, old, input.TaskID)
	})
}

func (r *BunStorageRemovalRepo) MarkRemovalReverted(ctx context.Context, input RemovalRequestInput) error {
	return r.SetRemovalAttention(ctx, RemovalAttentionInput{RequestID: input.RequestID, TaskID: input.TaskID, Code: storageremoval.AttentionTransactionReverted, Message: input.Message, Now: input.Now})
}

func (r *BunStorageRemovalRepo) queued(ctx context.Context, input RemovalQueuedInput, withoutSend bool) error {
	if input.QueuedBlock > math.MaxInt64 {
		return ErrInvalidInput
	}
	statuses := []storageremoval.RequestStatus{storageremoval.RequestStatusSubmitted}
	if withoutSend {
		statuses = []storageremoval.RequestStatus{storageremoval.RequestStatusReady}
	}
	return r.owned(ctx, input.RequestID, input.TaskID, statuses, func(db bun.IDB, request *storageremoval.Request, _ []model.StorageCleanupCopy) error {
		old := request.Status
		now := removalTime(input.QueuedAt)
		block := int64(input.QueuedBlock)
		request.Status = storageremoval.RequestStatusQueued
		request.QueuedAt = &now
		request.QueuedBlock = &block
		request.AttentionCode, request.AttentionAt = nil, nil
		return saveRemovalRequest(ctx, db, request, old, input.TaskID)
	})
}

func (r *BunStorageRemovalRepo) MarkRemovalQueued(ctx context.Context, input RemovalQueuedInput) error {
	return r.queued(ctx, input, false)
}

func (r *BunStorageRemovalRepo) MarkQueuedWithoutSend(ctx context.Context, input RemovalQueuedInput) error {
	return r.queued(ctx, input, true)
}

func (r *BunStorageRemovalRepo) SettleRemovalRequest(ctx context.Context, input RemovalRequestInput) error {
	return r.owned(ctx, input.RequestID, input.TaskID, []storageremoval.RequestStatus{storageremoval.RequestStatusQueued}, func(db bun.IDB, request *storageremoval.Request, members []model.StorageCleanupCopy) error {
		for _, row := range members {
			if row.Status != model.StorageCleanupCopyStatusRemoved {
				return ErrConflict
			}
		}
		request.Status = storageremoval.RequestStatusSettled
		request.TaskID = nil
		request.AttentionCode, request.AttentionAt = nil, nil
		return saveRemovalRequest(ctx, db, request, storageremoval.RequestStatusQueued, input.TaskID)
	})
}

func (r *BunStorageRemovalRepo) AbandonRemovalRequest(ctx context.Context, input RemovalRequestInput) error {
	return r.owned(ctx, input.RequestID, input.TaskID, []storageremoval.RequestStatus{storageremoval.RequestStatusCollecting, storageremoval.RequestStatusReady}, func(db bun.IDB, request *storageremoval.Request, members []model.StorageCleanupCopy) error {
		accepted, err := (&BunStorageRemovalRepo{db: db}).LatestPossiblyAcceptedSend(ctx, input.RequestID)
		if err != nil {
			return err
		}
		if accepted != nil {
			return ErrConflict
		}
		if err := detachRemovalMembers(ctx, db, members); err != nil {
			return err
		}
		old := request.Status
		request.Status = storageremoval.RequestStatusAbandoned
		request.TaskID = nil
		request.AttentionCode, request.AttentionAt = nil, nil
		message := input.Message
		if message == "" {
			message = "Removal stopped before submission"
		}
		message = errtext.Redact(message)
		request.LastError = &message
		return saveRemovalRequest(ctx, db, request, old, input.TaskID)
	})
}

func (r *BunStorageRemovalRepo) TransferRemovalTaskOwner(ctx context.Context, id string, oldTaskID, newTaskID int64) error {
	return r.owned(ctx, id, oldTaskID, nil, func(db bun.IDB, request *storageremoval.Request, _ []model.StorageCleanupCopy) error {
		if err := validateTransferredTask(ctx, db, oldTaskID, newTaskID); err != nil {
			return err
		}
		task, err := taskForBinding(ctx, db, newTaskID, model.TaskSubjectStorageRemovalRequest, id, model.TaskTypeStorageRemoval)
		if err != nil {
			return err
		}
		var input storageremoval.Input
		if err := json.Unmarshal(task.Input, &input); err != nil || input.RequestID != id {
			return ErrConflict
		}
		old := request.Status
		request.TaskID = &newTaskID
		if request.AttentionCode != nil {
			code, err := storageremoval.ParseAttentionCode(*request.AttentionCode)
			if err == nil && code.IsStop() {
				request.AttentionCode, request.AttentionAt = nil, nil
			}
		}
		return saveRemovalRequest(ctx, db, request, old, oldTaskID)
	})
}

func (r *BunStorageCleanupRepo) ReauthorizeFailedCopies(ctx context.Context, contentID, generation, taskID int64) error {
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		rows, err := lockStorageContentsByID(ctx, db, []int64{contentID})
		if err != nil {
			return err
		}
		content := rows[contentID]
		if content.CleanupTaskID == nil || *content.CleanupTaskID != taskID || content.CleanupGeneration != generation {
			return ErrConflict
		}
		_, err = db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Set("status = ?", model.StorageCleanupCopyStatusPending).Set("updated_at = ?", removalTime(time.Time{})).Where("content_id = ? AND status = ? AND removal_request_id IS NULL", contentID, model.StorageCleanupCopyStatusFailed).Exec(ctx)
		return err
	})
}

func (r *BunStorageRemovalRepo) AdoptLegacyCleanupCopy(ctx context.Context, input AdoptLegacyCleanupCopyInput) (*storageremoval.Request, error) {
	if input.Enqueue == nil || !input.Identity.Complete() {
		return nil, ErrInvalidInput
	}
	var request *storageremoval.Request
	err := runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		contents, err := lockStorageContentsByID(ctx, db, []int64{input.ContentID})
		if err != nil {
			return err
		}
		content := contents[input.ContentID]
		if content.CleanupTaskID == nil || *content.CleanupTaskID != input.ContentTaskID || content.CleanupGeneration != input.Generation {
			return ErrConflict
		}
		copyRow := new(model.StorageCleanupCopy)
		if err := db.NewSelect().Model(copyRow).Where("id = ? AND content_id = ?", input.CopyID, input.ContentID).Scan(ctx); err != nil {
			return err
		}
		if _, err := lockCommitDataSet(ctx, db, copyRow.StorageDataSetID); err != nil {
			return err
		}
		if err := db.NewSelect().Model(copyRow).Where("id = ?", input.CopyID).For("NO KEY UPDATE").Scan(ctx); err != nil {
			return err
		}
		if copyRow.RemovalRequestID != nil {
			request, err = (&BunStorageRemovalRepo{db: db}).GetRemovalRequest(ctx, *copyRow.RemovalRequestID)
			return err
		}
		if copyRow.Status != model.StorageCleanupCopyStatusPending && copyRow.Status != model.StorageCleanupCopyStatusDeleteScheduled {
			return ErrConflict
		}
		var sourceTaskID *int64
		var attemptedAt *time.Time
		if !input.AttemptedAt.IsZero() {
			var history []model.Task
			if err := db.NewRaw(`WITH RECURSIVE history AS (SELECT * FROM tasks WHERE id = ? UNION ALL SELECT parent.* FROM tasks AS parent JOIN history ON parent.id = history.retry_of_task_id) SELECT * FROM history ORDER BY id ASC`, input.ContentTaskID).Scan(ctx, &history); err != nil {
				return err
			}
			for _, task := range history {
				var checkpoint struct {
					storagepipeline.ExecutionIdentity
					CopyID      int64     `json:"copy_id"`
					AttemptedAt time.Time `json:"attempted_at"`
				}
				var runtime model.TaskRuntime
				if json.Unmarshal(task.Checkpoint, &checkpoint) == nil && json.Unmarshal(task.Runtime, &runtime) == nil && checkpoint.CopyID == input.CopyID && checkpoint.AttemptedAt.Equal(input.AttemptedAt) && runtime.OperationKey == fmt.Sprintf("cleanup:%d", input.CopyID) && runtime.LastAdmittedAttempt > 0 {
					if err := checkpoint.Match(input.Identity); err != nil {
						return err
					}
					taskID := task.ID
					sourceTaskID = &taskID
					at := removalTime(input.AttemptedAt)
					attemptedAt = &at
					break
				}
			}
		}
		oldHash := copyRow.DeleteTxHash
		if oldHash == nil && input.TransactionHash == "" && sourceTaskID == nil {
			return ErrTaskDataCorrupted
		}
		id, err := storageremoval.NewRequestID()
		if err != nil {
			return err
		}
		taskID, err := input.Enqueue(ctx, NewRepositories(db), storageremoval.Input{ExecutionIdentity: input.Identity, RequestID: id})
		if err != nil {
			return err
		}
		now := removalTime(input.Now)
		sealed := now
		if attemptedAt != nil {
			sealed = *attemptedAt
		} else if copyRow.ScheduledAt != nil {
			sealed = *copyRow.ScheduledAt
		}
		request = &storageremoval.Request{RequestID: id, StorageDataSetID: copyRow.StorageDataSetID, Status: storageremoval.RequestStatusSubmitted, TaskID: &taskID, SealedAt: &sealed, CreatedAt: now, UpdatedAt: now}
		if _, err := db.NewInsert().Model(request).Exec(ctx); err != nil {
			return err
		}
		separate := oldHash != nil && ((input.RetryOfTxHash == *oldHash && !input.AttemptedAt.IsZero()) || (input.TransactionHash != "" && input.TransactionHash != *oldHash))
		if separate && input.TransactionHash == "" && sourceTaskID == nil {
			return ErrTaskDataCorrupted
		}
		send := &storageremoval.Send{RequestID: id, SendNo: 1, TxHash: oldHash, TaskID: sourceTaskID, AttemptedAt: attemptedAt, CreatedAt: now, UpdatedAt: now}
		if send.TxHash == nil && input.TransactionHash != "" {
			send.TxHash = &input.TransactionHash
		}
		if separate {
			send.TaskID, send.AttemptedAt = nil, nil
		}
		if err := insertRemovalSend(ctx, db, send, []model.StorageCleanupCopy{*copyRow}); err != nil {
			return err
		}
		if separate {
			unknown := &storageremoval.Send{RequestID: id, SendNo: 2, TaskID: sourceTaskID, AttemptedAt: attemptedAt, CreatedAt: now, UpdatedAt: now}
			if input.TransactionHash != "" {
				unknown.TxHash = &input.TransactionHash
			}
			if err := insertRemovalSend(ctx, db, unknown, []model.StorageCleanupCopy{*copyRow}); err != nil {
				return err
			}
		}
		_, err = db.NewUpdate().Model((*model.StorageCleanupCopy)(nil)).Set("removal_request_id = ?", id).Where("id = ? AND removal_request_id IS NULL", input.CopyID).Exec(ctx)
		return err
	})
	return request, err
}
