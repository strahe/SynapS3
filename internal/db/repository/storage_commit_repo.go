package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/storagecommit"
	idtypes "github.com/strahe/synaps3/internal/types"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
)

// A commit request moves through its lifecycle only under its own task: every
// mutation below names the request and the task that drives it and changes
// the request only from the status the caller observed. Locks are taken in one
// order everywhere — member contents by ascending id, the data set, the
// request, then its copies — so two writers never wait on each other in a
// cycle.

// JoinCommitRequestInput adds one transferred copy to a collecting request of
// its data set.
type JoinCommitRequestInput struct {
	CopyID           int64
	StorageDataSetID int64
	MaxPieces        int
	Now              time.Time
}

// JoinCollectingCommitRequest adds the copy to the oldest collecting request of
// its data set that has room and a live task. It returns the request and its
// member count, or ErrNotFound when no such request exists.
func (r *BunStorageContentRepo) JoinCollectingCommitRequest(ctx context.Context, input JoinCommitRequestInput) (string, int, error) {
	if input.CopyID <= 0 || input.StorageDataSetID <= 0 || input.MaxPieces < 1 {
		return "", 0, fmt.Errorf("joining storage commit request: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	var requestID string
	var members int
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		var candidates []string
		if err := db.NewSelect().
			TableExpr("storage_commit_requests AS commit_request").
			Column("commit_request.request_id").
			Join("JOIN tasks AS commit_task ON commit_task.id = commit_request.task_id").
			Where("commit_request.storage_data_set_id = ?", input.StorageDataSetID).
			Where("commit_request.status = ?", storagecommit.RequestStatusCollecting).
			Where("commit_task.status IN (?)", bun.List([]model.TaskStatus{model.TaskStatusPending, model.TaskStatusRunning})).
			OrderExpr("commit_request.created_at ASC, commit_request.request_id ASC").
			Scan(ctx, &candidates); err != nil {
			return fmt.Errorf("selecting collecting storage commit request: %w", err)
		}
		for _, candidate := range candidates {
			if _, err := lockCommitRequest(ctx, db, candidate, storagecommit.RequestStatusCollecting); err != nil {
				if errors.Is(err, ErrConflict) {
					continue
				}
				return err
			}
			count, err := countCommitMembers(ctx, db, candidate)
			if err != nil {
				return err
			}
			if count >= input.MaxPieces {
				continue
			}
			if err := attachCopyToCommitRequest(ctx, db, input.CopyID, input.StorageDataSetID, candidate, now); err != nil {
				return err
			}
			requestID, members = candidate, count+1
			return nil
		}
		return ErrNotFound
	})
	if err != nil {
		return "", 0, fmt.Errorf("joining storage commit request: %w", err)
	}
	return requestID, members, nil
}

// CreateCommitRequestInput starts a collecting request driven by TaskID.
type CreateCommitRequestInput struct {
	RequestID        string
	TaskID           int64
	StorageDataSetID int64
	CopyIDs          []int64
	Now              time.Time
}

// CreateCollectingCommitRequest inserts a collecting request and adds the
// copies to it.
func (r *BunStorageContentRepo) CreateCollectingCommitRequest(ctx context.Context, input CreateCommitRequestInput) error {
	if input.RequestID == "" || input.TaskID <= 0 || input.StorageDataSetID <= 0 || len(input.CopyIDs) == 0 {
		return fmt.Errorf("creating storage commit request: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		taskID := input.TaskID
		request := &storagecommit.Request{
			RequestID: input.RequestID, StorageDataSetID: input.StorageDataSetID,
			Status: storagecommit.RequestStatusCollecting, TaskID: &taskID,
			CreatedAt: now, UpdatedAt: now,
		}
		if _, err := db.NewInsert().Model(request).Exec(ctx); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("creating storage commit request: %w", ErrConflict)
			}
			return fmt.Errorf("creating storage commit request: %w", err)
		}
		for _, copyID := range input.CopyIDs {
			if err := attachCopyToCommitRequest(ctx, db, copyID, input.StorageDataSetID, input.RequestID, now); err != nil {
				return fmt.Errorf("creating storage commit request: %w", err)
			}
		}
		return nil
	})
}

func attachCopyToCommitRequest(ctx context.Context, db bun.IDB, copyID, storageDataSetID int64, requestID string, now time.Time) error {
	res, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("commit_request_id = ?", requestID).
		Set("commit_ready_at = COALESCE(commit_ready_at, ?)", now).
		Set("updated_at = ?", now).
		Where("id = ? AND storage_data_set_id = ?", copyID, storageDataSetID).
		Where("status = ?", model.StorageCopyStatusPieceReady).
		Where("commit_request_id IS NULL AND commit_position IS NULL").
		Exec(ctx)
	return requireCommitRows(res, err, "adding storage copy to commit request")
}

// CreatePullCommitRequestInput records the single-piece request a Pull sends
// to its target provider. It is signed, so it starts ready.
type CreatePullCommitRequestInput struct {
	RequestID        string
	TaskID           int64
	CopyID           int64
	ContentID        int64
	StorageDataSetID int64
	PieceCID         string
	ExtraDataHex     string
	Now              time.Time
}

func createPullCommitRequest(ctx context.Context, db bun.IDB, input CreatePullCommitRequestInput) error {
	if input.RequestID == "" || input.TaskID <= 0 || input.CopyID <= 0 || input.ContentID <= 0 ||
		input.StorageDataSetID <= 0 || input.PieceCID == "" || input.ExtraDataHex == "" {
		return fmt.Errorf("recording pull commit request: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	taskID, extra := input.TaskID, input.ExtraDataHex
	request := &storagecommit.Request{
		RequestID: input.RequestID, StorageDataSetID: input.StorageDataSetID,
		Status: storagecommit.RequestStatusReady, TaskID: &taskID,
		PieceCount: 1, ExtraDataHex: &extra, SealedAt: &now,
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := db.NewInsert().Model(request).Exec(ctx); err != nil {
		return fmt.Errorf("recording pull commit request: %w", err)
	}
	piece := &storagecommit.RequestPiece{
		RequestID: input.RequestID, Position: 0, ContentID: input.ContentID,
		StorageDataSetID: input.StorageDataSetID, PieceCID: input.PieceCID, CreatedAt: now,
	}
	if _, err := db.NewInsert().Model(piece).Exec(ctx); err != nil {
		return fmt.Errorf("recording pull commit request piece: %w", err)
	}
	res, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("commit_request_id = ?", input.RequestID).
		Set("commit_position = 0").
		Set("updated_at = ?", now).
		Where("id = ? AND content_id = ? AND storage_data_set_id = ?", input.CopyID, input.ContentID, input.StorageDataSetID).
		Where("status = ?", model.StorageCopyStatusPending).
		Where("commit_request_id IS NULL").
		Exec(ctx)
	return requireCommitRows(res, err, "binding pull commit request")
}

// GetCommitRequest loads one request.
func (r *BunStorageContentRepo) GetCommitRequest(ctx context.Context, requestID string) (*storagecommit.Request, error) {
	request := new(storagecommit.Request)
	if err := r.db.NewSelect().Model(request).Where("request_id = ?", requestID).Scan(ctx); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("loading storage commit request: %w", err)
	}
	return request, nil
}

// ListCommitRequestPieces returns a sealed request's pieces in position order.
func (r *BunStorageContentRepo) ListCommitRequestPieces(ctx context.Context, requestID string) ([]storagecommit.RequestPiece, error) {
	var pieces []storagecommit.RequestPiece
	if err := r.db.NewSelect().Model(&pieces).
		Where("request_id = ?", requestID).
		OrderExpr("position ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing storage commit request pieces: %w", err)
	}
	return pieces, nil
}

// CommitRequestMember is a copy of a request together with its content's
// piece CID.
type CommitRequestMember struct {
	model.StorageCopy
	PieceCID string `bun:"member_piece_cid"`
}

// ListCommitRequestMembers returns the copies that belong to a request: sealed
// members in position order, then collecting members in the order they became
// ready.
func (r *BunStorageContentRepo) ListCommitRequestMembers(ctx context.Context, requestID string) ([]CommitRequestMember, error) {
	var members []CommitRequestMember
	q := r.db.NewSelect().Model(&members).ModelTableExpr("storage_copies AS storage_copy")
	projectCommitRequest(q, "storage_copy")
	if err := q.
		ColumnExpr("COALESCE(member_content.piece_cid, '') AS member_piece_cid").
		Join("JOIN storage_contents AS member_content ON member_content.id = storage_copy.content_id").
		Where("storage_copy.commit_request_id = ?", requestID).
		OrderExpr("CASE WHEN storage_copy.commit_position IS NULL THEN 1 ELSE 0 END, storage_copy.commit_position ASC, storage_copy.commit_ready_at ASC, storage_copy.id ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing storage commit request members: %w", err)
	}
	return members, nil
}

// CommitQueueState is what a collecting request needs to decide when to seal.
type CommitQueueState struct {
	Submitted int
	// ReadyHead is the ready request that may be sent next, if any.
	ReadyHead         string
	TransfersInFlight bool
	Draining          bool
}

// CommitQueueState reads the data set's registration queue at now.
func (r *BunStorageContentRepo) CommitQueueState(ctx context.Context, storageDataSetID int64, now time.Time) (CommitQueueState, error) {
	var out CommitQueueState
	submitted, err := countSubmittedCommitRequests(ctx, r.db, storageDataSetID)
	if err != nil {
		return out, err
	}
	out.Submitted = submitted
	head, err := eligibleReadyCommitRequest(ctx, r.db, storageDataSetID, now)
	if err != nil {
		return out, err
	}
	out.ReadyHead = head
	inFlight, err := r.db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Join("JOIN tasks AS transfer_task ON transfer_task.id = storage_copy.active_task_id").
		Where("storage_copy.storage_data_set_id = ?", storageDataSetID).
		Where("storage_copy.status = ?", model.StorageCopyStatusPending).
		Where("transfer_task.status IN (?)", bun.List([]model.TaskStatus{model.TaskStatusPending, model.TaskStatusRunning})).
		Exists(ctx)
	if err != nil {
		return out, fmt.Errorf("checking storage transfers in flight: %w", err)
	}
	out.TransfersInFlight = inFlight
	var status model.StorageDataSetStatus
	if err := r.db.NewSelect().Model((*model.StorageDataSet)(nil)).Column("status").
		Where("id = ?", storageDataSetID).Scan(ctx, &status); err != nil {
		return out, fmt.Errorf("loading storage data set status: %w", err)
	}
	out.Draining = status == model.StorageDataSetStatusDraining
	return out, nil
}

// SealMember is one copy a request was signed for.
type SealMember struct {
	CopyID    int64
	ContentID int64
	PieceCID  string
}

// SealCommitRequestInput freezes a collecting request into the signed set.
type SealCommitRequestInput struct {
	RequestID string
	TaskID    int64
	// Members are the copies the request was signed for, in position order.
	Members      []SealMember
	ExtraDataHex string
	Now          time.Time
}

// SealCommitRequest records the request's signature and its members'
// positions. Every signed member must still be waiting in the request; copies
// that joined after the members were read are returned to the caller, outside
// any request, so they can join the next one.
func (r *BunStorageContentRepo) SealCommitRequest(ctx context.Context, input SealCommitRequestInput) ([]int64, error) {
	if input.RequestID == "" || input.TaskID <= 0 || len(input.Members) == 0 || input.ExtraDataHex == "" {
		return nil, fmt.Errorf("sealing storage commit request: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	var spilled []int64
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockCommitMemberContents(ctx, db, input.RequestID); err != nil {
			return err
		}
		request, err := lockOwnedCommitRequest(ctx, db, input.RequestID, input.TaskID, storagecommit.RequestStatusCollecting)
		if err != nil {
			return err
		}
		var current []model.StorageCopy
		if err := db.NewSelect().Model(&current).
			Where("commit_request_id = ?", input.RequestID).
			Scan(ctx); err != nil {
			return fmt.Errorf("loading storage commit request members: %w", err)
		}
		byID := make(map[int64]model.StorageCopy, len(current))
		for _, copyRow := range current {
			byID[copyRow.ID] = copyRow
		}
		signed := make(map[int64]bool, len(input.Members))
		for position, member := range input.Members {
			copyRow, ok := byID[member.CopyID]
			if !ok || copyRow.ContentID != member.ContentID || copyRow.Status != model.StorageCopyStatusPieceReady ||
				copyRow.CommitPosition != nil || signed[member.CopyID] || member.PieceCID == "" {
				return ErrConflict
			}
			signed[member.CopyID] = true
			piece := &storagecommit.RequestPiece{
				RequestID: input.RequestID, Position: position, ContentID: member.ContentID,
				StorageDataSetID: request.StorageDataSetID, PieceCID: member.PieceCID, CreatedAt: now,
			}
			if _, err := db.NewInsert().Model(piece).Exec(ctx); err != nil {
				return fmt.Errorf("recording storage commit request piece: %w", err)
			}
			res, err := db.NewUpdate().
				Model((*model.StorageCopy)(nil)).
				Set("commit_position = ?", position).
				Set("status = ?", model.StorageCopyStatusCommitting).
				Set("updated_at = ?", now).
				Where("id = ? AND commit_request_id = ?", member.CopyID, input.RequestID).
				Where("status = ? AND commit_position IS NULL", model.StorageCopyStatusPieceReady).
				Exec(ctx)
			if err := requireCommitRows(res, err, "positioning storage commit request member"); err != nil {
				return err
			}
		}
		for _, copyRow := range current {
			if signed[copyRow.ID] {
				continue
			}
			res, err := db.NewUpdate().
				Model((*model.StorageCopy)(nil)).
				Set("commit_request_id = NULL").
				Set("updated_at = ?", now).
				Where("id = ? AND commit_request_id = ? AND commit_position IS NULL", copyRow.ID, input.RequestID).
				Exec(ctx)
			if err := requireCommitRows(res, err, "moving a late storage copy out of a sealed request"); err != nil {
				return err
			}
			spilled = append(spilled, copyRow.ID)
		}
		extra := input.ExtraDataHex
		res, err := db.NewUpdate().
			Model((*storagecommit.Request)(nil)).
			Set("status = ?", storagecommit.RequestStatusReady).
			Set("piece_count = ?", len(input.Members)).
			Set("extra_data_hex = ?", extra).
			Set("sealed_at = ?", now).
			Set("updated_at = ?", now).
			Where("request_id = ? AND task_id = ? AND status = ?", input.RequestID, input.TaskID, storagecommit.RequestStatusCollecting).
			Exec(ctx)
		return requireCommitRows(res, err, "sealing storage commit request")
	})
	if err != nil {
		return nil, fmt.Errorf("sealing storage commit request: %w", err)
	}
	slices.Sort(spilled)
	return spilled, nil
}

// BeginCommitSubmissionInput moves a ready request to submitted before its
// first send of this period.
type BeginCommitSubmissionInput struct {
	RequestID string
	TaskID    int64
	Now       time.Time
}

// BeginCommitSubmission records that the request is about to be sent. It
// refuses with storagecommit.ErrNotEligible unless the request is the oldest
// eligible ready request of a data set that has room and still takes commits.
func (r *BunStorageContentRepo) BeginCommitSubmission(ctx context.Context, input BeginCommitSubmissionInput) error {
	if input.RequestID == "" || input.TaskID <= 0 {
		return fmt.Errorf("beginning storage commit submission: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		var storageDataSetID int64
		if err := db.NewSelect().Model((*storagecommit.Request)(nil)).Column("storage_data_set_id").
			Where("request_id = ?", input.RequestID).Scan(ctx, &storageDataSetID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		dataSet, err := lockCommitDataSet(ctx, db, storageDataSetID)
		if err != nil {
			return err
		}
		writable := dataSet.IsCurrent && dataSet.Status == model.StorageDataSetStatusReady
		if !writable && dataSet.Status != model.StorageDataSetStatusDraining {
			return storagecommit.ErrNotEligible
		}
		if _, err := lockOwnedCommitRequest(ctx, db, input.RequestID, input.TaskID, storagecommit.RequestStatusReady); err != nil {
			return err
		}
		submitted, err := countSubmittedCommitRequests(ctx, db, storageDataSetID)
		if err != nil {
			return err
		}
		if submitted >= storagecommit.MaxSubmittedRequestsPerDataSet {
			return storagecommit.ErrNotEligible
		}
		head, err := eligibleReadyCommitRequest(ctx, db, storageDataSetID, now)
		if err != nil {
			return err
		}
		if head != input.RequestID {
			return storagecommit.ErrNotEligible
		}
		res, err := db.NewUpdate().
			Model((*storagecommit.Request)(nil)).
			Set("status = ?", storagecommit.RequestStatusSubmitted).
			Set("sends = 1").
			Set("first_sent_at = COALESCE(first_sent_at, ?)", now).
			Set("submitted_at = ?", now).
			Set("last_sent_at = ?", now).
			Set("retry_at = NULL").
			Set("transaction_id = NULL").
			Set("status_url = NULL").
			Set("updated_at = ?", now).
			Where("request_id = ? AND task_id = ? AND status = ?", input.RequestID, input.TaskID, storagecommit.RequestStatusReady).
			Exec(ctx)
		if err := requireCommitRows(res, err, "beginning storage commit submission"); err != nil {
			return err
		}
		// Room may remain; the next request in line must not sleep through it.
		return wakeCommitQueue(ctx, db, storageDataSetID, now)
	})
	if err != nil {
		return fmt.Errorf("beginning storage commit submission: %w", err)
	}
	return nil
}

// CommitSendInput names one send of a submitted request: Sends is the count it
// was recorded with.
type CommitSendInput struct {
	RequestID string
	TaskID    int64
	Sends     int
	Now       time.Time
}

// RecordCommitResend records that the request is about to be sent again.
func (r *BunStorageContentRepo) RecordCommitResend(ctx context.Context, input CommitSendInput) error {
	if input.RequestID == "" || input.TaskID <= 0 || input.Sends < 2 {
		return fmt.Errorf("recording storage commit resend: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	res, err := r.db.NewUpdate().
		Model((*storagecommit.Request)(nil)).
		Set("sends = ?", input.Sends).
		Set("last_sent_at = ?", now).
		Set("updated_at = ?", now).
		Where("request_id = ? AND task_id = ? AND status = ?", input.RequestID, input.TaskID, storagecommit.RequestStatusSubmitted).
		Where("sends = ?", input.Sends-1).
		Exec(ctx)
	return requireCommitRows(res, err, "recording storage commit resend")
}

// CommitSubmissionInput is the provider's receipt for one send.
type CommitSubmissionInput struct {
	CommitSendInput
	TransactionID string
	StatusURL     string
}

// RecordCommitSubmission keeps the receipt of the latest send. The first
// receipt answers whatever attention an unacknowledged send was flagged with.
func (r *BunStorageContentRepo) RecordCommitSubmission(ctx context.Context, input CommitSubmissionInput) error {
	if input.RequestID == "" || input.TaskID <= 0 || input.Sends < 1 || input.TransactionID == "" || input.StatusURL == "" {
		return fmt.Errorf("recording storage commit submission: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	res, err := r.db.NewUpdate().
		Model((*storagecommit.Request)(nil)).
		Set("attention_code = CASE WHEN transaction_id IS NULL THEN NULL ELSE attention_code END").
		Set("attention_at = CASE WHEN transaction_id IS NULL THEN NULL ELSE attention_at END").
		Set("transaction_id = ?", input.TransactionID).
		Set("status_url = ?", input.StatusURL).
		Set("updated_at = ?", now).
		Where("request_id = ? AND task_id = ? AND status = ?", input.RequestID, input.TaskID, storagecommit.RequestStatusSubmitted).
		Where("sends = ?", input.Sends).
		Exec(ctx)
	return requireCommitRows(res, err, "recording storage commit submission")
}

// CommitSubmitFailureInput keeps the provider's reply to a send whose outcome
// stayed unknown.
type CommitSubmitFailureInput struct {
	CommitSendInput
	Message string
}

func (r *BunStorageContentRepo) RecordCommitSubmitFailure(ctx context.Context, input CommitSubmitFailureInput) error {
	if input.RequestID == "" || input.TaskID <= 0 || strings.TrimSpace(input.Message) == "" {
		return fmt.Errorf("recording storage commit submit failure: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	res, err := r.db.NewUpdate().
		Model((*storagecommit.Request)(nil)).
		Set("submit_error = ?", input.Message).
		Set("updated_at = ?", now).
		Where("request_id = ? AND task_id = ? AND status = ?", input.RequestID, input.TaskID, storagecommit.RequestStatusSubmitted).
		Where("sends = ?", input.Sends).
		Exec(ctx)
	return requireCommitRows(res, err, "recording storage commit submit failure")
}

// ReturnCommitRequestInput sends a submitted request back to ready after the
// period's only send provably produced no transaction.
type ReturnCommitRequestInput struct {
	RequestID string
	TaskID    int64
	// Refused counts a provider refusal; false means the send never left
	// this process.
	Refused     bool
	SubmitError string
	RetryAt     time.Time
	Now         time.Time
}

func (r *BunStorageContentRepo) ReturnCommitRequestToReady(ctx context.Context, input ReturnCommitRequestInput) error {
	if input.RequestID == "" || input.TaskID <= 0 {
		return fmt.Errorf("returning storage commit request: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		request, err := lockOwnedCommitRequest(ctx, db, input.RequestID, input.TaskID, storagecommit.RequestStatusSubmitted)
		if err != nil {
			return err
		}
		if request.Sends != 1 || request.TransactionID != nil {
			return ErrConflict
		}
		q := db.NewUpdate().
			Model((*storagecommit.Request)(nil)).
			Set("status = ?", storagecommit.RequestStatusReady).
			Set("sends = 0").
			Set("submitted_at = NULL").
			Set("transaction_id = NULL").
			Set("status_url = NULL").
			Set("attention_code = NULL").
			Set("attention_at = NULL").
			Set("retry_at = ?", nullableTime(input.RetryAt)).
			Set("updated_at = ?", now).
			Where("request_id = ? AND task_id = ? AND status = ?", input.RequestID, input.TaskID, storagecommit.RequestStatusSubmitted)
		if input.SubmitError != "" {
			q = q.Set("submit_error = ?", input.SubmitError)
		}
		if input.Refused {
			q = q.Set("refusals = refusals + 1")
		} else if request.FirstSentAt != nil && request.SubmittedAt != nil && request.FirstSentAt.Equal(*request.SubmittedAt) {
			// A send that never left the process does not make the request
			// one that was sent.
			q = q.Set("first_sent_at = NULL")
		}
		res, err := q.Exec(ctx)
		if err := requireCommitRows(res, err, "returning storage commit request to ready"); err != nil {
			return err
		}
		return wakeCommitQueue(ctx, db, request.StorageDataSetID, now)
	})
}

// DropCommitEvidence forgets a receipt that did not describe the request. The
// request stays submitted: only its nonce can tell whether a send landed.
func (r *BunStorageContentRepo) DropCommitEvidence(ctx context.Context, requestID string, taskID int64, lastError string, now time.Time) error {
	if requestID == "" || taskID <= 0 || lastError == "" {
		return fmt.Errorf("dropping storage commit evidence: %w", ErrInvalidInput)
	}
	res, err := r.db.NewUpdate().
		Model((*storagecommit.Request)(nil)).
		Set("transaction_id = NULL").
		Set("status_url = NULL").
		Set("last_error = ?", lastError).
		Set("updated_at = ?", commitInputTime(now)).
		Where("request_id = ? AND task_id = ? AND status = ?", requestID, taskID, storagecommit.RequestStatusSubmitted).
		Exec(ctx)
	return requireCommitRows(res, err, "dropping storage commit evidence")
}

// MarkCommitRequestAttention flags a submitted request. The first code it is
// flagged with is kept until a receipt or a settlement clears it.
func (r *BunStorageContentRepo) MarkCommitRequestAttention(ctx context.Context, requestID string, taskID int64, code storagecommit.AttentionCode, now time.Time) error {
	if requestID == "" || taskID <= 0 || !code.Valid() {
		return fmt.Errorf("marking storage commit attention: %w", ErrInvalidInput)
	}
	at := commitInputTime(now)
	res, err := r.db.NewUpdate().
		Model((*storagecommit.Request)(nil)).
		Set("attention_code = COALESCE(attention_code, ?)", string(code)).
		Set("attention_at = COALESCE(attention_at, ?)", at).
		Set("updated_at = ?", at).
		Where("request_id = ? AND task_id = ? AND status = ?", requestID, taskID, storagecommit.RequestStatusSubmitted).
		Exec(ctx)
	return requireCommitRows(res, err, "marking storage commit attention")
}

// ConfirmCommitRequestInput settles a request the chain proved.
type ConfirmCommitRequestInput struct {
	RequestID              string
	TaskID                 int64
	ConfirmedTransactionID string
	FirstPieceID           idtypes.OnChainID
	// RetrievalURLs are the members' retrieval URLs in position order.
	RetrievalURLs []string
	Now           time.Time
}

// ConfirmCommitRequest settles the request and commits every member, including
// one still being transferred again: the chain already holds its piece. It
// returns the members as they were before settlement. A replay of the same
// confirmation returns the members again and changes nothing.
func (r *BunStorageContentRepo) ConfirmCommitRequest(ctx context.Context, input ConfirmCommitRequestInput) ([]model.StorageCopy, error) {
	if input.RequestID == "" || input.TaskID <= 0 || len(input.RetrievalURLs) == 0 {
		return nil, fmt.Errorf("confirming storage commit request: %w", ErrInvalidInput)
	}
	now := commitInputTime(input.Now)
	var members []model.StorageCopy
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockCommitMemberContents(ctx, db, input.RequestID); err != nil {
			return err
		}
		request, err := lockCommitRequest(ctx, db, input.RequestID, "")
		if err != nil {
			return err
		}
		if err := db.NewSelect().Model(&members).
			Where("commit_request_id = ?", input.RequestID).
			OrderExpr("commit_position ASC").
			Scan(ctx); err != nil {
			return fmt.Errorf("loading storage commit request members: %w", err)
		}
		if request.Status == storagecommit.RequestStatusConfirmed {
			if request.FirstPieceID == nil || !request.FirstPieceID.Equal(input.FirstPieceID) {
				return fmt.Errorf("replaying storage commit confirmation with conflicting evidence: %w", ErrConflict)
			}
			return nil
		}
		// A ready request is confirmed when the chain shows an earlier send, or
		// the provider a Pull disclosed it to, already landed it.
		if (request.Status != storagecommit.RequestStatusSubmitted && request.Status != storagecommit.RequestStatusReady) ||
			request.TaskID == nil || *request.TaskID != input.TaskID {
			return ErrConflict
		}
		if request.PieceCount != len(input.RetrievalURLs) {
			return fmt.Errorf("confirming storage commit request with %d retrieval URLs for %d pieces: %w",
				len(input.RetrievalURLs), request.PieceCount, ErrInvalidInput)
		}
		var pieces []storagecommit.RequestPiece
		if err := db.NewSelect().Model(&pieces).Where("request_id = ?", input.RequestID).
			OrderExpr("position ASC").Scan(ctx); err != nil {
			return fmt.Errorf("loading storage commit request pieces: %w", err)
		}
		if len(pieces) != request.PieceCount {
			return ErrConflict
		}
		firstPieceID := input.FirstPieceID
		q := db.NewUpdate().
			Model((*storagecommit.Request)(nil)).
			Set("status = ?", storagecommit.RequestStatusConfirmed).
			Set("task_id = NULL").
			Set("first_sent_at = COALESCE(first_sent_at, ?)", now).
			Set("first_piece_id = ?", &firstPieceID).
			Set("confirmed_at = ?", now).
			Set("attention_code = NULL").
			Set("attention_at = NULL").
			Set("updated_at = ?", now).
			Where("request_id = ? AND task_id = ? AND status = ?", input.RequestID, input.TaskID, request.Status)
		if input.ConfirmedTransactionID != "" {
			q = q.Set("confirmed_transaction_id = ?", input.ConfirmedTransactionID).Where("transaction_id IS NOT NULL")
		}
		res, err := q.Exec(ctx)
		if err := requireCommitRows(res, err, "confirming storage commit request"); err != nil {
			return err
		}
		// The request was set to confirmed just above, so the copies can name
		// it; the composite foreign key refuses any other status.
		for _, piece := range pieces {
			pieceID, err := commitPieceID(firstPieceID, piece.Position)
			if err != nil {
				return err
			}
			res, err := db.NewUpdate().
				Model((*model.StorageCopy)(nil)).
				Set("status = ?", model.StorageCopyStatusCommitted).
				Set("piece_id = ?", &pieceID).
				Set("retrieval_url = ?", input.RetrievalURLs[piece.Position]).
				Set("commit_request_status = ?", string(storagecommit.RequestStatusConfirmed)).
				Set("commit_ready_at = NULL").
				Set("last_error = NULL").
				Set("updated_at = ?", now).
				Where("commit_request_id = ? AND commit_position = ?", input.RequestID, piece.Position).
				Where("content_id = ?", piece.ContentID).
				Where("status IN (?)", bun.List([]model.StorageCopyStatus{model.StorageCopyStatusCommitting, model.StorageCopyStatusPending})).
				Exec(ctx)
			if err := requireCommitRows(res, err, "committing storage commit request member"); err != nil {
				return err
			}
			if err := updateUploadReadable(ctx, db, piece.ContentID, piece.PieceCID, now); err != nil {
				return err
			}
		}
		return wakeCommitQueue(ctx, db, request.StorageDataSetID, now)
	})
	if err != nil {
		return nil, fmt.Errorf("confirming storage commit request: %w", err)
	}
	return members, nil
}

// AbandonCommitRequestInput settles a request that can never land.
type AbandonCommitRequestInput struct {
	RequestID string
	// TaskID names the request's own task; zero lets another owner abandon a
	// request that was never sent.
	TaskID int64
	Reason string
	Now    time.Time
}

// AbandonCommitRequest settles the request as abandoned and releases its
// members. Sealed members that were waiting to register return to piece_ready;
// the caller decides whether each joins another request or fails. It returns
// the members as they were before release.
func (r *BunStorageContentRepo) AbandonCommitRequest(ctx context.Context, input AbandonCommitRequestInput) ([]model.StorageCopy, error) {
	if input.RequestID == "" || strings.TrimSpace(input.Reason) == "" || input.TaskID < 0 {
		return nil, fmt.Errorf("abandoning storage commit request: %w", ErrInvalidInput)
	}
	var members []model.StorageCopy
	err := r.runMaybeTx(ctx, func(db bun.IDB) error {
		var err error
		members, err = abandonCommitRequest(ctx, db, input)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("abandoning storage commit request: %w", err)
	}
	return members, nil
}

func abandonCommitRequest(ctx context.Context, db bun.IDB, input AbandonCommitRequestInput) ([]model.StorageCopy, error) {
	now := commitInputTime(input.Now)
	if err := lockCommitMemberContents(ctx, db, input.RequestID); err != nil {
		return nil, err
	}
	request, err := lockCommitRequest(ctx, db, input.RequestID, "")
	if err != nil {
		return nil, err
	}
	if request.Status.Terminal() {
		return nil, ErrConflict
	}
	if input.TaskID > 0 {
		if request.TaskID == nil || *request.TaskID != input.TaskID {
			return nil, ErrConflict
		}
	} else if request.Sent() || request.Status == storagecommit.RequestStatusSubmitted {
		// Only a request that never left the process, or a Pull's single-piece
		// request that was never sent, can be given up by someone else.
		return nil, ErrConflict
	}
	var members []model.StorageCopy
	if err := db.NewSelect().Model(&members).Where("commit_request_id = ?", input.RequestID).
		OrderExpr("commit_position ASC, id ASC").Scan(ctx); err != nil {
		return nil, fmt.Errorf("loading storage commit request members: %w", err)
	}
	res, err := db.NewUpdate().
		Model((*storagecommit.Request)(nil)).
		Set("status = ?", storagecommit.RequestStatusAbandoned).
		Set("task_id = NULL").
		Set("last_error = ?", input.Reason).
		Set("attention_code = NULL").
		Set("attention_at = NULL").
		Set("updated_at = ?", now).
		Where("request_id = ? AND status = ?", input.RequestID, request.Status).
		Exec(ctx)
	if err := requireCommitRows(res, err, "abandoning storage commit request"); err != nil {
		return nil, err
	}
	if _, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("commit_request_id = NULL").
		Set("commit_position = NULL").
		Set("status = CASE WHEN status = ? THEN ? ELSE status END", model.StorageCopyStatusCommitting, model.StorageCopyStatusPieceReady).
		Set("updated_at = ?", now).
		Where("commit_request_id = ?", input.RequestID).
		Exec(ctx); err != nil {
		return nil, fmt.Errorf("releasing storage commit request members: %w", err)
	}
	if request.TaskID != nil && input.TaskID == 0 {
		if _, err := (&BunTaskRepo{db: db}).WakePending(ctx, []int64{*request.TaskID}); err != nil {
			return nil, err
		}
	}
	return members, wakeCommitQueue(ctx, db, request.StorageDataSetID, now)
}

// ReturnCommitMembersToTransfer sends sealed members whose pieces the provider
// dropped back to transfer. They keep their positions, so the request is sent
// whole again once every member is transferred.
func (r *BunStorageContentRepo) ReturnCommitMembersToTransfer(ctx context.Context, requestID string, taskID int64, copyIDs []int64, now time.Time) error {
	if requestID == "" || taskID <= 0 || len(copyIDs) == 0 {
		return fmt.Errorf("returning storage commit members to transfer: %w", ErrInvalidInput)
	}
	at := commitInputTime(now)
	return r.runMaybeTx(ctx, func(db bun.IDB) error {
		if err := lockCommitMemberContents(ctx, db, requestID); err != nil {
			return err
		}
		request, err := lockCommitRequest(ctx, db, requestID, "")
		if err != nil {
			return err
		}
		if request.TaskID == nil || *request.TaskID != taskID ||
			(request.Status != storagecommit.RequestStatusReady && request.Status != storagecommit.RequestStatusSubmitted) {
			return ErrConflict
		}
		for _, copyID := range copyIDs {
			res, err := db.NewUpdate().
				Model((*model.StorageCopy)(nil)).
				Set("status = ?", model.StorageCopyStatusPending).
				Set("retrieval_url = NULL").
				Set("commit_ready_at = NULL").
				Set("updated_at = ?", at).
				Where("id = ? AND commit_request_id = ?", copyID, requestID).
				Where("commit_position IS NOT NULL AND status = ?", model.StorageCopyStatusCommitting).
				Where("active_task_id IS NULL").
				Exec(ctx)
			if err := requireCommitRows(res, err, "returning storage commit member to transfer"); err != nil {
				return err
			}
		}
		return nil
	})
}

// CountCommitBacklog counts the copies transferred to a data set that wait for
// a request that has not been sent.
func (r *BunStorageContentRepo) CountCommitBacklog(ctx context.Context, storageDataSetID int64) (int, error) {
	if storageDataSetID <= 0 {
		return 0, fmt.Errorf("counting storage commit backlog: %w", ErrInvalidInput)
	}
	count, err := r.db.NewSelect().
		Model((*model.StorageCopy)(nil)).
		Join("LEFT JOIN storage_commit_requests AS commit_request ON commit_request.request_id = storage_copy.commit_request_id").
		Where("storage_copy.storage_data_set_id = ?", storageDataSetID).
		Where("storage_copy.status IN (?)", bun.List([]model.StorageCopyStatus{model.StorageCopyStatusPieceReady, model.StorageCopyStatusCommitting})).
		Where("commit_request.request_id IS NULL OR commit_request.status IN (?)",
			bun.List([]storagecommit.RequestStatus{storagecommit.RequestStatusCollecting, storagecommit.RequestStatusReady})).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting storage commit backlog: %w", err)
	}
	return count, nil
}

// CountOpenCommitRequestsForDataSet counts the data set's requests that are not
// settled.
func (r *BunStorageContentRepo) CountOpenCommitRequestsForDataSet(ctx context.Context, storageDataSetID int64) (int, error) {
	return countOpenCommitRequests(ctx, r.db, storageDataSetID)
}

func countOpenCommitRequests(ctx context.Context, db bun.IDB, storageDataSetID int64) (int, error) {
	count, err := db.NewSelect().
		Model((*storagecommit.Request)(nil)).
		Where("storage_data_set_id = ?", storageDataSetID).
		Where("status NOT IN (?)", bun.List([]storagecommit.RequestStatus{storagecommit.RequestStatusConfirmed, storagecommit.RequestStatusAbandoned})).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting open storage commit requests: %w", err)
	}
	return count, nil
}

func countSubmittedCommitRequests(ctx context.Context, db bun.IDB, storageDataSetID int64) (int, error) {
	count, err := db.NewSelect().
		Model((*storagecommit.Request)(nil)).
		Where("storage_data_set_id = ? AND status = ?", storageDataSetID, storagecommit.RequestStatusSubmitted).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting submitted storage commit requests: %w", err)
	}
	return count, nil
}

// eligibleReadyCommitRequest returns the oldest ready request of the data set
// that may be sent now: it is not backing off and every member is transferred.
func eligibleReadyCommitRequest(ctx context.Context, db bun.IDB, storageDataSetID int64, now time.Time) (string, error) {
	var requestID string
	err := db.NewSelect().
		Model((*storagecommit.Request)(nil)).
		Column("request_id").
		Where("storage_data_set_id = ? AND status = ?", storageDataSetID, storagecommit.RequestStatusReady).
		Where("retry_at IS NULL OR retry_at <= ?", now).
		Where(`piece_count = (
			SELECT COUNT(*) FROM storage_copies AS transferred_member
			WHERE transferred_member.commit_request_id = commit_request.request_id
			  AND transferred_member.status = ?
		)`, model.StorageCopyStatusCommitting).
		OrderExpr("sealed_at ASC, request_id ASC").
		Limit(1).
		Scan(ctx, &requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("selecting next storage commit request: %w", err)
	}
	return requestID, nil
}

// wakeCommitQueue makes the data set's next request runnable once there is
// room for it: the oldest eligible ready request, or failing that every
// collecting request, which may now seal. A task another transaction holds is
// running and needs no wake, so it is skipped rather than waited for.
func wakeCommitQueue(ctx context.Context, db bun.IDB, storageDataSetID int64, now time.Time) error {
	submitted, err := countSubmittedCommitRequests(ctx, db, storageDataSetID)
	if err != nil || submitted >= storagecommit.MaxSubmittedRequestsPerDataSet {
		return err
	}
	head, err := eligibleReadyCommitRequest(ctx, db, storageDataSetID, now)
	if err != nil {
		return err
	}
	q := db.NewSelect().
		Model((*storagecommit.Request)(nil)).
		Column("task_id").
		Where("storage_data_set_id = ?", storageDataSetID).
		Where("task_id IS NOT NULL")
	if head != "" {
		q = q.Where("request_id = ?", head)
	} else {
		q = q.Where("status = ?", storagecommit.RequestStatusCollecting)
	}
	var taskIDs []int64
	if err := q.Scan(ctx, &taskIDs); err != nil {
		return fmt.Errorf("selecting storage commit tasks to wake: %w", err)
	}
	return wakeCommitTasks(ctx, db, taskIDs)
}

// WakeCommitRequestTask makes one request's task runnable.
func (r *BunStorageContentRepo) WakeCommitRequestTask(ctx context.Context, requestID string) error {
	var taskID sql.NullInt64
	err := r.db.NewSelect().Model((*storagecommit.Request)(nil)).Column("task_id").
		Where("request_id = ?", requestID).Scan(ctx, &taskID)
	if errors.Is(err, sql.ErrNoRows) || err == nil && !taskID.Valid {
		return nil
	}
	if err != nil {
		return fmt.Errorf("loading storage commit request task: %w", err)
	}
	return wakeCommitTasks(ctx, r.db, []int64{taskID.Int64})
}

func wakeCommitTasks(ctx context.Context, db bun.IDB, taskIDs []int64) error {
	if len(taskIDs) == 0 {
		return nil
	}
	now := time.Now()
	ids := db.NewSelect().
		Model((*model.Task)(nil)).
		Column("id").
		Where("id IN (?)", bun.List(taskIDs)).
		Where("status = ?", model.TaskStatusPending).
		Where("available_at > ?", now)
	if db.Dialect().Name() == dialect.PG {
		ids = ids.For("UPDATE SKIP LOCKED")
	}
	if _, err := db.NewUpdate().
		Model((*model.Task)(nil)).
		Set("available_at = ?", now).
		Set("updated_at = ?", now).
		Where("id IN (?)", ids).
		Exec(ctx); err != nil {
		return fmt.Errorf("waking storage commit tasks: %w", err)
	}
	return nil
}

// releaseCollectingCommitMembership takes a copy out of the collecting request
// it waits in. A request left with no copies can never be signed and is given
// up so its task stops.
func releaseCollectingCommitMembership(ctx context.Context, db bun.IDB, copyID int64, now time.Time) error {
	var copyRow model.StorageCopy
	if err := db.NewSelect().Model(&copyRow).Column("commit_request_id", "commit_position").
		Where("id = ?", copyID).Scan(ctx); err != nil {
		return fmt.Errorf("loading storage copy commit request: %w", err)
	}
	if copyRow.CommitRequestID == nil || copyRow.CommitPosition != nil {
		return nil
	}
	requestID := *copyRow.CommitRequestID
	request, err := lockCommitRequest(ctx, db, requestID, storagecommit.RequestStatusCollecting)
	if err != nil {
		return err
	}
	res, err := db.NewUpdate().
		Model((*model.StorageCopy)(nil)).
		Set("commit_request_id = NULL").
		Set("updated_at = ?", now).
		Where("id = ? AND commit_request_id = ? AND commit_position IS NULL", copyID, requestID).
		Exec(ctx)
	if err := requireCommitRows(res, err, "leaving storage commit request"); err != nil {
		return err
	}
	remaining, err := countCommitMembers(ctx, db, requestID)
	if err != nil || remaining > 0 {
		return err
	}
	if _, err := db.NewUpdate().
		Model((*storagecommit.Request)(nil)).
		Set("status = ?", storagecommit.RequestStatusAbandoned).
		Set("task_id = NULL").
		Set("last_error = ?", "no storage copies are left to register").
		Set("updated_at = ?", now).
		Where("request_id = ? AND status = ?", requestID, storagecommit.RequestStatusCollecting).
		Exec(ctx); err != nil {
		return fmt.Errorf("abandoning empty storage commit request: %w", err)
	}
	if request.TaskID != nil {
		return wakeCommitTasks(ctx, db, []int64{*request.TaskID})
	}
	return nil
}

// abandonUnsentPullCommitRequest gives up the single-piece request a Pull
// signed for the copy when it was never sent, so the copy can fail, switch to
// Store, or be pulled again under a new request.
func abandonUnsentPullCommitRequest(ctx context.Context, db bun.IDB, copyID int64, reason string, now time.Time) error {
	var copyRow model.StorageCopy
	if err := db.NewSelect().Model(&copyRow).Column("commit_request_id", "commit_position").
		Where("id = ?", copyID).Scan(ctx); err != nil {
		return fmt.Errorf("loading storage copy commit request: %w", err)
	}
	if copyRow.CommitRequestID == nil {
		return nil
	}
	if copyRow.CommitPosition == nil {
		return releaseCollectingCommitMembership(ctx, db, copyID, now)
	}
	request := new(storagecommit.Request)
	if err := db.NewSelect().Model(request).Where("request_id = ?", *copyRow.CommitRequestID).Scan(ctx); err != nil {
		return fmt.Errorf("loading storage commit request: %w", err)
	}
	if request.Status.Terminal() {
		return nil
	}
	if request.PieceCount != 1 || request.Sent() {
		return ErrConflict
	}
	_, err := abandonCommitRequest(ctx, db, AbandonCommitRequestInput{RequestID: request.RequestID, Reason: reason, Now: now})
	return err
}

// pinnedCommitMemberSQL matches a copy that belongs to a signed request that is
// not settled. Such a copy is decided by its request alone.
func pinnedCommitMemberSQL(alias string) string {
	return fmt.Sprintf("(%[1]s.commit_position IS NOT NULL AND %[1]s.status NOT IN ('committed', 'failed'))", alias)
}

// projectCommitRequest selects a copy with its commit request's state.
func projectCommitRequest(q *bun.SelectQuery, copyAlias string) {
	q.ColumnExpr(copyAlias + ".*").
		ColumnExpr("commit_request.status AS commit_state").
		ColumnExpr("commit_request.task_id AS commit_task_id").
		ColumnExpr("commit_request.piece_count AS commit_piece_count").
		ColumnExpr("commit_request.first_sent_at AS commit_sent_at").
		ColumnExpr("commit_request.transaction_id AS commit_transaction_id").
		ColumnExpr("commit_request.submit_error AS commit_submit_error").
		ColumnExpr("commit_request.confirmed_transaction_id AS commit_confirmed_transaction_id").
		ColumnExpr("commit_request.attention_code AS commit_attention_code").
		ColumnExpr("commit_request.attention_at AS commit_attention_at").
		Join("LEFT JOIN storage_commit_requests AS commit_request ON commit_request.request_id = " + copyAlias + ".commit_request_id")
}

func lockCommitMemberContents(ctx context.Context, db bun.IDB, requestID string) error {
	var contentIDs []int64
	if err := db.NewSelect().Model((*model.StorageCopy)(nil)).Column("content_id").
		Where("commit_request_id = ?", requestID).Scan(ctx, &contentIDs); err != nil {
		return fmt.Errorf("loading storage commit request contents: %w", err)
	}
	if _, err := lockStorageContentsByID(ctx, db, contentIDs); err != nil {
		return fmt.Errorf("locking storage commit request contents: %w", err)
	}
	return nil
}

func lockCommitDataSet(ctx context.Context, db bun.IDB, storageDataSetID int64) (*model.StorageDataSet, error) {
	dataSet := new(model.StorageDataSet)
	err := db.NewRaw(`UPDATE storage_data_sets
		SET updated_at = updated_at
		WHERE id = ?
		RETURNING *`, storageDataSetID).Scan(ctx, dataSet)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("locking storage data set for commit: %w", err)
	}
	return dataSet, nil
}

// lockCommitRequest locks the request row, requiring status when it is set.
func lockCommitRequest(ctx context.Context, db bun.IDB, requestID string, status storagecommit.RequestStatus) (*storagecommit.Request, error) {
	request := new(storagecommit.Request)
	q := `UPDATE storage_commit_requests SET updated_at = updated_at WHERE request_id = ?`
	args := []any{requestID}
	if status != "" {
		q += ` AND status = ?`
		args = append(args, status)
	}
	err := db.NewRaw(q+` RETURNING *`, args...).Scan(ctx, request)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrConflict
	}
	if err != nil {
		return nil, fmt.Errorf("locking storage commit request: %w", err)
	}
	return request, nil
}

func lockOwnedCommitRequest(ctx context.Context, db bun.IDB, requestID string, taskID int64, status storagecommit.RequestStatus) (*storagecommit.Request, error) {
	request, err := lockCommitRequest(ctx, db, requestID, status)
	if err != nil {
		return nil, err
	}
	if request.TaskID == nil || *request.TaskID != taskID {
		return nil, ErrConflict
	}
	return request, nil
}

func countCommitMembers(ctx context.Context, db bun.IDB, requestID string) (int, error) {
	count, err := db.NewSelect().Model((*model.StorageCopy)(nil)).Where("commit_request_id = ?", requestID).Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("counting storage commit request members: %w", err)
	}
	return count, nil
}

func commitPieceID(first idtypes.OnChainID, position int) (idtypes.OnChainID, error) {
	id, err := sdktypes.BigIntFromBig(new(big.Int).Add(first.SDK().Big(), big.NewInt(int64(position))))
	if err != nil {
		return idtypes.OnChainID{}, fmt.Errorf("deriving storage commit piece ID: %w", err)
	}
	return idtypes.OnChainIDFromSDK(id), nil
}

func nullableTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func commitInputTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now()
	}
	return value
}

// ListCommitAttention lists submitted requests flagged for attention, oldest
// flag first.
func (r *BunStorageContentRepo) ListCommitAttention(ctx context.Context, limit int) ([]storagecommit.AttentionRecord, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("listing storage confirmation attention: %w", ErrInvalidInput)
	}
	return r.scanCommitAttention(ctx, commitAttentionQuery(r.db).
		OrderExpr("commit_request.attention_at ASC, commit_request.request_id ASC").
		Limit(limit))
}

// ListCommitAttentionForTasks lists the flagged requests driven by the tasks.
func (r *BunStorageContentRepo) ListCommitAttentionForTasks(ctx context.Context, taskIDs []int64) ([]storagecommit.AttentionRecord, error) {
	if len(taskIDs) == 0 {
		return nil, nil
	}
	return r.scanCommitAttention(ctx, commitAttentionQuery(r.db).
		Where("commit_request.task_id IN (?)", bun.List(taskIDs)))
}

// CountStoppedCommitAttentionByDataSet counts, per data set, the flagged
// requests whose task stopped for an operator.
func (r *BunStorageContentRepo) CountStoppedCommitAttentionByDataSet(ctx context.Context) (map[int64]int64, error) {
	var rows []struct {
		StorageDataSetID int64 `bun:"storage_data_set_id"`
		Count            int64 `bun:"count"`
	}
	err := r.db.NewSelect().
		TableExpr("storage_commit_requests AS commit_request").
		ColumnExpr("commit_request.storage_data_set_id").
		ColumnExpr("COUNT(*) AS count").
		Join("JOIN tasks AS owner_task ON owner_task.id = commit_request.task_id").
		Where("commit_request.status = ?", storagecommit.RequestStatusSubmitted).
		Where("commit_request.attention_at IS NOT NULL").
		Where("owner_task.status = ?", model.TaskStatusFailed).
		GroupExpr("commit_request.storage_data_set_id").
		Scan(ctx, &rows)
	if err != nil {
		return nil, fmt.Errorf("counting stopped storage confirmations: %w", err)
	}
	counts := make(map[int64]int64, len(rows))
	for _, row := range rows {
		counts[row.StorageDataSetID] = row.Count
	}
	return counts, nil
}

func commitAttentionQuery(db bun.IDB) *bun.SelectQuery {
	return db.NewSelect().
		TableExpr("storage_commit_requests AS commit_request").
		ColumnExpr("commit_request.request_id").
		ColumnExpr("commit_request.task_id").
		ColumnExpr("commit_request.storage_data_set_id AS data_set_row_id").
		ColumnExpr("CAST(storage_data_set.provider_id AS TEXT) AS provider_id").
		ColumnExpr("COALESCE(CAST(storage_data_set.data_set_id AS TEXT), '') AS data_set_id").
		ColumnExpr("COALESCE(commit_request.transaction_id, '') AS transaction_id").
		ColumnExpr("COALESCE(commit_request.submit_error, '') AS submit_error").
		ColumnExpr("commit_request.attention_code").
		ColumnExpr("commit_request.submitted_at").
		ColumnExpr("commit_request.attention_at").
		Join("JOIN storage_data_sets AS storage_data_set ON storage_data_set.id = commit_request.storage_data_set_id").
		Where("commit_request.status = ?", storagecommit.RequestStatusSubmitted).
		Where("commit_request.attention_at IS NOT NULL")
}

func (r *BunStorageContentRepo) scanCommitAttention(ctx context.Context, q *bun.SelectQuery) ([]storagecommit.AttentionRecord, error) {
	type attentionRow struct {
		RequestID     string    `bun:"request_id"`
		TaskID        *int64    `bun:"task_id"`
		DataSetRowID  int64     `bun:"data_set_row_id"`
		ProviderID    string    `bun:"provider_id"`
		DataSetID     string    `bun:"data_set_id"`
		TransactionID string    `bun:"transaction_id"`
		SubmitError   string    `bun:"submit_error"`
		Code          string    `bun:"attention_code"`
		SubmittedAt   time.Time `bun:"submitted_at"`
		AttentionAt   time.Time `bun:"attention_at"`
	}
	var rows []attentionRow
	if err := q.Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing storage confirmation attention: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	requestIDs := make([]string, len(rows))
	for i, row := range rows {
		requestIDs[i] = row.RequestID
	}
	var pieces []storagecommit.RequestPiece
	if err := r.db.NewSelect().Model(&pieces).
		Where("request_id IN (?)", bun.List(requestIDs)).
		OrderExpr("request_id ASC, position ASC").
		Scan(ctx); err != nil {
		return nil, fmt.Errorf("listing flagged storage commit pieces: %w", err)
	}
	pieceCIDs := make(map[string][]string, len(rows))
	for _, piece := range pieces {
		pieceCIDs[piece.RequestID] = append(pieceCIDs[piece.RequestID], piece.PieceCID)
	}
	out := make([]storagecommit.AttentionRecord, 0, len(rows))
	for _, row := range rows {
		out = append(out, storagecommit.AttentionRecord{
			RequestID: row.RequestID, TaskID: row.TaskID, DataSetRowID: row.DataSetRowID,
			ProviderID: row.ProviderID, DataSetID: row.DataSetID, PieceCIDs: pieceCIDs[row.RequestID],
			TransactionID: row.TransactionID, SubmitError: row.SubmitError,
			Code: storagecommit.AttentionCode(row.Code), SubmittedAt: row.SubmittedAt, AttentionAt: row.AttentionAt,
		})
	}
	return out, nil
}

// requireCommitRows refuses a request mutation that matched nothing: the
// request moved on, or is driven by another task.
func requireCommitRows(res sql.Result, err error, action string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: reading row count: %w", action, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s: %w", action, ErrConflict)
	}
	return nil
}
