package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/strahe/synaps3/internal/walletoperation"

	"github.com/strahe/synaps3/internal/model"
	"github.com/uptrace/bun"
)

var (
	ErrWalletOperationConflict      = errors.New("wallet operation request conflicts with existing operation")
	ErrWalletOperationInvalidAmount = errors.New("wallet operation amount must be a positive integer string")
)

type BunWalletOperationRepo struct {
	db bun.IDB
}

var _ WalletOperationRepository = (*BunWalletOperationRepo)(nil)

// CreateOrGet records a wallet request once per client request ID. A request
// that loses a race to the same ID gets the winner's operation: the conflict
// never raises an error, which on PostgreSQL would abort the caller's
// transaction before the winner could be read.
func (r *BunWalletOperationRepo) CreateOrGet(ctx context.Context, input CreateWalletOperationInput) (*model.WalletOperation, bool, error) {
	if !validWalletOperationAmount(input.Type, input.Amount) {
		return nil, false, ErrWalletOperationInvalidAmount
	}
	now := time.Now()
	op := &model.WalletOperation{
		Type:            input.Type,
		ClientRequestID: input.ClientRequestID,
		Amount:          input.Amount,
		Status:          model.WalletOperationStatusPending,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	res, err := r.db.NewInsert().
		Model(op).
		On("CONFLICT (type, client_request_id) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("inserting wallet operation: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 1 {
		return op, true, nil
	}
	existing, err := r.getByTypeAndClientRequestID(ctx, input.Type, input.ClientRequestID)
	if err != nil {
		return nil, false, err
	}
	if existing == nil {
		return nil, false, fmt.Errorf("wallet operation %s/%s conflicted but was not found: %w", input.Type, input.ClientRequestID, ErrConflict)
	}
	if existing.Amount != input.Amount {
		return nil, false, ErrWalletOperationConflict
	}
	return existing, false, nil
}

func (r *BunWalletOperationRepo) GetByID(ctx context.Context, id int64) (*model.WalletOperation, error) {
	op := new(model.WalletOperation)
	err := r.db.NewSelect().Model(op).Where("id = ?", id).Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting wallet operation by id: %w", err)
	}
	return op, nil
}

func (r *BunWalletOperationRepo) BindTask(ctx context.Context, id, taskID int64) error {
	if id < 1 || taskID < 1 {
		return ErrInvalidInput
	}
	return runMaybeTx(ctx, r.db, func(db bun.IDB) error {
		task, err := taskForBinding(ctx, db, taskID, "wallet_operation", strconv.FormatInt(id, 10), model.TaskTypeWalletOperation)
		if err != nil {
			return err
		}
		var input walletoperation.Input
		if json.Unmarshal(task.Input, &input) != nil || input.OperationID != id {
			return ErrConflict
		}

		res, err := db.NewUpdate().
			Model((*model.WalletOperation)(nil)).
			Set("task_id = ?", taskID).
			Set("updated_at = ?", time.Now()).
			Where("id = ? AND status = ?", id, model.WalletOperationStatusPending).
			Where("task_id IS NULL OR task_id = ?", taskID).
			Exec(ctx)
		return requireRows(res, err, "binding wallet operation task")
	})
}

func (r *BunWalletOperationRepo) MarkBroadcastAttempted(ctx context.Context, id, taskID int64) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("broadcast_attempted_at = COALESCE(broadcast_attempted_at, ?)", now).
		Set("started_at = COALESCE(started_at, ?)", now).
		Set("updated_at = ?", now).
		Where("id = ? AND task_id = ? AND status = ?", id, taskID, model.WalletOperationStatusPending).
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation broadcast attempted")
}

func (r *BunWalletOperationRepo) MarkSubmitted(ctx context.Context, id, taskID int64, txHash string) error {
	if txHash == "" {
		return ErrInvalidInput
	}
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("status = ?", model.WalletOperationStatusSubmitted).
		Set("tx_hash = ?", txHash).
		Set("submitted_at = COALESCE(submitted_at, ?)", now).
		Set("updated_at = ?", now).
		Where("id = ? AND task_id = ?", id, taskID).
		Where("status IN (?, ?)", model.WalletOperationStatusPending, model.WalletOperationStatusSubmitted).
		Where("tx_hash IS NULL OR tx_hash = ?", txHash).
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation submitted")
}

func (r *BunWalletOperationRepo) MarkConfirmed(ctx context.Context, id, taskID int64, txHash string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("status = ?", model.WalletOperationStatusConfirmed).
		Set("tx_hash = ?", txHash).
		Set("last_error = NULL").
		Set("task_id = NULL").
		Set("completed_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ? AND task_id = ?", id, taskID).
		Where("status IN (?, ?)", model.WalletOperationStatusPending, model.WalletOperationStatusSubmitted).
		Where("tx_hash IS NULL OR tx_hash = ?", txHash).
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation confirmed")
}

func (r *BunWalletOperationRepo) MarkConfirmedWithoutTransaction(ctx context.Context, id, taskID int64) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("status = ?", model.WalletOperationStatusConfirmed).
		Set("tx_hash = NULL").
		Set("last_error = NULL").
		Set("task_id = NULL").
		Set("submitted_at = NULL").
		Set("completed_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ? AND task_id = ?", id, taskID).
		Where("status = ?", model.WalletOperationStatusPending).
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation confirmed without transaction")
}

func (r *BunWalletOperationRepo) MarkFailed(ctx context.Context, id, taskID int64, lastError string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("status = ?", model.WalletOperationStatusFailed).
		Set("last_error = ?", lastError).
		Set("task_id = NULL").
		Set("completed_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ? AND task_id = ?", id, taskID).
		Where("status IN (?, ?)", model.WalletOperationStatusSubmitted, model.WalletOperationStatusPending).
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation failed")
}

func (r *BunWalletOperationRepo) MarkUnknown(ctx context.Context, id, taskID int64, lastError string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("status = ?", model.WalletOperationStatusUnknown).
		Set("last_error = ?", lastError).
		Set("task_id = NULL").
		Set("completed_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ? AND task_id = ?", id, taskID).
		Where("status = ?", model.WalletOperationStatusPending).
		Where("broadcast_attempted_at IS NOT NULL").
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation unknown")
}

func (r *BunWalletOperationRepo) ListRecent(ctx context.Context, limit int) ([]model.WalletOperation, error) {
	limit = normalizeWalletOperationLimit(limit)
	var ops []model.WalletOperation
	err := r.db.NewSelect().
		Model(&ops).
		OrderExpr("created_at DESC, id DESC").
		Limit(limit).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing recent wallet operations: %w", err)
	}
	return ops, nil
}

func (r *BunWalletOperationRepo) getByTypeAndClientRequestID(ctx context.Context, opType model.WalletOperationType, clientRequestID string) (*model.WalletOperation, error) {
	op := new(model.WalletOperation)
	err := r.db.NewSelect().
		Model(op).
		Where("type = ?", opType).
		Where("client_request_id = ?", clientRequestID).
		Scan(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting wallet operation by request id: %w", err)
	}
	return op, nil
}

func normalizeWalletOperationLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func validWalletOperationAmount(opType model.WalletOperationType, amount string) bool {
	//exhaustive:enforce
	switch opType {
	case model.WalletOperationTypeApprove:
		return amount == "0"
	case model.WalletOperationTypeFund, model.WalletOperationTypeWithdraw:
		// A positive amount, checked below.
	default:
		return false
	}
	if amount == "" || amount[0] == '0' {
		return false
	}
	for _, r := range amount {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func requireRows(res sql.Result, err error, action string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: reading row count: %w", action, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s: %w", action, ErrNotFound)
	}
	return nil
}
