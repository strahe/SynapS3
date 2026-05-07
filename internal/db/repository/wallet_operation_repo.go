package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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

func (r *BunWalletOperationRepo) CreateOrGet(ctx context.Context, input CreateWalletOperationInput) (*model.WalletOperation, bool, error) {
	if !validWalletOperationAmount(input.Amount) {
		return nil, false, ErrWalletOperationInvalidAmount
	}
	if existing, err := r.getByTypeAndClientRequestID(ctx, input.Type, input.ClientRequestID); err != nil {
		return nil, false, err
	} else if existing != nil {
		if existing.Amount != input.Amount {
			return nil, false, ErrWalletOperationConflict
		}
		return existing, false, nil
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
	if _, err := r.db.NewInsert().Model(op).Exec(ctx); err != nil {
		if isUniqueViolation(err) {
			existing, selectErr := r.getByTypeAndClientRequestID(ctx, input.Type, input.ClientRequestID)
			if selectErr != nil {
				return nil, false, selectErr
			}
			if existing != nil && existing.Amount != input.Amount {
				return nil, false, ErrWalletOperationConflict
			}
			return existing, false, nil
		}
		return nil, false, fmt.Errorf("inserting wallet operation: %w", err)
	}
	return op, true, nil
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

func (r *BunWalletOperationRepo) ClaimPending(ctx context.Context, leaseDuration time.Duration) (*model.WalletOperation, error) {
	now := time.Now()
	leaseUntil := now.Add(leaseDuration)
	op := new(model.WalletOperation)
	err := r.db.NewRaw(
		`UPDATE wallet_operations SET status = ?, started_at = ?, lease_until = ?, updated_at = ?
		 WHERE id = (
		     SELECT id FROM wallet_operations
		     WHERE status = ?
		       AND NOT EXISTS (
		           SELECT 1 FROM wallet_operations in_flight
		           WHERE in_flight.status IN (?, ?)
		       )
		     ORDER BY created_at ASC, id ASC
		     LIMIT 1
		 )
		 AND status = ?
		 RETURNING *`,
		model.WalletOperationStatusRunning, now, leaseUntil, now,
		model.WalletOperationStatusPending,
		model.WalletOperationStatusRunning,
		model.WalletOperationStatusSubmitted,
		model.WalletOperationStatusPending,
	).Scan(ctx, op)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("claiming pending wallet operation: %w", err)
	}
	return op, nil
}

func (r *BunWalletOperationRepo) MarkSubmitted(ctx context.Context, id int64, txHash string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("status = ?", model.WalletOperationStatusSubmitted).
		Set("tx_hash = ?", txHash).
		Set("submitted_at = ?", now).
		Set("lease_until = NULL").
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Where("status IN (?, ?)", model.WalletOperationStatusRunning, model.WalletOperationStatusSubmitted).
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation submitted")
}

func (r *BunWalletOperationRepo) MarkConfirmed(ctx context.Context, id int64) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("status = ?", model.WalletOperationStatusConfirmed).
		Set("last_error = NULL").
		Set("lease_until = NULL").
		Set("completed_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Where("status = ?", model.WalletOperationStatusSubmitted).
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation confirmed")
}

func (r *BunWalletOperationRepo) MarkFailed(ctx context.Context, id int64, lastError string) error {
	now := time.Now()
	res, err := r.db.NewUpdate().
		Model((*model.WalletOperation)(nil)).
		Set("status = ?", model.WalletOperationStatusFailed).
		Set("last_error = ?", lastError).
		Set("lease_until = NULL").
		Set("completed_at = ?", now).
		Set("updated_at = ?", now).
		Where("id = ?", id).
		Where("status IN (?, ?, ?)", model.WalletOperationStatusRunning, model.WalletOperationStatusSubmitted, model.WalletOperationStatusPending).
		Exec(ctx)
	return requireRows(res, err, "marking wallet operation failed")
}

func (r *BunWalletOperationRepo) MarkExpiredRunningUnknown(ctx context.Context) ([]model.WalletOperation, error) {
	now := time.Now()
	var ops []model.WalletOperation
	err := r.db.NewRaw(
		`UPDATE wallet_operations
		    SET status = ?, last_error = ?, lease_until = NULL, completed_at = ?, updated_at = ?
		  WHERE status = ?
		    AND (tx_hash IS NULL OR tx_hash = '')
		    AND lease_until IS NOT NULL
		    AND lease_until < ?
		  RETURNING *`,
		model.WalletOperationStatusUnknown,
		"operation state is unknown after restart before transaction hash was recorded",
		now,
		now,
		model.WalletOperationStatusRunning,
		now,
	).Scan(ctx, &ops)
	if err != nil {
		return nil, fmt.Errorf("marking expired wallet operations unknown: %w", err)
	}
	return ops, nil
}

func (r *BunWalletOperationRepo) ListSubmitted(ctx context.Context, limit int) ([]model.WalletOperation, error) {
	limit = normalizeWalletOperationLimit(limit)
	var ops []model.WalletOperation
	err := r.db.NewSelect().
		Model(&ops).
		Where("status = ?", model.WalletOperationStatusSubmitted).
		Where("tx_hash IS NOT NULL AND tx_hash <> ''").
		OrderExpr("submitted_at ASC, id ASC").
		Limit(limit).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing submitted wallet operations: %w", err)
	}
	return ops, nil
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

func validWalletOperationAmount(amount string) bool {
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
