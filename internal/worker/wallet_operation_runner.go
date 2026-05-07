package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/admin"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
)

const walletOperationUpdatedTopic = "wallet_operation_updated"
const (
	walletOperationMarkSubmittedAttempts = 3
	walletOperationMarkSubmittedTimeout  = 5 * time.Second
	walletOperationBroadcastTimeout      = 2 * time.Minute
	walletOperationReceiptLookupTimeout  = 15 * time.Second
	walletOperationSubmittedScanLimit    = 20
)

type WalletReceiptChecker interface {
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*ethtypes.Receipt, error)
}

type WalletOperationRunner struct {
	repos        *repository.Repositories
	operator     synapse.WalletOperator
	receipts     WalletReceiptChecker
	publisher    admin.EventPublisher
	pollInterval time.Duration
	leaseTTL     time.Duration
	broadcastTTL time.Duration
	receiptTTL   time.Duration
	logger       *slog.Logger
	*livenessTracker
}

type WalletOperationRunnerOption func(*WalletOperationRunner)

func WithWalletOperationEventPublisher(publisher admin.EventPublisher) WalletOperationRunnerOption {
	return func(r *WalletOperationRunner) {
		r.publisher = publisher
	}
}

func WithWalletOperationTimeouts(broadcast, receipt time.Duration) WalletOperationRunnerOption {
	return func(r *WalletOperationRunner) {
		if broadcast > 0 {
			r.broadcastTTL = broadcast
		}
		if receipt > 0 {
			r.receiptTTL = receipt
		}
	}
}

func NewWalletOperationRunner(
	repos *repository.Repositories,
	operator synapse.WalletOperator,
	receipts WalletReceiptChecker,
	pollInterval time.Duration,
	logger *slog.Logger,
	opts ...WalletOperationRunnerOption,
) *WalletOperationRunner {
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &WalletOperationRunner{
		repos:           repos,
		operator:        operator,
		receipts:        receipts,
		pollInterval:    pollInterval,
		leaseTTL:        5 * time.Minute,
		broadcastTTL:    walletOperationBroadcastTimeout,
		receiptTTL:      walletOperationReceiptLookupTimeout,
		logger:          logger,
		livenessTracker: newLivenessTracker(pollInterval),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *WalletOperationRunner) Name() string { return "wallet_operations" }

func (r *WalletOperationRunner) Healthy() bool { return r.healthy() }

func (r *WalletOperationRunner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.runOnce(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *WalletOperationRunner) runOnce(ctx context.Context) {
	if r == nil || r.repos == nil || r.repos.WalletOperations == nil {
		return
	}
	r.recordTick()
	r.markExpiredRunningUnknown(ctx)
	r.confirmSubmitted(ctx)
	r.processPending(ctx)
}

func (r *WalletOperationRunner) markExpiredRunningUnknown(ctx context.Context) {
	ops, err := r.repos.WalletOperations.MarkExpiredRunningUnknown(ctx)
	if err != nil {
		r.logger.Error("marking expired wallet operations unknown", "error", err)
		return
	}
	for i := range ops {
		r.publish(&ops[i])
	}
}

func (r *WalletOperationRunner) confirmSubmitted(ctx context.Context) {
	if r.receipts == nil {
		return
	}
	ops, err := r.repos.WalletOperations.ListSubmitted(ctx, walletOperationSubmittedScanLimit)
	if err != nil {
		r.logger.Error("listing submitted wallet operations", "error", err)
		return
	}
	if len(ops) == 0 {
		return
	}
	r.recordWorkStarted()
	defer r.recordWorkFinished()
	for i := range ops {
		r.confirmOperation(ctx, &ops[i])
	}
}

func (r *WalletOperationRunner) processPending(ctx context.Context) {
	if r.operator == nil {
		return
	}
	op, err := r.repos.WalletOperations.ClaimPending(ctx, r.leaseTTL)
	if err != nil {
		r.logger.Error("claiming wallet operation", "error", err)
		return
	}
	if op == nil {
		return
	}
	r.recordWorkStarted()
	defer r.recordWorkFinished()
	r.publish(op)
	r.processClaimed(ctx, op)
}

func (r *WalletOperationRunner) processClaimed(ctx context.Context, op *model.WalletOperation) {
	amount, ok := new(big.Int).SetString(op.Amount, 10)
	if !ok || amount.Sign() <= 0 {
		r.markFailedAndPublish(ctx, op.ID, "invalid wallet operation amount")
		return
	}

	broadcastCtx, cancel := context.WithTimeout(ctx, r.broadcastTTL)
	txHash, err := r.broadcast(broadcastCtx, op.Type, amount)
	cancel()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.logger.Warn("wallet operation broadcast did not finish before context ended", "id", op.ID, "error", err)
			return
		}
		r.markFailedAndPublish(ctx, op.ID, walletOperationError(err))
		return
	}
	if txHash == "" {
		r.markFailedAndPublish(ctx, op.ID, "wallet operation broadcast did not return a transaction hash")
		return
	}
	if !r.markSubmittedAfterBroadcast(context.WithoutCancel(ctx), op.ID, txHash) {
		return
	}
	submitted := r.mustGet(ctx, op.ID)
	r.publish(submitted)
	r.confirmOperation(ctx, submitted)
}

func (r *WalletOperationRunner) markSubmittedAfterBroadcast(ctx context.Context, id int64, txHash string) bool {
	ctx, cancel := context.WithTimeout(ctx, walletOperationMarkSubmittedTimeout)
	defer cancel()

	var lastErr error
	for attempt := 1; attempt <= walletOperationMarkSubmittedAttempts; attempt++ {
		err := r.repos.WalletOperations.MarkSubmitted(ctx, id, txHash)
		if err == nil {
			return true
		}
		lastErr = err
		r.logger.Warn("marking wallet operation submitted after broadcast failed", "id", id, "tx_hash", txHash, "attempt", attempt, "error", err)
		if attempt == walletOperationMarkSubmittedAttempts || ctx.Err() != nil {
			break
		}
	}
	r.logger.Error("wallet operation broadcast transaction hash was not persisted", "id", id, "tx_hash", txHash, "error", lastErr)
	return false
}

func (r *WalletOperationRunner) broadcast(ctx context.Context, opType model.WalletOperationType, amount *big.Int) (string, error) {
	switch opType {
	case model.WalletOperationTypeFund:
		return r.operator.FundUSDFC(ctx, amount)
	case model.WalletOperationTypeWithdraw:
		return r.operator.WithdrawUSDFC(ctx, amount)
	default:
		return "", fmt.Errorf("unsupported wallet operation type %q", opType)
	}
}

func (r *WalletOperationRunner) confirmOperation(ctx context.Context, op *model.WalletOperation) {
	if op == nil || op.TxHash == nil || *op.TxHash == "" || r.receipts == nil {
		return
	}
	receiptCtx, cancel := context.WithTimeout(ctx, r.receiptTTL)
	defer cancel()

	receipt, err := r.receipts.TransactionReceipt(receiptCtx, common.HexToHash(*op.TxHash))
	if err != nil {
		if errors.Is(err, ethereum.NotFound) {
			return
		}
		r.logger.Warn("wallet operation receipt lookup failed", "id", op.ID, "tx_hash", *op.TxHash, "error", err)
		return
	}
	if receipt == nil {
		return
	}
	if receipt.Status == ethtypes.ReceiptStatusSuccessful {
		if err := r.repos.WalletOperations.MarkConfirmed(ctx, op.ID); err != nil {
			r.logger.Error("marking wallet operation confirmed", "id", op.ID, "error", err)
			return
		}
		r.publish(r.mustGet(ctx, op.ID))
		return
	}
	r.markFailedAndPublish(ctx, op.ID, revertedReceiptMessage(op, receipt))
}

func (r *WalletOperationRunner) markFailedAndPublish(ctx context.Context, id int64, message string) {
	if err := r.repos.WalletOperations.MarkFailed(ctx, id, message); err != nil {
		r.logger.Error("marking wallet operation failed", "id", id, "error", err)
		return
	}
	r.publish(r.mustGet(ctx, id))
}

func (r *WalletOperationRunner) mustGet(ctx context.Context, id int64) *model.WalletOperation {
	op, err := r.repos.WalletOperations.GetByID(ctx, id)
	if err != nil {
		r.logger.Error("loading wallet operation", "id", id, "error", err)
		return nil
	}
	return op
}

func (r *WalletOperationRunner) publish(op *model.WalletOperation) {
	if r == nil || r.publisher == nil || op == nil {
		return
	}
	r.publisher.Publish(walletOperationUpdatedTopic, map[string]any{
		"operation": walletOperationEventFromModel(op),
	})
}

type walletOperationEventPayload struct {
	ID              int64   `json:"id"`
	Type            string  `json:"type"`
	ClientRequestID string  `json:"client_request_id"`
	Amount          string  `json:"amount"`
	Status          string  `json:"status"`
	TxHash          *string `json:"tx_hash,omitempty"`
	LastError       *string `json:"last_error,omitempty"`
	LeaseUntil      *string `json:"lease_until,omitempty"`
	StartedAt       *string `json:"started_at,omitempty"`
	SubmittedAt     *string `json:"submitted_at,omitempty"`
	CompletedAt     *string `json:"completed_at,omitempty"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
}

func walletOperationEventFromModel(op *model.WalletOperation) walletOperationEventPayload {
	return walletOperationEventPayload{
		ID:              op.ID,
		Type:            string(op.Type),
		ClientRequestID: op.ClientRequestID,
		Amount:          op.Amount,
		Status:          string(op.Status),
		TxHash:          op.TxHash,
		LastError:       op.LastError,
		LeaseUntil:      operationTimeString(op.LeaseUntil),
		StartedAt:       operationTimeString(op.StartedAt),
		SubmittedAt:     operationTimeString(op.SubmittedAt),
		CompletedAt:     operationTimeString(op.CompletedAt),
		CreatedAt:       op.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:       op.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

func operationTimeString(v *time.Time) *string {
	if v == nil {
		return nil
	}
	out := v.UTC().Format(time.RFC3339Nano)
	return &out
}

func walletOperationError(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func revertedReceiptMessage(op *model.WalletOperation, receipt *ethtypes.Receipt) string {
	txHash := ""
	if op != nil && op.TxHash != nil {
		txHash = *op.TxHash
	}
	blockNumber := "unknown"
	if receipt != nil && receipt.BlockNumber != nil {
		blockNumber = receipt.BlockNumber.String()
	}
	return fmt.Sprintf("transaction reverted: status=%d tx_hash=%s block_number=%s", receipt.Status, txHash, blockNumber)
}
