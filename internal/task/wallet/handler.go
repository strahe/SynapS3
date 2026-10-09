package wallet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/synapse"
	"github.com/strahe/synaps3/internal/walletoperation"
	taskengine "github.com/strahe/synaps3/internal/worker"
	"github.com/strahe/synapse-go/payments"
)

type WalletReceiptChecker interface {
	TransactionReceipt(context.Context, common.Hash) (*ethtypes.Receipt, error)
}
type Dependencies struct {
	Repositories           *repository.Repositories
	Wallet                 synapse.WalletOperator
	Receipts               WalletReceiptChecker
	WalletBroadcastTimeout time.Duration
	WalletReceiptTimeout   time.Duration
}
type Handler struct {
	*taskengine.FuncHandler
	deps Dependencies
}

func NewHandler(deps Dependencies) (*Handler, error) {
	if deps.Repositories == nil || deps.Repositories.Tasks == nil {
		return nil, errors.New("wallet handler requires repositories")
	}
	if deps.WalletBroadcastTimeout < 0 || deps.WalletReceiptTimeout < 0 {
		return nil, errors.New("wallet timeouts cannot be negative")
	}
	if deps.WalletBroadcastTimeout == 0 {
		deps.WalletBroadcastTimeout = 2 * time.Minute
	}
	if deps.WalletReceiptTimeout == 0 {
		deps.WalletReceiptTimeout = 15 * time.Second
	}
	h := &Handler{deps: deps}
	h.FuncHandler = h.newHandler()
	return h, nil
}

const externalPollInterval = 5 * time.Second

func (h *Handler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeWalletOperation, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		MaxConcurrency: 1,
		Codec:          taskengine.StrictJSONCodec(func(input *walletoperation.Input) error { return walletoperation.ValidateInput(*input) }),
		Subject:        taskengine.SubjectFromInput("wallet_operation", func(input walletoperation.Input) int64 { return input.OperationID }),
		Policy:         taskengine.ExecutionPolicy{MaxAttempts: 6, Backoff: taskengine.DefaultBackoffPolicy()}, AllowRetry: true,
		CanManualRetry: func(source *model.Task) bool {
			return source != nil && (source.FailureReason == nil || (*source.FailureReason != "invalid_checkpoint" && *source.FailureReason != "wallet_broadcast_unknown" && *source.FailureReason != "wallet_transaction_reverted"))
		},
	}

	definition.InspectRetry = func(ctx context.Context, repos *repository.Repositories, source *model.Task) error {
		var input walletoperation.Input
		if err := json.Unmarshal(source.Input, &input); err != nil {
			return err
		}
		row, err := repos.WalletOperations.GetByID(ctx, input.OperationID)
		if err != nil {
			return err
		}
		if row == nil || row.TaskID == nil || *row.TaskID != source.ID || row.Status == model.WalletOperationStatusUnknown || row.Status == model.WalletOperationStatusConfirmed {
			return repository.ErrConflict
		}
		if row.BroadcastAttemptedAt != nil && (row.TxHash == nil || *row.TxHash == "") {
			return repository.ErrConflict
		}
		return nil
	}
	definition.LegacyHandoff = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
		var input walletoperation.Input
		if err := json.Unmarshal(old.Input, &input); err != nil {
			return err
		}
		return repos.WalletOperations.TransferTaskOwner(ctx, input.OperationID, old.ID, next.ID)
	}
	return taskengine.NewFuncHandler(definition, func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
		return h.executeWalletOperation(ctx, execution)
	},
		func(ctx context.Context, execution taskengine.Execution) taskengine.Result {
			return h.recoverWalletOperation(ctx, execution)
		},
	)
}

func (h *Handler) executeWalletOperation(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	input, err := taskengine.DecodeInput[walletoperation.Input](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeWalletOperation), err)
	}
	op, err := h.deps.Repositories.WalletOperations.GetByID(ctx, input.OperationID)
	if err != nil {
		return h.retryWalletOperation(execution, input.OperationID, err, "wallet_load_failed")
	}
	if result, done := h.walletTerminalResult(op, execution.ID()); done {
		return result
	}
	if op.Status == model.WalletOperationStatusSubmitted || op.BroadcastAttemptedAt != nil {
		return h.recoverWalletOperation(ctx, execution)
	}
	if h.deps.Wallet == nil {
		return taskengine.Fail(errors.New("wallet operator is unavailable"), "dependency_unavailable", nil)
	}
	amount, ok := new(big.Int).SetString(op.Amount, 10)
	if !ok || !validTaskWalletAmount(op.Type, amount) {
		return taskengine.Fail(errors.New("invalid wallet operation amount"), "wallet_amount_invalid", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkFailed(ctx, op.ID, execution.ID(), "invalid wallet operation amount")
		})
	}
	checkpoint := walletoperation.Checkpoint{BroadcastAttempted: true}
	var txHash string
	var alreadyComplete bool
	attempted, err := execution.WithCheckpointedEffect(ctx, fmt.Sprintf("wallet:%d", op.ID), checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkBroadcastAttempted(ctx, op.ID, execution.ID())
	}, func(ctx context.Context) error {
		requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletBroadcastTimeout)
		defer cancel()
		var broadcastErr error
		txHash, alreadyComplete, broadcastErr = broadcastWalletOperation(requestCtx, h.deps.Wallet, op.Type, amount)
		return broadcastErr
	})
	if err != nil {
		if !attempted {
			return retryTask(err, "wallet_broadcast_not_started")
		}
		message := fmt.Sprintf("wallet broadcast outcome is unknown: %v", err)
		return taskengine.Fail(err, "wallet_broadcast_unknown", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkUnknown(ctx, op.ID, execution.ID(), message)
		})
	}
	if alreadyComplete {
		return taskengine.Complete("Wallet authorization already satisfied", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkConfirmedWithoutTransaction(ctx, op.ID, execution.ID())
		})
	}
	if txHash == "" {
		err := errors.New("wallet broadcast returned no transaction hash")
		return taskengine.Fail(err, "wallet_broadcast_unknown", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkUnknown(ctx, op.ID, execution.ID(), err.Error())
		})
	}
	checkpoint.TransactionHash = txHash
	if err := execution.WriteCheckpointWith(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkSubmitted(ctx, op.ID, execution.ID(), txHash)
	}); err != nil {
		return taskengine.Wait(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Recording wallet transaction", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkSubmitted(ctx, op.ID, execution.ID(), txHash)
		})
	}
	return taskengine.Wait(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Waiting for wallet transaction", nil)
}

func (h *Handler) recoverWalletOperation(ctx context.Context, execution taskengine.Execution) (result taskengine.Result) {
	input, err := taskengine.DecodeInput[walletoperation.Input](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeWalletOperation), err)
	}
	op, err := h.deps.Repositories.WalletOperations.GetByID(ctx, input.OperationID)
	if err != nil {
		return h.retryWalletOperation(execution, input.OperationID, err, "wallet_load_failed")
	}
	if op != nil && op.TaskID != nil && *op.TaskID == execution.ID() && op.BroadcastAttemptedAt != nil {
		startedAt := time.Now().UTC()
		defer func() { result = result.WithWorkStartedAt(startedAt) }()
	}
	if result, done := h.walletTerminalResult(op, execution.ID()); done {
		return result
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[walletoperation.Checkpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	txHash := ""
	if op.TxHash != nil {
		txHash = *op.TxHash
	}
	if txHash == "" && hasCheckpoint {
		txHash = checkpoint.TransactionHash
	}
	if txHash == "" {
		if !hasCheckpoint && op.BroadcastAttemptedAt == nil {
			return taskengine.Wait(model.TaskResumeModeExecute, 0, "safe_to_execute", "Wallet operation is ready", nil)
		}
		err := errors.New("wallet transaction identity could not be recovered")
		return taskengine.Fail(err, "wallet_broadcast_unknown", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkUnknown(ctx, op.ID, execution.ID(), err.Error())
		})
	}
	if h.deps.Receipts == nil {
		return taskengine.Wait(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Waiting for wallet transaction", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkSubmitted(ctx, op.ID, execution.ID(), txHash)
		})
	}
	requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletReceiptTimeout)
	receipt, err := h.deps.Receipts.TransactionReceipt(requestCtx, common.HexToHash(txHash))
	cancel()
	markSubmitted := func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkSubmitted(ctx, op.ID, execution.ID(), txHash)
	}
	if err != nil && !errors.Is(err, ethereum.NotFound) {
		return taskengine.RetryInMode(synapse.SummarizedError(err), "transaction_confirmation", model.TaskResumeModeRecover, externalPollInterval, markSubmitted)
	}
	if err != nil || receipt == nil {
		return taskengine.Wait(model.TaskResumeModeRecover, externalPollInterval, "transaction_confirmation", "Waiting for wallet transaction", markSubmitted)
	}
	if receipt.Status == ethtypes.ReceiptStatusSuccessful {
		return taskengine.Complete("Wallet transaction confirmed", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkConfirmed(ctx, op.ID, execution.ID(), txHash)
		})
	}
	message := fmt.Sprintf("wallet transaction reverted: status=%d transaction=%s", receipt.Status, txHash)
	return taskengine.Fail(errors.New(message), "wallet_transaction_reverted", func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkFailed(ctx, op.ID, execution.ID(), message)
	})
}

func (h *Handler) retryWalletOperation(_ taskengine.Execution, _ int64, err error, reason string) taskengine.Result {
	return retryTask(err, reason)
}

func (h *Handler) walletTerminalResult(op *model.WalletOperation, taskID int64) (taskengine.Result, bool) {
	if op == nil {
		return taskengine.Fail(repository.ErrNotFound, "wallet_operation_missing", nil), true
	}
	if op.TaskID == nil || *op.TaskID != taskID {
		switch op.Status {
		case model.WalletOperationStatusConfirmed:
			return taskengine.Complete("Wallet operation completed", nil), true
		case model.WalletOperationStatusFailed, model.WalletOperationStatusUnknown:
			return taskengine.Fail(errors.New("wallet operation requires attention"), "wallet_operation_terminal", nil), true
		default:
			return taskengine.Fail(repository.ErrConflict, "wallet_task_superseded", nil), true
		}
	}
	return taskengine.Result{}, false
}

func validTaskWalletAmount(operationType model.WalletOperationType, amount *big.Int) bool {
	if amount == nil {
		return false
	}
	//exhaustive:enforce
	switch operationType {
	case model.WalletOperationTypeApprove:
		return amount.Sign() == 0
	case model.WalletOperationTypeFund, model.WalletOperationTypeWithdraw:
		return amount.Sign() > 0
	default:
		return false
	}
}

func broadcastWalletOperation(ctx context.Context, operator synapse.WalletOperator, operationType model.WalletOperationType, amount *big.Int) (string, bool, error) {
	//exhaustive:enforce
	switch operationType {
	case model.WalletOperationTypeFund:
		hash, err := operator.FundUSDFC(ctx, amount)
		return hash, false, err
	case model.WalletOperationTypeWithdraw:
		hash, err := operator.WithdrawUSDFC(ctx, amount)
		return hash, false, err
	case model.WalletOperationTypeApprove:
		hash, err := operator.ApproveFWSS(ctx)
		if errors.Is(err, payments.ErrNothingToFund) {
			return "", true, nil
		}
		return hash, false, err
	default:
		return "", false, fmt.Errorf("unsupported wallet operation type %q", operationType)
	}
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.RetryBackoff(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
