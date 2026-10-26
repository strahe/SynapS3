package wallet

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/errtext"
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

func (h *Handler) newHandler() *taskengine.FuncHandler {
	definition := taskengine.Definition{
		Type: model.TaskTypeWalletOperation, InputVersion: 1, WorkStart: taskengine.WorkStartOnEffect,
		MaxConcurrency: 1,
		Codec:          taskengine.StrictJSONCodec(func(input *walletoperation.Input) error { return walletoperation.ValidateInput(*input) }),
		Identity:       taskengine.IdentityFromInput("wallet_operation", func(input walletoperation.Input) (int64, int64) { return input.OperationID, 0 }),
		Policy:         taskengine.ExecutionPolicy{MaxAttempts: 10, ObservationWindow: 5 * time.Minute, ObservationInterval: 30 * time.Second, Backoff: taskengine.BackoffPolicy{InitialDelay: 30 * time.Second, Multiplier: 2, MaximumDelay: 5 * time.Minute, Jitter: 0.2}}, AllowRetry: true,
		CanManualRetry: func(source *model.Task) bool {
			if source == nil {
				return false
			}
			if source.FailureReason == nil {
				return true
			}
			switch *source.FailureReason {
			case "invalid_checkpoint", "wallet_broadcast_unknown", "wallet_transaction_reverted", "wallet_permit_expired", "wallet_transaction_displaced":
				return false
			default:
				return true
			}
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
		var checkpoint walletoperation.Checkpoint
		if row.BroadcastAttemptedAt != nil && len(source.Checkpoint) == 0 {
			return repository.ErrConflict
		}
		if len(source.Checkpoint) != 0 {
			if err := json.Unmarshal(source.Checkpoint, &checkpoint); err != nil {
				return repository.ErrConflict
			}
			if err := checkpoint.Match(input.ExecutionIdentity); err != nil {
				return fmt.Errorf("%w: %w", repository.ErrConflict, err)
			}
		}
		submission, err := walletSubmission(row, checkpoint)
		if err != nil {
			return fmt.Errorf("%w: %w", repository.ErrConflict, err)
		}
		if row.BroadcastAttemptedAt != nil {
			amount, ok := new(big.Int).SetString(row.Amount, 10)
			if !ok || !validTaskWalletAmount(row.Type, amount) {
				return repository.ErrConflict
			}
			if err := synapse.ValidateWalletSubmission(input.ExecutionIdentity, row.Type, amount, submission); err != nil {
				return fmt.Errorf("%w: %w", repository.ErrConflict, err)
			}
		}
		return nil
	}
	definition.BindSuccessor = func(ctx context.Context, repos *repository.Repositories, old, next *model.Task) error {
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
	return h.processWalletOperation(ctx, execution, true)
}

func (h *Handler) recoverWalletOperation(ctx context.Context, execution taskengine.Execution) taskengine.Result {
	return h.processWalletOperation(ctx, execution, false)
}

func (h *Handler) processWalletOperation(ctx context.Context, execution taskengine.Execution, execute bool) (result taskengine.Result) {
	input, err := taskengine.DecodeInput[walletoperation.Input](execution)
	if err != nil {
		return decodeFailure(string(model.TaskTypeWalletOperation), err)
	}
	if h.deps.Wallet == nil {
		return taskengine.Fail(errors.New("wallet operator is unavailable"), "dependency_unavailable", nil)
	}
	if err := input.Match(h.deps.Wallet.WalletIdentity()); err != nil {
		return taskengine.Fail(err, "wallet_identity_changed", nil)
	}
	op, err := h.deps.Repositories.WalletOperations.GetByID(ctx, input.OperationID)
	if err != nil {
		return h.retryWalletOperation(execution, input.OperationID, err, "wallet_load_failed")
	}
	if result, done := h.walletTerminalResult(op, execution.ID()); done {
		return result
	}
	if op.BroadcastAttemptedAt != nil {
		startedAt := time.Now().UTC()
		defer func() { result = result.WithWorkStartedAt(startedAt) }()
	}
	amount, ok := new(big.Int).SetString(op.Amount, 10)
	if !ok || !validTaskWalletAmount(op.Type, amount) {
		err := errors.New("invalid wallet operation amount")
		var settlement taskengine.Settlement
		if op.Status == model.WalletOperationStatusPending && op.BroadcastAttemptedAt == nil && op.TxHash == nil && len(op.SignedTransaction) == 0 && len(execution.Checkpoint()) == 0 {
			settlement = func(ctx context.Context, repos *repository.Repositories) error {
				return repos.WalletOperations.MarkFailed(ctx, op.ID, execution.ID(), errtext.Format(err))
			}
		}
		return taskengine.Fail(err, "wallet_amount_invalid", settlement)
	}
	checkpoint, hasCheckpoint, err := taskengine.DecodeCheckpoint[walletoperation.Checkpoint](execution)
	if err != nil {
		return taskengine.Fail(err, "invalid_checkpoint", nil)
	}
	if !hasCheckpoint && (op.BroadcastAttemptedAt != nil || op.TxHash != nil || len(op.SignedTransaction) != 0 || op.Status == model.WalletOperationStatusSubmitted) {
		return taskengine.Fail(errors.New("wallet broadcast identity is missing"), "wallet_evidence_conflict", nil)
	}
	if hasCheckpoint {
		if err := checkpoint.Match(input.ExecutionIdentity); err != nil {
			return taskengine.Fail(err, "wallet_identity_changed", nil)
		}
		if !checkpoint.BroadcastAttempted {
			return taskengine.Fail(errors.New("saved wallet broadcast details are incomplete"), "wallet_evidence_conflict", nil)
		}
	}
	submission, err := walletSubmission(op, checkpoint)
	if err != nil {
		return taskengine.Fail(err, "wallet_evidence_conflict", nil)
	}
	if submission.TxHash == "" {
		if hasCheckpoint || op.BroadcastAttemptedAt != nil || len(submission.SignedTransaction) != 0 || op.Status == model.WalletOperationStatusSubmitted {
			if op.BroadcastAttemptedAt == nil {
				return taskengine.Fail(errors.New("saved wallet broadcast details do not match"), "wallet_evidence_conflict", nil)
			}
			return h.unknownWalletOperation(execution, op, errors.New("wallet transaction identity could not be recovered"))
		}
		if !execute {
			return taskengine.Wait(model.TaskResumeModeExecute, "safe_to_execute", "Wallet operation is ready", nil)
		}
		return h.broadcastWalletOperation(ctx, execution, input, op, amount)
	}
	if err := synapse.ValidateWalletSubmission(input.ExecutionIdentity, op.Type, amount, submission); err != nil {
		return taskengine.Fail(err, "wallet_evidence_conflict", nil)
	}
	checkpoint = walletoperation.Checkpoint{ExecutionIdentity: input.ExecutionIdentity, BroadcastAttempted: true, TransactionHash: submission.TxHash}
	if op.TxHash == nil || op.Status != model.WalletOperationStatusSubmitted {
		if op.BroadcastAttemptedAt == nil {
			return taskengine.Fail(errors.New("wallet transaction has no broadcast attempt"), "wallet_evidence_conflict", nil)
		}
		if err := execution.WriteCheckpointWith(ctx, checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.RecordSubmission(ctx, op.ID, execution.ID(), submission.TxHash, submission.SignedTransaction, "")
		}); err != nil {
			return h.walletRetry(execution, op, err, "wallet_record_submission_failed")
		}
		op.Status = model.WalletOperationStatusSubmitted
		op.TxHash = &submission.TxHash
	}
	if h.deps.Receipts == nil {
		return h.walletObservation(execution, op, submission, nil)
	}
	requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletReceiptTimeout)
	receipt, receiptErr := h.deps.Receipts.TransactionReceipt(requestCtx, common.HexToHash(submission.TxHash))
	cancel()
	if receiptErr != nil && !errors.Is(receiptErr, ethereum.NotFound) {
		return h.walletRetry(execution, op, receiptErr, "transaction_confirmation")
	}
	if receiptErr == nil && receipt != nil {
		if receipt.TxHash != common.HexToHash(submission.TxHash) {
			return taskengine.Fail(errors.New("wallet receipt belongs to another transaction"), "wallet_evidence_conflict", nil)
		}
		if receipt.Status != ethtypes.ReceiptStatusSuccessful && receipt.Status != ethtypes.ReceiptStatusFailed {
			return taskengine.Fail(errors.New("wallet receipt has an invalid transaction status"), "wallet_evidence_conflict", nil)
		}
		if receipt.Status == ethtypes.ReceiptStatusSuccessful {
			return taskengine.Complete("Wallet transaction confirmed", func(ctx context.Context, repos *repository.Repositories) error {
				return repos.WalletOperations.MarkConfirmed(ctx, op.ID, execution.ID(), submission.TxHash)
			})
		}
		message := fmt.Sprintf("wallet transaction reverted: status=%d transaction=%s", receipt.Status, submission.TxHash)
		return taskengine.Fail(errors.New(message), "wallet_transaction_reverted", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkFailed(ctx, op.ID, execution.ID(), message)
		})
	}
	if !execute || len(submission.SignedTransaction) == 0 {
		return h.walletObservation(execution, op, submission, nil)
	}
	attempted, replayErr := execution.WithCheckpointedEffect(ctx, fmt.Sprintf("wallet:%d", op.ID), checkpoint, nil, func(ctx context.Context) error {
		requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletBroadcastTimeout)
		defer cancel()
		return h.deps.Wallet.RebroadcastSignedTransaction(requestCtx, op.Type, amount, submission, h.deps.WalletReceiptTimeout)
	})
	if errors.Is(replayErr, taskengine.ErrEffectAlreadyAdmitted) {
		return h.walletObservation(execution, op, submission, nil)
	}
	if errors.Is(replayErr, synapse.ErrWalletPermitExpired) {
		message := fmt.Sprintf("Transaction %s expired before it was included in a block, so no funds moved. Start the operation again.", submission.TxHash)
		return taskengine.Fail(replayErr, "wallet_permit_expired", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkFailed(ctx, op.ID, execution.ID(), message)
		})
	}
	if errors.Is(replayErr, synapse.ErrWalletTransactionDisplaced) {
		message := fmt.Sprintf("Another transaction from this wallet took the place of transaction %s, so it will not be included. Check the wallet's recent transactions on chain to see whether the operation happened.", submission.TxHash)
		return taskengine.Fail(replayErr, "wallet_transaction_displaced", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkSubmissionUnknown(ctx, op.ID, execution.ID(), message)
		})
	}
	if errors.Is(replayErr, synapse.ErrWalletReplayInvalidEvidence) {
		return taskengine.Fail(replayErr, "wallet_evidence_conflict", nil)
	}
	if replayErr != nil && !errors.Is(replayErr, synapse.ErrWalletReplaySkipped) && !errors.Is(replayErr, synapse.ErrWalletReplayBroadcast) {
		return h.walletRetry(execution, op, replayErr, "wallet_replay_failed")
	}
	var settlement taskengine.Settlement
	if attempted && errors.Is(replayErr, synapse.ErrWalletReplayBroadcast) {
		settlement = func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.RecordConfirmationError(ctx, op.ID, execution.ID(), errtext.Format(replayErr))
		}
	}
	return h.walletObservation(execution, op, submission, settlement)
}

func walletSubmission(op *model.WalletOperation, checkpoint walletoperation.Checkpoint) (synapse.WalletSubmission, error) {
	submission := synapse.WalletSubmission{SignedTransaction: op.SignedTransaction}
	if op.TxHash != nil {
		submission.TxHash = *op.TxHash
		if !validTransactionHash(submission.TxHash) {
			return submission, errors.New("wallet transaction hash is invalid")
		}
	}
	if checkpoint.TransactionHash != "" {
		if !validTransactionHash(checkpoint.TransactionHash) || (submission.TxHash != "" && !strings.EqualFold(submission.TxHash, checkpoint.TransactionHash)) {
			return submission, errors.New("wallet transaction hashes disagree")
		}
		if submission.TxHash == "" {
			submission.TxHash = checkpoint.TransactionHash
		}
	}
	return submission, nil
}

func validTransactionHash(hash string) bool {
	if len(hash) != 66 || !strings.HasPrefix(hash, "0x") {
		return false
	}
	_, err := hex.DecodeString(hash[2:])
	return err == nil && common.HexToHash(hash) != (common.Hash{})
}

func (h *Handler) broadcastWalletOperation(ctx context.Context, execution taskengine.Execution, input walletoperation.Input, op *model.WalletOperation, amount *big.Int) taskengine.Result {
	checkpoint := walletoperation.Checkpoint{ExecutionIdentity: input.ExecutionIdentity, BroadcastAttempted: true}
	var submission synapse.WalletSubmission
	var alreadyComplete bool
	attempted, broadcastErr := execution.WithCheckpointedEffect(ctx, fmt.Sprintf("wallet:%d", op.ID), checkpoint, func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkBroadcastAttempted(ctx, op.ID, execution.ID())
	}, func(ctx context.Context) error {
		requestCtx, cancel := context.WithTimeout(ctx, h.deps.WalletBroadcastTimeout)
		defer cancel()
		var err error
		submission, alreadyComplete, err = broadcastWalletOperation(requestCtx, h.deps.Wallet, op.Type, amount)
		return err
	})
	if !attempted {
		if errors.Is(broadcastErr, taskengine.ErrEffectAlreadyAdmitted) {
			return taskengine.Wait(model.TaskResumeModeRecover, "transaction_confirmation", "Recovering wallet transaction", nil)
		}
		return taskengine.RetryInMode(broadcastErr, "wallet_broadcast_not_started", model.TaskResumeModeRecover, nil)
	}
	if alreadyComplete {
		return taskengine.Complete("Wallet authorization already satisfied", func(ctx context.Context, repos *repository.Repositories) error {
			return repos.WalletOperations.MarkConfirmedWithoutTransaction(ctx, op.ID, execution.ID())
		})
	}
	if errors.Is(broadcastErr, synapse.ErrWalletReplayInvalidEvidence) {
		if validTransactionHash(submission.TxHash) {
			broadcastErr = fmt.Errorf("wallet transaction %s: %w", submission.TxHash, broadcastErr)
		}
		return taskengine.Fail(broadcastErr, "wallet_evidence_conflict", nil)
	}
	if submission.TxHash == "" && len(submission.SignedTransaction) == 0 {
		if broadcastErr == nil {
			broadcastErr = errors.New("wallet broadcast returned no transaction hash")
		}
		return h.unknownWalletOperation(execution, op, broadcastErr)
	}
	if err := synapse.ValidateWalletSubmission(input.ExecutionIdentity, op.Type, amount, submission); err != nil {
		if validTransactionHash(submission.TxHash) {
			err = fmt.Errorf("wallet transaction %s: %w", submission.TxHash, err)
		}
		return taskengine.Fail(err, "wallet_evidence_conflict", nil)
	}
	checkpoint.TransactionHash = submission.TxHash
	lastError := ""
	if broadcastErr != nil {
		lastError = errtext.Format(broadcastErr)
	}
	persist := func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.RecordSubmission(ctx, op.ID, execution.ID(), submission.TxHash, submission.SignedTransaction, lastError)
	}
	if err := execution.RecordEffectEvidence(ctx, checkpoint, persist); err != nil {
		rawCheckpoint, marshalErr := json.Marshal(checkpoint)
		if marshalErr != nil {
			return taskengine.Fail(marshalErr, "invalid_checkpoint", nil)
		}
		return h.walletObservation(execution, op, submission, func(ctx context.Context, repos *repository.Repositories) error {
			if err := repos.Tasks.WriteCheckpoint(ctx, execution.ID(), execution.ClaimGeneration(), rawCheckpoint); err != nil {
				return err
			}
			return persist(ctx, repos)
		})
	}
	return h.walletObservation(execution, op, submission, nil)
}

func (h *Handler) unknownWalletOperation(execution taskengine.Execution, op *model.WalletOperation, err error) taskengine.Result {
	message := fmt.Sprintf("wallet broadcast outcome is unknown: %v", err)
	return taskengine.Fail(err, "wallet_broadcast_unknown", func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.MarkUnknown(ctx, op.ID, execution.ID(), message)
	})
}

func (h *Handler) walletObservation(execution taskengine.Execution, op *model.WalletOperation, submission synapse.WalletSubmission, settlement taskengine.Settlement) taskengine.Result {
	mode := model.TaskResumeModeRecover
	if len(submission.SignedTransaction) != 0 {
		mode = model.TaskResumeModeExecute
	}
	err := fmt.Errorf("wallet transaction %s is still unconfirmed after %s", submission.TxHash, execution.Policy().ObservationWindow)
	return taskengine.WaitForObservation(model.TaskResumeModeRecover, "transaction_confirmation", "Waiting for wallet transaction", settlement).
		WithObservationWindow(fmt.Sprintf("wallet:%d:%s", op.ID, submission.TxHash), err, mode).
		WithRetrySettlements(nil, h.confirmationStopped(execution, op, err))
}

func (h *Handler) walletRetry(execution taskengine.Execution, op *model.WalletOperation, err error, reason string) taskengine.Result {
	if op.Status != model.WalletOperationStatusSubmitted {
		return taskengine.RetryInMode(err, reason, model.TaskResumeModeRecover, nil)
	}
	return taskengine.RetryInMode(err, reason, model.TaskResumeModeRecover, nil).
		WithRetrySettlements(func(ctx context.Context, repos *repository.Repositories, _ time.Time) error {
			return repos.WalletOperations.RecordConfirmationError(ctx, op.ID, execution.ID(), errtext.Format(err))
		}, h.confirmationStopped(execution, op, err))
}

func (h *Handler) confirmationStopped(execution taskengine.Execution, op *model.WalletOperation, cause error) taskengine.Settlement {
	return func(ctx context.Context, repos *repository.Repositories) error {
		return repos.WalletOperations.RecordConfirmationError(ctx, op.ID, execution.ID(), errtext.Format(fmt.Errorf("automatic wallet confirmation stopped after %d attempts: %w", execution.RetryCount()+1, cause)))
	}
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
	if amount == nil || amount.BitLen() > 256 {
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

func broadcastWalletOperation(ctx context.Context, operator synapse.WalletOperator, operationType model.WalletOperationType, amount *big.Int) (synapse.WalletSubmission, bool, error) {
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
			return synapse.WalletSubmission{}, true, nil
		}
		return hash, false, err
	default:
		return synapse.WalletSubmission{}, false, fmt.Errorf("unsupported wallet operation type %q", operationType)
	}
}

func retryTask(err error, reason string) taskengine.Result {
	return taskengine.Retry(err, reason, nil)
}

func decodeFailure(taskType string, err error) taskengine.Result {
	return taskengine.Fail(fmt.Errorf("decoding %s input: %w", taskType, err), "invalid_input", nil)
}
