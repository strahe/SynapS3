package synapse

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/walletoperation"
)

var (
	ErrWalletReplayInvalidEvidence = errors.New("could not verify the saved wallet transaction")
	ErrWalletReplaySkipped         = errors.New("wallet transaction replay skipped")
	ErrWalletReplayBroadcast       = errors.New("wallet transaction replay broadcast failed")
)

// WalletReplayClient observes and sends the exact transaction retained by the ledger.
type WalletReplayClient interface {
	ChainID(context.Context) (*big.Int, error)
	TransactionReceipt(context.Context, common.Hash) (*ethtypes.Receipt, error)
	TransactionByHash(context.Context, common.Hash) (*ethtypes.Transaction, bool, error)
	NonceAt(context.Context, common.Address, *big.Int) (uint64, error)
	PendingNonceAt(context.Context, common.Address) (uint64, error)
	SendTransaction(context.Context, *ethtypes.Transaction) error
}

// These signatures describe the payment methods used by the SDK's Fund and Withdraw.
const walletPaymentMethods = `[
 {"type":"function","name":"depositWithPermit","inputs":[{"type":"address"},{"type":"address"},{"type":"uint256"},{"type":"uint256"},{"type":"uint8"},{"type":"bytes32"},{"type":"bytes32"}]},
 {"type":"function","name":"depositWithPermitAndApproveOperator","inputs":[{"type":"address"},{"type":"address"},{"type":"uint256"},{"type":"uint256"},{"type":"uint8"},{"type":"bytes32"},{"type":"bytes32"},{"type":"address"},{"type":"uint256"},{"type":"uint256"},{"type":"uint256"}]},
 {"type":"function","name":"setOperatorApproval","inputs":[{"type":"address"},{"type":"address"},{"type":"bool"},{"type":"uint256"},{"type":"uint256"},{"type":"uint256"}]},
 {"type":"function","name":"withdraw","inputs":[{"type":"address"},{"type":"uint256"}]}
]`

var walletPaymentABI = func() abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(walletPaymentMethods))
	if err != nil {
		panic(err)
	}
	return parsed
}()

// ValidateWalletSubmission checks frozen identity and transaction intent without RPC calls.
func ValidateWalletSubmission(identity walletoperation.ExecutionIdentity, operationType model.WalletOperationType, amount *big.Int, submission WalletSubmission) error {
	_, _, err := decodeWalletSubmission(identity, operationType, amount, submission)
	return err
}

func decodeWalletSubmission(identity walletoperation.ExecutionIdentity, operationType model.WalletOperationType, amount *big.Int, submission WalletSubmission) (*ethtypes.Transaction, *big.Int, error) {
	invalid := func(reason string) (*ethtypes.Transaction, *big.Int, error) {
		return nil, nil, fmt.Errorf("%w: %s", ErrWalletReplayInvalidEvidence, reason)
	}
	if !identity.Complete() {
		return invalid("wallet identity is incomplete")
	}
	if amount == nil || amount.Sign() < 0 || amount.BitLen() > 256 {
		return invalid("amount is outside uint256")
	}
	//exhaustive:enforce
	switch operationType {
	case model.WalletOperationTypeFund, model.WalletOperationTypeWithdraw:
		if amount.Sign() <= 0 {
			return invalid("amount must be positive")
		}
	case model.WalletOperationTypeApprove:
		if amount.Sign() != 0 {
			return invalid("approval amount must be zero")
		}
	default:
		return invalid("unsupported operation")
	}
	if len(submission.TxHash) != 66 || !strings.HasPrefix(submission.TxHash, "0x") {
		return invalid("malformed transaction hash")
	}
	hashBytes, err := hex.DecodeString(submission.TxHash[2:])
	if err != nil || common.BytesToHash(hashBytes) == (common.Hash{}) {
		return invalid("malformed transaction hash")
	}
	if len(submission.SignedTransaction) == 0 {
		return nil, nil, nil
	}
	var tx ethtypes.Transaction
	if err := tx.UnmarshalBinary(submission.SignedTransaction); err != nil {
		return invalid("cannot decode signed transaction")
	}
	switch tx.Type() {
	case ethtypes.LegacyTxType, ethtypes.AccessListTxType, ethtypes.DynamicFeeTxType:
	default:
		return invalid("unsupported payment transaction type")
	}
	encoded, err := tx.MarshalBinary()
	if err != nil || !bytes.Equal(encoded, submission.SignedTransaction) {
		return invalid("noncanonical signed transaction")
	}
	if tx.Hash() != common.BytesToHash(hashBytes) {
		return invalid("transaction hash mismatch")
	}
	if tx.ChainId().Cmp(identity.Identity.ChainID.BigInt()) != 0 {
		return invalid("transaction chain mismatch")
	}
	sender, err := ethtypes.Sender(ethtypes.LatestSignerForChainID(tx.ChainId()), &tx)
	if err != nil || sender != identity.Identity.Payer {
		return invalid("transaction signer mismatch")
	}
	if tx.To() == nil || *tx.To() != *identity.Payments || tx.Value().Sign() != 0 {
		return invalid("transaction target or native value mismatch")
	}
	if len(tx.Data()) < 4 {
		return invalid("missing payment method")
	}
	method, err := walletPaymentABI.MethodById(tx.Data()[:4])
	if err != nil {
		return invalid("unsupported payment method")
	}
	args, err := method.Inputs.Unpack(tx.Data()[4:])
	if err != nil {
		return invalid("invalid payment arguments")
	}
	encodedArgs, err := method.Inputs.Pack(args...)
	if err != nil || !bytes.Equal(encodedArgs, tx.Data()[4:]) {
		return invalid("noncanonical payment arguments")
	}
	if args[0].(common.Address) != *identity.USDFC {
		return invalid("payment token mismatch")
	}
	var deadline *big.Int
	switch method.Name {
	case "depositWithPermit", "depositWithPermitAndApproveOperator":
		if operationType != model.WalletOperationTypeFund || args[1].(common.Address) != identity.Identity.Payer || args[2].(*big.Int).Cmp(amount) != 0 {
			return invalid("funding intent mismatch")
		}
		deadline = args[3].(*big.Int)
		if method.Name == "depositWithPermitAndApproveOperator" && args[7].(common.Address) != identity.Identity.RecordKeeper {
			return invalid("funding operator mismatch")
		}
	case "setOperatorApproval":
		if operationType != model.WalletOperationTypeApprove || args[1].(common.Address) != identity.Identity.RecordKeeper || !args[2].(bool) {
			return invalid("approval intent mismatch")
		}
	case "withdraw":
		if operationType != model.WalletOperationTypeWithdraw || args[1].(*big.Int).Cmp(amount) != 0 {
			return invalid("withdrawal intent mismatch")
		}
	default:
		return invalid("unsupported payment method")
	}
	return &tx, deadline, nil
}

func (o *walletOperator) RebroadcastSignedTransaction(ctx context.Context, operationType model.WalletOperationType, amount *big.Int, submission WalletSubmission, observationTimeout time.Duration) (replayErr error) {
	tx, deadline, err := decodeWalletSubmission(o.identity, operationType, amount, submission)
	if err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf("%w: signed bytes are unavailable", ErrWalletReplaySkipped)
	}
	defer func() { replayErr = redactSignedTransactionError(replayErr, submission.SignedTransaction) }()
	if o.replay == nil {
		return errors.New("wallet replay client is unavailable")
	}
	if observationTimeout <= 0 {
		return errors.New("wallet replay observation timeout must be positive")
	}
	readCtx, cancel := context.WithTimeout(ctx, observationTimeout)
	defer cancel()
	chainID, err := o.replay.ChainID(readCtx)
	if err != nil {
		return fmt.Errorf("read replay chain: %w", err)
	}
	if chainID == nil || chainID.Cmp(tx.ChainId()) != 0 {
		return fmt.Errorf("%w: RPC chain mismatch", ErrWalletReplayInvalidEvidence)
	}
	receipt, err := o.replay.TransactionReceipt(readCtx, tx.Hash())
	if err == nil {
		if receipt == nil || receipt.TxHash != tx.Hash() {
			return fmt.Errorf("%w: receipt hash mismatch", ErrWalletReplayInvalidEvidence)
		}
		return fmt.Errorf("%w: receipt is available", ErrWalletReplaySkipped)
	}
	if !errors.Is(err, ethereum.NotFound) {
		return fmt.Errorf("read replay receipt: %w", err)
	}
	visible, _, err := o.replay.TransactionByHash(readCtx, tx.Hash())
	if err == nil {
		if visible == nil || visible.Hash() != tx.Hash() {
			return fmt.Errorf("%w: visible transaction hash mismatch", ErrWalletReplayInvalidEvidence)
		}
		return fmt.Errorf("%w: transaction is visible", ErrWalletReplaySkipped)
	}
	if !errors.Is(err, ethereum.NotFound) {
		return fmt.Errorf("read replay transaction: %w", err)
	}
	if deadline != nil && deadline.Cmp(big.NewInt(time.Now().Unix())) <= 0 {
		return fmt.Errorf("%w: permit has expired", ErrWalletReplaySkipped)
	}
	latest, err := o.replay.NonceAt(readCtx, o.identity.Identity.Payer, nil)
	if err != nil {
		return fmt.Errorf("read replay latest nonce: %w", err)
	}
	pending, err := o.replay.PendingNonceAt(readCtx, o.identity.Identity.Payer)
	if err != nil {
		return fmt.Errorf("read replay pending nonce: %w", err)
	}
	if latest > tx.Nonce() || pending > tx.Nonce() {
		return fmt.Errorf("%w: sender nonce has advanced", ErrWalletReplaySkipped)
	}
	if err := o.replay.SendTransaction(ctx, tx); err != nil {
		return fmt.Errorf("%w: %w", ErrWalletReplayBroadcast, err)
	}
	return nil
}

type walletTransactionError struct {
	cause   error
	message string
	encoded string
}

func (e *walletTransactionError) Error() string { return e.message }
func (e *walletTransactionError) Unwrap() error { return e.cause }

func (e *walletTransactionError) SensitiveErrorValues() []string { return []string{e.encoded} }

func redactSignedTransactionError(err error, raw []byte) error {
	if err == nil || len(raw) == 0 {
		return err
	}
	message := err.Error()
	encoded := hex.EncodeToString(raw)
	message = regexp.MustCompile("(?i)(0x)?"+encoded).ReplaceAllString(message, "[signed transaction]")
	return &walletTransactionError{cause: err, message: message, encoded: encoded}
}
