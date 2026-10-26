package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/strahe/synaps3/internal/walletoperation"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/payments"
	sdktypes "github.com/strahe/synapse-go/types"
)

type walletPaymentOperator interface {
	Fund(ctx context.Context, amount *big.Int, opts ...payments.WriteOption) (*sdktypes.WriteResult, error)
	Withdraw(ctx context.Context, token common.Address, amount *big.Int, opts ...payments.WriteOption) (*sdktypes.WriteResult, error)
}

type walletOperator struct {
	payments walletPaymentOperator
	usdfc    common.Address
	identity walletoperation.ExecutionIdentity
	replay   WalletReplayClient
}

func NewWalletOperator(paySvc *payments.Service, usdfc common.Address, identity walletoperation.ExecutionIdentity, replay WalletReplayClient) WalletOperator {
	return &walletOperator{payments: paySvc, usdfc: usdfc, identity: identity, replay: replay}
}

func (o *walletOperator) WalletIdentity() walletoperation.ExecutionIdentity { return o.identity }

func (o *walletOperator) FundUSDFC(ctx context.Context, amount *big.Int) (WalletSubmission, error) {
	res, err := o.payments.Fund(ctx, amount)
	return WalletSubmissionFromWriteResult(res, err, "fund USDFC")
}

func (o *walletOperator) WithdrawUSDFC(ctx context.Context, amount *big.Int) (WalletSubmission, error) {
	res, err := o.payments.Withdraw(ctx, o.usdfc, amount)
	return WalletSubmissionFromWriteResult(res, err, "withdraw USDFC")
}

func (o *walletOperator) ApproveFWSS(ctx context.Context) (WalletSubmission, error) {
	res, err := o.payments.Fund(ctx, big.NewInt(0))
	return WalletSubmissionFromWriteResult(res, err, "approve FWSS")
}

// WalletSubmissionFromWriteResult retains broadcast evidence and its error chain
// while keeping signed transaction bytes out of error messages.
func WalletSubmissionFromWriteResult(res *sdktypes.WriteResult, err error, action string) (WalletSubmission, error) {
	var submission WalletSubmission
	if res != nil && res.Hash != (common.Hash{}) {
		submission.TxHash = res.Hash.Hex()
	}
	if err != nil {
		broadcast, isBroadcast := errors.AsType[*sdktypes.BroadcastError](err)
		submission.NotSent = res == nil && !isBroadcast
		if isBroadcast && broadcast != nil && broadcast.Transaction != nil {
			raw, marshalErr := broadcast.Transaction.MarshalBinary()
			if marshalErr != nil {
				return submission, fmt.Errorf("%s: %w", action, errors.Join(err, marshalErr))
			}
			if submission.TxHash != "" && submission.TxHash != broadcast.Transaction.Hash().Hex() {
				return WalletSubmission{}, fmt.Errorf("%s: result hash %s, signed transaction hash %s: %w", action, submission.TxHash, broadcast.Transaction.Hash().Hex(), errors.Join(redactSignedTransactionError(err, raw), ErrWalletReplayInvalidEvidence))
			}
			submission.TxHash = broadcast.Transaction.Hash().Hex()
			submission.SignedTransaction = raw
			err = redactSignedTransactionError(err, raw)
		}
		return submission, fmt.Errorf("%s: %w", action, err)
	}
	if res == nil {
		return submission, fmt.Errorf("%s: %w", action, errors.New("missing transaction result"))
	}
	if res.Hash == (common.Hash{}) {
		return submission, fmt.Errorf("%s: %w", action, errors.New("missing transaction hash"))
	}
	return submission, nil
}
