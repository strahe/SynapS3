package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"

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
}

func NewWalletOperator(paySvc *payments.Service, usdfc common.Address) WalletOperator {
	return &walletOperator{payments: paySvc, usdfc: usdfc}
}

func (o *walletOperator) FundUSDFC(ctx context.Context, amount *big.Int) (string, error) {
	res, err := o.payments.Fund(ctx, amount)
	return writeResultHash(res, err, "fund USDFC")
}

func (o *walletOperator) WithdrawUSDFC(ctx context.Context, amount *big.Int) (string, error) {
	res, err := o.payments.Withdraw(ctx, o.usdfc, amount)
	return writeResultHash(res, err, "withdraw USDFC")
}

func (o *walletOperator) ApproveFWSS(ctx context.Context) (string, error) {
	res, err := o.payments.Fund(ctx, big.NewInt(0))
	return writeResultHash(res, err, "approve FWSS")
}

func writeResultHash(res *sdktypes.WriteResult, err error, action string) (string, error) {
	if err != nil {
		return "", fmt.Errorf("%s: %w", action, err)
	}
	if res == nil {
		return "", fmt.Errorf("%s: %w", action, errors.New("missing transaction result"))
	}
	if res.Hash == (common.Hash{}) {
		return "", fmt.Errorf("%s: %w", action, errors.New("missing transaction hash"))
	}
	return res.Hash.Hex(), nil
}
