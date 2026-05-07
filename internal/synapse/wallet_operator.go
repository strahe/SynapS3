package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/payments"
	sdktypes "github.com/strahe/synapse-go/types"
)

type walletPaymentOperator interface {
	Fund(ctx context.Context, amount *big.Int, opts ...payments.WriteOption) (*sdktypes.WriteResult, error)
	Withdraw(ctx context.Context, token common.Address, amount *big.Int, opts ...payments.WriteOption) (*sdktypes.WriteResult, error)
}

type walletOperator struct {
	payments walletPaymentOperator
	chain    chain.Chain
}

func NewWalletOperator(paySvc *payments.Service, c chain.Chain) WalletOperator {
	return &walletOperator{payments: paySvc, chain: c}
}

func (o *walletOperator) FundUSDFC(ctx context.Context, amount *big.Int) (string, error) {
	res, err := o.payments.Fund(ctx, amount)
	return writeResultHash(res, err, "fund USDFC")
}

func (o *walletOperator) WithdrawUSDFC(ctx context.Context, amount *big.Int) (string, error) {
	res, err := o.payments.Withdraw(ctx, o.chain.Addresses().USDFC, amount)
	return writeResultHash(res, err, "withdraw USDFC")
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
