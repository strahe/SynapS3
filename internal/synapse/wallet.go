package synapse

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	sdk "github.com/strahe/synapse-go"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/payments"
)

type walletPayments interface {
	WalletBalance(ctx context.Context, token, account common.Address) (*big.Int, error)
	AccountInfo(ctx context.Context, token, owner common.Address) (*payments.AccountState, error)
}

type walletQuerier struct {
	payments walletPayments
	address  common.Address
	chain    chain.Chain
	addrs    sdk.ResolvedAddresses
}

// NewWalletQuerier creates a WalletQuerier backed by synapse-go payments.Service.
func NewWalletQuerier(paySvc *payments.Service, address common.Address, c chain.Chain, addrs sdk.ResolvedAddresses) WalletQuerier {
	return &walletQuerier{
		payments: paySvc,
		address:  address,
		chain:    c,
		addrs:    addrs,
	}
}

// GetWalletInfo queries on-chain state in parallel. Individual RPC failures
// produce nil fields and entries in the Errors map instead of aborting.
func (w *walletQuerier) GetWalletInfo(ctx context.Context) (*WalletInfo, error) {
	currentEpoch := chain.CurrentEpoch(w.chain)

	info := &WalletInfo{
		Address:              w.address.Hex(),
		Network:              w.chain.String(),
		ChainID:              w.chain.ChainID(),
		CurrentEpoch:         currentEpoch,
		EpochDurationSeconds: chain.EpochDurationSeconds,
		PaymentsAddress:      w.addrs.Payments.Hex(),
		USDFCAddress:         w.addrs.USDFC.Hex(),
		USDFCDecimals:        18,
		Errors:               make(map[string]string),
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	setErr := func(field string, err error) {
		mu.Lock()
		info.Errors[field] = sanitizeRPCError(err)
		mu.Unlock()
	}

	wg.Add(3)

	go func() {
		defer wg.Done()
		bal, err := w.payments.WalletBalance(ctx, w.addrs.USDFC, w.address)
		if err != nil {
			setErr("usdfc_wallet_balance", err)
			return
		}
		mu.Lock()
		info.USDFCWalletBalance = cloneBigInt(bal)
		mu.Unlock()
	}()

	go func() {
		defer wg.Done()
		bal, err := w.payments.WalletBalance(ctx, payments.ZeroAddress, w.address)
		if err != nil {
			setErr("fil_gas_balance", err)
			return
		}
		mu.Lock()
		info.FILGasBalance = cloneBigInt(bal)
		mu.Unlock()
	}()

	go func() {
		defer wg.Done()
		acct, err := w.payments.AccountInfo(ctx, w.addrs.USDFC, w.address)
		if err != nil {
			setErr("usdfc_account", err)
			return
		}
		mu.Lock()
		info.PaymentAccount = convertAccountState(acct, w.chain, currentEpoch)
		mu.Unlock()
	}()

	wg.Wait()
	info.CurrentEpoch = cloneBigInt(info.CurrentEpoch)
	return info, nil
}

func convertAccountState(acct *payments.AccountState, c chain.Chain, currentEpoch *big.Int) *PaymentAccountInfo {
	if acct == nil {
		return nil
	}
	out := &PaymentAccountInfo{
		Funds:               cloneBigInt(acct.Funds),
		AvailableFunds:      acct.AvailableFunds(),
		LockupCurrent:       cloneBigInt(acct.LockupCurrent),
		LockupRate:          cloneBigInt(acct.LockupRate),
		LockupLastSettledAt: cloneBigInt(acct.LockupLastSettledAt),
		FundedUntilEpoch:    cloneBigInt(acct.FundedUntilEpoch),
	}
	out.LockupRatePerDay = multiplyBigInt(out.LockupRate, chain.EpochsPerDay)
	out.LockupRatePerMonth = multiplyBigInt(out.LockupRate, chain.EpochsPerMonth)

	out.NoActiveSpend = noActivePaymentSpend(out.LockupRate, out.FundedUntilEpoch)
	if out.NoActiveSpend {
		return out
	}

	if fundedUntil := chain.EpochToTime(c, out.FundedUntilEpoch); !fundedUntil.IsZero() {
		out.FundedUntilTime = &fundedUntil
	}
	if seconds, ok := runwaySeconds(currentEpoch, out.FundedUntilEpoch); ok {
		out.RunwaySeconds = &seconds
	}
	return out
}

func multiplyBigInt(v *big.Int, n int64) *big.Int {
	if v == nil {
		return nil
	}
	return new(big.Int).Mul(new(big.Int).Set(v), big.NewInt(n))
}

func noActivePaymentSpend(lockupRate, fundedUntilEpoch *big.Int) bool {
	if lockupRate == nil || lockupRate.Sign() <= 0 {
		return true
	}
	if fundedUntilEpoch == nil {
		return false
	}
	return fundedUntilEpoch.Cmp(uint256Max()) == 0
}

func runwaySeconds(currentEpoch, fundedUntilEpoch *big.Int) (int64, bool) {
	if currentEpoch == nil || fundedUntilEpoch == nil {
		return 0, false
	}
	delta := new(big.Int).Sub(new(big.Int).Set(fundedUntilEpoch), currentEpoch)
	if delta.Sign() < 0 {
		return 0, true
	}
	seconds := delta.Mul(delta, big.NewInt(chain.EpochDurationSeconds))
	if !seconds.IsInt64() {
		return 0, false
	}
	return seconds.Int64(), true
}

func uint256Max() *big.Int {
	return new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
}

func cloneBigInt(v *big.Int) *big.Int {
	if v == nil {
		return nil
	}
	return new(big.Int).Set(v)
}

// sanitizeRPCError returns a user-safe error description, stripping
// potential RPC endpoint URLs that may contain API keys.
func sanitizeRPCError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "request timed out"
	case errors.Is(err, context.Canceled):
		return "request cancelled"
	default:
		return "RPC call failed"
	}
}
