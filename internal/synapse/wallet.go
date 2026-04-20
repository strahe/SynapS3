package synapse

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/payments"
)

type walletQuerier struct {
	payments *payments.Service
	address  common.Address
	chain    chain.Chain
}

// NewWalletQuerier creates a WalletQuerier backed by synapse-go payments.Service.
func NewWalletQuerier(paySvc *payments.Service, address common.Address, c chain.Chain) WalletQuerier {
	return &walletQuerier{
		payments: paySvc,
		address:  address,
		chain:    c,
	}
}

// GetWalletInfo queries on-chain state in parallel. Individual RPC failures
// produce nil fields and entries in the Errors map instead of aborting.
func (w *walletQuerier) GetWalletInfo(ctx context.Context) (*WalletInfo, error) {
	addrs := w.chain.Addresses()

	info := &WalletInfo{
		Address:         w.address.Hex(),
		Network:         w.chain.String(),
		ChainID:         w.chain.ChainID(),
		PaymentsAddress: addrs.Payments.Hex(),
		USDFCAddress:    addrs.USDFC.Hex(),
		USDFCDecimals:   18,
		Errors:          make(map[string]string),
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

	wg.Add(4)

	go func() {
		defer wg.Done()
		bal, err := w.payments.WalletBalance(ctx, payments.ZeroAddress, w.address)
		if err != nil {
			setErr("fil_balance", err)
			return
		}
		mu.Lock()
		info.FILBalance = bal
		mu.Unlock()
	}()

	go func() {
		defer wg.Done()
		bal, err := w.payments.WalletBalance(ctx, addrs.USDFC, w.address)
		if err != nil {
			setErr("usdfc_balance", err)
			return
		}
		mu.Lock()
		info.USDFCBalance = bal
		mu.Unlock()
	}()

	go func() {
		defer wg.Done()
		acct, err := w.payments.AccountInfo(ctx, payments.ZeroAddress, w.address)
		if err != nil {
			setErr("fil_account", err)
			return
		}
		mu.Lock()
		info.FILAccount = convertAccountState(acct)
		mu.Unlock()
	}()

	go func() {
		defer wg.Done()
		acct, err := w.payments.AccountInfo(ctx, addrs.USDFC, w.address)
		if err != nil {
			setErr("usdfc_account", err)
			return
		}
		mu.Lock()
		info.USDFCAccount = convertAccountState(acct)
		mu.Unlock()
	}()

	wg.Wait()
	return info, nil
}

func convertAccountState(acct *payments.AccountState) *TokenAccountInfo {
	if acct == nil {
		return nil
	}
	return &TokenAccountInfo{
		Funds:               acct.Funds,
		AvailableFunds:      acct.AvailableFunds(),
		LockupCurrent:       acct.LockupCurrent,
		LockupRate:          acct.LockupRate,
		LockupLastSettledAt: acct.LockupLastSettledAt,
		FundedUntilEpoch:    acct.FundedUntilEpoch,
	}
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
