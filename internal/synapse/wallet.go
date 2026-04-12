package synapse

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/data-preservation-programs/go-synapse/payments"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

type walletQuerier struct {
	ethClient       *ethclient.Client
	payments        *payments.Service
	address         common.Address
	network         string
	chainID         int64
	paymentsAddress common.Address
	usdfcAddress    common.Address
}

// NewWalletQuerier creates a WalletQuerier backed by go-synapse payments.Service.
func NewWalletQuerier(
	ethClient *ethclient.Client,
	privateKey *ecdsa.PrivateKey,
	network string,
	chainID int64,
	paymentsAddress common.Address,
) (WalletQuerier, error) {
	paySvc, err := payments.NewService(ethClient, privateKey, big.NewInt(chainID), paymentsAddress)
	if err != nil {
		return nil, err
	}

	return &walletQuerier{
		ethClient:       ethClient,
		payments:        paySvc,
		address:         crypto.PubkeyToAddress(privateKey.PublicKey),
		network:         network,
		chainID:         chainID,
		paymentsAddress: paySvc.PaymentsAddress(),
		usdfcAddress:    paySvc.USDFCAddress(),
	}, nil
}

// GetWalletInfo queries on-chain state in parallel. Individual RPC failures
// produce nil fields and entries in the Errors map instead of aborting.
func (w *walletQuerier) GetWalletInfo(ctx context.Context) (*WalletInfo, error) {
	info := &WalletInfo{
		Address:         w.address.Hex(),
		Network:         w.network,
		ChainID:         w.chainID,
		PaymentsAddress: w.paymentsAddress.Hex(),
		USDFCAddress:    w.usdfcAddress.Hex(),
		USDFCDecimals:   payments.TokenDecimals,
		Errors:          make(map[string]string),
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	// Apply a 15-second timeout to prevent hanging on slow RPC nodes.
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	setErr := func(field string, err error) {
		mu.Lock()
		info.Errors[field] = sanitizeRPCError(err)
		mu.Unlock()
	}

	wg.Add(5)

	go func() {
		defer wg.Done()
		nonce, err := w.ethClient.NonceAt(ctx, w.address, nil)
		if err != nil {
			setErr("nonce", err)
			return
		}
		mu.Lock()
		info.Nonce = &nonce
		mu.Unlock()
	}()

	go func() {
		defer wg.Done()
		bal, err := w.payments.WalletBalance(ctx, payments.TokenFIL)
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
		bal, err := w.payments.WalletBalance(ctx, payments.TokenUSDFC)
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
		acct, err := w.payments.AccountInfo(ctx, payments.TokenFIL)
		if err != nil {
			setErr("fil_account", err)
			return
		}
		mu.Lock()
		info.FILAccount = convertAccountInfo(acct)
		mu.Unlock()
	}()

	go func() {
		defer wg.Done()
		acct, err := w.payments.AccountInfo(ctx, payments.TokenUSDFC)
		if err != nil {
			setErr("usdfc_account", err)
			return
		}
		mu.Lock()
		info.USDFCAccount = convertAccountInfo(acct)
		mu.Unlock()
	}()

	wg.Wait()
	return info, nil
}

func convertAccountInfo(acct *payments.AccountInfo) *TokenAccountInfo {
	if acct == nil {
		return nil
	}
	return &TokenAccountInfo{
		Funds:             acct.Funds,
		AvailableFunds:    acct.AvailableFunds,
		LockupCurrent:     acct.LockupCurrent,
		LockupRate:        acct.LockupRate,
		LockupLastSettled: acct.LockupLastSettled,
		FundedUntilEpoch:  acct.FundedUntilEpoch,
		CurrentLockupRate: acct.CurrentLockupRate,
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
