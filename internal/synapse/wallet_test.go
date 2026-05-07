package synapse

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/chain"
	"github.com/strahe/synapse-go/payments"
)

type mockWalletPayments struct {
	walletBalanceFunc func(ctx context.Context, token, account common.Address) (*big.Int, error)
	accountInfoFunc   func(ctx context.Context, token, owner common.Address) (*payments.AccountState, error)
	mu                sync.Mutex
	walletBalanceCall map[common.Address]int
	accountInfoCall   map[common.Address]int
}

func newMockWalletPayments() *mockWalletPayments {
	return &mockWalletPayments{
		walletBalanceCall: make(map[common.Address]int),
		accountInfoCall:   make(map[common.Address]int),
	}
}

func (m *mockWalletPayments) WalletBalance(ctx context.Context, token, account common.Address) (*big.Int, error) {
	m.mu.Lock()
	m.walletBalanceCall[token]++
	m.mu.Unlock()
	if m.walletBalanceFunc != nil {
		return m.walletBalanceFunc(ctx, token, account)
	}
	return big.NewInt(0), nil
}

func (m *mockWalletPayments) AccountInfo(ctx context.Context, token, owner common.Address) (*payments.AccountState, error) {
	m.mu.Lock()
	m.accountInfoCall[token]++
	m.mu.Unlock()
	if m.accountInfoFunc != nil {
		return m.accountInfoFunc(ctx, token, owner)
	}
	return accountState(0), nil
}

func (m *mockWalletPayments) walletBalanceCalls(token common.Address) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.walletBalanceCall[token]
}

func (m *mockWalletPayments) accountInfoCalls(token common.Address) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.accountInfoCall[token]
}

func TestWalletQuerier_ReturnsGasBalanceAndUSDFCPaymentAccount(t *testing.T) {
	addrs := chain.Calibration.Addresses()
	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	pay := newMockWalletPayments()
	pay.accountInfoFunc = func(_ context.Context, token, _ common.Address) (*payments.AccountState, error) {
		switch token {
		case addrs.USDFC:
			return accountStateWithRate(456, 100, 2), nil
		default:
			t.Fatalf("unexpected AccountInfo token %s", token.Hex())
			return nil, nil
		}
	}
	pay.walletBalanceFunc = func(_ context.Context, token, _ common.Address) (*big.Int, error) {
		if token == payments.ZeroAddress {
			return big.NewInt(123), nil
		}
		return big.NewInt(789), nil
	}

	info, err := (&walletQuerier{payments: pay, address: owner, chain: chain.Calibration}).GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("GetWalletInfo: %v", err)
	}
	if got := pay.walletBalanceCalls(payments.ZeroAddress); got != 1 {
		t.Fatalf("FIL WalletBalance calls = %d, want 1", got)
	}
	if got := pay.accountInfoCalls(payments.ZeroAddress); got != 0 {
		t.Fatalf("FIL AccountInfo calls = %d, want 0", got)
	}
	if got := info.FILGasBalance.String(); got != "123" {
		t.Fatalf("FILGasBalance = %s, want 123", got)
	}
	if info.PaymentAccount == nil {
		t.Fatal("PaymentAccount = nil, want USDFC account")
	}
	if got := info.USDFCWalletBalance.String(); got != "789" {
		t.Fatalf("USDFCWalletBalance = %s, want 789", got)
	}
	if got := info.PaymentAccount.LockupRatePerDay.String(); got != "5760" {
		t.Fatalf("LockupRatePerDay = %s, want 5760", got)
	}
	if got := info.PaymentAccount.LockupRatePerMonth.String(); got != "172800" {
		t.Fatalf("LockupRatePerMonth = %s, want 172800", got)
	}
}

func TestWalletQuerier_FILGasBalanceErrorDoesNotHidePaymentAccount(t *testing.T) {
	addrs := chain.Calibration.Addresses()
	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	pay := newMockWalletPayments()
	pay.accountInfoFunc = func(_ context.Context, token, _ common.Address) (*payments.AccountState, error) {
		switch token {
		case addrs.USDFC:
			return accountState(456), nil
		default:
			t.Fatalf("unexpected AccountInfo token %s", token.Hex())
			return nil, nil
		}
	}
	pay.walletBalanceFunc = func(_ context.Context, token, _ common.Address) (*big.Int, error) {
		if token == payments.ZeroAddress {
			return nil, errors.New("fil rpc down")
		}
		return big.NewInt(789), nil
	}

	info, err := (&walletQuerier{payments: pay, address: owner, chain: chain.Calibration}).GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("GetWalletInfo: %v", err)
	}
	if info.FILGasBalance != nil {
		t.Fatalf("FILGasBalance = %s, want nil", info.FILGasBalance)
	}
	if info.PaymentAccount == nil {
		t.Fatal("PaymentAccount = nil, want USDFC account")
	}
	if got := info.Errors["fil_gas_balance"]; got != "RPC call failed" {
		t.Fatalf("fil_balance error = %q, want RPC call failed", got)
	}
}

func TestWalletQuerier_ZeroLockupRateHasNoActiveSpend(t *testing.T) {
	addrs := chain.Calibration.Addresses()
	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	pay := newMockWalletPayments()
	pay.accountInfoFunc = func(_ context.Context, token, _ common.Address) (*payments.AccountState, error) {
		if token != addrs.USDFC {
			t.Fatalf("unexpected AccountInfo token %s", token.Hex())
		}
		return accountStateWithRate(456, 0, 0), nil
	}

	info, err := (&walletQuerier{payments: pay, address: owner, chain: chain.Calibration}).GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("GetWalletInfo: %v", err)
	}
	if info.PaymentAccount == nil {
		t.Fatal("PaymentAccount = nil")
	}
	if !info.PaymentAccount.NoActiveSpend {
		t.Fatal("NoActiveSpend = false, want true")
	}
	if info.PaymentAccount.RunwaySeconds != nil {
		t.Fatalf("RunwaySeconds = %v, want nil", info.PaymentAccount.RunwaySeconds)
	}
}

func TestWalletQuerier_Uint256MaxFundedUntilHasNoActiveSpend(t *testing.T) {
	addrs := chain.Calibration.Addresses()
	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	pay := newMockWalletPayments()
	pay.accountInfoFunc = func(_ context.Context, token, _ common.Address) (*payments.AccountState, error) {
		if token != addrs.USDFC {
			t.Fatalf("unexpected AccountInfo token %s", token.Hex())
		}
		acct := accountStateWithRate(456, 0, 1)
		acct.FundedUntilEpoch = uint256Max()
		return acct, nil
	}

	info, err := (&walletQuerier{payments: pay, address: owner, chain: chain.Calibration}).GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("GetWalletInfo: %v", err)
	}
	if info.PaymentAccount == nil {
		t.Fatal("PaymentAccount = nil")
	}
	if !info.PaymentAccount.NoActiveSpend {
		t.Fatal("NoActiveSpend = false, want true")
	}
	if info.PaymentAccount.RunwaySeconds != nil {
		t.Fatalf("RunwaySeconds = %v, want nil", info.PaymentAccount.RunwaySeconds)
	}
}

func accountState(funds int64) *payments.AccountState {
	return accountStateWithRate(funds, 0, 0)
}

func accountStateWithRate(funds, lockupCurrent, lockupRate int64) *payments.AccountState {
	return &payments.AccountState{
		Funds:               big.NewInt(funds),
		LockupCurrent:       big.NewInt(lockupCurrent),
		LockupRate:          big.NewInt(lockupRate),
		LockupLastSettledAt: big.NewInt(0),
		FundedUntilEpoch:    big.NewInt(0),
	}
}
