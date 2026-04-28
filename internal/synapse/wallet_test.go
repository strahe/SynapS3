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

func TestWalletQuerier_ReusesFILAccountInfoForBalance(t *testing.T) {
	addrs := chain.Calibration.Addresses()
	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	pay := newMockWalletPayments()
	pay.accountInfoFunc = func(_ context.Context, token, _ common.Address) (*payments.AccountState, error) {
		switch token {
		case payments.ZeroAddress:
			return accountState(123), nil
		case addrs.USDFC:
			return accountState(456), nil
		default:
			t.Fatalf("unexpected AccountInfo token %s", token.Hex())
			return nil, nil
		}
	}
	pay.walletBalanceFunc = func(_ context.Context, token, _ common.Address) (*big.Int, error) {
		if token == payments.ZeroAddress {
			t.Fatal("FIL WalletBalance should not be called")
		}
		return big.NewInt(789), nil
	}

	info, err := (&walletQuerier{payments: pay, address: owner, chain: chain.Calibration}).GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("GetWalletInfo: %v", err)
	}
	if got := pay.walletBalanceCalls(payments.ZeroAddress); got != 0 {
		t.Fatalf("FIL WalletBalance calls = %d, want 0", got)
	}
	if got := pay.accountInfoCalls(payments.ZeroAddress); got != 1 {
		t.Fatalf("FIL AccountInfo calls = %d, want 1", got)
	}
	if got := info.FILBalance.String(); got != "123" {
		t.Fatalf("FILBalance = %s, want 123", got)
	}
	if got := info.FILAccount.Funds.String(); got != "123" {
		t.Fatalf("FILAccount funds = %s, want 123", got)
	}
	if got := info.USDFCBalance.String(); got != "789" {
		t.Fatalf("USDFCBalance = %s, want 789", got)
	}
}

func TestWalletQuerier_FILAccountErrorMarksBalanceUnavailable(t *testing.T) {
	addrs := chain.Calibration.Addresses()
	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	pay := newMockWalletPayments()
	pay.accountInfoFunc = func(_ context.Context, token, _ common.Address) (*payments.AccountState, error) {
		switch token {
		case payments.ZeroAddress:
			return nil, errors.New("fil rpc down")
		case addrs.USDFC:
			return accountState(456), nil
		default:
			t.Fatalf("unexpected AccountInfo token %s", token.Hex())
			return nil, nil
		}
	}
	pay.walletBalanceFunc = func(_ context.Context, token, _ common.Address) (*big.Int, error) {
		if token == payments.ZeroAddress {
			t.Fatal("FIL WalletBalance should not be called")
		}
		return big.NewInt(789), nil
	}

	info, err := (&walletQuerier{payments: pay, address: owner, chain: chain.Calibration}).GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("GetWalletInfo: %v", err)
	}
	if info.FILBalance != nil {
		t.Fatalf("FILBalance = %s, want nil", info.FILBalance)
	}
	if info.FILAccount != nil {
		t.Fatalf("FILAccount = %#v, want nil", info.FILAccount)
	}
	if got := info.Errors["fil_account"]; got != "RPC call failed" {
		t.Fatalf("fil_account error = %q, want RPC call failed", got)
	}
	if got := info.Errors["fil_balance"]; got != "RPC call failed" {
		t.Fatalf("fil_balance error = %q, want RPC call failed", got)
	}
}

func accountState(funds int64) *payments.AccountState {
	return &payments.AccountState{
		Funds:               big.NewInt(funds),
		LockupCurrent:       big.NewInt(0),
		LockupRate:          big.NewInt(0),
		LockupLastSettledAt: big.NewInt(0),
		FundedUntilEpoch:    big.NewInt(0),
	}
}
