package admin

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/synapse"
)

func TestCachedWalletQuerier_UsesCachedValueUntilTTLExpires(t *testing.T) {
	now := time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC)
	var calls atomic.Int32
	q := newCachedWalletQuerier(&testWalletQuerier{
		fn: func(context.Context) (*synapse.WalletInfo, error) {
			call := calls.Add(1)
			return &synapse.WalletInfo{Address: "wallet-call-" + string(rune('0'+call))}, nil
		},
	}, 10*time.Second, func() time.Time { return now })

	first, err := q.GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("first GetWalletInfo: %v", err)
	}
	second, err := q.GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("second GetWalletInfo: %v", err)
	}
	if first.Address != "wallet-call-1" || second.Address != "wallet-call-1" {
		t.Fatalf("cached addresses = %q/%q, want wallet-call-1", first.Address, second.Address)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls before TTL expiry = %d, want 1", got)
	}

	now = now.Add(11 * time.Second)
	third, err := q.GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("third GetWalletInfo: %v", err)
	}
	if third.Address != "wallet-call-2" {
		t.Fatalf("address after TTL expiry = %q, want wallet-call-2", third.Address)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls after TTL expiry = %d, want 2", got)
	}
}

func TestCachedWalletQuerier_CoalescesConcurrentMisses(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	q := newCachedWalletQuerier(&testWalletQuerier{
		fn: func(context.Context) (*synapse.WalletInfo, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return &synapse.WalletInfo{Address: "coalesced"}, nil
		},
	}, 10*time.Second, time.Now)

	const callers = 8
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := q.GetWalletInfo(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if info.Address != "coalesced" {
				errs <- errors.New("unexpected address")
			}
		}()
	}

	<-started
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestCachedWalletQuerier_WaiterContextCanCancel(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	q := newCachedWalletQuerier(&testWalletQuerier{
		fn: func(context.Context) (*synapse.WalletInfo, error) {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			return &synapse.WalletInfo{Address: "late"}, nil
		},
	}, 10*time.Second, time.Now)

	done := make(chan error, 1)
	go func() {
		_, err := q.GetWalletInfo(context.Background())
		done <- err
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.GetWalletInfo(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("background waiter: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestCachedWalletQuerier_ReturnsClonedWalletInfo(t *testing.T) {
	now := time.Date(2026, 4, 28, 0, 0, 0, 0, time.UTC)
	nonce := uint64(7)
	q := newCachedWalletQuerier(&testWalletQuerier{
		fn: func(context.Context) (*synapse.WalletInfo, error) {
			return &synapse.WalletInfo{
				Nonce:      &nonce,
				FILBalance: big.NewInt(123),
				FILAccount: &synapse.TokenAccountInfo{
					Funds: big.NewInt(456),
				},
				Errors: map[string]string{"chain": "RPC call failed"},
			}, nil
		},
	}, 10*time.Second, func() time.Time { return now })

	first, err := q.GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("first GetWalletInfo: %v", err)
	}
	first.Errors["mutated"] = "yes"
	first.FILBalance.SetInt64(999)
	first.FILAccount.Funds.SetInt64(888)
	*first.Nonce = 99

	second, err := q.GetWalletInfo(context.Background())
	if err != nil {
		t.Fatalf("second GetWalletInfo: %v", err)
	}
	if _, ok := second.Errors["mutated"]; ok {
		t.Fatal("cached Errors map was shared with caller")
	}
	if got := second.FILBalance.String(); got != "123" {
		t.Fatalf("FILBalance = %s, want 123", got)
	}
	if got := second.FILAccount.Funds.String(); got != "456" {
		t.Fatalf("FILAccount funds = %s, want 456", got)
	}
	if second.Nonce == nil || *second.Nonce != 7 {
		t.Fatalf("Nonce = %v, want 7", second.Nonce)
	}
}

type testWalletQuerier struct {
	fn func(context.Context) (*synapse.WalletInfo, error)
}

func (t *testWalletQuerier) GetWalletInfo(ctx context.Context) (*synapse.WalletInfo, error) {
	return t.fn(ctx)
}
