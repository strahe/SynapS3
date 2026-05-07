package admin

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/synapse"
	"golang.org/x/sync/singleflight"
)

const (
	walletCacheTTL          = 10 * time.Second
	walletCacheFetchTimeout = 15 * time.Second
)

type cachedWalletQuerier struct {
	inner synapse.WalletQuerier
	ttl   time.Duration
	now   func() time.Time

	mu         sync.Mutex
	info       *synapse.WalletInfo
	expiresAt  time.Time
	generation uint64
	group      singleflight.Group
}

func newCachedWalletQuerier(inner synapse.WalletQuerier, ttl time.Duration, now func() time.Time) synapse.WalletQuerier {
	if inner == nil {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	return &cachedWalletQuerier{
		inner: inner,
		ttl:   ttl,
		now:   now,
	}
}

func (c *cachedWalletQuerier) Invalidate() {
	c.mu.Lock()
	c.info = nil
	c.expiresAt = time.Time{}
	c.generation++
	c.mu.Unlock()
	c.group.Forget("wallet")
}

func (c *cachedWalletQuerier) GetWalletInfo(ctx context.Context) (*synapse.WalletInfo, error) {
	if info := c.cached(); info != nil {
		return info, nil
	}

	ch := c.group.DoChan("wallet", func() (any, error) {
		if info := c.cached(); info != nil {
			return info, nil
		}
		generation := c.currentGeneration()

		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), walletCacheFetchTimeout)
		defer cancel()

		info, err := c.inner.GetWalletInfo(fetchCtx)
		if err != nil {
			return nil, err
		}

		cached := cloneWalletInfo(info)
		c.mu.Lock()
		if generation == c.generation {
			c.info = cached
			c.expiresAt = c.now().Add(c.ttl)
		}
		c.mu.Unlock()

		return cached, nil
	})

	select {
	case result := <-ch:
		if result.Err != nil {
			return nil, result.Err
		}
		info, ok := result.Val.(*synapse.WalletInfo)
		if !ok {
			return nil, errors.New("wallet cache returned unexpected value")
		}
		return cloneWalletInfo(info), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *cachedWalletQuerier) cached() *synapse.WalletInfo {
	c.mu.Lock()
	info := c.info
	valid := info != nil && c.now().Before(c.expiresAt)
	c.mu.Unlock()
	if !valid {
		return nil
	}
	return cloneWalletInfo(info)
}

func (c *cachedWalletQuerier) currentGeneration() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func cloneWalletInfo(info *synapse.WalletInfo) *synapse.WalletInfo {
	if info == nil {
		return nil
	}
	out := *info
	out.Nonce = cloneUint64(info.Nonce)
	out.CurrentEpoch = cloneBigInt(info.CurrentEpoch)
	out.FILGasBalance = cloneBigInt(info.FILGasBalance)
	out.USDFCWalletBalance = cloneBigInt(info.USDFCWalletBalance)
	out.PaymentAccount = clonePaymentAccountInfo(info.PaymentAccount)
	out.Errors = cloneStringMap(info.Errors)
	return &out
}

func clonePaymentAccountInfo(info *synapse.PaymentAccountInfo) *synapse.PaymentAccountInfo {
	if info == nil {
		return nil
	}
	out := &synapse.PaymentAccountInfo{
		Funds:               cloneBigInt(info.Funds),
		AvailableFunds:      cloneBigInt(info.AvailableFunds),
		LockupCurrent:       cloneBigInt(info.LockupCurrent),
		LockupRate:          cloneBigInt(info.LockupRate),
		LockupLastSettledAt: cloneBigInt(info.LockupLastSettledAt),
		FundedUntilEpoch:    cloneBigInt(info.FundedUntilEpoch),
		FundedUntilTime:     cloneTime(info.FundedUntilTime),
		RunwaySeconds:       cloneInt64(info.RunwaySeconds),
		LockupRatePerDay:    cloneBigInt(info.LockupRatePerDay),
		LockupRatePerMonth:  cloneBigInt(info.LockupRatePerMonth),
		NoActiveSpend:       info.NoActiveSpend,
	}
	return out
}

func cloneBigInt(v *big.Int) *big.Int {
	if v == nil {
		return nil
	}
	return new(big.Int).Set(v)
}

func cloneUint64(v *uint64) *uint64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func cloneInt64(v *int64) *int64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func cloneTime(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func cloneStringMap(v map[string]string) map[string]string {
	if v == nil {
		return nil
	}
	out := make(map[string]string, len(v))
	for key, value := range v {
		out[key] = value
	}
	return out
}
