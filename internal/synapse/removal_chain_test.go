package synapse

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/payments"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

type removalStorageStub struct {
	info       *warmstorage.DataSetInfo
	prices     *warmstorage.PriceList
	config     *warmstorage.PDPConfig
	err        error
	priceCalls int
}

func (s *removalStorageStub) GetDataSet(context.Context, sdktypes.BigInt) (*warmstorage.DataSetInfo, error) {
	return s.info, s.err
}

func (s *removalStorageStub) GetPriceList(context.Context) (*warmstorage.PriceList, error) {
	s.priceCalls++
	return s.prices, s.err
}

func (s *removalStorageStub) GetPDPConfig(context.Context) (*warmstorage.PDPConfig, error) {
	return s.config, s.err
}

type removalPaymentsStub struct {
	rail     *payments.RailView
	account  *payments.AccountState
	approval *payments.OperatorApproval
	err      error
	calls    int
}

func (s *removalPaymentsStub) GetRail(context.Context, sdktypes.BigInt) (*payments.RailView, error) {
	s.calls++
	return s.rail, s.err
}

func (s *removalPaymentsStub) AccountInfo(context.Context, common.Address, common.Address) (*payments.AccountState, error) {
	s.calls++
	return s.account, s.err
}

func (s *removalPaymentsStub) ServiceApproval(context.Context, common.Address, common.Address, common.Address) (*payments.OperatorApproval, error) {
	s.calls++
	return s.approval, s.err
}

type removalEpochStub struct{}

func (removalEpochStub) CurrentEpoch(context.Context) (int64, error) { return 100, nil }

func TestRemovalPreflightReserveAndLockupBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name       string
		change     func(*removalStorageStub, *removalPaymentsStub)
		wantReason string
	}{
		{name: "stale settlement epoch with sufficient funds"},
		{name: "no operator approval flag required for an existing rail", change: func(_ *removalStorageStub, p *removalPaymentsStub) { p.approval.IsApproved = false }},
		{name: "unchanged period ignores lower maximum", change: func(_ *removalStorageStub, p *removalPaymentsStub) { p.approval.MaxLockupPeriod.SetInt64(0) }},
		{name: "zero account rate can settle", change: func(_ *removalStorageStub, p *removalPaymentsStub) { p.account.LockupRate.SetInt64(0) }},
		{name: "exact balance", change: func(_ *removalStorageStub, p *removalPaymentsStub) { p.account.Funds.SetInt64(622) }},
		{name: "balance shortfall", change: func(_ *removalStorageStub, p *removalPaymentsStub) { p.account.Funds.SetInt64(621) }, wantReason: "balance"},
		{name: "account cannot settle current epoch", change: func(_ *removalStorageStub, p *removalPaymentsStub) { p.account.Funds.SetInt64(110) }, wantReason: "settle"},
		{name: "exact allowance", change: func(_ *removalStorageStub, p *removalPaymentsStub) { p.approval.LockupAllowance.SetInt64(607) }},
		{name: "allowance shortfall", change: func(_ *removalStorageStub, p *removalPaymentsStub) { p.approval.LockupAllowance.SetInt64(606) }, wantReason: "amount"},
		{name: "period increase exceeds approval", change: func(_ *removalStorageStub, p *removalPaymentsStub) {
			p.rail.LockupPeriod.SetInt64(10)
			p.approval.MaxLockupPeriod.SetInt64(29)
		}, wantReason: "period"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dataSets, payer, identity := newRemovalPreflightStubs()
			if tt.change != nil {
				tt.change(dataSets, payer)
			}
			reader := NewRemovalChainReader(dataSets, payer, identity, removalEpochStub{})
			got, err := reader.Preflight(t.Context(), sdktypes.NewBigInt(11))
			if err != nil || got.PayerUnderfunded != (tt.wantReason != "") || (tt.wantReason != "" && !strings.Contains(got.Reason, tt.wantReason)) {
				t.Fatalf("preflight = %#v, err=%v", got, err)
			}
			if got.ReserveTopUp == nil || got.ReserveTopUp.Sign() <= 0 {
				t.Fatalf("missing reserve increase: %#v", got)
			}
		})
	}
}

func TestRemovalPreflightSkipsUnnecessaryPaymentReads(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*removalStorageStub)
	}{
		{name: "terminated", change: func(s *removalStorageStub) { s.info.PDPEndEpoch = 123 }},
		{name: "reserve at threshold", change: func(s *removalStorageStub) { s.info.LifecycleReserveBalance.SetInt64(32) }},
		{name: "record removed", change: func(s *removalStorageStub) { s.info = nil }},
		{name: "SDK not found", change: func(s *removalStorageStub) { s.err = fmt.Errorf("data set lookup: %w", warmstorage.ErrNotFound) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dataSets, payer, identity := newRemovalPreflightStubs()
			tt.change(dataSets)
			payer.err = errors.New("payment reader must not be called")
			got, err := NewRemovalChainReader(dataSets, payer, identity, removalEpochStub{}).Preflight(t.Context(), sdktypes.NewBigInt(11))
			if err != nil || payer.calls != 0 || got.PayerUnderfunded {
				t.Fatalf("preflight=%#v payment calls=%d, err=%v", got, payer.calls, err)
			}
			if tt.name == "terminated" && (got.EndEpoch != 123 || dataSets.priceCalls != 0) {
				t.Fatalf("termination evidence = %#v price calls=%d", got, dataSets.priceCalls)
			}
			if (tt.name == "record removed" || tt.name == "SDK not found") && !got.DataSetMissing {
				t.Fatal("missing service record was not reported")
			}
		})
	}
	dataSets, payer, identity := newRemovalPreflightStubs()
	dataSets.info.Payer = common.HexToAddress("0x99")
	_, err := NewRemovalChainReader(dataSets, payer, identity, removalEpochStub{}).Preflight(t.Context(), sdktypes.NewBigInt(11))
	if !errors.Is(err, ErrServicePaidByAnother) || payer.calls != 0 {
		t.Fatalf("foreign payer check: %v, payment calls=%d", err, payer.calls)
	}
	dataSets, payer, identity = newRemovalPreflightStubs()
	payer.err = errors.New("payment RPC unavailable")
	_, err = NewRemovalChainReader(dataSets, payer, identity, removalEpochStub{}).Preflight(t.Context(), sdktypes.NewBigInt(11))
	if !errors.Is(err, payer.err) {
		t.Fatalf("payment read failure was lost: %v", err)
	}
}

func TestRemovalProvingConfig(t *testing.T) {
	dataSets, payer, identity := newRemovalPreflightStubs()
	reader := NewRemovalChainReader(dataSets, payer, identity, removalEpochStub{})
	got, err := reader.ProvingConfig(t.Context())
	if err != nil || got.MaxProvingPeriod != 2880 || got.ChallengeWindow != 60 {
		t.Fatalf("proving configuration=%#v, err=%v", got, err)
	}
	dataSets.config.ChallengeWindowSize.SetUint64(2880)
	if _, err := reader.ProvingConfig(t.Context()); err == nil {
		t.Fatal("inconsistent proving window was accepted")
	}
}

func newRemovalPreflightStubs() (*removalStorageStub, *removalPaymentsStub, storage.ContextIdentity) {
	payer := common.HexToAddress("0x01")
	operator := common.HexToAddress("0x02")
	token := common.HexToAddress("0x03")
	identity := storage.ContextIdentity{Payer: payer, RecordKeeper: operator, ChainID: 314159}
	dataSets := &removalStorageStub{
		info: &warmstorage.DataSetInfo{
			DataSetID: sdktypes.NewBigInt(11), PDPRailID: sdktypes.NewBigInt(3), Payer: payer,
			PendingOneTimePayments: new(big.Int), LifecycleReserveBalance: new(big.Int),
		},
		prices: &warmstorage.PriceList{
			Token:   token,
			Fees:    warmstorage.PriceListFees{SchedulePieceRemovalsFee: big.NewInt(7)},
			Lockups: warmstorage.PriceListLockups{LifecycleReserveTarget: big.NewInt(500), ReplenishThreshold: big.NewInt(25), DefaultLockupPeriod: big.NewInt(30)},
		},
		config: &warmstorage.PDPConfig{MaxProvingPeriod: 2880, ChallengeWindowSize: big.NewInt(60)},
	}
	account := &removalPaymentsStub{
		rail: &payments.RailView{
			Token: token, From: payer, Operator: operator, PaymentRate: big.NewInt(2),
			LockupPeriod: big.NewInt(30), LockupFixed: new(big.Int), EndEpoch: new(big.Int),
		},
		account:  &payments.AccountState{Funds: big.NewInt(2000), LockupCurrent: big.NewInt(100), LockupRate: big.NewInt(3), LockupLastSettledAt: big.NewInt(95)},
		approval: &payments.OperatorApproval{IsApproved: true, LockupAllowance: big.NewInt(1000), LockupUsage: big.NewInt(100), MaxLockupPeriod: big.NewInt(1000)},
	}
	return dataSets, account, identity
}
