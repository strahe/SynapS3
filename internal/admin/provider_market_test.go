package admin

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

type testWarmStorageMarket struct {
	price       *warmstorage.PriceList
	approved    map[string]bool
	approvalErr error
}

func (m *testWarmStorageMarket) FWSSAddress() common.Address { return common.HexToAddress("0x100") }
func (m *testWarmStorageMarket) GetPriceList(context.Context) (*warmstorage.PriceList, error) {
	return m.price, nil
}

func (m *testWarmStorageMarket) IsProviderApproved(_ context.Context, providerID sdktypes.BigInt) (bool, error) {
	if m.approvalErr != nil {
		return false, m.approvalErr
	}
	if m.approved == nil {
		return true, nil
	}
	return m.approved[providerID.String()], nil
}

func testPriceList() *warmstorage.PriceList {
	one := func() *big.Int { return big.NewInt(1) }
	return &warmstorage.PriceList{
		Token:   common.HexToAddress("0x200"),
		Rates:   warmstorage.PriceListRates{StoragePerTiBPerMonth: one(), DatasetFeePerMonth: one(), CDNEgressPerTiB: one(), CacheMissEgressPerTiB: one()},
		Fees:    warmstorage.PriceListFees{CreateDataSetFee: one(), AddPiecesBaseFee: one(), AddPiecesPerPieceFee: one(), SchedulePieceRemovalsFee: one(), TerminateFee: one()},
		Lockups: warmstorage.PriceListLockups{LifecycleReserveTarget: one(), ReplenishThreshold: one(), DefaultLockupPeriod: one(), CDNLockupAmount: one(), CacheMissLockupAmount: one(), CDNLockupPeriod: one()},
	}
}

func TestWarmStoragePriceFingerprintIgnoresReadTimeAndTracksEveryFee(t *testing.T) {
	market := &testWarmStorageMarket{price: testPriceList()}
	srv := (&Server{}).WithWarmStorageMarket(market, 314, market.price.Token.Hex())
	first, err := srv.currentWarmStoragePrice(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := srv.currentWarmStoragePrice(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint != second.Fingerprint {
		t.Fatal("same price list changed fingerprint")
	}
	market.price.Lockups.CDNLockupPeriod = big.NewInt(2)
	third, err := srv.currentWarmStoragePrice(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if third.Fingerprint == first.Fingerprint {
		t.Fatal("changed lockup period kept fingerprint")
	}
}
