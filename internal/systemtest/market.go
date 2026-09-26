//go:build systemtest

package systemtest

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/types"
	sdktypes "github.com/strahe/synapse-go/types"
	"github.com/strahe/synapse-go/warmstorage"
)

var memoryUSDFCAddress = common.HexToAddress("0x200")

type memoryWarmStorageMarket struct{ filecoin *MemoryFilecoin }

func (m memoryWarmStorageMarket) FWSSAddress() common.Address { return common.HexToAddress("0x100") }

func (m memoryWarmStorageMarket) GetPriceList(context.Context) (*warmstorage.PriceList, error) {
	one := func() *big.Int { return big.NewInt(1) }
	return &warmstorage.PriceList{
		Token: memoryUSDFCAddress,
		Rates: warmstorage.PriceListRates{
			StoragePerTiBPerMonth: one(), DatasetFeePerMonth: one(), CDNEgressPerTiB: one(), CacheMissEgressPerTiB: one(),
		},
		Fees: warmstorage.PriceListFees{
			CreateDataSetFee: one(), AddPiecesBaseFee: one(), AddPiecesPerPieceFee: one(), SchedulePieceRemovalsFee: one(), TerminateFee: one(),
		},
		Lockups: warmstorage.PriceListLockups{
			LifecycleReserveTarget: one(), ReplenishThreshold: one(), DefaultLockupPeriod: one(), CDNLockupAmount: one(), CacheMissLockupAmount: one(), CDNLockupPeriod: one(),
		},
	}, nil
}

func (m memoryWarmStorageMarket) ReadApprovedProviderIDs(context.Context) ([]types.OnChainID, error) {
	ids := make([]types.OnChainID, 0, len(m.filecoin.providers))
	for _, id := range m.filecoin.providers {
		ids = append(ids, types.OnChainIDFromSDK(id))
	}
	return ids, nil
}

func (m memoryWarmStorageMarket) GetEndorsedProviderIDs(context.Context) ([]sdktypes.BigInt, error) {
	return append([]sdktypes.BigInt(nil), m.filecoin.providers...), nil
}

func (m memoryWarmStorageMarket) IsProviderApproved(_ context.Context, id sdktypes.BigInt) (bool, error) {
	return m.filecoin.hasProvider(id), nil
}
