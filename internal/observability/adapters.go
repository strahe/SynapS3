package observability

import (
	"context"
	"math/big"

	"github.com/strahe/synaps3/internal/provider"
	idtypes "github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/storage"
	sdktypes "github.com/strahe/synapse-go/types"
)

type RegistryProviderSource struct {
	registry *provider.RegistryService
}

func NewRegistryProviderSource(registry *provider.RegistryService) *RegistryProviderSource {
	return &RegistryProviderSource{registry: registry}
}

func (s *RegistryProviderSource) ListActiveProviders(ctx context.Context) ([]Provider, error) {
	details, err := provider.ListProviders(ctx, s.registry, provider.ListOptions{ActiveOnly: true})
	if err != nil {
		return nil, err
	}
	return providersFromDetails(details), nil
}

func (s *RegistryProviderSource) LookupProvider(ctx context.Context, id idtypes.OnChainID) (Provider, error) {
	detail, err := provider.LookupProvider(ctx, s.registry, id.SDK())
	if err != nil {
		return Provider{}, err
	}
	return providerFromDetail(detail), nil
}

type StorageDataSetScanner struct {
	storage dataSetFinder
}

type dataSetFinder interface {
	FindDataSets(context.Context, *storage.FindDataSetsOptions) ([]*storage.DataSetInfo, error)
}

func NewStorageDataSetScanner(storage dataSetFinder) *StorageDataSetScanner {
	return &StorageDataSetScanner{storage: storage}
}

func (s *StorageDataSetScanner) ScanWalletDataSets(ctx context.Context) ([]ChainDataSet, error) {
	dataSets, err := s.storage.FindDataSets(ctx, &storage.FindDataSetsOptions{OnlyManaged: false})
	if err != nil {
		return nil, err
	}
	out := make([]ChainDataSet, 0, len(dataSets))
	for _, dataSet := range dataSets {
		if dataSet == nil || dataSet.DataSetInfo == nil {
			continue
		}
		out = append(out, ChainDataSet{
			DataSetID:        idtypes.OnChainIDFromSDK(dataSet.DataSetID),
			ClientDataSetID:  onChainIDPtrFromSDK(dataSet.ClientDataSetID),
			ProviderID:       idtypes.OnChainIDFromSDK(dataSet.ProviderID),
			IsLive:           dataSet.IsLive,
			IsManaged:        dataSet.IsManaged,
			ActivePieceCount: activePieceCountInt64(dataSet.ActivePieceCount),
			Metadata:         dataSet.Metadata,
		})
	}
	return out, nil
}

func providersFromDetails(details []provider.ProviderDetail) []Provider {
	out := make([]Provider, 0, len(details))
	for _, detail := range details {
		out = append(out, providerFromDetail(detail))
	}
	return out
}

func providerFromDetail(detail provider.ProviderDetail) Provider {
	return Provider{
		ID:           detail.ID,
		Active:       detail.Active,
		HasPDP:       detail.HasPDP,
		ServiceURL:   detail.ServiceURL,
		HealthStatus: detail.HealthStatus,
	}
}

func onChainIDPtrFromSDK(id sdktypes.BigInt) *idtypes.OnChainID {
	if id.IsZero() {
		return nil
	}
	out := idtypes.OnChainIDFromSDK(id)
	return &out
}

func activePieceCountInt64(value *big.Int) *int64 {
	if value == nil || !value.IsInt64() {
		return nil
	}
	out := value.Int64()
	return &out
}
