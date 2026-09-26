package observability

import (
	"context"
	"encoding/hex"
	"encoding/json"
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
	FindDataSets(context.Context, *storage.FindDataSetsOptions) ([]*storage.DataSetDetails, error)
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
		if dataSet == nil || dataSet.DataSetID.IsZero() {
			continue
		}
		hasActivePieces := dataSet.HasActivePieces
		var activePieceCount *int64
		if dataSet.IsLive && !hasActivePieces {
			zero := int64(0)
			activePieceCount = &zero
		}
		out = append(out, ChainDataSet{
			DataSetID:        idtypes.OnChainIDFromSDK(dataSet.DataSetID),
			ClientDataSetID:  onChainIDPtrFromSDK(dataSet.ClientDataSetID),
			ProviderID:       idtypes.OnChainIDFromSDK(dataSet.ProviderID),
			IsLive:           dataSet.IsLive,
			IsManaged:        dataSet.IsManaged,
			ActivePieceCount: activePieceCount,
			HasActivePieces:  &hasActivePieces,
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
	extra := make(map[string]string, len(detail.ExtraCapabilities))
	for key, value := range detail.ExtraCapabilities {
		extra[key] = "0x" + hex.EncodeToString(value)
	}
	var offering any
	if detail.HasPDP {
		offering = map[string]any{
			"service_url":                   detail.ServiceURL,
			"min_piece_size_bytes":          decimalBigInt(detail.MinPieceSize),
			"max_piece_size_bytes":          decimalBigInt(detail.MaxPieceSize),
			"storage_price_per_tib_per_day": decimalBigInt(detail.StoragePrice),
			"min_proving_period_epochs":     decimalBigInt(detail.MinProvingPeriod),
			"location":                      detail.Location,
			"payment_token_address":         detail.PaymentToken.Hex(),
			"ipni_piece":                    detail.IPNIPiece,
			"ipni_ipfs":                     detail.IPNIIPFS,
			"ipni_peer_id":                  detail.IPNIPeerID,
			"extra_capabilities_hex":        extra,
		}
	}
	snapshot, _ := json.Marshal(map[string]any{
		"version": 1,
		"provider": map[string]any{
			"id": detail.ID.String(), "name": detail.Name,
			"description":              detail.Description,
			"service_provider_address": detail.Address.Hex(),
			"payee_address":            detail.Payee.Hex(), "active": detail.Active,
		},
		"pdp_offering": offering,
	})
	return Provider{
		ID:           detail.ID,
		Active:       detail.Active,
		HasPDP:       detail.HasPDP,
		ServiceURL:   detail.ServiceURL,
		HealthStatus: detail.HealthStatus,
		Profile: &ProviderProfile{
			ProviderID: detail.ID, Name: detail.Name, Description: detail.Description,
			ServiceProviderAddress: detail.Address.Hex(), PayeeAddress: detail.Payee.Hex(),
			Active: detail.Active, ServiceURL: detail.ServiceURL, RegistrySnapshot: snapshot,
		},
	}
}

func decimalBigInt(value *big.Int) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func onChainIDPtrFromSDK(id sdktypes.BigInt) *idtypes.OnChainID {
	if id.IsZero() {
		return nil
	}
	out := idtypes.OnChainIDFromSDK(id)
	return &out
}
