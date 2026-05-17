package provider

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/types"
	"github.com/strahe/synapse-go/spregistry"
	sdktypes "github.com/strahe/synapse-go/types"
)

// ListOptions controls provider listing behavior.
type ListOptions struct {
	ActiveOnly bool
}

// ProviderDetail is the enriched output combining provider info, PDP offering, and health status.
type ProviderDetail struct {
	ID           types.OnChainID `json:"id"`
	Name         string          `json:"name"`
	Description  string          `json:"description,omitempty"`
	Active       bool            `json:"active"`
	Address      common.Address  `json:"address"`
	HasPDP       bool            `json:"has_pdp"`
	ServiceURL   string          `json:"service_url,omitempty"`
	MinPieceSize *big.Int        `json:"min_piece_size,omitempty"`
	MaxPieceSize *big.Int        `json:"max_piece_size,omitempty"`
	StoragePrice *big.Int        `json:"storage_price,omitempty"`
	Location     string          `json:"location,omitempty"`
	HealthStatus string          `json:"health_status"`
}

// RegistryService wraps the synapse-go spregistry.Service for provider listing.
type RegistryService struct {
	svc registryProviderSource
}

type registryProviderSource interface {
	GetPDPProviders(ctx context.Context, onlyActive bool, opts sdktypes.ListOptions) (*spregistry.PaginatedPDPProviders, error)
	GetProviderCount(ctx context.Context) (*big.Int, error)
	GetPDPProvider(ctx context.Context, providerID sdktypes.BigInt) (*spregistry.PDPProvider, error)
	GetProvider(ctx context.Context, providerID sdktypes.BigInt) (*spregistry.ProviderInfo, error)
}

// NewRegistryService creates a RegistryService from a synapse-go SPRegistry service.
func NewRegistryService(svc *spregistry.Service) *RegistryService {
	return &RegistryService{svc: svc}
}

// ListProviders returns PDP providers, optionally filtered to active-only.
func ListProviders(ctx context.Context, reg *RegistryService, opts ListOptions) ([]ProviderDetail, error) {
	if opts.ActiveOnly {
		return listActiveProviders(ctx, reg)
	}
	return listAllProviders(ctx, reg)
}

func LookupProvider(ctx context.Context, reg *RegistryService, providerID sdktypes.BigInt) (ProviderDetail, error) {
	p, err := reg.svc.GetPDPProvider(ctx, providerID)
	if err != nil {
		if errors.Is(err, spregistry.ErrNotFound) {
			return providerInfoDetail(ctx, reg, providerID, providerID)
		}
		return ProviderDetail{}, err
	}
	if p == nil {
		return providerInfoDetail(ctx, reg, providerID, providerID)
	}
	return pdpProviderToDetail(*p)
}

func listActiveProviders(ctx context.Context, reg *RegistryService) ([]ProviderDetail, error) {
	var all []ProviderDetail
	var offset uint64
	const limit uint64 = 50
	var skipped int

	for {
		page, err := reg.svc.GetPDPProviders(ctx, true, sdktypes.ListOptions{Offset: offset, Limit: limit})
		if err != nil {
			return nil, fmt.Errorf("fetching active PDP providers: %w", err)
		}
		for _, p := range page.Providers {
			detail, err := pdpProviderToDetail(p)
			if err != nil {
				skipped++
				continue
			}
			all = append(all, detail)
		}
		if !page.HasMore {
			break
		}
		offset += limit
	}
	if skipped > 0 {
		fmt.Fprintf(os.Stderr, "Warning: %d provider(s) skipped due to invalid provider IDs\n", skipped)
	}
	return all, nil
}

func listAllProviders(ctx context.Context, reg *RegistryService) ([]ProviderDetail, error) {
	count, err := reg.svc.GetProviderCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching provider count: %w", err)
	}

	var skipped int
	if !count.IsUint64() || count.Sign() <= 0 {
		return nil, fmt.Errorf("provider count invalid: %s", count.String())
	}
	n := count.Uint64()
	if n == ^uint64(0) {
		return nil, fmt.Errorf("provider count too large: %s", count.String())
	}
	details := make([]ProviderDetail, 0)

	for id := uint64(1); id <= n; id++ {
		providerID := sdktypes.NewBigInt(id)
		p, err := reg.svc.GetPDPProvider(ctx, providerID)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(err, spregistry.ErrNotFound) {
				detail, detailErr := providerInfoDetailWithFallback(ctx, reg, providerID, id)
				if detailErr != nil {
					skipped++
					continue
				}
				details = append(details, detail)
				continue
			}
			skipped++
			continue
		}
		if p == nil {
			detail, detailErr := providerInfoDetailWithFallback(ctx, reg, providerID, id)
			if detailErr != nil {
				skipped++
				continue
			}
			details = append(details, detail)
			continue
		}
		detail, err := pdpProviderToDetailWithFallback(*p, id)
		if err != nil {
			skipped++
			continue
		}
		details = append(details, detail)
	}

	if skipped > 0 {
		fmt.Fprintf(os.Stderr, "Warning: %d provider(s) skipped due to RPC errors\n", skipped)
	}
	return details, nil
}

func pdpProviderToDetail(p spregistry.PDPProvider) (ProviderDetail, error) {
	return pdpProviderToDetailWithFallback(p, 0)
}

func pdpProviderToDetailWithFallback(p spregistry.PDPProvider, fallbackID uint64) (ProviderDetail, error) {
	id, err := providerID(p.Info.ID, sdktypes.NewBigInt(fallbackID))
	if err != nil {
		return ProviderDetail{}, err
	}
	d := ProviderDetail{
		ID:           id,
		Name:         p.Info.Name,
		Description:  p.Info.Description,
		Active:       p.Info.IsActive,
		Address:      p.Info.ServiceProvider,
		HasPDP:       true,
		ServiceURL:   p.Offering.ServiceURL,
		MinPieceSize: p.Offering.MinPieceSizeInBytes,
		MaxPieceSize: p.Offering.MaxPieceSizeInBytes,
		StoragePrice: p.Offering.StoragePricePerTiBPerDay,
		Location:     p.Offering.Location,
		HealthStatus: "skipped",
	}
	return d, nil
}

func providerInfoToDetail(info *spregistry.ProviderInfo) (ProviderDetail, error) {
	return providerInfoToDetailWithFallback(info, 0)
}

func providerInfoDetail(ctx context.Context, reg *RegistryService, providerID sdktypes.BigInt, fallbackID sdktypes.BigInt) (ProviderDetail, error) {
	info, err := reg.svc.GetProvider(ctx, providerID)
	if err != nil {
		return ProviderDetail{}, err
	}
	if info == nil {
		return ProviderDetail{}, fmt.Errorf("provider info missing")
	}
	return providerInfoToDetailWithFallbackID(info, fallbackID)
}

func providerInfoDetailWithFallback(ctx context.Context, reg *RegistryService, providerID sdktypes.BigInt, fallbackID uint64) (ProviderDetail, error) {
	return providerInfoDetail(ctx, reg, providerID, sdktypes.NewBigInt(fallbackID))
}

func providerInfoToDetailWithFallback(info *spregistry.ProviderInfo, fallbackID uint64) (ProviderDetail, error) {
	return providerInfoToDetailWithFallbackID(info, sdktypes.NewBigInt(fallbackID))
}

func providerInfoToDetailWithFallbackID(info *spregistry.ProviderInfo, fallbackID sdktypes.BigInt) (ProviderDetail, error) {
	id, err := providerID(info.ID, fallbackID)
	if err != nil {
		return ProviderDetail{}, err
	}
	return ProviderDetail{
		ID:           id,
		Name:         info.Name,
		Description:  info.Description,
		Active:       info.IsActive,
		Address:      info.ServiceProvider,
		HealthStatus: "skipped",
	}, nil
}

func providerID(v sdktypes.BigInt, fallback sdktypes.BigInt) (types.OnChainID, error) {
	if v.IsZero() {
		if !fallback.IsZero() {
			return types.OnChainIDFromSDK(fallback), nil
		}
		return types.OnChainID{}, fmt.Errorf("invalid provider ID")
	}
	return types.OnChainIDFromSDK(v), nil
}

// FormatSize converts a byte count to a human-readable size string (e.g., "1 MiB", "32 GiB").
func FormatSize(b *big.Int) string {
	if b == nil || b.Sign() <= 0 {
		return "—"
	}

	if !b.IsInt64() {
		return b.String() + " B"
	}

	const (
		kib = 1024
		mib = kib * 1024
		gib = mib * 1024
		tib = gib * 1024
	)

	v := b.Int64()
	switch {
	case v >= tib && v%tib == 0:
		return fmt.Sprintf("%d TiB", v/tib)
	case v >= gib && v%gib == 0:
		return fmt.Sprintf("%d GiB", v/gib)
	case v >= mib && v%mib == 0:
		return fmt.Sprintf("%d MiB", v/mib)
	case v >= kib && v%kib == 0:
		return fmt.Sprintf("%d KiB", v/kib)
	default:
		return fmt.Sprintf("%d B", v)
	}
}
