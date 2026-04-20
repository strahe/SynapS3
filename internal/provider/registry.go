package provider

import (
	"context"
	"fmt"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/spregistry"
)

// ListOptions controls provider listing behavior.
type ListOptions struct {
	ActiveOnly bool
}

// ProviderDetail is the enriched output combining provider info, PDP offering, and health status.
type ProviderDetail struct {
	ID           int            `json:"id"`
	Name         string         `json:"name"`
	Description  string         `json:"description,omitempty"`
	Active       bool           `json:"active"`
	Address      common.Address `json:"address"`
	HasPDP       bool           `json:"has_pdp"`
	ServiceURL   string         `json:"service_url,omitempty"`
	MinPieceSize *big.Int       `json:"min_piece_size,omitempty"`
	MaxPieceSize *big.Int       `json:"max_piece_size,omitempty"`
	StoragePrice *big.Int       `json:"storage_price,omitempty"`
	Location     string         `json:"location,omitempty"`
	HealthStatus string         `json:"health_status"`
}

// RegistryService wraps the synapse-go spregistry.Service for provider listing.
type RegistryService struct {
	svc *spregistry.Service
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

func listActiveProviders(ctx context.Context, reg *RegistryService) ([]ProviderDetail, error) {
	var all []ProviderDetail
	offset := big.NewInt(0)
	limit := big.NewInt(50)
	var skipped int

	for {
		page, err := reg.svc.GetPDPProviders(ctx, true, offset, limit)
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
		offset = new(big.Int).Add(offset, limit)
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
	if !count.IsInt64() || count.Sign() <= 0 {
		return nil, fmt.Errorf("provider count invalid: %s", count.String())
	}
	n := int(count.Int64())
	details := make([]ProviderDetail, 0, n)

	for id := 1; id <= n; id++ {
		p, err := reg.svc.GetPDPProvider(ctx, big.NewInt(int64(id)))
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			skipped++
			continue
		}
		if p == nil {
			// Provider has no PDP product — add basic info.
			info, err := reg.svc.GetProvider(ctx, big.NewInt(int64(id)))
			if err != nil || info == nil {
				skipped++
				continue
			}
			detail, err := providerInfoToDetailWithFallback(info, id)
			if err != nil {
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

func pdpProviderToDetailWithFallback(p spregistry.PDPProvider, fallbackID int) (ProviderDetail, error) {
	id, err := providerID(p.Info.ID, fallbackID)
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

func providerInfoToDetailWithFallback(info *spregistry.ProviderInfo, fallbackID int) (ProviderDetail, error) {
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

func providerID(v *big.Int, fallback int) (int, error) {
	if v == nil || !v.IsInt64() || v.Sign() <= 0 {
		if fallback > 0 {
			return fallback, nil
		}
		return 0, fmt.Errorf("invalid provider ID")
	}
	return int(v.Int64()), nil
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
