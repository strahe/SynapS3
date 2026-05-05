package provider

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/spregistry"
	sdktypes "github.com/strahe/synapse-go/types"
)

type fakeRegistryProviderSource struct {
	count     *big.Int
	pdp       map[string]spregistry.PDPProvider
	providers map[string]spregistry.ProviderInfo
}

func (f *fakeRegistryProviderSource) GetPDPProviders(context.Context, bool, sdktypes.ListOptions) (*spregistry.PaginatedPDPProviders, error) {
	return nil, fmt.Errorf("GetPDPProviders not implemented")
}

func (f *fakeRegistryProviderSource) GetProviderCount(context.Context) (*big.Int, error) {
	return new(big.Int).Set(f.count), nil
}

func (f *fakeRegistryProviderSource) GetPDPProvider(_ context.Context, providerID sdktypes.BigInt) (*spregistry.PDPProvider, error) {
	p, ok := f.pdp[providerID.String()]
	if !ok {
		return nil, fmt.Errorf("missing PDP product: %w", spregistry.ErrNotFound)
	}
	return &p, nil
}

func (f *fakeRegistryProviderSource) GetProvider(_ context.Context, providerID sdktypes.BigInt) (*spregistry.ProviderInfo, error) {
	p, ok := f.providers[providerID.String()]
	if !ok {
		return nil, fmt.Errorf("missing provider: %w", spregistry.ErrNotFound)
	}
	return &p, nil
}

func newTestPDPProvider(id int, name string, active bool, pdpURL string) spregistry.PDPProvider {
	p := spregistry.PDPProvider{
		Info: spregistry.ProviderInfo{
			ID:              sdktypes.NewBigInt(uint64(id)),
			Name:            name,
			IsActive:        active,
			ServiceProvider: common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
		},
		Product: spregistry.ServiceProduct{
			ProductType: spregistry.ProductTypePDP,
			IsActive:    true,
		},
	}
	if pdpURL != "" {
		p.Offering = spregistry.PDPOffering{
			ServiceURL:          pdpURL,
			MinPieceSizeInBytes: big.NewInt(1024 * 1024),             // 1 MiB
			MaxPieceSizeInBytes: big.NewInt(32 * 1024 * 1024 * 1024), // 32 GiB
			Location:            "US-East",
		}
	}
	return p
}

func TestPDPProviderToDetail_WithPDP(t *testing.T) {
	p := newTestPDPProvider(1, "Alpha", true, "https://alpha.example.com")
	d, err := pdpProviderToDetail(p)
	if err != nil {
		t.Fatalf("pdpProviderToDetail: %v", err)
	}

	if d.ID.String() != "1" {
		t.Errorf("expected ID 1, got %s", d.ID.String())
	}
	if d.Name != "Alpha" {
		t.Errorf("expected name Alpha, got %s", d.Name)
	}
	if !d.HasPDP {
		t.Error("expected HasPDP to be true")
	}
	if d.ServiceURL != "https://alpha.example.com" {
		t.Errorf("expected service URL, got %q", d.ServiceURL)
	}
	if d.Location != "US-East" {
		t.Errorf("expected location US-East, got %q", d.Location)
	}
	if !d.Active {
		t.Error("expected Active to be true")
	}
}

func TestPDPProviderToDetail_NoPDP(t *testing.T) {
	p := newTestPDPProvider(2, "NoPDP", true, "")
	d, err := pdpProviderToDetail(p)
	if err != nil {
		t.Fatalf("pdpProviderToDetail: %v", err)
	}

	// HasPDP is always true for pdpProviderToDetail since it's only called for PDP providers.
	// Verify offering-specific fields are empty when no offering data is set.
	if d.ServiceURL != "" {
		t.Errorf("expected empty service URL, got %q", d.ServiceURL)
	}
}

func TestProviderInfoToDetail(t *testing.T) {
	info := &spregistry.ProviderInfo{
		ID:              sdktypes.NewBigInt(42),
		Name:            "Basic",
		Description:     "A basic provider",
		IsActive:        false,
		ServiceProvider: common.HexToAddress("0xabcdef1234567890abcdef1234567890abcdef12"),
	}
	d, err := providerInfoToDetail(info)
	if err != nil {
		t.Fatalf("providerInfoToDetail: %v", err)
	}

	if d.ID.String() != "42" {
		t.Errorf("expected ID 42, got %s", d.ID.String())
	}
	if d.Name != "Basic" {
		t.Errorf("expected name Basic, got %s", d.Name)
	}
	if d.Description != "A basic provider" {
		t.Errorf("expected description, got %q", d.Description)
	}
	if d.Active {
		t.Error("expected Active to be false")
	}
	if d.HasPDP {
		t.Error("expected HasPDP to be false")
	}
	if d.HealthStatus != "skipped" {
		t.Errorf("expected health_status skipped, got %q", d.HealthStatus)
	}
}

func TestListAllProviders_FallsBackWhenPDPProductMissing(t *testing.T) {
	reg := &RegistryService{svc: &fakeRegistryProviderSource{
		count: big.NewInt(2),
		pdp: map[string]spregistry.PDPProvider{
			"1": newTestPDPProvider(1, "Alpha", true, "https://alpha.example.com"),
		},
		providers: map[string]spregistry.ProviderInfo{
			"2": {
				ID:              sdktypes.NewBigInt(2),
				Name:            "Basic",
				Description:     "No PDP product",
				IsActive:        true,
				ServiceProvider: common.HexToAddress("0xabcdef1234567890abcdef1234567890abcdef12"),
			},
		},
	}}

	got, err := listAllProviders(context.Background(), reg)
	if err != nil {
		t.Fatalf("listAllProviders: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(listAllProviders) = %d, want 2", len(got))
	}
	if !got[0].HasPDP || got[0].ID.String() != "1" {
		t.Fatalf("first provider = %+v, want PDP provider ID 1", got[0])
	}
	if got[1].HasPDP {
		t.Fatalf("second provider HasPDP = true, want false")
	}
	if got[1].ID.String() != "2" || got[1].Name != "Basic" {
		t.Fatalf("second provider = %+v, want basic provider ID 2", got[1])
	}
}

func TestPDPProviderToDetail_InvalidID(t *testing.T) {
	p := newTestPDPProvider(1, "Alpha", true, "https://alpha.example.com")
	p.Info.ID = sdktypes.NewBigInt(0)

	if _, err := pdpProviderToDetail(p); err == nil {
		t.Fatal("expected invalid provider ID error")
	}
}

func TestPDPProviderToDetail_LargeProviderID(t *testing.T) {
	p := newTestPDPProvider(1, "Alpha", true, "https://alpha.example.com")
	id, err := sdktypes.ParseBigInt("18446744073709551616")
	if err != nil {
		t.Fatalf("ParseBigInt: %v", err)
	}
	p.Info.ID = id

	d, err := pdpProviderToDetail(p)
	if err != nil {
		t.Fatalf("pdpProviderToDetail: %v", err)
	}
	if d.ID.String() != "18446744073709551616" {
		t.Fatalf("ID = %s, want 18446744073709551616", d.ID.String())
	}
}

func TestFormatSize(t *testing.T) {
	hugeVal := new(big.Int).Lsh(big.NewInt(1), 128) // 2^128, exceeds int64

	tests := []struct {
		input    *big.Int
		expected string
	}{
		{nil, "—"},
		{big.NewInt(0), "—"},
		{big.NewInt(-1024), "—"},
		{big.NewInt(1024), "1 KiB"},
		{big.NewInt(1024 * 1024), "1 MiB"},
		{big.NewInt(32 * 1024 * 1024 * 1024), "32 GiB"},
		{big.NewInt(1024 * 1024 * 1024 * 1024), "1 TiB"},
		{big.NewInt(500), "500 B"},
		{big.NewInt(1536), "1536 B"},       // 1.5 KiB — not evenly divisible
		{hugeVal, hugeVal.String() + " B"}, // overflow guard
	}
	for _, tt := range tests {
		got := FormatSize(tt.input)
		if got != tt.expected {
			t.Errorf("FormatSize(%v) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
