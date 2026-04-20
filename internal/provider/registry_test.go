package provider

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synapse-go/spregistry"
)

func newTestPDPProvider(id int, name string, active bool, pdpURL string) spregistry.PDPProvider {
	p := spregistry.PDPProvider{
		Info: spregistry.ProviderInfo{
			ID:              big.NewInt(int64(id)),
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

	if d.ID != 1 {
		t.Errorf("expected ID 1, got %d", d.ID)
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
		ID:              big.NewInt(42),
		Name:            "Basic",
		Description:     "A basic provider",
		IsActive:        false,
		ServiceProvider: common.HexToAddress("0xabcdef1234567890abcdef1234567890abcdef12"),
	}
	d, err := providerInfoToDetail(info)
	if err != nil {
		t.Fatalf("providerInfoToDetail: %v", err)
	}

	if d.ID != 42 {
		t.Errorf("expected ID 42, got %d", d.ID)
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

func TestPDPProviderToDetail_InvalidID(t *testing.T) {
	p := newTestPDPProvider(1, "Alpha", true, "https://alpha.example.com")
	p.Info.ID = new(big.Int).Lsh(big.NewInt(1), 128)

	if _, err := pdpProviderToDetail(p); err == nil {
		t.Fatal("expected invalid provider ID error")
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
