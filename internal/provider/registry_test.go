package provider

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/data-preservation-programs/go-synapse/spregistry"
	"github.com/ethereum/go-ethereum/common"
)

// mockRegistry implements SPRegistry for testing.
type mockRegistry struct {
	count     int
	countErr  error
	providers map[int]*spregistry.ProviderInfo
	getErr    error
	active    []*spregistry.ProviderInfo
	activeErr error
}

func (m *mockRegistry) GetProviderCount(_ context.Context) (int, error) {
	return m.count, m.countErr
}

func (m *mockRegistry) GetProvider(_ context.Context, id int) (*spregistry.ProviderInfo, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.providers[id], nil
}

func (m *mockRegistry) GetAllActiveProviders(_ context.Context) ([]*spregistry.ProviderInfo, error) {
	return m.active, m.activeErr
}

func newTestProvider(id int, name string, active bool, pdpURL string) *spregistry.ProviderInfo {
	p := &spregistry.ProviderInfo{
		ID:              id,
		Name:            name,
		Active:          active,
		ServiceProvider: common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
		Products:        make(map[string]*spregistry.ServiceProduct),
	}
	if pdpURL != "" {
		p.Products["PDP"] = &spregistry.ServiceProduct{
			Type:     "PDP",
			IsActive: true,
			Data: &spregistry.PDPOffering{
				ServiceURL:          pdpURL,
				MinPieceSizeInBytes: big.NewInt(1024 * 1024),             // 1 MiB
				MaxPieceSizeInBytes: big.NewInt(32 * 1024 * 1024 * 1024), // 32 GiB
				Location:            "US-East",
			},
		}
	}
	return p
}

func TestListProviders_AllMode(t *testing.T) {
	p1 := newTestProvider(1, "Alpha", true, "https://alpha.example.com")
	p2 := newTestProvider(2, "Beta", true, "https://beta.example.com")
	p3 := newTestProvider(3, "Gamma", false, "")

	reg := &mockRegistry{
		count: 3,
		providers: map[int]*spregistry.ProviderInfo{
			1: p1, 2: p2, 3: p3,
		},
	}

	details, err := ListProviders(context.Background(), reg, ListOptions{ActiveOnly: false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(details) != 3 {
		t.Fatalf("expected 3 providers, got %d", len(details))
	}

	// Verify enrichment.
	if !details[0].HasPDP {
		t.Error("expected Alpha to have PDP")
	}
	if details[0].ServiceURL != "https://alpha.example.com" {
		t.Errorf("expected Alpha service URL, got %q", details[0].ServiceURL)
	}
	if details[2].HasPDP {
		t.Error("expected Gamma to NOT have PDP")
	}
}

func TestListProviders_ActiveOnly(t *testing.T) {
	p1 := newTestProvider(1, "Alpha", true, "https://alpha.example.com")
	p2 := newTestProvider(2, "Beta", true, "https://beta.example.com")

	reg := &mockRegistry{
		active: []*spregistry.ProviderInfo{p1, p2},
	}

	details, err := ListProviders(context.Background(), reg, ListOptions{ActiveOnly: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(details) != 2 {
		t.Fatalf("expected 2 providers, got %d", len(details))
	}
	for _, d := range details {
		if !d.Active {
			t.Errorf("expected all providers to be active, got inactive: %s", d.Name)
		}
	}
}

func TestListProviders_ActiveOnly_NilElement(t *testing.T) {
	p1 := newTestProvider(1, "Alpha", true, "https://alpha.example.com")

	reg := &mockRegistry{
		active: []*spregistry.ProviderInfo{p1, nil, nil},
	}

	details, err := ListProviders(context.Background(), reg, ListOptions{ActiveOnly: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(details) != 1 {
		t.Fatalf("expected 1 provider (nil elements skipped), got %d", len(details))
	}
}

func TestListProviders_ZeroProviders(t *testing.T) {
	reg := &mockRegistry{count: 0, providers: map[int]*spregistry.ProviderInfo{}}

	details, err := ListProviders(context.Background(), reg, ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(details) != 0 {
		t.Fatalf("expected 0 providers, got %d", len(details))
	}
}

func TestListProviders_NilProvider(t *testing.T) {
	// Provider ID 2 returns nil (deleted/invalid).
	reg := &mockRegistry{
		count: 3,
		providers: map[int]*spregistry.ProviderInfo{
			1: newTestProvider(1, "Alpha", true, "https://alpha.example.com"),
			// 2 is missing (returns nil)
			3: newTestProvider(3, "Gamma", true, ""),
		},
	}

	details, err := ListProviders(context.Background(), reg, ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(details) != 2 {
		t.Fatalf("expected 2 providers (skipping nil), got %d", len(details))
	}
}

func TestToProviderDetail_WithPDP(t *testing.T) {
	p := newTestProvider(1, "Alpha", true, "https://alpha.example.com")
	d := toProviderDetail(p)

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
}

func TestToProviderDetail_NoPDP(t *testing.T) {
	p := newTestProvider(1, "NoPDP", true, "")
	d := toProviderDetail(p)

	if d.HasPDP {
		t.Error("expected HasPDP to be false")
	}
	if d.ServiceURL != "" {
		t.Errorf("expected empty service URL, got %q", d.ServiceURL)
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

func TestListProviders_CountError(t *testing.T) {
	reg := &mockRegistry{countErr: errors.New("rpc error")}

	_, err := ListProviders(context.Background(), reg, ListOptions{})
	if err == nil {
		t.Fatal("expected error from GetProviderCount, got nil")
	}
}

func TestListProviders_ActiveError(t *testing.T) {
	reg := &mockRegistry{activeErr: errors.New("rpc error")}

	_, err := ListProviders(context.Background(), reg, ListOptions{ActiveOnly: true})
	if err == nil {
		t.Fatal("expected error from GetAllActiveProviders, got nil")
	}
}

func TestListProviders_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	reg := &mockRegistry{
		count:  3,
		getErr: errors.New("connection refused"),
	}

	_, err := ListProviders(ctx, reg, ListOptions{})
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
}
