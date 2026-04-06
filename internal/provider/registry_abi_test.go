package provider

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/data-preservation-programs/go-synapse/spregistry"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

type abiProviderInfo struct {
	ServiceProvider common.Address
	Payee           common.Address
	Name            string
	Description     string
	IsActive        bool
}

type abiProduct struct {
	ProductType    uint8
	CapabilityKeys []string
	IsActive       bool
}

type abiProviderWithProductTuple struct {
	ProviderId              *big.Int
	ProviderInfo            abiProviderInfo
	Product                 abiProduct
	ProductCapabilityValues [][]byte
}

func TestDecodeProviderWithProductResult_ParsesSingleTupleOutput(t *testing.T) {
	parsedABI := mustParseSPRegistryABI(t)

	keys, values, err := spregistry.EncodePDPCapabilities(&spregistry.PDPOffering{
		ServiceURL:               "https://alpha.example.com",
		MinPieceSizeInBytes:      big.NewInt(1024),
		MaxPieceSizeInBytes:      big.NewInt(1024 * 1024),
		StoragePricePerTiBPerDay: big.NewInt(42),
		MinProvingPeriodInEpochs: big.NewInt(2880),
		Location:                 "US-East",
		PaymentTokenAddress:      common.HexToAddress("0x3333333333333333333333333333333333333333"),
		IPNIPiece:                true,
		IPNIIPFS:                 true,
	}, nil)
	if err != nil {
		t.Fatalf("EncodePDPCapabilities returned error: %v", err)
	}

	raw, err := parsedABI.Methods["getProviderWithProduct"].Outputs.Pack(abiProviderWithProductTuple{
		ProviderId: big.NewInt(7),
		ProviderInfo: abiProviderInfo{
			ServiceProvider: common.HexToAddress("0x1111111111111111111111111111111111111111"),
			Payee:           common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Name:            "Alpha",
			Description:     "Test provider",
			IsActive:        true,
		},
		Product: abiProduct{
			ProductType:    uint8(spregistry.ProductTypePDP),
			CapabilityKeys: keys,
			IsActive:       true,
		},
		ProductCapabilityValues: values,
	})
	if err != nil {
		t.Fatalf("packing getProviderWithProduct output: %v", err)
	}

	got, err := decodeProviderWithProductResult(raw)
	if err != nil {
		t.Fatalf("decodeProviderWithProductResult returned error: %v", err)
	}
	if got == nil {
		t.Fatal("expected provider info, got nil")
	}
	if got.ID != 7 {
		t.Fatalf("expected provider ID 7, got %d", got.ID)
	}
	if got.Name != "Alpha" {
		t.Fatalf("expected provider name Alpha, got %q", got.Name)
	}
	if !got.Active {
		t.Fatal("expected provider to be active")
	}

	pdp := got.Products["PDP"]
	if pdp == nil || pdp.Data == nil {
		t.Fatal("expected active PDP product to be decoded")
	}
	if pdp.Data.ServiceURL != "https://alpha.example.com" {
		t.Fatalf("expected service URL to be decoded, got %q", pdp.Data.ServiceURL)
	}
	if pdp.Data.MinPieceSizeInBytes.Cmp(big.NewInt(1024)) != 0 {
		t.Fatalf("expected min piece size 1024, got %v", pdp.Data.MinPieceSizeInBytes)
	}
	if pdp.Data.Location != "US-East" {
		t.Fatalf("expected location US-East, got %q", pdp.Data.Location)
	}
}

func TestDecodeActiveProviderPage_ParsesIDsAndHasMore(t *testing.T) {
	parsedABI := mustParseSPRegistryABI(t)

	raw, err := parsedABI.Methods["getAllActiveProviders"].Outputs.Pack(
		[]*big.Int{big.NewInt(2), big.NewInt(4), big.NewInt(8)},
		true,
	)
	if err != nil {
		t.Fatalf("packing getAllActiveProviders output: %v", err)
	}

	ids, hasMore, err := decodeActiveProviderPage(raw)
	if err != nil {
		t.Fatalf("decodeActiveProviderPage returned error: %v", err)
	}
	if !hasMore {
		t.Fatal("expected hasMore=true")
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}
	want := []int{2, 4, 8}
	for i, got := range ids {
		if got != want[i] {
			t.Fatalf("id[%d] = %d, want %d", i, got, want[i])
		}
	}
}

func TestRegistryService_GetAllActiveProviders_RejectsEmptyPageWithHasMore(t *testing.T) {
	parsedABI := mustParseSPRegistryABI(t)

	svc := &registryService{
		client: &stubContractCaller{
			t:         t,
			parsedABI: parsedABI,
		},
		contractAddr: common.HexToAddress("0x9999999999999999999999999999999999999999"),
		parsedABI:    parsedABI,
	}

	_, err := svc.GetAllActiveProviders(context.Background())
	if err == nil {
		t.Fatal("expected empty-page pagination error, got nil")
	}
	if !strings.Contains(err.Error(), "hasMore=true with empty results") {
		t.Fatalf("expected empty-page pagination error, got %v", err)
	}
}

func mustParseSPRegistryABI(t *testing.T) abi.ABI {
	t.Helper()

	parsedABI, err := abi.JSON(strings.NewReader(spregistry.SPRegistryABIJSON))
	if err != nil {
		t.Fatalf("failed to parse SPRegistry ABI: %v", err)
	}
	return parsedABI
}

type stubContractCaller struct {
	t         *testing.T
	parsedABI abi.ABI
	calls     int
}

func (s *stubContractCaller) CallContract(_ context.Context, _ ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	s.calls++
	if s.calls > 1 {
		return nil, errors.New("unexpected second page request")
	}

	raw, err := s.parsedABI.Methods["getAllActiveProviders"].Outputs.Pack([]*big.Int{}, true)
	if err != nil {
		s.t.Fatalf("packing getAllActiveProviders output: %v", err)
	}
	return raw, nil
}
