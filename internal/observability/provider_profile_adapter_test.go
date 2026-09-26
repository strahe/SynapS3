package observability

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/strahe/synaps3/internal/provider"
	"github.com/strahe/synaps3/internal/types"
)

func TestProviderFromDetailPreservesRegistryOfferingAndRawCapabilities(t *testing.T) {
	id, err := types.ParseOnChainID("provider_id", "23")
	if err != nil {
		t.Fatal(err)
	}
	p := providerFromDetail(provider.ProviderDetail{
		ID: id, Name: "Example", Description: "Declared description", Active: true,
		Address: common.HexToAddress("0x100"), Payee: common.HexToAddress("0x200"),
		HasPDP: true, ServiceURL: "https://example.test", MinPieceSize: big.NewInt(128), MaxPieceSize: big.NewInt(1024),
		StoragePrice: big.NewInt(42), MinProvingPeriod: big.NewInt(96), Location: "declared-location",
		PaymentToken: common.HexToAddress("0x300"), IPNIPiece: true, IPNIIPFS: true,
		IPNIPeerID: "peer", ExtraCapabilities: map[string][]byte{"binary": {0, 255, 1}},
	})
	if p.Profile == nil || p.Profile.Name != "Example" || p.Profile.PayeeAddress != common.HexToAddress("0x200").Hex() {
		t.Fatalf("profile = %#v", p.Profile)
	}
	var snapshot struct {
		Version  int `json:"version"`
		Offering struct {
			MinProvingPeriod string            `json:"min_proving_period_epochs"`
			Price            string            `json:"storage_price_per_tib_per_day"`
			Extras           map[string]string `json:"extra_capabilities_hex"`
		} `json:"pdp_offering"`
	}
	if err := json.Unmarshal(p.Profile.RegistrySnapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != 1 || snapshot.Offering.MinProvingPeriod != "96" || snapshot.Offering.Price != "42" || snapshot.Offering.Extras["binary"] != "0x00ff01" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}
