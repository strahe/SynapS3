package admin

import (
	"math/big"
	"net/http"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestReplacementBindsPriceAtConfirmationAndPreservesReplay(t *testing.T) {
	fixture := newReplacementAPIFixture(t, &stubProviderSelector{providers: []string{"202"}})
	body := `{"mode":"manual","provider_id":"202","client_request_id":"price-request"}`
	first := fixture.startRaw(t, body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d body=%s", first.Code, first.Body.String())
	}
	fixture.market.price.Fees.CreateDataSetFee = big.NewInt(2)
	replay := fixture.startRaw(t, body)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d body=%s", replay.Code, replay.Body.String())
	}
	conflict := fixture.startRaw(t, `{"mode":"manual","provider_id":"202","client_request_id":"price-request","price_list_fingerprint":"0000000000000000000000000000000000000000000000000000000000000000"}`)
	if conflict.Code != http.StatusConflict || decodeAPIError(t, conflict)["code"] != "replacement_idempotency_conflict" {
		t.Fatalf("changed replay status = %d body=%s", conflict.Code, conflict.Body.String())
	}
	changed := fixture.startRaw(t, `{"mode":"manual","provider_id":"202","client_request_id":"new-price-request"}`)
	if changed.Code != http.StatusConflict || decodeAPIError(t, changed)["code"] != "price_list_changed" {
		t.Fatalf("changed price status = %d body=%s", changed.Code, changed.Body.String())
	}
	fixture.market.price.Token = common.HexToAddress("0x300")
	unsupported := fixture.startRaw(t, `{"mode":"manual","provider_id":"202","client_request_id":"unsupported-token"}`)
	if unsupported.Code != http.StatusConflict || decodeAPIError(t, unsupported)["code"] != "unsupported_price_token" {
		t.Fatalf("unsupported token status = %d body=%s", unsupported.Code, unsupported.Body.String())
	}
	fixture.market.price = nil
	unavailable := fixture.startRaw(t, `{"mode":"manual","provider_id":"202","client_request_id":"price-unavailable"}`)
	if unavailable.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable price status = %d body=%s", unavailable.Code, unavailable.Body.String())
	}
}
