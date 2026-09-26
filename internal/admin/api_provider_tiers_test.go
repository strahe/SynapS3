package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/types"
	sdktypes "github.com/strahe/synapse-go/types"
)

type failingEndorsements struct{}

type emptyApprovedProviderSource struct{}

func (emptyApprovedProviderSource) ReadApprovedProviderIDs(context.Context) ([]types.OnChainID, error) {
	return nil, nil
}

func (failingEndorsements) GetEndorsedProviderIDs(context.Context) ([]sdktypes.BigInt, error) {
	return nil, errors.New("endorsement RPC unavailable")
}

type staticEndorsements struct{}

func (staticEndorsements) GetEndorsedProviderIDs(context.Context) ([]sdktypes.BigInt, error) {
	return nil, nil
}

func TestRefreshProviderTiersReportsPartialSuccess(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	srv.observability = observability.NewService(observability.ServiceOptions{
		Store: repos.Observability, ApprovedProviders: emptyApprovedProviderSource{},
		EndorsedProviders: failingEndorsements{}, RefreshInterval: time.Minute,
	})
	rec := httptest.NewRecorder()
	srv.handleAPIRefreshProviderTiers(rec, httptest.NewRequest(http.MethodPost, "/api/v1/observability/provider-tiers/refresh", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Approved providerTierRefreshResult `json:"approved_result"`
		Endorsed providerTierRefreshResult `json:"endorsed_result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Approved.Success || body.Approved.CheckedAt == nil || body.Endorsed.Success || body.Endorsed.Error == "" || body.Endorsed.CheckedAt != nil {
		t.Fatalf("results = %#v", body)
	}
}

func TestRefreshProviderTiersCommitsBothEmptyLists(t *testing.T) {
	srv, repos := newBucketAPITestServer(t)
	srv.observability = observability.NewService(observability.ServiceOptions{
		Store: repos.Observability, ApprovedProviders: emptyApprovedProviderSource{},
		EndorsedProviders: staticEndorsements{}, RefreshInterval: time.Minute,
	})
	rec := httptest.NewRecorder()
	srv.handleAPIRefreshProviderTiers(rec, httptest.NewRequest(http.MethodPost, "/api/v1/observability/provider-tiers/refresh", nil))
	var body struct {
		Approved providerTierRefreshResult `json:"approved_result"`
		Endorsed providerTierRefreshResult `json:"endorsed_result"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || !body.Approved.Success || !body.Endorsed.Success {
		t.Fatalf("status=%d results=%#v", rec.Code, body)
	}
}
