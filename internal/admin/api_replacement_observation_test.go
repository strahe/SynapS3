package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/storagereplacement"
	idtypes "github.com/strahe/synaps3/internal/types"
)

func TestReplacementCandidatesAndSubmitRejectStaleHealth(t *testing.T) {
	fixture := newReplacementAPIFixtureWithHealth(t, &stubProviderSelector{providers: []string{"101", "202", "303", "404"}}, false)
	now := time.Now().UTC()
	fixture.srv.observability = observability.NewService(observability.ServiceOptions{
		Store: fixture.srv.repos.Observability, RefreshInterval: 5 * time.Minute,
		Now: func() time.Time { return now },
	})
	active, pdp, url := true, true, "https://provider.example"
	state := func(id string) observability.ProviderState {
		return observability.ProviderState{
			ProviderID: onChainIDValue(id), Status: observability.StatusAvailable,
			Active: &active, HasPDP: &pdp, ServiceURL: &url,
			ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
		}
	}
	if err := fixture.srv.repos.Observability.UpsertProviderObservation(context.Background(), now.Add(-11*time.Minute), state("303")); err != nil {
		t.Fatal(err)
	}
	if err := fixture.srv.repos.Observability.UpsertProviderObservation(context.Background(), now.Add(-time.Minute), state("202")); err != nil {
		t.Fatal(err)
	}
	if err := fixture.srv.repos.Observability.UpsertProviderObservation(context.Background(), now, state("404")); err != nil {
		t.Fatal(err)
	}
	if err := fixture.srv.repos.Observability.UpsertProviderObservation(context.Background(), now, observability.ProviderState{
		ProviderID: onChainIDValue("202"), Status: observability.StatusAvailable, Active: &active, HasPDP: &pdp, ServiceURL: &url,
		Profile: &observability.ProviderProfile{ProviderID: onChainIDValue("202"), Active: true, ServiceURL: url, RegistrySnapshot: json.RawMessage(`{"version":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.srv.repos.Observability.RecordApprovedProviders(context.Background(), now, []idtypes.OnChainID{onChainIDValue("202")}); err != nil {
		t.Fatal(err)
	}
	rec := fixture.listProviders(t)
	if rec.Code != http.StatusOK {
		t.Fatalf("candidate status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Providers []replacementProviderResponse `json:"providers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Providers) != 3 || !body.Providers[0].ManualSelectable || body.Providers[1].ManualSelectable || body.Providers[1].ManualBlockReason != "observation_stale" || body.Providers[2].ManualSelectable || body.Providers[2].ManualBlockReason != "profile_missing" {
		t.Fatalf("candidates = %#v", body.Providers)
	}
	denied := fixture.startRaw(t, `{"mode":"manual","provider_id":"303","client_request_id":"stale-target"}`)
	if denied.Code != http.StatusBadRequest || decodeAPIError(t, denied)["code"] != storagereplacement.CodeTargetUnavailable {
		t.Fatalf("stale target status = %d body=%s", denied.Code, denied.Body.String())
	}
	denied = fixture.startRaw(t, `{"mode":"manual","provider_id":"404","client_request_id":"missing-profile"}`)
	if denied.Code != http.StatusBadRequest || decodeAPIError(t, denied)["code"] != storagereplacement.CodeTargetUnavailable {
		t.Fatalf("missing profile target status = %d body=%s", denied.Code, denied.Body.String())
	}
	automatic := fixture.startRaw(t, `{"mode":"automatic","client_request_id":"fresh-target"}`)
	if automatic.Code != http.StatusCreated {
		t.Fatalf("automatic status = %d body=%s", automatic.Code, automatic.Body.String())
	}
	if selected := decodeReplacement(t, automatic).Target.ProviderID; selected != "202" {
		t.Fatalf("automatic provider = %q, want 202", selected)
	}
}

func TestReplacementRejectsHealthForPreviousServiceURL(t *testing.T) {
	fixture := newReplacementAPIFixtureWithHealth(t, &stubProviderSelector{providers: []string{"202"}}, false)
	now := time.Now().UTC()
	fixture.srv.observability = observability.NewService(observability.ServiceOptions{
		Store: fixture.srv.repos.Observability, RefreshInterval: 5 * time.Minute,
		Now: func() time.Time { return now },
	})
	id := onChainIDValue("202")
	active, pdp, oldURL := true, true, "https://old.example"
	if err := fixture.srv.repos.Observability.UpsertProviderObservation(context.Background(), now.Add(-time.Minute), observability.ProviderState{
		ProviderID: id, Status: observability.StatusAvailable, Active: &active, HasPDP: &pdp, ServiceURL: &oldURL,
	}); err != nil {
		t.Fatal(err)
	}
	failure := "health check failed"
	if err := fixture.srv.repos.Observability.UpsertProviderObservation(context.Background(), now, observability.ProviderState{
		ProviderID: id, Status: observability.StatusUnknown, LastError: &failure,
		Profile: &observability.ProviderProfile{
			ProviderID: id, Active: true, ServiceURL: "https://new.example", RegistrySnapshot: json.RawMessage(`{"version":1}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	rec := fixture.listProviders(t)
	if rec.Code != http.StatusOK {
		t.Fatalf("candidate status = %d body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Providers []replacementProviderResponse `json:"providers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Providers) != 1 || body.Providers[0].ManualSelectable || body.Providers[0].ManualBlockReason != "profile_url_changed" {
		t.Fatalf("candidate with changed URL = %#v", body.Providers)
	}
	if denied := fixture.startRaw(t, `{"mode":"manual","provider_id":"202","client_request_id":"changed-url"}`); denied.Code != http.StatusBadRequest || decodeAPIError(t, denied)["code"] != storagereplacement.CodeTargetUnavailable {
		t.Fatalf("changed URL target status = %d body=%s", denied.Code, denied.Body.String())
	}
	if _, eligible, err := providerbenchmark.CurrentServiceURL(context.Background(), fixture.srv.observability, id); err != nil || eligible {
		t.Fatalf("benchmark URL eligibility = %t, err=%v", eligible, err)
	}
}

func TestReplacementRejectsInactiveRegistryProfileAfterHealthTimeout(t *testing.T) {
	fixture := newReplacementAPIFixtureWithHealth(t, &stubProviderSelector{providers: []string{"202"}}, false)
	now := time.Now().UTC()
	fixture.srv.observability = observability.NewService(observability.ServiceOptions{
		Store: fixture.srv.repos.Observability, RefreshInterval: 5 * time.Minute,
		Now: func() time.Time { return now },
	})
	id := onChainIDValue("202")
	active, pdp, serviceURL := true, true, "https://provider.example"
	if err := fixture.srv.repos.Observability.UpsertProviderObservation(context.Background(), now.Add(-time.Minute), observability.ProviderState{
		ProviderID: id, Status: observability.StatusAvailable, Active: &active, HasPDP: &pdp, ServiceURL: &serviceURL,
	}); err != nil {
		t.Fatal(err)
	}
	failure := "health check did not complete"
	if err := fixture.srv.repos.Observability.UpsertProviderObservation(context.Background(), now, observability.ProviderState{
		ProviderID: id, Status: observability.StatusUnknown, LastError: &failure,
		Profile: &observability.ProviderProfile{
			ProviderID: id, Active: false, ServiceURL: serviceURL, RegistrySnapshot: json.RawMessage(`{"version":1}`),
		},
	}); err != nil {
		t.Fatal(err)
	}
	var body struct {
		Providers []replacementProviderResponse `json:"providers"`
	}
	if err := json.Unmarshal(fixture.listProviders(t).Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Providers) != 1 || body.Providers[0].ManualSelectable || body.Providers[0].ManualBlockReason != "provider_unavailable" {
		t.Fatalf("inactive candidate = %#v", body.Providers)
	}
	if denied := fixture.startRaw(t, `{"mode":"manual","provider_id":"202","client_request_id":"inactive-target"}`); denied.Code != http.StatusBadRequest || decodeAPIError(t, denied)["code"] != storagereplacement.CodeTargetUnavailable {
		t.Fatalf("inactive target status = %d body=%s", denied.Code, denied.Body.String())
	}
}
