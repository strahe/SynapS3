package repository_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/observability"
	idtypes "github.com/strahe/synaps3/internal/types"
)

func TestProviderCatalogKeepsNewerTargetedReadAndLastGoodProfile(t *testing.T) {
	ctx := context.Background()
	repos := repository.NewRepositories(testDB(t))
	id := onChainID(t, "101")
	start := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	state := func(name string) observability.ProviderState {
		return observability.ProviderState{
			ProviderID: id, Status: observability.StatusAvailable,
			ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
			Profile: &observability.ProviderProfile{ProviderID: id, Name: name, RegistrySnapshot: json.RawMessage(`{"version":1}`)},
		}
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, start, []observability.ProviderState{state("Original")}); err != nil {
		t.Fatal(err)
	}
	newer := state("New name")
	newer.Status = observability.StatusDegraded
	if err := repos.Observability.UpsertProviderObservation(ctx, start.Add(2*time.Minute), newer); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, start.Add(time.Minute), []observability.ProviderState{state("Old scan")}); err != nil {
		t.Fatal(err)
	}
	profiles, err := repos.Observability.ProviderProfiles(ctx, []idtypes.OnChainID{id})
	if err != nil {
		t.Fatal(err)
	}
	if profiles[id.String()].Name != "New name" {
		t.Fatalf("profile = %q, want newer targeted read", profiles[id.String()].Name)
	}
	page, err := repos.Observability.ListProviderStates(ctx, observability.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Status != observability.StatusDegraded {
		t.Fatalf("health = %#v, want newer targeted read", page.Items)
	}
	failure := "Registry lookup failed"
	failed := observability.ProviderState{
		ProviderID: id, Status: observability.StatusUnknown,
		ReasonCodes: []observability.ReasonCode{observability.ReasonRegistryLookupFailed},
		LastError:   &failure,
	}
	if err := repos.Observability.UpsertProviderObservation(ctx, start.Add(3*time.Minute), failed); err != nil {
		t.Fatal(err)
	}
	page, err = repos.Observability.ListProviderStates(ctx, observability.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Status != observability.StatusDegraded || !page.Items[0].LastCheckedAt.Equal(start.Add(2*time.Minute)) || !page.Items[0].LastAttemptAt.Equal(start.Add(3*time.Minute)) || page.Items[0].LastError == nil {
		t.Fatalf("failed lookup erased last valid observation: %#v", page.Items)
	}
	partial := state("New details")
	partial.Status = observability.StatusUnknown
	partial.LastError = &failure
	if err := repos.Observability.UpsertProviderObservation(ctx, start.Add(3*time.Minute+30*time.Second), partial); err != nil {
		t.Fatal(err)
	}
	profiles, err = repos.Observability.ProviderProfiles(ctx, []idtypes.OnChainID{id})
	if err != nil || profiles[id.String()].Name != "New details" {
		t.Fatalf("successful profile read = %#v, err=%v", profiles, err)
	}
	page, err = repos.Observability.ListProviderStates(ctx, observability.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Status != observability.StatusDegraded || !page.Items[0].LastCheckedAt.Equal(start.Add(2*time.Minute)) || !page.Items[0].LastAttemptAt.Equal(start.Add(3*time.Minute+30*time.Second)) {
		t.Fatalf("failed health check erased last valid observation: %#v", page.Items)
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, start.Add(3*time.Minute+15*time.Second), []observability.ProviderState{state("Older scan")}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, start.Add(3*time.Minute+20*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	page, err = repos.Observability.ListProviderStates(ctx, observability.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Status != observability.StatusDegraded || !page.Items[0].LastAttemptAt.Equal(start.Add(3*time.Minute+30*time.Second)) || page.Items[0].LastError == nil {
		t.Fatalf("older scan replaced newer failed refresh: %#v", page.Items)
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, start.Add(4*time.Minute), nil); err != nil {
		t.Fatal(err)
	}
	page, err = repos.Observability.ListProviderStates(ctx, observability.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("health rows = %d, want removed", len(page.Items))
	}
	profiles, err = repos.Observability.ProviderProfiles(ctx, []idtypes.OnChainID{id})
	if err != nil || profiles[id.String()].Name != "New details" {
		t.Fatalf("retained profile = %#v, err=%v", profiles, err)
	}
}

func TestProviderTierMembershipKeepsIndependentLastCompleteReads(t *testing.T) {
	ctx := context.Background()
	repos := repository.NewRepositories(testDB(t))
	start := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	first, second, later := onChainID(t, "101"), onChainID(t, "202"), onChainID(t, "303")
	seed := func(id idtypes.OnChainID) {
		t.Helper()
		if err := repos.Observability.UpsertProviderObservation(ctx, start, observability.ProviderState{
			ProviderID: id, Status: observability.StatusAvailable,
			Profile: &observability.ProviderProfile{ProviderID: id, Name: id.String(), RegistrySnapshot: json.RawMessage(`{"version":1}`)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	seed(first)
	seed(second)
	read := func(id idtypes.OnChainID) observability.ProviderProfile {
		t.Helper()
		rows, err := repos.Observability.ProviderProfiles(ctx, []idtypes.OnChainID{id})
		if err != nil {
			t.Fatal(err)
		}
		return rows[id.String()]
	}
	if profile := read(first); profile.Approved || profile.ApprovedCheckedAt != nil || profile.Endorsed || profile.EndorsedCheckedAt != nil {
		t.Fatalf("new profile = %#v, want unknown tier membership", profile)
	}
	if err := repos.Observability.RecordApprovedProviders(ctx, start.Add(time.Minute), []idtypes.OnChainID{first}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.RecordEndorsedProviders(ctx, start.Add(2*time.Minute), []idtypes.OnChainID{second}); err != nil {
		t.Fatal(err)
	}
	if profile := read(second); profile.Approved || profile.ApprovedCheckedAt == nil || !profile.Endorsed || profile.EndorsedCheckedAt == nil {
		t.Fatalf("independent tiers = %#v", profile)
	}
	if err := repos.Observability.RecordApprovedProviders(ctx, start.Add(3*time.Minute), nil); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.RecordApprovedProviders(ctx, start.Add(2*time.Minute), []idtypes.OnChainID{first}); err != nil {
		t.Fatal(err)
	}
	if profile := read(first); profile.Approved || profile.ApprovedCheckedAt == nil || !profile.ApprovedCheckedAt.Equal(start.Add(3*time.Minute)) {
		t.Fatalf("empty newer approval set = %#v", profile)
	}
	seed(later)
	if profile := read(later); profile.Approved || profile.ApprovedCheckedAt == nil || profile.Endorsed || profile.EndorsedCheckedAt == nil {
		t.Fatalf("profile added after tier read = %#v, want latest tier snapshot", profile)
	}
	if err := repos.Observability.UpsertProviderObservation(ctx, start.Add(4*time.Minute), observability.ProviderState{
		ProviderID: later, Status: observability.StatusAvailable,
		Profile: &observability.ProviderProfile{ProviderID: later, Name: "New profile", RegistrySnapshot: json.RawMessage(`{"version":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.RecordApprovedProviders(ctx, start.Add(2*time.Minute), []idtypes.OnChainID{later}); err != nil {
		t.Fatal(err)
	}
	if profile := read(later); profile.Approved || profile.ApprovedCheckedAt == nil || !profile.ApprovedCheckedAt.Equal(start.Add(3*time.Minute)) {
		t.Fatalf("older tier read replaced latest snapshot = %#v", profile)
	}
}

func TestProviderTierSnapshotAppliesToProfileCreatedAfterRead(t *testing.T) {
	ctx := context.Background()
	repos := repository.NewRepositories(testDB(t))
	approved := onChainID(t, "101")
	other := onChainID(t, "202")
	checkedAt := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	if err := repos.Observability.RecordApprovedProviders(ctx, checkedAt, []idtypes.OnChainID{approved}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []idtypes.OnChainID{approved, other} {
		if err := repos.Observability.UpsertProviderObservation(ctx, checkedAt.Add(time.Minute), observability.ProviderState{
			ProviderID: id, Status: observability.StatusAvailable,
			Profile: &observability.ProviderProfile{ProviderID: id, Name: id.String(), RegistrySnapshot: json.RawMessage(`{"version":1}`)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	profiles, err := repos.Observability.ProviderProfiles(ctx, []idtypes.OnChainID{approved, other})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []idtypes.OnChainID{approved, other} {
		profile := profiles[id.String()]
		if profile.Approved != id.Equal(approved) || profile.ApprovedCheckedAt == nil || !profile.ApprovedCheckedAt.Equal(checkedAt) {
			t.Fatalf("profile %s tier = %#v", id, profile)
		}
	}
}

func TestPostgresProviderCatalogRepeatedUpsertKeepsLatestTierSnapshot(t *testing.T) {
	ctx := context.Background()
	repos := repository.NewRepositories(newPostgresTaskDB(t))
	id := onChainID(t, "101")
	start := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	state := func(name string) observability.ProviderState {
		return observability.ProviderState{
			ProviderID: id, Status: observability.StatusAvailable,
			ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
			Profile: &observability.ProviderProfile{ProviderID: id, Name: name, RegistrySnapshot: json.RawMessage(`{"version":1}`)},
		}
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, start, []observability.ProviderState{state("First")}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.RecordApprovedProviders(ctx, start.Add(time.Minute), []idtypes.OnChainID{id}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.UpsertProviderObservation(ctx, start.Add(2*time.Minute), state("Second")); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.RecordApprovedProviders(ctx, start.Add(90*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.ReplaceProviderStates(ctx, start.Add(time.Minute), []observability.ProviderState{state("Old scan")}); err != nil {
		t.Fatal(err)
	}
	profiles, err := repos.Observability.ProviderProfiles(ctx, []idtypes.OnChainID{id})
	if err != nil {
		t.Fatal(err)
	}
	profile := profiles[id.String()]
	if profile.Name != "Second" || profile.Approved || profile.ApprovedCheckedAt == nil || !profile.ApprovedCheckedAt.Equal(start.Add(90*time.Second)) {
		t.Fatalf("profile = %#v, want latest Registry details and tier snapshot", profile)
	}
	page, err := repos.Observability.ListProviderStates(ctx, observability.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || !page.Items[0].LastCheckedAt.Equal(start.Add(2*time.Minute)) {
		t.Fatalf("observations = %#v, want latest upsert", page.Items)
	}
}
