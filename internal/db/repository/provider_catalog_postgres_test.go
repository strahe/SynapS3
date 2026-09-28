//go:build postgres

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
	if _, err := repos.Observability.RecordApprovedProviders(ctx, start.Add(time.Minute), []idtypes.OnChainID{id}); err != nil {
		t.Fatal(err)
	}
	if err := repos.Observability.UpsertProviderObservation(ctx, start.Add(2*time.Minute), state("Second")); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Observability.RecordApprovedProviders(ctx, start.Add(90*time.Second), nil); err != nil {
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
