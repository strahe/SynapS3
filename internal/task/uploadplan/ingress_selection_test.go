package uploadplan

import (
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	bindingtask "github.com/strahe/synaps3/internal/task/binding"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
)

func TestFastestIngressRequiresValidSpeedForEveryProvider(t *testing.T) {
	db := testutil.NewTestFileDB(t)
	repos := repository.NewRepositories(db)
	now := time.Now().UTC()
	states := make([]observability.ProviderState, 2)
	plan := make([]bindingtask.Plan, 2)
	for i, textID := range []string{"101", "102"} {
		id, err := types.ParseOnChainID("provider_id", textID)
		if err != nil {
			t.Fatal(err)
		}
		url := "https://provider-" + textID + ".example"
		states[i] = observability.ProviderState{
			ProviderID: id, Status: observability.StatusAvailable,
			Active: new(true), HasPDP: new(true), ServiceURL: &url,
			LastCheckedAt: now, ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
		}
		plan[i] = bindingtask.Plan{CopyIndex: i, Provider: id}
		seconds := int64(10 + 10*i)
		duration := int64(1000)
		row := &providerbenchmark.Result{
			ProviderID: textID, State: providerbenchmark.StateSucceeded,
			ServiceURLHash: providerbenchmark.URLHash(url), SampleBytes: providerbenchmark.SampleBytes,
			DurationMS: &duration, BytesPerSecond: &seconds, TestedAt: &now,
			CreatedAt: now, UpdatedAt: now,
		}
		if _, err := db.NewInsert().Model(row).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if err := repos.Observability.ReplaceProviderStates(t.Context(), now, states); err != nil {
		t.Fatal(err)
	}
	h := &Handler{deps: Dependencies{
		Repositories: repos, Observability: observability.NewService(observability.ServiceOptions{Store: repos.Observability}),
	}}
	if got := h.fastestIngressIndex(t.Context(), plan); got != 1 {
		t.Fatalf("fastest ingress = %d, want 1", got)
	}
	if _, err := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("service_url_hash = ?", providerbenchmark.URLHash("https://changed.example")).
		Where("provider_id = ?", "102").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := h.fastestIngressIndex(t.Context(), plan); got != 0 {
		t.Fatalf("changed provider URL ingress = %d, want original order", got)
	}
	if _, err := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("service_url_hash = ?", providerbenchmark.URLHash(*states[1].ServiceURL)).
		Where("provider_id = ?", "102").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewDelete().Model((*providerbenchmark.Result)(nil)).Where("provider_id = ?", "101").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := h.fastestIngressIndex(t.Context(), plan); got != 0 {
		t.Fatalf("missing speed ingress = %d, want original order", got)
	}
}
