package uploadplan

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	bindingtask "github.com/strahe/synaps3/internal/task/binding"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
)

func TestIngressSelectsFastestCurrentMeasurement(t *testing.T) {
	db := testutil.NewTestDB(t)
	repos := repository.NewRepositories(db)
	now := time.Now().UTC()
	states := make([]observability.ProviderState, 3)
	plan := make([]bindingtask.Plan, 3)
	for i, textID := range []string{"101", "102", "103"} {
		id, err := types.ParseOnChainID("provider_id", textID)
		if err != nil {
			t.Fatal(err)
		}
		url := "https://provider-" + textID + ".example"
		states[i] = observability.ProviderState{
			ProviderID: id, Status: observability.StatusAvailable,
			Active: new(true), HasPDP: new(true), ServiceURL: &url,
			LastCheckedAt: now, ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
			Profile: &observability.ProviderProfile{ProviderID: id, Active: true, ServiceURL: url, RegistrySnapshot: json.RawMessage(`{"version":1}`)},
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
		if i == 1 {
			row.State = providerbenchmark.StateFailed
			row.DurationMS, row.BytesPerSecond = nil, nil
			row.FailureCode = new("upload_failed")
		}
		if _, err := db.NewInsert().Model(row).Exec(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if err := repos.Observability.ReplaceProviderStates(t.Context(), now, states); err != nil {
		t.Fatal(err)
	}
	h := &Handler{deps: Dependencies{Repositories: repos}}
	checkIngress := func(want string) {
		t.Helper()
		got, err := h.ingressProviderID(t.Context(), plan)
		if err != nil || got.String() != want {
			t.Fatalf("ingress provider = %s, err=%v, want %s", got, err, want)
		}
	}
	checkIngress("103")
	if _, err := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("service_url_hash = ?", providerbenchmark.URLHash("https://changed.example")).
		Where("provider_id = ?", "103").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkIngress("101")
	if _, err := db.NewUpdate().Model((*providerbenchmark.Result)(nil)).
		Set("service_url_hash = ?", providerbenchmark.URLHash(*states[2].ServiceURL)).
		Where("provider_id = ?", "103").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.NewDelete().Model((*providerbenchmark.Result)(nil)).Where("provider_id = ?", "101").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkIngress("103")
	if _, err := db.NewDelete().Model((*providerbenchmark.Result)(nil)).Where("provider_id = ?", "103").Exec(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkIngress("101")
}
