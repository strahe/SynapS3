package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/providerbenchmark"
	"github.com/strahe/synaps3/internal/providerselect"
	"github.com/strahe/synaps3/internal/testutil"
	"github.com/strahe/synaps3/internal/types"
)

type uploadSpeedSetupTierSource struct{ calls atomic.Int32 }

func (s *uploadSpeedSetupTierSource) ReadApprovedProviderIDs(context.Context) ([]types.OnChainID, error) {
	s.calls.Add(1)
	return nil, errors.New("unexpected tier refresh")
}

func seedUploadSpeedSetup(t *testing.T, count int) (*Server, *repository.Repositories, []observability.ProviderState) {
	t.Helper()
	repos := repository.NewRepositories(testutil.NewTestDB(t))
	now := time.Now().UTC()
	states := make([]observability.ProviderState, count)
	for i := range states {
		id := types.NewOnChainID(uint64(101 + i))
		url := fmt.Sprintf("https://provider-%s.example", id)
		states[i] = observability.ProviderState{
			ProviderID: id, Active: new(true), HasPDP: new(true), ServiceURL: &url,
			Status: observability.StatusAvailable, LastCheckedAt: now,
			ReasonCodes: []observability.ReasonCode{}, Evidence: map[string]any{},
			Profile: &observability.ProviderProfile{ProviderID: id, Active: true, ServiceURL: url, RegistrySnapshot: json.RawMessage(`{}`)},
		}
	}
	if err := repos.Observability.ReplaceProviderStates(t.Context(), now, states); err != nil {
		t.Fatal(err)
	}
	srv := &Server{
		repos: repos, taskService: newAdminTestTaskService(t, repos), logger: testLogger(),
		observability:      observability.NewService(observability.ServiceOptions{Store: repos.Observability}),
		anchorProviderTier: providerselect.TierApproved, filecoinDefaultCopies: 2, uploadSpeedTestAutoScope: "trusted",
	}
	return srv, repos, states
}

func startSetupSpeedTest(t *testing.T, srv *Server, providerID string) *providerbenchmark.Result {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/observability/providers/"+providerID+"/upload-speed-test", nil)
	request.SetPathValue("provider_id", providerID)
	response := httptest.NewRecorder()
	srv.handleAPIProviderUploadSpeedTest(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("start speed test=%d %s", response.Code, response.Body.String())
	}
	row, err := srv.repos.ProviderUploadSpeed.Get(t.Context(), providerID)
	if err != nil || row == nil || row.ActiveTaskID == nil {
		t.Fatalf("active speed test=%+v err=%v", row, err)
	}
	return row
}

func TestAPIUploadSpeedSummaryUsesStoredTiersAndRetainedResults(t *testing.T) {
	srv, repos, states := seedUploadSpeedSetup(t, 4)
	source := &uploadSpeedSetupTierSource{}
	srv.observability = observability.NewService(observability.ServiceOptions{Store: repos.Observability, ApprovedProviders: source, RefreshInterval: time.Minute})
	if _, err := repos.Observability.RecordApprovedProviders(t.Context(), time.Now().Add(-10*time.Minute), []types.OnChainID{states[0].ProviderID, states[1].ProviderID}); err != nil {
		t.Fatal(err)
	}
	getSummary := func() uploadSpeedSummary {
		t.Helper()
		response := httptest.NewRecorder()
		srv.handleAPIUploadSpeedSummary(response, httptest.NewRequest(http.MethodGet, "/api/v1/observability/providers/upload-speed-summary", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("summary=%d %s", response.Code, response.Body.String())
		}
		var result uploadSpeedSummary
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	initial := getSummary()
	if initial.Ready || initial.CanProvisionDefault || !initial.CanTestRemaining || initial.Trusted.Untested != 2 || initial.Other.Untested != 2 || initial.RequiredCopies != 2 || initial.AutoScope != "trusted" {
		t.Fatalf("untested summary=%+v", initial)
	}
	first := startSetupSpeedTest(t, srv, "101")
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "101", *first.ActiveTaskID, providerbenchmark.StateSucceeded, providerbenchmark.SampleBytes, 1000, providerbenchmark.SampleBytes, ""); err != nil {
		t.Fatal(err)
	}
	second := startSetupSpeedTest(t, srv, "102")
	if err := repos.ProviderUploadSpeed.Finish(t.Context(), "102", *second.ActiveTaskID, providerbenchmark.StateFailed, providerbenchmark.SampleBytes, 0, 0, "timeout"); err != nil {
		t.Fatal(err)
	}
	startSetupSpeedTest(t, srv, "104")
	settled := getSummary()
	if !settled.Ready || settled.CanProvisionDefault || settled.SelectableProviders != 1 || settled.Trusted.Succeeded != 1 || settled.Trusted.Failed != 1 || settled.Other.Testing != 1 || settled.Other.Untested != 1 {
		t.Fatalf("settled summary=%+v", settled)
	}
	startSetupSpeedTest(t, srv, "101")
	srv.filecoinDefaultCopies = 1
	retesting := getSummary()
	if !retesting.Ready || retesting.CanProvisionDefault || retesting.SelectableProviders != 1 || retesting.Trusted.Testing != 1 || retesting.Trusted.Failed != 1 {
		t.Fatalf("retained result summary=%+v", retesting)
	}
	srv.anchorProviderTier = providerselect.TierNone
	if unrestricted := getSummary(); !unrestricted.Ready || !unrestricted.CanProvisionDefault {
		t.Fatalf("unrestricted summary=%+v", unrestricted)
	}
	srv.anchorProviderTier = providerselect.TierApproved
	if _, err := repos.Observability.RecordApprovedProviders(t.Context(), time.Now(), []types.OnChainID{states[0].ProviderID, states[1].ProviderID}); err != nil {
		t.Fatal(err)
	}
	if refreshed := getSummary(); !refreshed.Ready || !refreshed.CanProvisionDefault {
		t.Fatalf("fresh tier summary=%+v", refreshed)
	}
	if calls := source.calls.Load(); calls != 0 {
		t.Fatalf("summary refreshed expired tier %d times", calls)
	}
}

func TestAPIStartAllUploadSpeedTestsOrdersAndSkipsSettledOrActiveProviders(t *testing.T) {
	srv, repos, states := seedUploadSpeedSetup(t, 9)
	now := time.Now().UTC()
	if _, err := repos.Observability.RecordApprovedProviders(t.Context(), now, []types.OnChainID{states[0].ProviderID, states[1].ProviderID}); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Observability.RecordEndorsedProviders(t.Context(), now, []types.OnChainID{states[1].ProviderID}); err != nil {
		t.Fatal(err)
	}
	bucket := &model.Bucket{Name: "speed-bound", DefaultCopies: 1, MinimumDurableCopies: 1}
	if err := repos.Buckets.Create(t.Context(), bucket); err != nil {
		t.Fatal(err)
	}
	if _, err := repos.Contents.EnsureDataSetBinding(t.Context(), repository.EnsureDataSetBindingInput{BucketID: bucket.ID, ProviderID: states[2].ProviderID}); err != nil {
		t.Fatal(err)
	}
	for _, providerID := range []string{"105", "106", "108"} {
		row := startSetupSpeedTest(t, srv, providerID)
		state, duration, bps, code := providerbenchmark.StateSucceeded, int64(1000), providerbenchmark.SampleBytes, ""
		if providerID == "106" {
			state, duration, bps, code = providerbenchmark.StateFailed, 0, 0, "timeout"
		}
		if err := repos.ProviderUploadSpeed.Finish(t.Context(), providerID, *row.ActiveTaskID, state, providerbenchmark.SampleBytes, duration, bps, code); err != nil {
			t.Fatal(err)
		}
	}
	active := startSetupSpeedTest(t, srv, "107")
	states[7].Profile.ServiceURL = "https://changed-provider.example"
	states[7].ServiceURL = &states[7].Profile.ServiceURL
	states[8].Status = observability.StatusUnavailable
	if err := repos.Observability.ReplaceProviderStates(t.Context(), now.Add(time.Millisecond), states); err != nil {
		t.Fatal(err)
	}
	queue := func(want int) {
		t.Helper()
		response := httptest.NewRecorder()
		srv.handleAPIStartAllUploadSpeedTests(response, httptest.NewRequest(http.MethodPost, "/api/v1/observability/providers/upload-speed-tests", nil))
		var result struct {
			Queued int `json:"queued"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || response.Code != http.StatusAccepted || result.Queued != want {
			t.Fatalf("batch=%d %s err=%v, want %d queued", response.Code, response.Body.String(), err, want)
		}
	}
	queue(5)
	queue(0)
	page, err := repos.Tasks.List(t.Context(), repository.TaskListFilter{Type: model.TaskTypeProviderUploadSpeedTest, Status: model.TaskStatusPending, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	var ordered []string
	for _, task := range slices.Backward(page.Tasks) {
		if task.ID > *active.ActiveTaskID {
			ordered = append(ordered, task.SubjectKey)
		}
	}
	if !slices.Equal(ordered, []string{"102", "101", "103", "104", "108"}) {
		t.Fatalf("queued order=%v", ordered)
	}
	if len(page.Tasks) != 9 {
		t.Fatalf("task count=%d, want 9 without duplicate queue entries", len(page.Tasks))
	}
}
