package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/observability"
	"github.com/strahe/synaps3/internal/synapse"
)

func TestFilecoinReadinessRuntime(t *testing.T) {
	tests := []struct {
		name       string
		probe      *fakeFilecoinReadinessProbe
		wantStatus int
	}{
		{
			name:       "available",
			probe:      &fakeFilecoinReadinessProbe{runtime: readyFilecoinReadinessResult(synapse.ReadinessModeRuntime)},
			wantStatus: http.StatusOK,
		},
		{name: "unavailable", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := &Server{logger: testLogger()}
			if tt.probe != nil {
				srv.WithFilecoinReadiness(tt.probe)
			}
			req := httptest.NewRequest(http.MethodGet, "/api/v1/filecoin/readiness", nil)
			rr := httptest.NewRecorder()

			srv.handleAPIFilecoinReadiness(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantStatus == http.StatusOK {
				assertRuntimeReadinessResponse(t, rr.Body.Bytes())
			}
		})
	}
}

func TestFilecoinReadinessRuntimeIgnoresObservabilityState(t *testing.T) {
	checkedAt := time.Now().UTC()
	tests := []struct {
		name  string
		store *observabilityStateStore
	}{
		{name: "no state", store: &observabilityStateStore{}},
		{
			name: "provider and data set warnings",
			store: &observabilityStateStore{
				providers: observability.ProviderStatePage{Summary: observability.Summary{Total: 11, Degraded: 11}, LastCheckedAt: &checkedAt},
				dataSets:  observability.DataSetStatePage{Summary: observability.Summary{Total: 2, Unavailable: 1, Unknown: 1}, LastCheckedAt: &checkedAt},
			},
		},
		{
			name: "query failure",
			store: &observabilityStateStore{
				providerErr: errors.New("provider query failed"),
				dataSetErr:  errors.New("data set query failed"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := (&Server{logger: testLogger()}).
				WithFilecoinReadiness(&fakeFilecoinReadinessProbe{runtime: readyFilecoinReadinessResult(synapse.ReadinessModeRuntime)}).
				WithObservability(observability.NewService(observability.ServiceOptions{
					Store:           tt.store,
					RefreshInterval: time.Minute,
				}))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/filecoin/readiness", nil)
			rr := httptest.NewRecorder()

			srv.handleAPIFilecoinReadiness(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
			}
			var body synapse.ReadinessResult
			if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if body.Status != synapse.ReadinessStatusReady {
				t.Fatalf("readiness status = %s, want ready", body.Status)
			}
			assertReadinessCheckMissing(t, body.Checks, "observability_providers")
			assertReadinessCheckMissing(t, body.Checks, "observability_data_sets")
			if len(body.PartialErrors) > 0 {
				t.Fatalf("partial errors = %#v, want none", body.PartialErrors)
			}
		})
	}
}

func TestFilecoinReadinessPreflightRequiresJSONContentType(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	probe := &fakeFilecoinReadinessProbe{draft: readyFilecoinReadinessResult(synapse.ReadinessModeDraft)}

	tests := []struct {
		name        string
		addr        string
		contentType string
		wantStatus  int
	}{
		{name: "missing content type", addr: "127.0.0.1:9090", wantStatus: http.StatusBadRequest},
		{name: "json content type", addr: "0.0.0.0:9090", contentType: "application/json", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newSettingsAPITestServer(t, tt.addr, cfg, source).WithFilecoinReadiness(probe)
			req := newFilecoinPreflightRequest(`{"filecoin":{}}`)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			} else {
				req.Header.Del("Content-Type")
			}
			assertPreflightStatus(t, srv, req, tt.wantStatus)
		})
	}
}

func TestFilecoinReadinessPreflightRejectsUnknownPrivateKeyWithoutLeakingIt(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	probe := &fakeFilecoinReadinessProbe{draft: readyFilecoinReadinessResult(synapse.ReadinessModeDraft)}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source).WithFilecoinReadiness(probe)
	req := newFilecoinPreflightRequest(`{"filecoin":{"private_key":"raw-private-key"}}`)
	rr := assertPreflightStatus(t, srv, req, http.StatusBadRequest)

	if strings.Contains(rr.Body.String(), "raw-private-key") {
		t.Fatalf("response leaked private key: %s", rr.Body.String())
	}
	if probe.draftCalls != 0 {
		t.Fatalf("draft probe calls = %d, want 0", probe.draftCalls)
	}
}

func TestFilecoinReadinessPreflightUsesEffectiveConfigBaselineInSetupMode(t *testing.T) {
	effective := validSettingsConfig(t)
	effective.Filecoin.Network = "calibration"
	effective.Filecoin.RPCURL = "https://effective.example.invalid/rpc"
	effective.Filecoin.DefaultCopies = 2

	persisted := *effective
	persisted.Filecoin.Network = "mainnet"
	persisted.Filecoin.RPCURL = "https://persisted.example.invalid/rpc"
	persisted.Filecoin.DefaultCopies = 3

	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml"), Exists: true}
	if err := config.Save(source.Path, &persisted); err != nil {
		t.Fatalf("Save persisted config: %v", err)
	}
	probe := &fakeFilecoinReadinessProbe{draft: readyFilecoinReadinessResult(synapse.ReadinessModeDraft)}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", effective, source).WithFilecoinReadiness(probe)
	req := newFilecoinPreflightRequest(`{"filecoin":{"default_copies":1}}`)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	assertPreflightStatus(t, srv, req, http.StatusOK)

	if probe.draftCalls != 1 {
		t.Fatalf("draft probe calls = %d, want 1", probe.draftCalls)
	}
	if probe.draftConfig.Network != "calibration" || probe.draftConfig.RPCURL != "https://effective.example.invalid/rpc" {
		t.Fatalf("draft baseline = %#v, want effective config values", probe.draftConfig)
	}
	if probe.draftConfig.DefaultCopies != 1 {
		t.Fatalf("draft default copies = %d, want payload override", probe.draftConfig.DefaultCopies)
	}
}

func TestFilecoinReadinessPreflightRejectsEnvManagedAndInvalidDraftFields(t *testing.T) {
	tests := []struct {
		name    string
		envName string
		payload string
		want    string
	}{
		{
			name:    "env-managed",
			envName: "SYNAPS3_FILECOIN_RPC_URL",
			payload: `{"filecoin":{"rpc_url":"https://draft.example.invalid/rpc"}}`,
			want:    "filecoin.rpc_url",
		},
		{
			name:    "invalid draft",
			payload: `{"filecoin":{"default_copies":0}}`,
			want:    "filecoin.default_copies",
		},
		{
			name:    "invalid observability draft",
			payload: `{"filecoin":{"observability":{"timeout":"0s"}}}`,
			want:    "filecoin.observability.timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envName != "" {
				t.Setenv(tt.envName, "https://managed.example.invalid/rpc")
			}
			cfg := validSettingsConfig(t)
			source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
			probe := &fakeFilecoinReadinessProbe{draft: readyFilecoinReadinessResult(synapse.ReadinessModeDraft)}
			srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source).WithFilecoinReadiness(probe)
			rr := assertPreflightStatus(t, srv, newFilecoinPreflightRequest(tt.payload), http.StatusBadRequest)

			if !strings.Contains(rr.Body.String(), tt.want) {
				t.Fatalf("body should mention %q: %s", tt.want, rr.Body.String())
			}
			if probe.draftCalls != 0 {
				t.Fatalf("draft probe calls = %d, want 0", probe.draftCalls)
			}
		})
	}
}

func assertRuntimeReadinessResponse(t *testing.T, body []byte) {
	t.Helper()
	var resp synapse.ReadinessResult
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if resp.Mode != synapse.ReadinessModeRuntime || resp.Status != synapse.ReadinessStatusReady {
		t.Fatalf("readiness = %#v, want runtime ready", resp)
	}
	for _, removed := range []string{`"wallet"`, `"storage"`, `"estimate"`, `"severity"`} {
		if strings.Contains(string(body), removed) {
			t.Fatalf("runtime readiness response included removed field %s: %s", removed, body)
		}
	}
}

func newFilecoinPreflightRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/filecoin/readiness/preflight", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func assertPreflightStatus(t *testing.T, srv *Server, req *http.Request, want int) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	srv.handleAPIFilecoinReadinessPreflight(rr, req)
	if rr.Code != want {
		t.Fatalf("status = %d, want %d, body=%s", rr.Code, want, rr.Body.String())
	}
	return rr
}

func assertReadinessCheckMissing(t *testing.T, checks []synapse.ReadinessCheck, id string) {
	t.Helper()
	for _, check := range checks {
		if check.ID == id {
			t.Fatalf("check %s present: %#v", id, check)
		}
	}
}

func readyFilecoinReadinessResult(mode synapse.ReadinessMode) synapse.ReadinessResult {
	return synapse.ReadinessResult{
		Status:    synapse.ReadinessStatusReady,
		Mode:      mode,
		CheckedAt: time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC),
		Checks:    []synapse.ReadinessCheck{{ID: "network_match", Status: synapse.ReadinessStatusReady, Message: "ready"}},
	}
}

type fakeFilecoinReadinessProbe struct {
	runtime      synapse.ReadinessResult
	draft        synapse.ReadinessResult
	draftConfig  synapse.ReadinessConfig
	runtimeCalls int
	draftCalls   int
}

func (f *fakeFilecoinReadinessProbe) CheckRuntime(context.Context) synapse.ReadinessResult {
	f.runtimeCalls++
	return f.runtime
}

func (f *fakeFilecoinReadinessProbe) CheckDraft(_ context.Context, cfg synapse.ReadinessConfig) synapse.ReadinessResult {
	f.draftCalls++
	f.draftConfig = cfg
	return f.draft
}

type observabilityStateStore struct {
	providers   observability.ProviderStatePage
	dataSets    observability.DataSetStatePage
	providerErr error
	dataSetErr  error
}

func (s *observabilityStateStore) ReplaceProviderStates(context.Context, time.Time, []observability.ProviderState) error {
	return nil
}

func (s *observabilityStateStore) ListProviderStates(context.Context, observability.ListOptions) (observability.ProviderStatePage, error) {
	if s.providerErr != nil {
		return observability.ProviderStatePage{}, s.providerErr
	}
	return s.providers, nil
}

func (s *observabilityStateStore) ReplaceDataSetStates(context.Context, time.Time, []observability.DataSetState) error {
	return nil
}

func (s *observabilityStateStore) ListDataSetStates(context.Context, observability.ListOptions) (observability.DataSetStatePage, error) {
	if s.dataSetErr != nil {
		return observability.DataSetStatePage{}, s.dataSetErr
	}
	return s.dataSets, nil
}

func (s *observabilityStateStore) GetDataSetStatesByLocalIDs(context.Context, []int64) (map[int64]observability.DataSetState, error) {
	return nil, nil
}
