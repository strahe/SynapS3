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
	"github.com/strahe/synaps3/internal/db/repository"
	"github.com/strahe/synaps3/internal/model"
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

func TestFilecoinReadinessObservabilityNoStateWarns(t *testing.T) {
	srv := (&Server{logger: testLogger()}).
		WithFilecoinReadiness(&fakeFilecoinReadinessProbe{runtime: readyFilecoinReadinessResult(synapse.ReadinessModeRuntime)}).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store:           &observabilityAPIStore{},
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
	if body.Status != synapse.ReadinessStatusWarning {
		t.Fatalf("readiness status = %s, want warning", body.Status)
	}
	assertReadinessCheckStatus(t, body.Checks, "observability_providers", synapse.ReadinessStatusWarning)
	assertReadinessCheckStatus(t, body.Checks, "observability_data_sets", synapse.ReadinessStatusReady)
}

func TestFilecoinReadinessEmptyDataSetInventoryIsReady(t *testing.T) {
	checkedAt := time.Now().UTC()
	srv, repos := newBucketAPITestServer(t)
	srv.WithFilecoinReadiness(&fakeFilecoinReadinessProbe{runtime: readyFilecoinReadinessResult(synapse.ReadinessModeRuntime)}).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityReadinessStore{
				providers: observability.ProviderStatePage{
					Summary:       observability.Summary{Total: 1, Available: 1},
					LastCheckedAt: &checkedAt,
				},
				dataSets: observability.DataSetStatePage{},
			},
			LocalDataSets:   observability.LocalDataSetSourceFunc(testObservabilityLocalDataSets(repos)),
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
	assertReadinessCheckStatus(t, body.Checks, "observability_data_sets", synapse.ReadinessStatusReady)
	if body.Status != synapse.ReadinessStatusReady {
		t.Fatalf("readiness status = %s, want ready", body.Status)
	}
}

func TestFilecoinReadinessLocalDataSetWithoutStateWarns(t *testing.T) {
	checkedAt := time.Now().UTC()
	srv, repos := newBucketAPITestServer(t)
	ctx := context.Background()
	bucket := &model.Bucket{Name: "readiness-local-dataset", Status: model.BucketStatusActive}
	if err := repos.Buckets.Create(ctx, bucket); err != nil {
		t.Fatalf("Buckets.Create: %v", err)
	}
	upload, err := repos.Uploads.StartObjectUploadAttempt(ctx, repository.StartObjectUploadAttemptInput{
		BucketID:        bucket.ID,
		SourceVersionID: "01J000000000000000READYDS",
		ContentSize:     1,
		Checksum:        "checksum-readiness-local-dataset",
		RequestedCopies: 1,
	})
	if err != nil {
		t.Fatalf("StartObjectUploadAttempt: %v", err)
	}
	seedAdminCommittedCopies(t, repos, bucket.ID, upload.ID, "bafk2bzacereadinessdataset", []adminStorageCopySeed{
		{ProviderID: onChainID(t, "101"), DataSetID: onChainID(t, "1001"), PieceID: onChainIDPtr(t, "2001"), TransferMethod: model.StorageCopyTransferMethodIngress, RetrievalURL: "https://provider.example/1"},
	})
	srv.WithFilecoinReadiness(&fakeFilecoinReadinessProbe{runtime: readyFilecoinReadinessResult(synapse.ReadinessModeRuntime)}).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityReadinessStore{
				providers: observability.ProviderStatePage{
					Summary:       observability.Summary{Total: 1, Available: 1},
					LastCheckedAt: &checkedAt,
				},
				dataSets: observability.DataSetStatePage{},
			},
			LocalDataSets:   observability.LocalDataSetSourceFunc(testObservabilityLocalDataSets(repos)),
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
	check := assertReadinessCheckStatus(t, body.Checks, "observability_data_sets", synapse.ReadinessStatusWarning)
	if !strings.Contains(check.Message, "Some local data sets have no recorded health state.") {
		t.Fatalf("data set readiness message = %q, want missing local state detail", check.Message)
	}
}

func TestFilecoinReadinessProviderHTTPDegradedDoesNotWarn(t *testing.T) {
	checkedAt := time.Now().UTC()
	srv := (&Server{logger: testLogger()}).
		WithFilecoinReadiness(&fakeFilecoinReadinessProbe{runtime: readyFilecoinReadinessResult(synapse.ReadinessModeRuntime)}).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityReadinessStore{
				providers: observability.ProviderStatePage{
					Summary:       observability.Summary{Total: 11, Degraded: 11},
					LastCheckedAt: &checkedAt,
				},
				dataSets: observability.DataSetStatePage{
					Summary:       observability.Summary{Total: 1, Available: 1},
					LastCheckedAt: &checkedAt,
				},
			},
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
	assertReadinessCheckStatus(t, body.Checks, "observability_providers", synapse.ReadinessStatusReady)
}

func TestFilecoinReadinessObservabilityPartialErrorsAreSanitized(t *testing.T) {
	srv := (&Server{logger: testLogger()}).
		WithFilecoinReadiness(&fakeFilecoinReadinessProbe{runtime: readyFilecoinReadinessResult(synapse.ReadinessModeRuntime)}).
		WithObservability(observability.NewService(observability.ServiceOptions{
			Store: &observabilityReadinessStore{
				providerErr: errors.New("rpc failed with sensitive detail"),
			},
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
	if got := body.PartialErrors["observability_providers"]; got != "health query failed" {
		t.Fatalf("provider health partial error = %q, want sanitized message", got)
	}
}

func TestFinishReadinessResultPrioritizesWarningOverUnknown(t *testing.T) {
	result := finishReadinessResult(synapse.ReadinessResult{
		Checks: []synapse.ReadinessCheck{
			{ID: "unknown", Status: synapse.ReadinessStatusUnknown},
			{ID: "warning", Status: synapse.ReadinessStatusWarning},
		},
	})
	if result.Status != synapse.ReadinessStatusWarning {
		t.Fatalf("Status = %q, want warning", result.Status)
	}
}

func TestFilecoinReadinessPreflightRequiresLoopbackContentTypeAndWriteHeader(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	probe := &fakeFilecoinReadinessProbe{draft: readyFilecoinReadinessResult(synapse.ReadinessModeDraft)}

	tests := []struct {
		name        string
		addr        string
		contentType string
		writeHeader string
		wantStatus  int
	}{
		{name: "non-loopback", addr: "0.0.0.0:9090", contentType: "application/json", writeHeader: "1", wantStatus: http.StatusForbidden},
		{name: "missing content type", addr: "127.0.0.1:9090", writeHeader: "1", wantStatus: http.StatusBadRequest},
		{name: "missing write header", addr: "127.0.0.1:9090", contentType: "application/json", wantStatus: http.StatusBadRequest},
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
			if tt.writeHeader != "" {
				req.Header.Set(settingsWriteHeader, tt.writeHeader)
			} else {
				req.Header.Del(settingsWriteHeader)
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
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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

func assertReadinessCheckStatus(t *testing.T, checks []synapse.ReadinessCheck, id string, want synapse.ReadinessStatus) synapse.ReadinessCheck {
	t.Helper()
	for _, check := range checks {
		if check.ID == id {
			if check.Status != want {
				t.Fatalf("check %s status = %s, want %s", id, check.Status, want)
			}
			return check
		}
	}
	t.Fatalf("check %s missing in %#v", id, checks)
	return synapse.ReadinessCheck{}
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
	runtime     synapse.ReadinessResult
	draft       synapse.ReadinessResult
	draftConfig synapse.ReadinessConfig
	draftCalls  int
}

func (f *fakeFilecoinReadinessProbe) CheckRuntime(context.Context) synapse.ReadinessResult {
	return f.runtime
}

func (f *fakeFilecoinReadinessProbe) CheckDraft(_ context.Context, cfg synapse.ReadinessConfig) synapse.ReadinessResult {
	f.draftCalls++
	f.draftConfig = cfg
	return f.draft
}

type observabilityReadinessStore struct {
	providers   observability.ProviderStatePage
	dataSets    observability.DataSetStatePage
	providerErr error
	dataSetErr  error
}

func (s *observabilityReadinessStore) ReplaceProviderStates(context.Context, time.Time, []observability.ProviderState) error {
	return nil
}

func (s *observabilityReadinessStore) ListProviderStates(context.Context, observability.ListOptions) (observability.ProviderStatePage, error) {
	if s.providerErr != nil {
		return observability.ProviderStatePage{}, s.providerErr
	}
	return s.providers, nil
}

func (s *observabilityReadinessStore) ReplaceDataSetStates(context.Context, time.Time, []observability.DataSetState) error {
	return nil
}

func (s *observabilityReadinessStore) ListDataSetStates(context.Context, observability.ListOptions) (observability.DataSetStatePage, error) {
	if s.dataSetErr != nil {
		return observability.DataSetStatePage{}, s.dataSetErr
	}
	return s.dataSets, nil
}

func (s *observabilityReadinessStore) GetDataSetStatesByLocalIDs(context.Context, []int64) (map[int64]observability.DataSetState, error) {
	return nil, nil
}

func testObservabilityLocalDataSets(repos *repository.Repositories) func(context.Context) ([]observability.LocalDataSet, error) {
	return func(ctx context.Context) ([]observability.LocalDataSet, error) {
		summaries, err := repos.Uploads.ListDataSetSummaries(ctx, 0)
		if err != nil {
			return nil, err
		}
		out := make([]observability.LocalDataSet, 0, len(summaries))
		for _, summary := range summaries {
			out = append(out, observability.LocalDataSet{
				ID:              summary.ID,
				BucketID:        summary.BucketID,
				BucketName:      summary.BucketName,
				CopyIndex:       summary.CopyIndex,
				ProviderID:      summary.ProviderID,
				DataSetID:       summary.DataSetID,
				ClientDataSetID: summary.ClientDataSetID,
				Status:          summary.Status,
			})
		}
		return out, nil
	}
}
