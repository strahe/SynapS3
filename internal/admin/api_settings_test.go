package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/strahe/synaps3/internal/config"
)

func newSettingsAPITestServer(t *testing.T, addr string, cfg *config.Config, source config.Source) *Server {
	t.Helper()

	svc, err := NewSettingsService(cfg, source)
	if err != nil {
		t.Fatalf("NewSettingsService: %v", err)
	}

	srv := NewSetup(addr, svc, testLogger())
	return srv
}

func TestSettingsGETRedactsSecretsAndReportsManualStatus(t *testing.T) {
	cfg := validSettingsConfig(t)
	cfg.Filecoin.PrivateKey = "private-key-value"
	cfg.Database.DSN = "postgres://synaps3:db-password@example.invalid:5432/synaps3?sslmode=disable"
	cfg.Admin.Addr = "10.20.30.40:19090"

	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.toml")})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIGetSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, leaked := range []string{"private-key-value", "db-password", cfg.Database.DSN, cfg.Admin.Addr} {
		if strings.Contains(body, leaked) {
			t.Fatalf("settings response leaked %q: %s", leaked, body)
		}
	}

	var resp settingsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if resp.Mode != "ready" {
		t.Fatalf("Mode = %q, want ready", resp.Mode)
	}
	if !resp.Secrets.FilecoinPrivateKeyConfigured {
		t.Fatalf("secret status = %#v, want filecoin private key configured", resp.Secrets)
	}
	if resp.Config.S3.Region != cfg.S3.Region {
		t.Fatalf("S3 region = %q, want %q", resp.Config.S3.Region, cfg.S3.Region)
	}
	if !resp.Manual.Database.DSNConfigured || resp.Manual.Database.DSN != "configured" {
		t.Fatalf("database DSN status = %#v, want configured without raw DSN", resp.Manual.Database)
	}
}

func TestSettingsGETIncludesFieldMetadata(t *testing.T) {
	cfg := validSettingsConfig(t)
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.toml")})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIGetSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var resp settingsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	tests := []struct {
		field  string
		env    string
		secret bool
	}{
		{field: "server.port", env: "SYNAPS3_SERVER_PORT"},
		{field: "s3.region", env: "SYNAPS3_S3_REGION"},
		{field: "filecoin.private_key", env: "SYNAPS3_FILECOIN_PRIVATE_KEY", secret: true},
		{field: "filecoin.default_copies", env: "SYNAPS3_FILECOIN_DEFAULT_COPIES"},
		{field: "cache.dir", env: "SYNAPS3_CACHE_DIR"},
	}
	for _, tt := range tests {
		meta, ok := resp.Metadata[tt.field]
		if !ok {
			t.Fatalf("metadata missing %q in %#v", tt.field, resp.Metadata)
		}
		if meta.Env != tt.env {
			t.Fatalf("metadata[%q].Env = %q, want %q", tt.field, meta.Env, tt.env)
		}
		if meta.Secret != tt.secret {
			t.Fatalf("metadata[%q].Secret = %v, want %v", tt.field, meta.Secret, tt.secret)
		}
		if strings.TrimSpace(meta.Label) == "" || strings.TrimSpace(meta.Description) == "" {
			t.Fatalf("metadata[%q] must include label and description: %#v", tt.field, meta)
		}
	}
}

func TestSettingsGETReportsS3UsersUnavailableInSetupMode(t *testing.T) {
	cfg := validSettingsConfig(t)
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.toml")})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIGetSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var resp settingsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if resp.S3Users.Available || strings.TrimSpace(resp.S3Users.Reason) == "" {
		t.Fatalf("S3Users = %#v, want unavailable with reason", resp.S3Users)
	}
}

func TestSettingsGETReportsRuntimeObservability(t *testing.T) {
	cfg := validSettingsConfig(t)
	setupSrv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.toml")})
	setupResp := getSettingsResponse(t, setupSrv)
	if setupResp.RuntimeAvailable {
		t.Fatalf("setup runtime_available = true, want false")
	}

	runtimeSrv := &Server{
		addr:     "127.0.0.1:9090",
		settings: setupSrv.settings,
		logger:   testLogger(),
	}
	runtimeResp := getSettingsResponse(t, runtimeSrv)
	if !runtimeResp.RuntimeAvailable {
		t.Fatalf("runtime runtime_available = false, want true")
	}
}

func TestSettingsGETIncludesFilecoinRPCDefaults(t *testing.T) {
	cfg := validSettingsConfig(t)
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.toml")})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIGetSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var resp settingsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for network, want := range config.DefaultFilecoinRPCURLs() {
		if got := resp.Defaults.FilecoinRPCURLs[network]; got != want {
			t.Fatalf("default rpc for %s = %q, want %q", network, got, want)
		}
	}
}

func getSettingsResponse(t *testing.T, srv *Server) settingsResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIGetSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var resp settingsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	return resp
}

func TestSettingsGETReportsManualSecretEnvSources(t *testing.T) {
	t.Setenv("SYNAPS3_FILECOIN_PRIVATE_KEY", "env-private")

	cfg := validSettingsConfig(t)
	cfg.Filecoin.PrivateKey = "env-private"

	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.toml")})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIGetSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var resp settingsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if resp.Manual.Filecoin.Env != "SYNAPS3_FILECOIN_PRIVATE_KEY" {
		t.Fatalf("filecoin env = %q, want SYNAPS3_FILECOIN_PRIVATE_KEY", resp.Manual.Filecoin.Env)
	}
}

func TestSettingsPUTRejectsSecretFieldsAndDoesNotPersistThem(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"s3":{"secret_key":"leak"}}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
	if data, err := os.ReadFile(source.Path); err == nil && strings.Contains(string(data), "leak") {
		t.Fatalf("secret field was persisted: %s", string(data))
	}
}

func TestSettingsPUTUsesAdminAuthAndRequiresJSONContentType(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	srv := newSettingsAPITestServer(t, "0.0.0.0:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":8}}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}

	srv = newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)
	req = httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":8}}`))
	req.Header.Set("Content-Type", "application/jsonp")
	rr = httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status with invalid media type = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
}

func TestSettingsValidateReportsDraftErrorsWithoutPersisting(t *testing.T) {
	cfg := validSettingsConfig(t)
	cfg.Server.Port = ":8080"
	cfg.Server.MaxConnections = 100
	cfg.Server.MaxRequests = 10
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	if err := config.Save(source.Path, cfg); err != nil {
		t.Fatalf("Save initial config: %v", err)
	}
	source.Exists = true
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)
	initialData, err := os.ReadFile(source.Path)
	if err != nil {
		t.Fatalf("ReadFile initial config: %v", err)
	}

	resp := postSettingsValidate(t, srv, `{"server":{"port":":80801"}}`)
	if !hasFieldError(resp.ValidationErrors, "server.port") {
		t.Fatalf("validation_errors = %#v, want server.port error", resp.ValidationErrors)
	}
	data, err := os.ReadFile(source.Path)
	if err != nil {
		t.Fatalf("ReadFile after invalid validate: %v", err)
	}
	if string(data) != string(initialData) {
		t.Fatalf("validate wrote config:\n%s", string(data))
	}

	resp = postSettingsValidate(t, srv, `{"server":{"port":":8080"}}`)
	if len(resp.ValidationErrors) != 0 {
		t.Fatalf("validation_errors = %#v, want none", resp.ValidationErrors)
	}
	if !strings.Contains(resp.Raw, `"validation_errors":[]`) {
		t.Fatalf("valid response should include empty validation_errors array: %s", resp.Raw)
	}
	data, err = os.ReadFile(source.Path)
	if err != nil {
		t.Fatalf("ReadFile after valid validate: %v", err)
	}
	if string(data) != string(initialData) {
		t.Fatalf("valid validate wrote config:\n%s", string(data))
	}
	if got := getSettingsResponse(t, srv).RestartRequired; got {
		t.Fatal("RestartRequired = true after validate, want false")
	}
}

func TestSettingsValidateReportsDraftValidationRules(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		field   string
		envName string
	}{
		{
			name:    "cross field server concurrency",
			payload: `{"server":{"max_connections":10,"max_requests":11}}`,
			field:   "server.max_requests",
		},
		{
			name:    "duration parse",
			payload: `{"worker":{"upload":{"poll_interval":"not-a-duration"}}}`,
			field:   "worker.upload.poll_interval",
		},
		{
			name:    "env managed field",
			payload: `{"server":{"port":":8088"}}`,
			field:   "server.port",
			envName: "SYNAPS3_SERVER_PORT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envName != "" {
				t.Setenv(tt.envName, ":9999")
			}
			cfg := validSettingsConfig(t)
			source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
			if err := config.Save(source.Path, cfg); err != nil {
				t.Fatalf("Save initial config: %v", err)
			}
			source.Exists = true
			srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

			resp := postSettingsValidate(t, srv, tt.payload)

			if !hasFieldError(resp.ValidationErrors, tt.field) {
				t.Fatalf("validation_errors = %#v, want %s", resp.ValidationErrors, tt.field)
			}
			if tt.envName != "" && !strings.Contains(resp.Raw, tt.envName) {
				t.Fatalf("response should mention %s: %s", tt.envName, resp.Raw)
			}
		})
	}
}

func TestSettingsValidateRequiresValidJSON(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}

	tests := []struct {
		name        string
		addr        string
		contentType string
		body        string
		wantStatus  int
	}{
		{
			name:        "invalid content type",
			addr:        "127.0.0.1:9090",
			contentType: "application/jsonp",
			body:        `{"cache":{"max_size_gb":8}}`,
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid json",
			addr:        "127.0.0.1:9090",
			contentType: "application/json",
			body:        `{"cache":{"max_size_gb":8}`,
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newSettingsAPITestServer(t, tt.addr, cfg, source)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/validate", strings.NewReader(tt.body))
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			rr := httptest.NewRecorder()

			srv.handleAPIValidateSettings(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
		})
	}
}

func TestSettingsPUTPersistsNonSecretFieldsAndReturnsRestartRequired(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	if err := config.Save(source.Path, cfg); err != nil {
		t.Fatalf("Save initial config: %v", err)
	}
	source.Exists = true
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{
		"server":{"port":":8088"},
		"filecoin":{"network":"mainnet","with_cdn":true,"default_copies":3},
		"cache":{"max_size_gb":8},
		"worker":{"upload":{"poll_interval":"9s"}},
		"logging":{"format":"text","s3_access":{"enabled":false,"level":"debug"}}
	}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var resp settingsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !resp.RestartRequired {
		t.Fatal("RestartRequired = false, want true")
	}
	if resp.Config.Server.Port != ":8088" || resp.Config.Cache.MaxSizeGB != 8 || resp.Config.Filecoin.DefaultCopies != 3 {
		t.Fatalf("updated config = %#v", resp.Config)
	}

	loaded, err := config.Load(source.Path)
	if err != nil {
		t.Fatalf("Load(saved): %v", err)
	}
	if loaded.Server.Port != ":8088" {
		t.Fatalf("saved server.port = %q, want :8088", loaded.Server.Port)
	}
	if loaded.Worker.Upload.PollInterval.String() != "9s" {
		t.Fatalf("saved worker.upload.poll_interval = %s, want 9s", loaded.Worker.Upload.PollInterval)
	}
	if loaded.Filecoin.DefaultCopies != 3 {
		t.Fatalf("saved filecoin.default_copies = %d, want 3", loaded.Filecoin.DefaultCopies)
	}
	if loaded.Logging.S3Access.Enabled || loaded.Logging.S3Access.Level != "debug" {
		t.Fatalf("saved logging.s3_access = %#v, want disabled debug", loaded.Logging.S3Access)
	}
}

func TestSettingsPUTPreservesManualFieldsChangedOnDiskAfterServiceStart(t *testing.T) {
	cfg := validSettingsConfig(t)
	cfg.Filecoin.PrivateKey = "old-private-key"
	cfg.Database.DSN = "postgres://synaps3:old-password@example.invalid:5432/synaps3"
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	if err := config.Save(source.Path, cfg); err != nil {
		t.Fatalf("Save initial config: %v", err)
	}
	source.Exists = true
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	manual := *cfg
	manual.Filecoin.PrivateKey = "new-private-key"
	manual.Database.DSN = "postgres://synaps3:new-password@example.invalid:5432/synaps3"
	if err := config.Save(source.Path, &manual); err != nil {
		t.Fatalf("Save manual config: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":9}}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	loaded, err := config.LoadFile(source.Path)
	if err != nil {
		t.Fatalf("LoadFile(saved): %v", err)
	}
	if loaded.Cache.MaxSizeGB != 9 {
		t.Fatalf("cache.max_size_gb = %d, want 9", loaded.Cache.MaxSizeGB)
	}
	if loaded.Filecoin.PrivateKey != manual.Filecoin.PrivateKey {
		t.Fatalf("filecoin.private_key = %q, want preserved manual value", loaded.Filecoin.PrivateKey)
	}
	if loaded.Database.DSN != manual.Database.DSN {
		t.Fatalf("database.dsn = %q, want preserved manual value", loaded.Database.DSN)
	}
}

func TestSettingsPUTDoesNotMaterializeMissingManualDatabaseDSN(t *testing.T) {
	t.Setenv("SYNAPS3_DATABASE_DSN", "postgres://synaps3:env-password@example.invalid:5432/synaps3")

	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml"), Exists: true}
	initial := []byte(`
[s3]
region = "us-east-1"

[filecoin]
private_key = "manual-filecoin-private-key"

[database]
driver = "sqlite"

[cache]
dir = "/tmp/synaps3-cache"
max_size_gb = 7
`)
	if err := os.WriteFile(source.Path, initial, 0o600); err != nil {
		t.Fatalf("WriteFile initial config: %v", err)
	}
	cfg, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource: %v", err)
	}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":8}}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	data, err := os.ReadFile(source.Path)
	if err != nil {
		t.Fatalf("ReadFile saved config: %v", err)
	}
	text := string(data)
	if containsEnabledTOMLKey(text, "dsn") {
		t.Fatalf("settings PUT materialized database.dsn in TOML:\n%s", text)
	}
}

func TestSettingsPUTUsesRuntimeDefaultForOmittedCacheDir(t *testing.T) {
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml"), Exists: true}
	initial := []byte(`
[s3]
region = "us-east-1"

[filecoin]
private_key = "manual-filecoin-private-key"
`)
	if err := os.WriteFile(source.Path, initial, 0o600); err != nil {
		t.Fatalf("WriteFile initial config: %v", err)
	}
	cfg, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource: %v", err)
	}
	if strings.TrimSpace(cfg.Cache.Dir) == "" {
		t.Fatal("runtime cache dir default was not applied")
	}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"logging":{"format":"text"}}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	data, err := os.ReadFile(source.Path)
	if err != nil {
		t.Fatalf("ReadFile saved config: %v", err)
	}
	text := string(data)
	if containsEnabledTOMLKey(text, "dir") {
		t.Fatalf("settings PUT materialized cache.dir in TOML:\n%s", text)
	}
	restarted, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource(saved): %v", err)
	}
	if strings.TrimSpace(restarted.Cache.Dir) == "" {
		t.Fatal("cache.dir default was lost after restart")
	}
	if restarted.Logging.Format != "text" {
		t.Fatalf("logging.format = %q, want text", restarted.Logging.Format)
	}
}

func TestSettingsLifecycleFallbackConfigEnvPrecedenceAndManualSecretPreservation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(t.TempDir())

	source, err := config.ResolveSource("config.toml", false)
	if err != nil {
		t.Fatalf("ResolveSource: %v", err)
	}
	if source.Path != filepath.Join(home, ".synaps3", "config.toml") {
		t.Fatalf("source path = %q, want app data config", source.Path)
	}
	cfg, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource: %v", err)
	}
	for _, want := range []string{"filecoin.private_key"} {
		if !hasFieldError(cfg.FieldValidationErrors(), want) {
			t.Fatalf("default config validation errors = %#v, want missing manual credential %q", cfg.FieldValidationErrors(), want)
		}
	}

	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{
		"s3":{"region":"ap-southeast-1"},
		"cache":{"max_size_gb":12}
	}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("initial PUT status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	persisted, err := config.LoadFile(source.Path)
	if err != nil {
		t.Fatalf("LoadFile after initial PUT: %v", err)
	}
	if persisted.S3.Region != "ap-southeast-1" || persisted.Cache.MaxSizeGB != 12 {
		t.Fatalf("persisted settings = %#v", persisted)
	}
	if persisted.Filecoin.PrivateKey != "" {
		t.Fatalf("manual secrets should remain empty until edited outside the browser: %#v", persisted.Filecoin)
	}

	persisted.Filecoin.PrivateKey = "manual-filecoin-private-key"
	if err := config.Save(source.Path, persisted); err != nil {
		t.Fatalf("Save manual secrets: %v", err)
	}

	t.Setenv("SYNAPS3_S3_REGION", "eu-west-1")
	restarted, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource after restart: %v", err)
	}
	if restarted.S3.Region != "eu-west-1" {
		t.Fatalf("env S3 region = %q, want eu-west-1", restarted.S3.Region)
	}
	if restarted.Filecoin.PrivateKey != "manual-filecoin-private-key" {
		t.Fatalf("manual secrets were not loaded after restart: %#v", restarted.Filecoin)
	}

	srv = newSettingsAPITestServer(t, "127.0.0.1:9090", restarted, source)
	req = httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":13}}`))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("second PUT status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	persisted, err = config.LoadFile(source.Path)
	if err != nil {
		t.Fatalf("LoadFile after second PUT: %v", err)
	}
	if persisted.Cache.MaxSizeGB != 13 {
		t.Fatalf("cache.max_size_gb = %d, want 13", persisted.Cache.MaxSizeGB)
	}
	if persisted.Filecoin.PrivateKey != "manual-filecoin-private-key" {
		t.Fatalf("manual secrets were overwritten: %#v", persisted.Filecoin)
	}
}

func TestSettingsPUTRejectsEnvManagedFieldChanges(t *testing.T) {
	tests := []struct {
		name     string
		envName  string
		payload  string
		field    string
		envValue string
	}{
		{name: "server port", envName: "SYNAPS3_SERVER_PORT", payload: `{"server":{"port":":8088"}}`, field: "server.port"},
		{name: "server max connections", envName: "SYNAPS3_SERVER_MAX_CONNECTIONS", payload: `{"server":{"max_connections":10}}`, field: "server.max_connections"},
		{name: "server max requests", envName: "SYNAPS3_SERVER_MAX_REQUESTS", payload: `{"server":{"max_requests":10}}`, field: "server.max_requests"},
		{name: "server tls enabled", envName: "SYNAPS3_SERVER_TLS_ENABLED", payload: `{"server":{"tls":{"enabled":true}}}`, field: "server.tls.enabled"},
		{name: "server tls cert file", envName: "SYNAPS3_SERVER_TLS_CERT_FILE", payload: `{"server":{"tls":{"cert_file":"/tmp/cert.pem"}}}`, field: "server.tls.cert_file"},
		{name: "server tls key file", envName: "SYNAPS3_SERVER_TLS_KEY_FILE", payload: `{"server":{"tls":{"key_file":"/tmp/key.pem"}}}`, field: "server.tls.key_file"},
		{name: "s3 region", envName: "SYNAPS3_S3_REGION", payload: `{"s3":{"region":"eu-west-1"}}`, field: "s3.region"},
		{name: "filecoin network", envName: "SYNAPS3_FILECOIN_NETWORK", payload: `{"filecoin":{"network":"mainnet"}}`, field: "filecoin.network"},
		{name: "filecoin rpc url", envName: "SYNAPS3_FILECOIN_RPC_URL", payload: `{"filecoin":{"rpc_url":"https://rpc.example.invalid"}}`, field: "filecoin.rpc_url"},
		{name: "filecoin source", envName: "SYNAPS3_FILECOIN_SOURCE", payload: `{"filecoin":{"source":"other"}}`, field: "filecoin.source"},
		{name: "filecoin cdn", envName: "SYNAPS3_FILECOIN_WITH_CDN", payload: `{"filecoin":{"with_cdn":true}}`, field: "filecoin.with_cdn"},
		{name: "filecoin private networks", envName: "SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS", payload: `{"filecoin":{"allow_private_networks":true}}`, field: "filecoin.allow_private_networks"},
		{name: "filecoin default copies", envName: "SYNAPS3_FILECOIN_DEFAULT_COPIES", payload: `{"filecoin":{"default_copies":3}}`, field: "filecoin.default_copies"},
		{name: "cache dir", envName: "SYNAPS3_CACHE_DIR", payload: `{"cache":{"dir":"/tmp/cache"}}`, field: "cache.dir"},
		{name: "cache max size", envName: "SYNAPS3_CACHE_MAX_SIZE_GB", payload: `{"cache":{"max_size_gb":8}}`, field: "cache.max_size_gb"},
		{name: "cache eviction policy", envName: "SYNAPS3_CACHE_EVICTION_POLICY", payload: `{"cache":{"eviction_policy":"manual"}}`, field: "cache.eviction_policy"},
		{name: "upload concurrency", envName: "SYNAPS3_WORKER_UPLOAD_CONCURRENCY", payload: `{"worker":{"upload":{"concurrency":2}}}`, field: "worker.upload.concurrency"},
		{name: "upload poll interval", envName: "SYNAPS3_WORKER_UPLOAD_POLL_INTERVAL", payload: `{"worker":{"upload":{"poll_interval":"9s"}}}`, field: "worker.upload.poll_interval"},
		{name: "upload max retries", envName: "SYNAPS3_WORKER_UPLOAD_MAX_RETRIES", payload: `{"worker":{"upload":{"max_retries":9}}}`, field: "worker.upload.max_retries"},
		{name: "evictor concurrency", envName: "SYNAPS3_WORKER_EVICTOR_CONCURRENCY", payload: `{"worker":{"evictor":{"concurrency":2}}}`, field: "worker.evictor.concurrency"},
		{name: "evictor poll interval", envName: "SYNAPS3_WORKER_EVICTOR_POLL_INTERVAL", payload: `{"worker":{"evictor":{"poll_interval":"2m"}}}`, field: "worker.evictor.poll_interval"},
		{name: "evictor max retries", envName: "SYNAPS3_WORKER_EVICTOR_MAX_RETRIES", payload: `{"worker":{"evictor":{"max_retries":4}}}`, field: "worker.evictor.max_retries"},
		{name: "logging level", envName: "SYNAPS3_LOGGING_LEVEL", payload: `{"logging":{"level":"debug"}}`, field: "logging.level"},
		{name: "logging format", envName: "SYNAPS3_LOGGING_FORMAT", payload: `{"logging":{"format":"text"}}`, field: "logging.format"},
		{name: "s3 access logging enabled", envName: "SYNAPS3_LOGGING_S3_ACCESS_ENABLED", payload: `{"logging":{"s3_access":{"enabled":false}}}`, field: "logging.s3_access.enabled", envValue: "true"},
		{name: "s3 access logging level", envName: "SYNAPS3_LOGGING_S3_ACCESS_LEVEL", payload: `{"logging":{"s3_access":{"level":"debug"}}}`, field: "logging.s3_access.level", envValue: "info"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			envValue := tt.envValue
			if envValue == "" {
				envValue = "managed"
			}
			t.Setenv(tt.envName, envValue)

			cfg := validSettingsConfig(t)
			source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
			srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

			req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(tt.payload))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()

			srv.handleAPIUpdateSettings(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
			}
			body := rr.Body.String()
			if !strings.Contains(body, tt.field) || !strings.Contains(body, tt.envName) {
				t.Fatalf("body should mention %s and %s: %s", tt.field, tt.envName, body)
			}
		})
	}
}

func TestSettingsPUTRejectsInvalidEditableFields(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{
		"server":{"port":"not-a-port"},
		"s3":{"region":""},
		"filecoin":{"rpc_url":"ftp://example.invalid/rpc","source":"","default_copies":0},
		"worker":{"upload":{"max_retries":-1}},
		"logging":{"level":"verbose","format":"xml","s3_access":{"level":"verbose"}}
	}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{
		"server.port",
		"s3.region",
		"filecoin.rpc_url",
		"filecoin.source",
		"filecoin.default_copies",
		"worker.upload.max_retries",
		"logging.level",
		"logging.format",
		"logging.s3_access.level",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q: %s", want, body)
		}
	}
}

func TestSettingsPUTRejectsTrailingJSON(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.toml")}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":8}} {}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
}

func validSettingsConfig(t *testing.T) *config.Config {
	t.Helper()

	cfg, err := config.DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig: %v", err)
	}
	cfg.Filecoin.PrivateKey = "filecoin-private-key"
	cfg.Admin.Auth.PasswordHash = "$2a$10$7EqJtq98hPqEX7fNZaFWoOhi6r4aIvJrDWHtqK4V0GaQYe7TzTx6W"
	cfg.Admin.Auth.SessionSecret = "admin-session-secret-with-enough-entropy"
	return cfg
}

func hasFieldError(errs []config.FieldError, field string) bool {
	for _, err := range errs {
		if err.Field == field {
			return true
		}
	}
	return false
}

func containsEnabledTOMLKey(text, key string) bool {
	prefix := key + " ="
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), prefix) {
			return true
		}
	}
	return false
}

type settingsValidateTestResponse struct {
	ValidationErrors []config.FieldError `json:"validation_errors"`
	Raw              string
}

func postSettingsValidate(t *testing.T, srv *Server, payload string) settingsValidateTestResponse {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/validate", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.handleAPIValidateSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var resp settingsValidateTestResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	resp.Raw = rr.Body.String()
	return resp
}
