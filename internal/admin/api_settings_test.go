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
	cfg.S3.AccessKey = "access-value"
	cfg.S3.SecretKey = "secret-value"
	cfg.Filecoin.PrivateKey = "private-key-value"
	cfg.Database.DSN = "postgres://synaps3:db-password@example.invalid:5432/synaps3?sslmode=disable"
	cfg.Admin.Addr = "10.20.30.40:19090"

	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil)
	rr := httptest.NewRecorder()

	srv.handleAPIGetSettings(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, leaked := range []string{"access-value", "secret-value", "private-key-value", "db-password", cfg.Database.DSN, cfg.Admin.Addr} {
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
	if !resp.Secrets.S3AccessKeyConfigured || !resp.Secrets.S3SecretKeyConfigured || !resp.Secrets.FilecoinPrivateKeyConfigured {
		t.Fatalf("secret status = %#v, want all configured", resp.Secrets)
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
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")})
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
		{field: "s3.access_key", env: "SYNAPS3_S3_ACCESS_KEY", secret: true},
		{field: "s3.secret_key", env: "SYNAPS3_S3_SECRET_KEY", secret: true},
		{field: "s3.iam_dir", env: "SYNAPS3_S3_IAM_DIR"},
		{field: "filecoin.private_key", env: "SYNAPS3_FILECOIN_PRIVATE_KEY", secret: true},
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
	if !resp.Metadata["s3.iam_dir"].Editable {
		t.Fatal("metadata[s3.iam_dir].Editable = false, want true")
	}
}

func TestSettingsGETReportsS3UsersUnavailableInSetupMode(t *testing.T) {
	cfg := validSettingsConfig(t)
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")})
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

func TestSettingsGETIncludesFilecoinRPCDefaults(t *testing.T) {
	cfg := validSettingsConfig(t)
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")})
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

func TestSettingsGETReportsManualSecretEnvSources(t *testing.T) {
	t.Setenv("SYNAPS3_S3_ACCESS_KEY", "env-access")
	t.Setenv("SYNAPS3_S3_SECRET_KEY", "env-secret")
	t.Setenv("SYNAPS3_FILECOIN_PRIVATE_KEY", "env-private")

	cfg := validSettingsConfig(t)
	cfg.S3.AccessKey = "env-access"
	cfg.S3.SecretKey = "env-secret"
	cfg.Filecoin.PrivateKey = "env-private"

	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")})
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
	if resp.Manual.S3Access.Env != "SYNAPS3_S3_ACCESS_KEY" {
		t.Fatalf("s3 access env = %q, want SYNAPS3_S3_ACCESS_KEY", resp.Manual.S3Access.Env)
	}
	if resp.Manual.S3Secret.Env != "SYNAPS3_S3_SECRET_KEY" {
		t.Fatalf("s3 secret env = %q, want SYNAPS3_S3_SECRET_KEY", resp.Manual.S3Secret.Env)
	}
	if resp.Manual.Filecoin.Env != "SYNAPS3_FILECOIN_PRIVATE_KEY" {
		t.Fatalf("filecoin env = %q, want SYNAPS3_FILECOIN_PRIVATE_KEY", resp.Manual.Filecoin.Env)
	}
}

func TestSettingsGenerateS3CredentialsPersistsAndReturnsPlaintextOnce(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
	if err := config.Save(source.Path, cfg); err != nil {
		t.Fatalf("Save initial config: %v", err)
	}
	source.Exists = true
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/s3-credentials", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()

	srv.handleAPIGenerateS3Credentials(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var resp settingsS3CredentialsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if strings.TrimSpace(resp.Credentials.AccessKey) == "" || strings.TrimSpace(resp.Credentials.SecretKey) == "" {
		t.Fatalf("generated credentials must be returned once: %#v", resp.Credentials)
	}
	if resp.Credentials.AccessKey == cfg.S3.AccessKey || resp.Credentials.SecretKey == cfg.S3.SecretKey {
		t.Fatalf("generated credentials should rotate existing credentials")
	}
	if !resp.Settings.RestartRequired {
		t.Fatal("RestartRequired = false, want true")
	}
	if !resp.Settings.Secrets.S3AccessKeyConfigured || !resp.Settings.Secrets.S3SecretKeyConfigured {
		t.Fatalf("secret status = %#v, want S3 credentials configured", resp.Settings.Secrets)
	}

	loaded, err := config.LoadFile(source.Path)
	if err != nil {
		t.Fatalf("LoadFile(saved): %v", err)
	}
	if loaded.S3.AccessKey != resp.Credentials.AccessKey {
		t.Fatalf("saved access_key = %q, want generated access key", loaded.S3.AccessKey)
	}
	if loaded.S3.SecretKey != resp.Credentials.SecretKey {
		t.Fatalf("saved secret_key = %q, want generated secret key", loaded.S3.SecretKey)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/settings", nil)
	rr = httptest.NewRecorder()
	srv.handleAPIGetSettings(rr, req)
	if strings.Contains(rr.Body.String(), resp.Credentials.AccessKey) || strings.Contains(rr.Body.String(), resp.Credentials.SecretKey) {
		t.Fatalf("settings GET leaked generated credentials: %s", rr.Body.String())
	}
}

func TestSettingsGenerateS3CredentialsPreservesOmittedCacheDir(t *testing.T) {
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml"), Exists: true}
	initial := []byte(`
s3:
  access_key: manual-access
  secret_key: manual-secret
  region: us-east-1
filecoin:
  private_key: manual-filecoin-private-key
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
	if strings.TrimSpace(cfg.S3.IAMDir) == "" {
		t.Fatal("runtime s3.iam_dir default was not applied")
	}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/s3-credentials", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()

	srv.handleAPIGenerateS3Credentials(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	data, err := os.ReadFile(source.Path)
	if err != nil {
		t.Fatalf("ReadFile saved config: %v", err)
	}
	if strings.Contains(string(data), "dir:") {
		t.Fatalf("settings generated credentials materialized cache.dir in YAML:\n%s", string(data))
	}
	restarted, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource(saved): %v", err)
	}
	if strings.TrimSpace(restarted.Cache.Dir) == "" {
		t.Fatal("cache.dir default was lost after restart")
	}
	if hasFieldError(restarted.FieldValidationErrors(), "cache.dir") {
		t.Fatalf("restarted config has cache.dir validation error: %#v", restarted.FieldValidationErrors())
	}
}

func TestSettingsGenerateS3CredentialsRejectsEnvManagedFields(t *testing.T) {
	t.Setenv("SYNAPS3_S3_SECRET_KEY", "env-secret")

	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
	if err := config.Save(source.Path, cfg); err != nil {
		t.Fatalf("Save initial config: %v", err)
	}
	source.Exists = true
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/settings/s3-credentials", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()

	srv.handleAPIGenerateS3Credentials(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "s3.secret_key") || !strings.Contains(rr.Body.String(), "SYNAPS3_S3_SECRET_KEY") {
		t.Fatalf("body should mention env-managed secret key: %s", rr.Body.String())
	}
	loaded, err := config.LoadFile(source.Path)
	if err != nil {
		t.Fatalf("LoadFile(saved): %v", err)
	}
	if loaded.S3.AccessKey != cfg.S3.AccessKey || loaded.S3.SecretKey != cfg.S3.SecretKey {
		t.Fatalf("credentials changed despite env rejection: %#v", loaded.S3)
	}
}

func TestSettingsPUTRejectsSecretFieldsAndDoesNotPersistThem(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"s3":{"secret_key":"leak"}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
	if data, err := os.ReadFile(source.Path); err == nil && strings.Contains(string(data), "leak") {
		t.Fatalf("secret field was persisted: %s", string(data))
	}
}

func TestSettingsPUTRequiresLoopbackAndWriteHeaders(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
	srv := newSettingsAPITestServer(t, "0.0.0.0:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":8}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403, body=%s", rr.Code, rr.Body.String())
	}

	srv = newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)
	req = httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":8}}`))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status without write header = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}

	srv = newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)
	req = httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":8}}`))
	req.Header.Set("Content-Type", "application/jsonp")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr = httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status with invalid media type = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
}

func TestSettingsPUTPersistsNonSecretFieldsAndReturnsRestartRequired(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
	if err := config.Save(source.Path, cfg); err != nil {
		t.Fatalf("Save initial config: %v", err)
	}
	source.Exists = true
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{
		"server":{"port":":8088"},
		"filecoin":{"network":"mainnet","with_cdn":true},
		"cache":{"max_size_gb":8},
		"worker":{"upload":{"poll_interval":"9s"}},
		"logging":{"format":"text"}
	}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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
	if resp.Config.Server.Port != ":8088" || resp.Config.Cache.MaxSizeGB != 8 {
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
	if loaded.S3.SecretKey != cfg.S3.SecretKey {
		t.Fatalf("saved secret key changed")
	}
}

func TestSettingsPUTPreservesManualFieldsChangedOnDiskAfterServiceStart(t *testing.T) {
	cfg := validSettingsConfig(t)
	cfg.S3.SecretKey = "old-secret"
	cfg.Filecoin.PrivateKey = "old-private-key"
	cfg.Database.DSN = "postgres://synaps3:old-password@example.invalid:5432/synaps3"
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
	if err := config.Save(source.Path, cfg); err != nil {
		t.Fatalf("Save initial config: %v", err)
	}
	source.Exists = true
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	manual := *cfg
	manual.S3.SecretKey = "new-secret"
	manual.Filecoin.PrivateKey = "new-private-key"
	manual.Database.DSN = "postgres://synaps3:new-password@example.invalid:5432/synaps3"
	if err := config.Save(source.Path, &manual); err != nil {
		t.Fatalf("Save manual config: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":9}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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
	if loaded.S3.SecretKey != manual.S3.SecretKey {
		t.Fatalf("s3.secret_key = %q, want preserved manual value", loaded.S3.SecretKey)
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

	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml"), Exists: true}
	initial := []byte(`
s3:
  access_key: manual-access
  secret_key: manual-secret
  region: us-east-1
filecoin:
  private_key: manual-filecoin-private-key
database:
  driver: sqlite
cache:
  dir: /tmp/synaps3-cache
  max_size_gb: 7
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
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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
	if strings.Contains(text, "dsn:") {
		t.Fatalf("settings PUT materialized database.dsn in YAML:\n%s", text)
	}
}

func TestSettingsPUTUsesRuntimeDefaultForOmittedCacheDir(t *testing.T) {
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml"), Exists: true}
	initial := []byte(`
s3:
  access_key: manual-access
  secret_key: manual-secret
  region: us-east-1
filecoin:
  private_key: manual-filecoin-private-key
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
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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
	if strings.Contains(text, "  dir:") {
		t.Fatalf("settings PUT materialized cache.dir in YAML:\n%s", text)
	}
	if strings.Contains(text, "iam_dir:") {
		t.Fatalf("settings PUT materialized s3.iam_dir in YAML:\n%s", text)
	}
	restarted, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource(saved): %v", err)
	}
	if strings.TrimSpace(restarted.Cache.Dir) == "" {
		t.Fatal("cache.dir default was lost after restart")
	}
	if strings.TrimSpace(restarted.S3.IAMDir) == "" {
		t.Fatal("s3.iam_dir default was lost after restart")
	}
	if restarted.Logging.Format != "text" {
		t.Fatalf("logging.format = %q, want text", restarted.Logging.Format)
	}
}

func TestSettingsLifecycleFallbackConfigEnvPrecedenceAndManualSecretPreservation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(t.TempDir())

	source, err := config.ResolveSource("config.yaml", false)
	if err != nil {
		t.Fatalf("ResolveSource: %v", err)
	}
	if source.Path != filepath.Join(home, ".synaps3", "config.yaml") {
		t.Fatalf("source path = %q, want app data config", source.Path)
	}
	cfg, err := config.LoadSource(source)
	if err != nil {
		t.Fatalf("LoadSource: %v", err)
	}
	for _, want := range []string{"s3.access_key", "s3.secret_key", "filecoin.private_key"} {
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
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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
	if persisted.S3.AccessKey != "" || persisted.S3.SecretKey != "" || persisted.Filecoin.PrivateKey != "" {
		t.Fatalf("manual secrets should remain empty until edited outside the browser: %#v %#v", persisted.S3, persisted.Filecoin)
	}

	persisted.S3.AccessKey = "manual-access"
	persisted.S3.SecretKey = "manual-secret"
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
	if restarted.S3.SecretKey != "manual-secret" || restarted.Filecoin.PrivateKey != "manual-filecoin-private-key" {
		t.Fatalf("manual secrets were not loaded after restart: %#v %#v", restarted.S3, restarted.Filecoin)
	}

	srv = newSettingsAPITestServer(t, "127.0.0.1:9090", restarted, source)
	req = httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":13}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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
	if persisted.S3.SecretKey != "manual-secret" || persisted.Filecoin.PrivateKey != "manual-filecoin-private-key" {
		t.Fatalf("manual secrets were overwritten: %#v %#v", persisted.S3, persisted.Filecoin)
	}
}

func TestSettingsPUTRejectsEnvManagedFieldChanges(t *testing.T) {
	tests := []struct {
		name    string
		envName string
		payload string
		field   string
	}{
		{name: "server port", envName: "SYNAPS3_SERVER_PORT", payload: `{"server":{"port":":8088"}}`, field: "server.port"},
		{name: "server max connections", envName: "SYNAPS3_SERVER_MAX_CONNECTIONS", payload: `{"server":{"max_connections":10}}`, field: "server.max_connections"},
		{name: "server max requests", envName: "SYNAPS3_SERVER_MAX_REQUESTS", payload: `{"server":{"max_requests":10}}`, field: "server.max_requests"},
		{name: "server tls enabled", envName: "SYNAPS3_SERVER_TLS_ENABLED", payload: `{"server":{"tls":{"enabled":true}}}`, field: "server.tls.enabled"},
		{name: "server tls cert file", envName: "SYNAPS3_SERVER_TLS_CERT_FILE", payload: `{"server":{"tls":{"cert_file":"/tmp/cert.pem"}}}`, field: "server.tls.cert_file"},
		{name: "server tls key file", envName: "SYNAPS3_SERVER_TLS_KEY_FILE", payload: `{"server":{"tls":{"key_file":"/tmp/key.pem"}}}`, field: "server.tls.key_file"},
		{name: "s3 region", envName: "SYNAPS3_S3_REGION", payload: `{"s3":{"region":"eu-west-1"}}`, field: "s3.region"},
		{name: "s3 iam dir", envName: "SYNAPS3_S3_IAM_DIR", payload: `{"s3":{"iam_dir":"/tmp/synaps3-iam"}}`, field: "s3.iam_dir"},
		{name: "filecoin network", envName: "SYNAPS3_FILECOIN_NETWORK", payload: `{"filecoin":{"network":"mainnet"}}`, field: "filecoin.network"},
		{name: "filecoin rpc url", envName: "SYNAPS3_FILECOIN_RPC_URL", payload: `{"filecoin":{"rpc_url":"https://rpc.example.invalid"}}`, field: "filecoin.rpc_url"},
		{name: "filecoin source", envName: "SYNAPS3_FILECOIN_SOURCE", payload: `{"filecoin":{"source":"other"}}`, field: "filecoin.source"},
		{name: "filecoin cdn", envName: "SYNAPS3_FILECOIN_WITH_CDN", payload: `{"filecoin":{"with_cdn":true}}`, field: "filecoin.with_cdn"},
		{name: "filecoin private networks", envName: "SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS", payload: `{"filecoin":{"allow_private_networks":true}}`, field: "filecoin.allow_private_networks"},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.envName, "managed")

			cfg := validSettingsConfig(t)
			source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
			srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

			req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(tt.payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{
		"server":{"port":"not-a-port"},
		"s3":{"region":"","iam_dir":""},
		"filecoin":{"rpc_url":"ftp://example.invalid/rpc","source":""},
		"worker":{"upload":{"max_retries":-1}},
		"logging":{"level":"verbose","format":"xml"}
	}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
	rr := httptest.NewRecorder()

	srv.handleAPIUpdateSettings(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{
		"server.port",
		"s3.region",
		"s3.iam_dir",
		"filecoin.rpc_url",
		"filecoin.source",
		"worker.upload.max_retries",
		"logging.level",
		"logging.format",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q: %s", want, body)
		}
	}
}

func TestSettingsPUTRejectsTrailingJSON(t *testing.T) {
	cfg := validSettingsConfig(t)
	source := config.Source{Path: filepath.Join(t.TempDir(), "config.yaml")}
	srv := newSettingsAPITestServer(t, "127.0.0.1:9090", cfg, source)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings", strings.NewReader(`{"cache":{"max_size_gb":8}} {}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(settingsWriteHeader, settingsWriteHeaderValue)
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
	cfg.S3.AccessKey = "admin"
	cfg.S3.SecretKey = "password"
	cfg.Filecoin.PrivateKey = "filecoin-private-key"
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
