package config

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// validConfig returns a Config that passes Validate().
func validConfig() *Config {
	cfg, err := DefaultConfig()
	if err != nil {
		panic(err)
	}
	cfg.Filecoin.PrivateKey = "filecoin-private-key"
	return cfg
}

func TestValidate_DefaultConfig(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}
}

func TestDefaultFilecoinRPCURLsReturnsKnownNetworkCopy(t *testing.T) {
	defaults := DefaultFilecoinRPCURLs()
	if defaults["calibration"] != "https://api.calibration.node.glif.io/rpc/v1" {
		t.Fatalf("calibration rpc = %q", defaults["calibration"])
	}
	if defaults["mainnet"] != "https://api.node.glif.io/rpc/v1" {
		t.Fatalf("mainnet rpc = %q", defaults["mainnet"])
	}

	defaults["mainnet"] = "https://mutated.example.invalid"
	got, ok := DefaultFilecoinRPCURL(" Mainnet ")
	if !ok {
		t.Fatal("DefaultFilecoinRPCURL(Mainnet) ok = false, want true")
	}
	if got != "https://api.node.glif.io/rpc/v1" {
		t.Fatalf("mainnet rpc after caller mutation = %q", got)
	}
}

func TestValidate_TLS_MissingCert(t *testing.T) {
	cfg := validConfig()
	cfg.Server.TLS.Enabled = true
	cfg.Server.TLS.KeyFile = "/path/to/key.pem"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing cert_file")
	}
	if !strings.Contains(err.Error(), "cert_file") {
		t.Fatalf("expected cert_file error, got: %v", err)
	}
}

func TestValidate_TLS_MissingKey(t *testing.T) {
	cfg := validConfig()
	cfg.Server.TLS.Enabled = true
	cfg.Server.TLS.CertFile = "/path/to/cert.pem"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for missing key_file")
	}
	if !strings.Contains(err.Error(), "key_file") {
		t.Fatalf("expected key_file error, got: %v", err)
	}
}

func TestValidate_InvalidEvictionPolicy(t *testing.T) {
	cfg := validConfig()
	cfg.Cache.EvictionPolicy = "random"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid eviction policy")
	}
	if !strings.Contains(err.Error(), "eviction_policy") {
		t.Fatalf("expected eviction_policy error, got: %v", err)
	}
}

func TestValidate_InvalidDriver(t *testing.T) {
	cfg := validConfig()
	cfg.Database.Driver = "mysql"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid driver")
	}
	if !strings.Contains(err.Error(), "database.driver") {
		t.Fatalf("expected database.driver error, got: %v", err)
	}
}

func TestValidate_MaxOpenConns_Zero(t *testing.T) {
	cfg := validConfig()
	cfg.Database.MaxOpenConns = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for MaxOpenConns=0")
	}
	if !strings.Contains(err.Error(), "max_open_conns") {
		t.Fatalf("expected max_open_conns error, got: %v", err)
	}
}

func TestValidate_EmptyFilecoinPrivateKey(t *testing.T) {
	cfg := validConfig()
	cfg.Filecoin.PrivateKey = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty filecoin private_key")
	}
	if !strings.Contains(err.Error(), "filecoin.private_key") {
		t.Fatalf("expected filecoin.private_key error, got: %v", err)
	}
}

func TestValidate_InvalidNetwork(t *testing.T) {
	cfg := validConfig()
	cfg.Filecoin.Network = "devnet"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid network")
	}
	if !strings.Contains(err.Error(), "filecoin.network") {
		t.Fatalf("expected filecoin.network error, got: %v", err)
	}
}

func TestValidate_WorkerConcurrency_Zero(t *testing.T) {
	cfg := validConfig()
	cfg.Worker.Upload.Concurrency = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for concurrency=0")
	}
	if !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("expected concurrency error, got: %v", err)
	}
}

func TestValidate_WorkerPollInterval_Zero(t *testing.T) {
	cfg := validConfig()
	cfg.Worker.Evictor.PollInterval = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for poll_interval=0")
	}
	if !strings.Contains(err.Error(), "poll_interval") {
		t.Fatalf("expected poll_interval error, got: %v", err)
	}
}

func TestValidate_FilecoinDefaultCopiesZero(t *testing.T) {
	cfg := validConfig()
	cfg.Filecoin.DefaultCopies = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for filecoin default_copies=0")
	}
	if !strings.Contains(err.Error(), "filecoin.default_copies") {
		t.Fatalf("expected filecoin.default_copies error, got: %v", err)
	}
}

func TestValidate_FilecoinDefaultCopiesTooHigh(t *testing.T) {
	cfg := validConfig()
	cfg.Filecoin.DefaultCopies = 9

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for filecoin default_copies=9")
	}
	if !strings.Contains(err.Error(), "filecoin.default_copies") {
		t.Fatalf("expected filecoin.default_copies error, got: %v", err)
	}
}

func TestValidate_EditableSettingsFields(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		mutate func(*Config)
	}{
		{
			name:  "server port",
			field: "server.port",
			mutate: func(cfg *Config) {
				cfg.Server.Port = "not-a-port"
			},
		},
		{
			name:  "s3 region",
			field: "s3.region",
			mutate: func(cfg *Config) {
				cfg.S3.Region = ""
			},
		},
		{
			name:  "filecoin rpc url",
			field: "filecoin.rpc_url",
			mutate: func(cfg *Config) {
				cfg.Filecoin.RPCURL = "ftp://example.invalid/rpc"
			},
		},
		{
			name:  "filecoin source",
			field: "filecoin.source",
			mutate: func(cfg *Config) {
				cfg.Filecoin.Source = ""
			},
		},
		{
			name:  "worker max retries",
			field: "worker.upload.max_retries",
			mutate: func(cfg *Config) {
				cfg.Worker.Upload.MaxRetries = -1
			},
		},
		{
			name:  "filecoin default copies",
			field: "filecoin.default_copies",
			mutate: func(cfg *Config) {
				cfg.Filecoin.DefaultCopies = 0
			},
		},
		{
			name:  "logging level",
			field: "logging.level",
			mutate: func(cfg *Config) {
				cfg.Logging.Level = "verbose"
			},
		},
		{
			name:  "logging format",
			field: "logging.format",
			mutate: func(cfg *Config) {
				cfg.Logging.Format = "xml"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			tt.mutate(cfg)

			if !hasConfigFieldError(cfg.FieldValidationErrors(), tt.field) {
				t.Fatalf("FieldValidationErrors() missing %q: %#v", tt.field, cfg.FieldValidationErrors())
			}
		})
	}
}

func TestValidate_MultipleErrors(t *testing.T) {
	cfg := validConfig()
	cfg.Database.Driver = "mysql"
	cfg.Database.MaxOpenConns = 0
	cfg.S3.Region = ""
	cfg.Filecoin.Network = "devnet"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected multiple errors")
	}

	msg := err.Error()
	for _, want := range []string{"database.driver", "max_open_conns", "s3.region", "filecoin.network"} {
		if !strings.Contains(msg, want) {
			t.Errorf("expected error to mention %q, got: %v", want, msg)
		}
	}
}

func TestLoad_DefaultConfig(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load('') failed: %v", err)
	}

	def, err := DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig() failed: %v", err)
	}
	if cfg.Server.Port != def.Server.Port {
		t.Errorf("Server.Port = %q, want %q", cfg.Server.Port, def.Server.Port)
	}
	if cfg.Database.Driver != def.Database.Driver {
		t.Errorf("Database.Driver = %q, want %q", cfg.Database.Driver, def.Database.Driver)
	}
	if cfg.Cache.MaxSizeGB != def.Cache.MaxSizeGB {
		t.Errorf("Cache.MaxSizeGB = %d, want %d", cfg.Cache.MaxSizeGB, def.Cache.MaxSizeGB)
	}
	if cfg.Worker.Upload.Concurrency != def.Worker.Upload.Concurrency {
		t.Errorf("Worker.Upload.Concurrency = %d, want %d", cfg.Worker.Upload.Concurrency, def.Worker.Upload.Concurrency)
	}
	if cfg.Worker.Upload.PollInterval != def.Worker.Upload.PollInterval {
		t.Errorf("Worker.Upload.PollInterval = %s, want %s", cfg.Worker.Upload.PollInterval, def.Worker.Upload.PollInterval)
	}
	if cfg.Filecoin.DefaultCopies != 2 {
		t.Errorf("Filecoin.DefaultCopies = %d, want 2", cfg.Filecoin.DefaultCopies)
	}

	wantAppDir := filepath.Join(home, ".synaps3")
	assertSQLiteDSNPath(t, cfg.Database.DSN, filepath.Join(wantAppDir, "db", "synaps3.db"))
	if cfg.Cache.Dir != filepath.Join(wantAppDir, "cache") {
		t.Errorf("Cache.Dir = %q, want %q", cfg.Cache.Dir, filepath.Join(wantAppDir, "cache"))
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("SYNAPS3_SERVER_PORT", ":9999")
	t.Setenv("SYNAPS3_DATABASE_DRIVER", "postgres")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load('') with env override failed: %v", err)
	}
	if cfg.Server.Port != ":9999" {
		t.Errorf("Server.Port = %q, want %q", cfg.Server.Port, ":9999")
	}
	if cfg.Database.Driver != "postgres" {
		t.Errorf("Database.Driver = %q, want %q", cfg.Database.Driver, "postgres")
	}
}

func TestLoad_EnvOverridePaths(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)

	dbDSN := "file:" + filepath.ToSlash(filepath.Join(t.TempDir(), "custom.db"))
	cacheDir := filepath.Join(t.TempDir(), "custom-cache")
	t.Setenv("SYNAPS3_DATABASE_DSN", dbDSN)
	t.Setenv("SYNAPS3_CACHE_DIR", cacheDir)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load('') with path env overrides failed: %v", err)
	}
	if cfg.Database.DSN != dbDSN {
		t.Errorf("Database.DSN = %q, want %q", cfg.Database.DSN, dbDSN)
	}
	if cfg.Cache.Dir != cacheDir {
		t.Errorf("Cache.Dir = %q, want %q", cfg.Cache.Dir, cacheDir)
	}
}

func TestLoad_EnvOverrideUnderscoreFields(t *testing.T) {
	t.Setenv("SYNAPS3_SERVER_MAX_CONNECTIONS", "42")
	t.Setenv("SYNAPS3_SERVER_MAX_REQUESTS", "24")
	t.Setenv("SYNAPS3_SERVER_TLS_CERT_FILE", "/tmp/cert.pem")
	t.Setenv("SYNAPS3_SERVER_TLS_KEY_FILE", "/tmp/key.pem")
	t.Setenv("SYNAPS3_FILECOIN_RPC_URL", "https://rpc.env.example")
	t.Setenv("SYNAPS3_FILECOIN_PRIVATE_KEY", "env-private")
	t.Setenv("SYNAPS3_FILECOIN_WITH_CDN", "true")
	t.Setenv("SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS", "true")
	t.Setenv("SYNAPS3_FILECOIN_DEFAULT_COPIES", "4")
	t.Setenv("SYNAPS3_CACHE_MAX_SIZE_GB", "7")
	t.Setenv("SYNAPS3_CACHE_EVICTION_POLICY", "manual")
	t.Setenv("SYNAPS3_WORKER_UPLOAD_POLL_INTERVAL", "9s")
	t.Setenv("SYNAPS3_WORKER_UPLOAD_MAX_RETRIES", "8")
	t.Setenv("SYNAPS3_WORKER_EVICTOR_POLL_INTERVAL", "2m")
	t.Setenv("SYNAPS3_WORKER_EVICTOR_MAX_RETRIES", "6")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load('') with underscore env overrides failed: %v", err)
	}
	if cfg.Server.MaxConnections != 42 || cfg.Server.MaxRequests != 24 {
		t.Fatalf("server concurrency = %d/%d, want 42/24", cfg.Server.MaxConnections, cfg.Server.MaxRequests)
	}
	if cfg.Server.TLS.CertFile != "/tmp/cert.pem" || cfg.Server.TLS.KeyFile != "/tmp/key.pem" {
		t.Fatalf("tls files = %#v, want env values", cfg.Server.TLS)
	}
	if cfg.Filecoin.RPCURL != "https://rpc.env.example" || cfg.Filecoin.PrivateKey != "env-private" {
		t.Fatalf("filecoin config = %#v, want env values", cfg.Filecoin)
	}
	if !cfg.Filecoin.WithCDN || !cfg.Filecoin.AllowPrivateNetworks || cfg.Filecoin.DefaultCopies != 4 {
		t.Fatalf("filecoin config = %#v, want env values", cfg.Filecoin)
	}
	if cfg.Cache.MaxSizeGB != 7 || cfg.Cache.EvictionPolicy != "manual" {
		t.Fatalf("cache config = %#v, want env values", cfg.Cache)
	}
	if cfg.Worker.Upload.PollInterval != 9*time.Second || cfg.Worker.Upload.MaxRetries != 8 {
		t.Fatalf("upload worker = %#v, want env values", cfg.Worker.Upload)
	}
	if cfg.Worker.Evictor.PollInterval != 2*time.Minute || cfg.Worker.Evictor.MaxRetries != 6 {
		t.Fatalf("evictor worker = %#v, want env values", cfg.Worker.Evictor)
	}
}

func TestLoad_PartialTOMLKeepsDefaultPaths(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfgTOML := []byte(`[server]
port = ":9999"
`)
	if err := os.WriteFile(cfgPath, cfgTOML, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load(%q) failed: %v", cfgPath, err)
	}
	if cfg.Server.Port != ":9999" {
		t.Fatalf("Server.Port = %q, want :9999", cfg.Server.Port)
	}
	wantAppDir := filepath.Join(home, ".synaps3")
	assertSQLiteDSNPath(t, cfg.Database.DSN, filepath.Join(wantAppDir, "db", "synaps3.db"))
	if cfg.Cache.Dir != filepath.Join(wantAppDir, "cache") {
		t.Errorf("Cache.Dir = %q, want %q", cfg.Cache.Dir, filepath.Join(wantAppDir, "cache"))
	}
}

func TestLoad_TOMLOverridesDefaultPaths(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbDSN := "file:" + filepath.ToSlash(filepath.Join(dir, "custom", "synaps3.db"))
	cacheDir := filepath.Join(dir, "custom-cache")
	cfgTOML := []byte("[database]\ndsn = \"" + dbDSN + "\"\n[cache]\ndir = \"" + filepath.ToSlash(cacheDir) + "\"\n")
	if err := os.WriteFile(cfgPath, cfgTOML, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load(%q) failed: %v", cfgPath, err)
	}
	if cfg.Database.DSN != dbDSN {
		t.Errorf("Database.DSN = %q, want %q", cfg.Database.DSN, dbDSN)
	}
	if cfg.Cache.Dir != filepath.ToSlash(cacheDir) {
		t.Errorf("Cache.Dir = %q, want %q", cfg.Cache.Dir, filepath.ToSlash(cacheDir))
	}
}

func TestLoad_DefaultRuntimePathsHomeError(t *testing.T) {
	original := userHomeDir
	userHomeDir = func() (string, error) {
		return "", errors.New("no home")
	}
	t.Cleanup(func() { userHomeDir = original })

	_, err := Load("")
	if err == nil {
		t.Fatal("expected Load to fail when user home is unavailable")
	}
	if !strings.Contains(err.Error(), "user home") {
		t.Fatalf("expected user home error, got: %v", err)
	}
}

func TestLoad_ExplicitRuntimePathsDoNotRequireHome(t *testing.T) {
	original := userHomeDir
	userHomeDir = func() (string, error) {
		return "", errors.New("no home")
	}
	t.Cleanup(func() { userHomeDir = original })

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	dbDSN := "file:" + filepath.ToSlash(filepath.Join(dir, "custom", "synaps3.db"))
	cacheDir := filepath.Join(dir, "custom-cache")
	cfgTOML := []byte("[database]\ndsn = \"" + dbDSN + "\"\n[cache]\ndir = \"" + filepath.ToSlash(cacheDir) + "\"\n")
	if err := os.WriteFile(cfgPath, cfgTOML, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load(%q) failed with explicit runtime paths: %v", cfgPath, err)
	}
	if cfg.Database.DSN != dbDSN {
		t.Errorf("Database.DSN = %q, want %q", cfg.Database.DSN, dbDSN)
	}
	if cfg.Cache.Dir != filepath.ToSlash(cacheDir) {
		t.Errorf("Cache.Dir = %q, want %q", cfg.Cache.Dir, filepath.ToSlash(cacheDir))
	}
}

func TestLoad_FilecoinSDKOptions(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfgTOML := []byte(`[filecoin]
source = "synaps3-test"
with_cdn = true
allow_private_networks = true
`)
	if err := os.WriteFile(cfgPath, cfgTOML, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load(%q) failed: %v", cfgPath, err)
	}
	if cfg.Filecoin.Source != "synaps3-test" {
		t.Fatalf("filecoin.source = %q, want synaps3-test", cfg.Filecoin.Source)
	}
	if !cfg.Filecoin.WithCDN {
		t.Fatal("filecoin.with_cdn = false, want true")
	}
	if !cfg.Filecoin.AllowPrivateNetworks {
		t.Fatal("filecoin.allow_private_networks = false, want true")
	}
}

func TestLoad_YAMLConfigIsRejected(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	cfgYAML := []byte("server:\n  port: ':9999'\n")
	if err := os.WriteFile(cfgPath, cfgYAML, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := Load(cfgPath)
	if err == nil {
		t.Fatal("expected YAML config to be rejected")
	}
}

func TestValidate_CacheDir_Empty(t *testing.T) {
	cfg := validConfig()
	cfg.Cache.Dir = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty cache dir")
	}
	if !strings.Contains(err.Error(), "cache.dir") {
		t.Fatalf("expected cache.dir error, got: %v", err)
	}
}

func TestValidate_CacheMaxSizeGB_Zero(t *testing.T) {
	cfg := validConfig()
	cfg.Cache.MaxSizeGB = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for MaxSizeGB=0")
	}
	if !strings.Contains(err.Error(), "max_size_gb") {
		t.Fatalf("expected max_size_gb error, got: %v", err)
	}
}

func TestValidate_AdminAddr_Empty(t *testing.T) {
	cfg := validConfig()
	cfg.Admin.Addr = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty admin addr")
	}
	if !strings.Contains(err.Error(), "admin.addr") {
		t.Fatalf("expected admin.addr error, got: %v", err)
	}
}

func TestValidate_EvictionPolicy_CaseInsensitive(t *testing.T) {
	for _, policy := range []string{"LRU", "Manual", "NONE", "lru", "manual", "none"} {
		cfg := validConfig()
		cfg.Cache.EvictionPolicy = policy
		if err := cfg.Validate(); err != nil {
			t.Errorf("eviction_policy=%q should be valid, got: %v", policy, err)
		}
	}
}

func TestValidate_TLS_Disabled_NoCerts(t *testing.T) {
	cfg := validConfig()
	cfg.Server.TLS.Enabled = false
	cfg.Server.TLS.CertFile = ""
	cfg.Server.TLS.KeyFile = ""

	if err := cfg.Validate(); err != nil {
		t.Fatalf("TLS disabled should not require certs, got: %v", err)
	}
}

func TestValidate_WorkerPollInterval_Negative(t *testing.T) {
	cfg := validConfig()
	cfg.Worker.Evictor.PollInterval = -1 * time.Second

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for negative poll_interval")
	}
	if !strings.Contains(err.Error(), "poll_interval") {
		t.Fatalf("expected poll_interval error, got: %v", err)
	}
}

func TestValidate_DatabaseDSN_Empty(t *testing.T) {
	cfg := validConfig()
	cfg.Database.DSN = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
	if !strings.Contains(err.Error(), "database.dsn") {
		t.Fatalf("expected database.dsn error, got: %v", err)
	}
}

func TestValidate_MaxIdleConns_ExceedsMaxOpen(t *testing.T) {
	cfg := validConfig()
	cfg.Database.MaxOpenConns = 5
	cfg.Database.MaxIdleConns = 10

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for MaxIdleConns > MaxOpenConns")
	}
	if !strings.Contains(err.Error(), "max_idle_conns") {
		t.Fatalf("expected max_idle_conns error, got: %v", err)
	}
}

func TestDefaultConfig_ServerConcurrency(t *testing.T) {
	cfg, err := DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig() failed: %v", err)
	}
	if cfg.Server.MaxConnections != 250000 {
		t.Errorf("Server.MaxConnections = %d, want 250000", cfg.Server.MaxConnections)
	}
	if cfg.Server.MaxRequests != 100000 {
		t.Errorf("Server.MaxRequests = %d, want 100000", cfg.Server.MaxRequests)
	}
}

func TestValidate_MaxConnections_Zero(t *testing.T) {
	cfg := validConfig()
	cfg.Server.MaxConnections = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for zero MaxConnections")
	}
	if !strings.Contains(err.Error(), "max_connections") {
		t.Fatalf("expected max_connections error, got: %v", err)
	}
}

func TestValidate_MaxRequests_Zero(t *testing.T) {
	cfg := validConfig()
	cfg.Server.MaxRequests = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for zero MaxRequests")
	}
	if !strings.Contains(err.Error(), "max_requests") {
		t.Fatalf("expected max_requests error, got: %v", err)
	}
}

func withUserHomeDir(t *testing.T, home string) {
	t.Helper()
	original := userHomeDir
	userHomeDir = func() (string, error) {
		return home, nil
	}
	t.Cleanup(func() { userHomeDir = original })
}

func assertSQLiteDSNPath(t *testing.T, dsn, wantPath string) {
	t.Helper()

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parsing DSN %q: %v", dsn, err)
	}
	if u.Scheme != "file" {
		t.Fatalf("DSN scheme = %q, want file", u.Scheme)
	}
	parsedPath := u.Path
	if parsedPath == "" {
		parsedPath = u.Opaque
	}
	if filepath.Clean(filepath.FromSlash(parsedPath)) != filepath.Clean(wantPath) {
		t.Fatalf("DSN path = %q, want %q", filepath.FromSlash(parsedPath), wantPath)
	}
	pragmas := u.Query()["_pragma"]
	if len(pragmas) != 2 || pragmas[0] != "journal_mode(WAL)" || pragmas[1] != "busy_timeout(5000)" {
		t.Fatalf("DSN _pragma values = %#v, want journal_mode(WAL), busy_timeout(5000)", pragmas)
	}
}

func hasConfigFieldError(errs []FieldError, field string) bool {
	for _, err := range errs {
		if err.Field == field {
			return true
		}
	}
	return false
}

func TestValidate_MaxRequests_ExceedsMaxConnections(t *testing.T) {
	cfg := validConfig()
	cfg.Server.MaxConnections = 100
	cfg.Server.MaxRequests = 200

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for MaxRequests > MaxConnections")
	}
	if !strings.Contains(err.Error(), "max_requests") {
		t.Fatalf("expected max_requests error, got: %v", err)
	}
}
