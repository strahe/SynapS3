package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func TestResolveSource_ExplicitPathWinsEvenWhenMissing(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)
	dir := t.TempDir()
	t.Chdir(dir)

	cfgPath := "custom.toml"
	src, err := ResolveSource(cfgPath, true)
	if err != nil {
		t.Fatalf("ResolveSource() error = %v", err)
	}

	wantPath := filepath.Join(dir, cfgPath)
	if src.Path != wantPath {
		t.Fatalf("Path = %q, want %q", src.Path, wantPath)
	}
	if !src.Explicit {
		t.Fatal("Explicit = false, want true")
	}
	if src.Exists {
		t.Fatal("Exists = true, want false")
	}
	if src.GeneratedDefault {
		t.Fatal("GeneratedDefault = true, want false")
	}
}

func TestResolveSource_DefaultIgnoresExistingConfigTOMLInWorkingDirectory(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)
	dir := t.TempDir()
	t.Chdir(dir)

	if err := os.WriteFile("config.toml", []byte("[server]\nport = \":9999\"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	src, err := ResolveSource("config.toml", false)
	if err != nil {
		t.Fatalf("ResolveSource() error = %v", err)
	}

	want := filepath.Join(home, ".synaps3", "config.toml")
	if src.Path != want {
		t.Fatalf("Path = %q, want %q", src.Path, want)
	}
	if src.Explicit {
		t.Fatal("Explicit = true, want false")
	}
	if src.Exists {
		t.Fatal("Exists = true, want false")
	}
	if !src.GeneratedDefault {
		t.Fatal("GeneratedDefault = false, want true")
	}
}

func TestResolveSource_DefaultFallsBackToAppDataConfig(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)
	t.Chdir(t.TempDir())

	src, err := ResolveSource("config.toml", false)
	if err != nil {
		t.Fatalf("ResolveSource() error = %v", err)
	}

	want := filepath.Join(home, ".synaps3", "config.toml")
	if src.Path != want {
		t.Fatalf("Path = %q, want %q", src.Path, want)
	}
	if src.Explicit {
		t.Fatal("Explicit = true, want false")
	}
	if src.Exists {
		t.Fatal("Exists = true, want false")
	}
	if !src.GeneratedDefault {
		t.Fatal("GeneratedDefault = false, want true")
	}
}

func TestInitAppDataDir_DefaultCreatesReferenceConfigAndRuntimeDirs(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)

	result, err := InitAppDataDir(InitOptions{})
	if err != nil {
		t.Fatalf("InitAppDataDir() error = %v", err)
	}

	wantDir := filepath.Join(home, ".synaps3")
	if result.Dir != wantDir {
		t.Fatalf("Dir = %q, want %q", result.Dir, wantDir)
	}
	if result.ConfigPath != filepath.Join(wantDir, "config.toml") {
		t.Fatalf("ConfigPath = %q, want config.toml under app data dir", result.ConfigPath)
	}
	if !result.DefaultDir {
		t.Fatal("DefaultDir = false, want true")
	}
	for _, path := range []string{result.Dir, result.DatabaseDir, result.CacheDir} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat(%q): %v", path, err)
		}
		if !info.IsDir() {
			t.Fatalf("%q is not a directory", path)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("%q mode = %o, want 700", path, got)
		}
	}
	info, err := os.Stat(result.ConfigPath)
	if err != nil {
		t.Fatalf("Stat(%q): %v", result.ConfigPath, err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("config mode = %o, want 600", got)
	}

	loaded, err := Load(result.ConfigPath)
	if err != nil {
		t.Fatalf("Load(%q): %v", result.ConfigPath, err)
	}
	assertSQLiteDSNPath(t, loaded.Database.DSN, filepath.Join(wantDir, "db", "synaps3.db"))
	if loaded.Cache.Dir != filepath.Join(wantDir, "cache") {
		t.Fatalf("Cache.Dir = %q, want %q", loaded.Cache.Dir, filepath.Join(wantDir, "cache"))
	}
	if loaded.Admin.Auth.Username != "admin" {
		t.Fatalf("Admin.Auth.Username = %q, want admin", loaded.Admin.Auth.Username)
	}
	if loaded.Admin.Auth.PasswordHash == "" {
		t.Fatal("Admin.Auth.PasswordHash is empty")
	}
	if _, err := bcrypt.Cost([]byte(loaded.Admin.Auth.PasswordHash)); err != nil {
		t.Fatalf("Admin.Auth.PasswordHash is not a bcrypt hash: %v", err)
	}
	if loaded.Admin.Auth.SessionSecret == "" {
		t.Fatal("Admin.Auth.SessionSecret is empty")
	}
	if result.AdminInitialPassword == "" {
		t.Fatal("AdminInitialPassword is empty")
	}
	configData, err := os.ReadFile(result.ConfigPath)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", result.ConfigPath, err)
	}
	if strings.Contains(string(configData), result.AdminInitialPassword) {
		t.Fatal("config must not contain the plaintext admin password")
	}
	defaults := defaultConfig()
	if loaded.Server.Port != defaults.Server.Port {
		t.Fatalf("Server.Port = %q, want default %q", loaded.Server.Port, defaults.Server.Port)
	}
	if loaded.Worker.Upload.Concurrency != defaults.Worker.Upload.Concurrency {
		t.Fatalf("Worker.Upload.Concurrency = %d, want default %d", loaded.Worker.Upload.Concurrency, defaults.Worker.Upload.Concurrency)
	}
	if loaded.Logging.Level != defaults.Logging.Level {
		t.Fatalf("Logging.Level = %q, want default %q", loaded.Logging.Level, defaults.Logging.Level)
	}
	if loaded.Cache.MaxSizeGB != defaults.Cache.MaxSizeGB {
		t.Fatalf("Cache.MaxSizeGB = %d, want default %d", loaded.Cache.MaxSizeGB, defaults.Cache.MaxSizeGB)
	}
}

func TestInitAppDataDir_CustomDirCreatesReferenceConfigForThatDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "synaps3-data")

	result, err := InitAppDataDir(InitOptions{Dir: dir})
	if err != nil {
		t.Fatalf("InitAppDataDir() error = %v", err)
	}

	if result.Dir != dir {
		t.Fatalf("Dir = %q, want %q", result.Dir, dir)
	}
	if result.DefaultDir {
		t.Fatal("DefaultDir = true, want false")
	}
	loaded, err := Load(result.ConfigPath)
	if err != nil {
		t.Fatalf("Load(%q): %v", result.ConfigPath, err)
	}
	assertSQLiteDSNPath(t, loaded.Database.DSN, filepath.Join(dir, "db", "synaps3.db"))
	if loaded.Cache.Dir != filepath.Join(dir, "cache") {
		t.Fatalf("Cache.Dir = %q, want %q", loaded.Cache.Dir, filepath.Join(dir, "cache"))
	}
}

func TestInitAppDataDir_ExistingConfigFailsWithoutChangingContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	original := []byte("[server]\nport = \":9999\"\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := InitAppDataDir(InitOptions{Dir: dir})
	if err == nil {
		t.Fatal("expected existing config error")
	}
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("ReadFile: %v", readErr)
	}
	if string(data) != string(original) {
		t.Fatalf("config changed to %q, want %q", data, original)
	}
}

func TestInitAppDataDir_WritesCommentedReferenceConfig(t *testing.T) {
	result, err := InitAppDataDir(InitOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("InitAppDataDir() error = %v", err)
	}

	data, err := os.ReadFile(result.ConfigPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"# SynapS3 configuration",
		"[server]",
		"# port = \":8080\"",
		"[server.tls]",
		"[worker.upload]",
		"[logging]",
		"[logging.s3_access]",
		"[admin.auth]",
		"enabled = true",
		"username = \"admin\"",
		"password_hash = ",
		"session_secret = ",
		"# session_ttl = \"12h0m0s\"",
		"[filecoin]",
		"private_key = \"\"",
		"# network = \"calibration\"",
		"[database]",
		"driver = \"sqlite\"",
		"dsn = ",
		"# max_open_conns = 4",
		"[cache]",
		"dir = ",
		"# max_size_gb = 100",
		"Host and port where the S3-compatible API listens.",
		"Env: SYNAPS3_SERVER_PORT",
		"Default target Filecoin copies for buckets without an explicit copy policy, from 1 to 8.",
		"\n\n# Maximum concurrent TCP connections accepted by the S3 server.",
	} {
		assertConfigContains(t, text, want)
	}
	if strings.Contains(text, "_pragma") {
		t.Fatalf("generated config contains SQLite pragmas:\n%s", text)
	}
	assertGeneratedTOMLOnlyEnablesInitFields(t, text)

	for _, disabled := range []string{
		"port = \":8080\"",
		"network = \"calibration\"",
		"level = \"info\"",
		"format = \"text\"",
		"max_open_conns = 4",
		"max_size_gb = 100",
		"eviction_policy = \"lru\"",
	} {
		assertConfigLacksEnabledLine(t, text, disabled)
	}
}

func TestInitAppDataDir_CommentedFieldsCanBeUncommentedInPlace(t *testing.T) {
	result, err := InitAppDataDir(InitOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("InitAppDataDir() error = %v", err)
	}

	data, err := os.ReadFile(result.ConfigPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := strings.ReplaceAll(string(data), "# port = \":8080\"", "port = \":9191\"")
	text = strings.ReplaceAll(text, "# level = \"info\"", "level = \"debug\"")
	text = strings.ReplaceAll(text, "# concurrency = 4", "concurrency = 6")
	if err := os.WriteFile(result.ConfigPath, []byte(text), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	loaded, err := Load(result.ConfigPath)
	if err != nil {
		t.Fatalf("Load(%q): %v", result.ConfigPath, err)
	}
	if loaded.Server.Port != ":9191" {
		t.Fatalf("Server.Port = %q, want :9191", loaded.Server.Port)
	}
	if loaded.Logging.Level != "debug" {
		t.Fatalf("Logging.Level = %q, want debug", loaded.Logging.Level)
	}
	if loaded.Worker.Upload.Concurrency != 6 {
		t.Fatalf("Worker.Upload.Concurrency = %d, want 6", loaded.Worker.Upload.Concurrency)
	}
}

func TestSaveGeneratedTOML_RoundTripsWithCommentsAndUsesPrivatePermissions(t *testing.T) {
	cfg := validConfig()
	cfg.Server.Port = ":9091"
	cfg.Filecoin.Network = "mainnet"
	cfg.Filecoin.RPCURL = "https://example.invalid/rpc"
	cfg.Filecoin.WithCDN = true
	cfg.Filecoin.AllowPrivateNetworks = true
	cfg.Cache.MaxSizeGB = 42
	cfg.Worker.Upload.PollInterval = 7 * time.Second
	cfg.Worker.Evictor.PollInterval = 2 * time.Minute
	cfg.Logging.S3Access.Enabled = false
	cfg.Logging.S3Access.Level = "debug"

	path := filepath.Join(t.TempDir(), "nested", "config.toml")
	if err := Save(path, cfg); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("config mode = %o, want 600", got)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"# Host and port where the S3-compatible API listens.",
		"[server]",
		"port = \":9091\"",
		"[filecoin]",
		"network = \"mainnet\"",
		"with_cdn = true",
		"allow_private_networks = true",
		"[filecoin.observability]",
		"interval = \"5m0s\"",
		"timeout = \"5s\"",
		"concurrency = 8",
		"[worker.upload]",
		"poll_interval = \"7s\"",
		"[worker.evictor]",
		"poll_interval = \"2m0s\"",
		"[logging.s3_access]",
		"enabled = false",
		"level = \"debug\"",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("generated TOML missing %q:\n%s", want, text)
		}
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load(saved) error = %v", err)
	}
	if loaded.Server.Port != cfg.Server.Port {
		t.Fatalf("Server.Port = %q, want %q", loaded.Server.Port, cfg.Server.Port)
	}
	if loaded.Worker.Upload.PollInterval != cfg.Worker.Upload.PollInterval {
		t.Fatalf("Upload poll interval = %s, want %s", loaded.Worker.Upload.PollInterval, cfg.Worker.Upload.PollInterval)
	}
	if loaded.Worker.Evictor.PollInterval != cfg.Worker.Evictor.PollInterval {
		t.Fatalf("Evictor poll interval = %s, want %s", loaded.Worker.Evictor.PollInterval, cfg.Worker.Evictor.PollInterval)
	}
}

func TestSaveForSettingsGeneratedTOMLCommentsAndPreservesAbsentManualFields(t *testing.T) {
	cfg := validConfig()
	cfg.Database.DSN = "postgres://synaps3:password@example.invalid:5432/synaps3"
	cfg.Cache.Dir = "/var/lib/synaps3/cache"
	path := filepath.Join(t.TempDir(), "config.toml")
	presence := PersistedFieldPresence{
		FilecoinPrivateKey: true,
		DatabaseDriver:     true,
		CacheDir:           true,
	}

	if err := SaveForSettings(path, cfg, presence); err != nil {
		t.Fatalf("SaveForSettings() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(data)
	for _, want := range []string{
		"# Database connection string.",
		"# dsn = \"\"",
		"# max_open_conns = 4",
		"driver = \"sqlite\"",
		"dir = \"/var/lib/synaps3/cache\"",
	} {
		assertConfigContains(t, text, want)
	}
	if strings.Contains(text, "password@example.invalid") {
		t.Fatalf("disabled secret value leaked into TOML comment:\n%s", text)
	}
	for _, disabled := range []string{
		"dsn = \"postgres://synaps3:password@example.invalid:5432/synaps3\"",
		"max_open_conns = 4",
		"max_idle_conns = 2",
	} {
		assertConfigLacksEnabledLine(t, text, disabled)
	}
	for _, disabledPrefix := range []string{
		"password_hash = ",
		"session_secret = ",
	} {
		assertConfigLacksEnabledPrefix(t, text, disabledPrefix)
	}
}

func assertConfigContains(t *testing.T, text, want string) {
	t.Helper()
	if !strings.Contains(text, want) {
		t.Fatalf("generated config missing %q:\n%s", want, text)
	}
}

func assertConfigLacksEnabledLine(t *testing.T, text, want string) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if line == want {
			t.Fatalf("generated config contains enabled line %q:\n%s", want, text)
		}
	}
}

func assertConfigLacksEnabledPrefix(t *testing.T, text, prefix string) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, prefix) {
			t.Fatalf("generated config contains enabled line with prefix %q:\n%s", prefix, text)
		}
	}
}

func assertGeneratedTOMLOnlyEnablesInitFields(t *testing.T, text string) {
	t.Helper()
	for _, want := range []string{
		"[filecoin]\n# Filecoin network used by synapse-go.",
		"private_key = \"\"",
		"[database]\n# Database backend used for metadata persistence.",
		"driver = \"sqlite\"",
		"dsn = ",
		"[cache]\n# Filesystem directory used for cached object data.",
		"dir = ",
		"[admin.auth]\n# Requires login for the Admin UI and Admin API.",
		"enabled = true",
		"username = \"admin\"",
		"password_hash = ",
		"session_secret = ",
	} {
		assertConfigContains(t, text, want)
	}
}

func TestEnvManagedFieldPaths_ReturnsRecognizedOverrides(t *testing.T) {
	t.Setenv("SYNAPS3_SERVER_PORT", ":9999")
	t.Setenv("SYNAPS3_CACHE_DIR", "/tmp/synaps3-cache")
	t.Setenv("SYNAPS3_FILECOIN_NETWORK", "mainnet")
	t.Setenv("SYNAPS3_FILECOIN_RPC_URL", "https://rpc.example.invalid")
	t.Setenv("SYNAPS3_LOGGING_S3_ACCESS_ENABLED", "false")
	t.Setenv("SYNAPS3_ADMIN_AUTH_PASSWORD_HASH", "$2a$10$7EqJtq98hPqEX7fNZaFWoOhi6r4aIvJrDWHtqK4V0GaQYe7TzTx6W")

	managed := EnvManagedFieldPaths()
	for _, want := range []string{"server.port", "cache.dir", "filecoin.network", "filecoin.rpc_url", "logging.s3_access.enabled"} {
		if managed[want] == "" {
			t.Fatalf("EnvManagedFieldPaths() missing %q in %#v", want, managed)
		}
	}
	if managed["admin.auth.password_hash"] != "" {
		t.Fatalf("EnvManagedFieldPaths() exposes admin password hash in %#v", managed)
	}
}

func TestFieldMetadataDefinesEnvMappings(t *testing.T) {
	metadata := FieldMetadataByPath()
	tests := []struct {
		env   string
		field string
	}{
		{env: "SYNAPS3_SERVER_PORT", field: "server.port"},
		{env: "SYNAPS3_S3_REGION", field: "s3.region"},
		{env: "SYNAPS3_FILECOIN_PRIVATE_KEY", field: "filecoin.private_key"},
		{env: "SYNAPS3_FILECOIN_OBSERVABILITY_INTERVAL", field: "filecoin.observability.interval"},
		{env: "SYNAPS3_FILECOIN_OBSERVABILITY_TIMEOUT", field: "filecoin.observability.timeout"},
		{env: "SYNAPS3_FILECOIN_OBSERVABILITY_CONCURRENCY", field: "filecoin.observability.concurrency"},
		{env: "SYNAPS3_CACHE_MAX_SIZE_GB", field: "cache.max_size_gb"},
		{env: "SYNAPS3_LOGGING_S3_ACCESS_ENABLED", field: "logging.s3_access.enabled"},
		{env: "SYNAPS3_LOGGING_S3_ACCESS_LEVEL", field: "logging.s3_access.level"},
		{env: "SYNAPS3_ADMIN_ADDR", field: "admin.addr"},
		{env: "SYNAPS3_ADMIN_AUTH_ENABLED", field: "admin.auth.enabled"},
		{env: "SYNAPS3_ADMIN_AUTH_USERNAME", field: "admin.auth.username"},
		{env: "SYNAPS3_ADMIN_AUTH_SESSION_SECRET", field: "admin.auth.session_secret"},
		{env: "SYNAPS3_ADMIN_AUTH_SESSION_TTL", field: "admin.auth.session_ttl"},
	}

	for _, tt := range tests {
		field, ok := EnvFieldForName(tt.env)
		if !ok {
			t.Fatalf("EnvFieldForName(%q) missing", tt.env)
		}
		if field != tt.field {
			t.Fatalf("EnvFieldForName(%q) = %q, want %q", tt.env, field, tt.field)
		}
		meta, ok := metadata[tt.field]
		if !ok {
			t.Fatalf("FieldMetadataByPath() missing %q", tt.field)
		}
		if meta.Env != tt.env {
			t.Fatalf("metadata[%q].Env = %q, want %q", tt.field, meta.Env, tt.env)
		}
		if strings.TrimSpace(meta.Label) == "" || strings.TrimSpace(meta.Description) == "" {
			t.Fatalf("metadata[%q] must include label and description: %#v", tt.field, meta)
		}
	}
	if field, ok := EnvFieldForName("SYNAPS3_ADMIN_AUTH_PASSWORD_HASH"); !ok || field != "admin.auth.password_hash" {
		t.Fatalf("EnvFieldForName(SYNAPS3_ADMIN_AUTH_PASSWORD_HASH) = %q/%v, want admin.auth.password_hash/true", field, ok)
	}
	if _, ok := metadata["admin.auth.password_hash"]; ok {
		t.Fatal("FieldMetadataByPath() exposes admin.auth.password_hash, want hidden")
	}

	if metadata["filecoin.private_key"].Editable {
		t.Fatal("filecoin.private_key Editable = true, want false")
	}
	if !metadata["admin.auth.session_secret"].Secret {
		t.Fatal("admin.auth.session_secret Secret = false, want true")
	}
}

func TestFieldValidationErrors_ReportsFieldNames(t *testing.T) {
	cfg, err := DefaultConfig()
	if err != nil {
		t.Fatalf("DefaultConfig() error = %v", err)
	}
	cfg.Server.MaxConnections = 0

	errs := cfg.FieldValidationErrors()
	var fields []string
	for _, err := range errs {
		fields = append(fields, err.Field)
	}

	for _, want := range []string{"server.max_connections", "filecoin.private_key"} {
		if !slices.Contains(fields, want) {
			t.Fatalf("fields = %#v, want %q", fields, want)
		}
	}
}
