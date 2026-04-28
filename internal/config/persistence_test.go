package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestResolveSource_ExplicitPathWinsEvenWhenMissing(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)
	dir := t.TempDir()
	t.Chdir(dir)

	cfgPath := "custom.yaml"
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

func TestResolveSource_DefaultIgnoresExistingConfigYAMLInWorkingDirectory(t *testing.T) {
	home := t.TempDir()
	withUserHomeDir(t, home)
	dir := t.TempDir()
	t.Chdir(dir)

	if err := os.WriteFile("config.yaml", []byte("server:\n  port: ':9999'\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	src, err := ResolveSource("config.yaml", false)
	if err != nil {
		t.Fatalf("ResolveSource() error = %v", err)
	}

	want := filepath.Join(home, ".synaps3", "config.yaml")
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

	src, err := ResolveSource("config.yaml", false)
	if err != nil {
		t.Fatalf("ResolveSource() error = %v", err)
	}

	want := filepath.Join(home, ".synaps3", "config.yaml")
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

func TestSaveGeneratedYAML_RoundTripsAndUsesPrivatePermissions(t *testing.T) {
	cfg := validConfig()
	cfg.Server.Port = ":9091"
	cfg.Filecoin.Network = "mainnet"
	cfg.Filecoin.RPCURL = "https://example.invalid/rpc"
	cfg.Filecoin.WithCDN = true
	cfg.Filecoin.AllowPrivateNetworks = true
	cfg.Cache.MaxSizeGB = 42
	cfg.Worker.Upload.PollInterval = 7 * time.Second
	cfg.Worker.Evictor.PollInterval = 2 * time.Minute

	path := filepath.Join(t.TempDir(), "nested", "config.yaml")
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
		"port: :9091",
		"poll_interval: 7s",
		"poll_interval: 2m0s",
		"with_cdn: true",
		"allow_private_networks: true",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("generated YAML missing %q:\n%s", want, text)
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

func TestEnvManagedFieldPaths_ReturnsRecognizedOverrides(t *testing.T) {
	t.Setenv("SYNAPS3_SERVER_PORT", ":9999")
	t.Setenv("SYNAPS3_CACHE_DIR", "/tmp/synaps3-cache")
	t.Setenv("SYNAPS3_FILECOIN_NETWORK", "mainnet")
	t.Setenv("SYNAPS3_FILECOIN_RPC_URL", "https://rpc.example.invalid")

	managed := EnvManagedFieldPaths()
	for _, want := range []string{"server.port", "cache.dir", "filecoin.network", "filecoin.rpc_url"} {
		if managed[want] == "" {
			t.Fatalf("EnvManagedFieldPaths() missing %q in %#v", want, managed)
		}
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
		{env: "SYNAPS3_CACHE_MAX_SIZE_GB", field: "cache.max_size_gb"},
		{env: "SYNAPS3_ADMIN_ADDR", field: "admin.addr"},
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

	if metadata["filecoin.private_key"].Editable {
		t.Fatal("filecoin.private_key Editable = true, want false")
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
