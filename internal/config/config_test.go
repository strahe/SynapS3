package config

import (
	"strings"
	"testing"
	"time"
)

// validConfig returns a Config that passes Validate().
func validConfig() *Config {
	cfg := DefaultConfig()
	cfg.S3.AccessKey = "minioadmin"
	cfg.S3.SecretKey = "minioadmin"
	return cfg
}

func TestValidate_DefaultConfig(t *testing.T) {
	cfg := validConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got: %v", err)
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

func TestValidate_EmptyAccessKey(t *testing.T) {
	cfg := validConfig()
	cfg.S3.AccessKey = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty access_key")
	}
	if !strings.Contains(err.Error(), "access_key") {
		t.Fatalf("expected access_key error, got: %v", err)
	}
}

func TestValidate_EmptySecretKey(t *testing.T) {
	cfg := validConfig()
	cfg.S3.SecretKey = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty secret_key")
	}
	if !strings.Contains(err.Error(), "secret_key") {
		t.Fatalf("expected secret_key error, got: %v", err)
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
	cfg.Worker.OnChain.PollInterval = 0

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for poll_interval=0")
	}
	if !strings.Contains(err.Error(), "poll_interval") {
		t.Fatalf("expected poll_interval error, got: %v", err)
	}
}

func TestValidate_MultipleErrors(t *testing.T) {
	cfg := validConfig()
	cfg.Database.Driver = "mysql"
	cfg.Database.MaxOpenConns = 0
	cfg.S3.AccessKey = ""
	cfg.Filecoin.Network = "devnet"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected multiple errors")
	}

	msg := err.Error()
	for _, want := range []string{"database.driver", "max_open_conns", "access_key", "filecoin.network"} {
		if !strings.Contains(msg, want) {
			t.Errorf("expected error to mention %q, got: %v", want, msg)
		}
	}
}

func TestLoad_DefaultConfig(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load('') failed: %v", err)
	}

	def := DefaultConfig()
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

func TestValidate_MaxSPDownloadSize_Negative(t *testing.T) {
	cfg := validConfig()
	cfg.Cache.MaxSPDownloadSize = -1

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for negative MaxSPDownloadSize")
	}
	if !strings.Contains(err.Error(), "max_sp_download_size") {
		t.Fatalf("expected max_sp_download_size error, got: %v", err)
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

