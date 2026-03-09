package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type Config struct {
	Server   ServerConfig   `koanf:"server"`
	S3       S3Config       `koanf:"s3"`
	Filecoin FilecoinConfig `koanf:"filecoin"`
	Database DatabaseConfig `koanf:"database"`
	Cache    CacheConfig    `koanf:"cache"`
	Worker   WorkerConfig   `koanf:"worker"`
	Logging  LoggingConfig  `koanf:"logging"`
}

type ServerConfig struct {
	Port string    `koanf:"port"`
	TLS  TLSConfig `koanf:"tls"`
}

type TLSConfig struct {
	Enabled  bool   `koanf:"enabled"`
	CertFile string `koanf:"cert_file"`
	KeyFile  string `koanf:"key_file"`
}

type S3Config struct {
	AccessKey string `koanf:"access_key"`
	SecretKey string `koanf:"secret_key"`
	Region    string `koanf:"region"`
}

type FilecoinConfig struct {
	Network     string `koanf:"network"` // calibration | mainnet
	RPCURL      string `koanf:"rpc_url"`
	PrivateKey  string `koanf:"private_key"`
	ProviderURL string `koanf:"provider_url"`
}

type DatabaseConfig struct {
	Driver       string `koanf:"driver"` // postgres | sqlite
	DSN          string `koanf:"dsn"`
	MaxOpenConns int    `koanf:"max_open_conns"`
	MaxIdleConns int    `koanf:"max_idle_conns"`
}

type CacheConfig struct {
	Dir               string `koanf:"dir"`
	MaxSizeGB         int    `koanf:"max_size_gb"`
	EvictionPolicy    string `koanf:"eviction_policy"` // lru | manual | none
	EvictAfterOnChain bool   `koanf:"evict_after_onchain"`
}

type WorkerConfig struct {
	Upload   WorkerPoolConfig `koanf:"upload"`
	OnChain  WorkerPoolConfig `koanf:"onchain"`
	ProofSet WorkerPoolConfig `koanf:"proofset"`
	Evictor  WorkerPoolConfig `koanf:"evictor"`
}

type WorkerPoolConfig struct {
	Concurrency  int           `koanf:"concurrency"`
	PollInterval time.Duration `koanf:"poll_interval"`
	MaxRetries   int           `koanf:"max_retries"`
}

type LoggingConfig struct {
	Level  string `koanf:"level"`  // debug | info | warn | error
	Format string `koanf:"format"` // json | text
}

func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port: ":8080",
		},
		S3: S3Config{
			Region: "us-east-1",
		},
		Filecoin: FilecoinConfig{
			Network: "calibration",
			RPCURL:  "https://api.calibration.node.glif.io/rpc/v1",
		},
		Database: DatabaseConfig{
			Driver:       "sqlite",
			DSN:          "file:synaps3.db?_pragma=journal_mode(WAL)",
			MaxOpenConns: 25,
			MaxIdleConns: 5,
		},
		Cache: CacheConfig{
			Dir:               "/var/lib/synaps3/cache",
			MaxSizeGB:         100,
			EvictionPolicy:    "lru",
			EvictAfterOnChain: true,
		},
		Worker: WorkerConfig{
			Upload: WorkerPoolConfig{
				Concurrency:  4,
				PollInterval: 5 * time.Second,
				MaxRetries:   5,
			},
			OnChain: WorkerPoolConfig{
				Concurrency:  2,
				PollInterval: 30 * time.Second,
				MaxRetries:   10,
			},
			ProofSet: WorkerPoolConfig{
				Concurrency:  1,
				PollInterval: 30 * time.Second,
				MaxRetries:   5,
			},
			Evictor: WorkerPoolConfig{
				Concurrency:  2,
				PollInterval: time.Minute,
				MaxRetries:   3,
			},
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// Load reads configuration from a YAML file (if it exists) and overlays
// environment variables prefixed with SYNAPS3_.
func Load(path string) (*Config, error) {
	k := koanf.New(".")
	cfg := DefaultConfig()

	// Load from YAML file if provided and exists.
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
				return nil, fmt.Errorf("loading config file %s: %w", path, err)
			}
		}
	}

	// Overlay environment variables: SYNAPS3_SERVER_PORT → server.port
	if err := k.Load(env.Provider("SYNAPS3_", ".", func(s string) string {
		return strings.ReplaceAll(
			strings.ToLower(strings.TrimPrefix(s, "SYNAPS3_")),
			"_", ".",
		)
	}), nil); err != nil {
		return nil, fmt.Errorf("loading env vars: %w", err)
	}

	if err := k.Unmarshal("", cfg); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	return cfg, nil
}
