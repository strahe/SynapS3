package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
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
	Admin    AdminConfig    `koanf:"admin"`
}

type ServerConfig struct {
	Port           string    `koanf:"port"`
	TLS            TLSConfig `koanf:"tls"`
	MaxConnections int       `koanf:"max_connections"`
	MaxRequests    int       `koanf:"max_requests"`
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
	Network              string `koanf:"network"` // calibration | mainnet
	RPCURL               string `koanf:"rpc_url"`
	PrivateKey           string `koanf:"private_key"`
	Source               string `koanf:"source"`
	WithCDN              bool   `koanf:"with_cdn"`
	AllowPrivateNetworks bool   `koanf:"allow_private_networks"`
}

type DatabaseConfig struct {
	Driver       string `koanf:"driver"` // postgres | sqlite
	DSN          string `koanf:"dsn"`
	MaxOpenConns int    `koanf:"max_open_conns"`
	MaxIdleConns int    `koanf:"max_idle_conns"`
}

type CacheConfig struct {
	Dir            string `koanf:"dir"`
	MaxSizeGB      int    `koanf:"max_size_gb"`
	EvictionPolicy string `koanf:"eviction_policy"` // lru | manual | none
}

type WorkerConfig struct {
	Upload  WorkerPoolConfig `koanf:"upload"`
	Evictor WorkerPoolConfig `koanf:"evictor"`
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

type AdminConfig struct {
	Addr string `koanf:"addr"`
}

const appDataDirName = ".synaps3"

var userHomeDir = os.UserHomeDir

func DefaultConfig() (*Config, error) {
	cfg := defaultConfig()
	if err := applyDefaultRuntimePaths(cfg, false, false); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Port:           ":8080",
			MaxConnections: 250000,
			MaxRequests:    100000,
		},
		S3: S3Config{
			Region: "us-east-1",
		},
		Filecoin: FilecoinConfig{
			Network: "calibration",
			RPCURL:  "https://api.calibration.node.glif.io/rpc/v1",
			Source:  "synaps3",
		},
		Database: DatabaseConfig{
			Driver:       "sqlite",
			MaxOpenConns: 25,
			MaxIdleConns: 5,
		},
		Cache: CacheConfig{
			MaxSizeGB:      100,
			EvictionPolicy: "lru",
		},
		Worker: WorkerConfig{
			Upload: WorkerPoolConfig{
				Concurrency:  4,
				PollInterval: 5 * time.Second,
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
		Admin: AdminConfig{
			Addr: "127.0.0.1:9090",
		},
	}
}

func defaultAppDataDir() (string, error) {
	home, err := userHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving user home directory: %w", err)
	}
	if strings.TrimSpace(home) == "" {
		return "", errors.New("resolving user home directory: empty path")
	}
	return filepath.Join(home, appDataDirName), nil
}

func defaultSQLiteDSN(appDataDir string) string {
	dbPath := filepath.Join(appDataDir, "db", "synaps3.db")
	urlPath := filepath.ToSlash(dbPath)
	if filepath.VolumeName(dbPath) != "" && !strings.HasPrefix(urlPath, "/") {
		urlPath = "/" + urlPath
	}

	u := url.URL{
		Scheme: "file",
		Path:   urlPath,
	}
	u.RawQuery = "_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	return u.String()
}

func applyDefaultRuntimePaths(cfg *Config, hasDatabaseDSN, hasCacheDir bool) error {
	if hasDatabaseDSN && hasCacheDir {
		return nil
	}

	appDataDir, err := defaultAppDataDir()
	if err != nil {
		return err
	}
	if !hasDatabaseDSN {
		cfg.Database.DSN = defaultSQLiteDSN(appDataDir)
	}
	if !hasCacheDir {
		cfg.Cache.Dir = filepath.Join(appDataDir, "cache")
	}
	return nil
}

// Load reads configuration from a YAML file (if it exists) and overlays
// environment variables prefixed with SYNAPS3_.
func Load(path string) (*Config, error) {
	k := koanf.New(".")
	cfg := defaultConfig()

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
	if err := applyDefaultRuntimePaths(cfg, k.Exists("database.dsn"), k.Exists("cache.dir")); err != nil {
		return nil, fmt.Errorf("loading default runtime paths: %w", err)
	}

	return cfg, nil
}

// Validate checks the Config for invalid or missing values and returns all
// validation errors joined together so the operator can fix them in one pass.
func (c *Config) Validate() error {
	var errs []error

	// Server concurrency.
	if c.Server.MaxConnections < 1 {
		errs = append(errs, fmt.Errorf("server.max_connections must be >= 1, got %d", c.Server.MaxConnections))
	}
	if c.Server.MaxRequests < 1 {
		errs = append(errs, fmt.Errorf("server.max_requests must be >= 1, got %d", c.Server.MaxRequests))
	}
	if c.Server.MaxRequests > c.Server.MaxConnections {
		errs = append(errs, fmt.Errorf("server.max_requests (%d) must not exceed server.max_connections (%d)", c.Server.MaxRequests, c.Server.MaxConnections))
	}

	// TLS: cert and key required when enabled.
	if c.Server.TLS.Enabled {
		if strings.TrimSpace(c.Server.TLS.CertFile) == "" {
			errs = append(errs, errors.New("server.tls.cert_file must be set when TLS is enabled"))
		}
		if strings.TrimSpace(c.Server.TLS.KeyFile) == "" {
			errs = append(errs, errors.New("server.tls.key_file must be set when TLS is enabled"))
		}
	}

	// Cache.
	if strings.TrimSpace(c.Cache.Dir) == "" {
		errs = append(errs, errors.New("cache.dir must be non-empty"))
	}
	if c.Cache.MaxSizeGB < 1 {
		errs = append(errs, fmt.Errorf("cache.max_size_gb must be >= 1, got %d", c.Cache.MaxSizeGB))
	}

	// Eviction policy.
	switch strings.ToLower(c.Cache.EvictionPolicy) {
	case "lru", "manual", "none":
	default:
		errs = append(errs, fmt.Errorf("cache.eviction_policy must be one of [lru, manual, none], got %q", c.Cache.EvictionPolicy))
	}

	// Database.
	if strings.TrimSpace(c.Database.DSN) == "" {
		errs = append(errs, errors.New("database.dsn must be non-empty"))
	}
	switch c.Database.Driver {
	case "postgres", "sqlite":
	default:
		errs = append(errs, fmt.Errorf("database.driver must be postgres or sqlite, got %q", c.Database.Driver))
	}
	if c.Database.MaxOpenConns < 1 {
		errs = append(errs, fmt.Errorf("database.max_open_conns must be >= 1, got %d", c.Database.MaxOpenConns))
	}
	if c.Database.MaxIdleConns < 0 {
		errs = append(errs, fmt.Errorf("database.max_idle_conns must be >= 0, got %d", c.Database.MaxIdleConns))
	}
	if c.Database.MaxIdleConns > c.Database.MaxOpenConns {
		errs = append(errs, fmt.Errorf("database.max_idle_conns (%d) must not exceed max_open_conns (%d)", c.Database.MaxIdleConns, c.Database.MaxOpenConns))
	}

	// Filecoin network.
	switch strings.ToLower(c.Filecoin.Network) {
	case "calibration", "mainnet":
	default:
		errs = append(errs, fmt.Errorf("filecoin.network must be calibration or mainnet, got %q", c.Filecoin.Network))
	}

	// S3 credentials.
	if strings.TrimSpace(c.S3.AccessKey) == "" {
		errs = append(errs, errors.New("s3.access_key must be non-empty"))
	}
	if strings.TrimSpace(c.S3.SecretKey) == "" {
		errs = append(errs, errors.New("s3.secret_key must be non-empty"))
	}

	// Worker pools.
	validatePool := func(name string, p WorkerPoolConfig) {
		if p.Concurrency < 1 {
			errs = append(errs, fmt.Errorf("worker.%s.concurrency must be >= 1, got %d", name, p.Concurrency))
		}
		if p.PollInterval <= 0 {
			errs = append(errs, fmt.Errorf("worker.%s.poll_interval must be > 0, got %s", name, p.PollInterval))
		}
	}
	validatePool("upload", c.Worker.Upload)
	validatePool("evictor", c.Worker.Evictor)

	// Admin.
	if strings.TrimSpace(c.Admin.Addr) == "" {
		errs = append(errs, errors.New("admin.addr must be non-empty"))
	}

	return errors.Join(errs...)
}
