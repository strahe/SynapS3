package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/toml/v2"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/strahe/synaps3/internal/model"
)

const (
	DefaultFilecoinCopies    = 3
	MaxFilecoinDefaultCopies = model.StorageCopiesMax
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
	Region string `koanf:"region"`
}

type FilecoinConfig struct {
	Network              string                      `koanf:"network"` // calibration | mainnet
	RPCURL               string                      `koanf:"rpc_url"`
	PrivateKey           string                      `koanf:"private_key"`
	Source               string                      `koanf:"source"`
	WithCDN              bool                        `koanf:"with_cdn"`
	AllowPrivateNetworks bool                        `koanf:"allow_private_networks"`
	DefaultCopies        int                         `koanf:"default_copies"`
	Observability        FilecoinObservabilityConfig `koanf:"observability"`
}

type FilecoinObservabilityConfig struct {
	Interval    time.Duration `koanf:"interval"`
	Timeout     time.Duration `koanf:"timeout"`
	Concurrency int           `koanf:"concurrency"`
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
	Upload         WorkerPoolConfig `koanf:"upload"`
	Evictor        WorkerPoolConfig `koanf:"evictor"`
	StorageCleanup WorkerPoolConfig `koanf:"storage_cleanup"`
}

type WorkerPoolConfig struct {
	Concurrency  int           `koanf:"concurrency"`
	PollInterval time.Duration `koanf:"poll_interval"`
	MaxRetries   int           `koanf:"max_retries"`
}

type LoggingConfig struct {
	Level    string                `koanf:"level"`  // debug | info | warn | error
	Format   string                `koanf:"format"` // json | text
	S3Access LoggingS3AccessConfig `koanf:"s3_access"`
}

type LoggingS3AccessConfig struct {
	Enabled bool   `koanf:"enabled"`
	Level   string `koanf:"level"` // debug | info | warn | error
}

type AdminConfig struct {
	Addr string `koanf:"addr"`
}

const appDataDirName = ".synaps3"

var userHomeDir = os.UserHomeDir

var defaultFilecoinRPCURLs = map[string]string{
	"calibration": "https://api.calibration.node.glif.io/rpc/v1",
	"mainnet":     "https://api.node.glif.io/rpc/v1",
}

// DefaultFilecoinRPCURL returns the built-in RPC URL for a Filecoin network.
func DefaultFilecoinRPCURL(network string) (string, bool) {
	rpcURL, ok := defaultFilecoinRPCURLs[strings.ToLower(strings.TrimSpace(network))]
	return rpcURL, ok
}

// DefaultFilecoinRPCURLs returns a copy of the built-in Filecoin RPC URL map.
func DefaultFilecoinRPCURLs() map[string]string {
	out := make(map[string]string, len(defaultFilecoinRPCURLs))
	for network, rpcURL := range defaultFilecoinRPCURLs {
		out[network] = rpcURL
	}
	return out
}

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
			MaxConnections: 4096,
			MaxRequests:    512,
		},
		S3: S3Config{
			Region: "us-east-1",
		},
		Filecoin: FilecoinConfig{
			Network:       "calibration",
			RPCURL:        defaultFilecoinRPCURLs["calibration"],
			Source:        "synaps3",
			DefaultCopies: DefaultFilecoinCopies,
			Observability: FilecoinObservabilityConfig{
				Interval:    5 * time.Minute,
				Timeout:     5 * time.Second,
				Concurrency: 8,
			},
		},
		Database: DatabaseConfig{
			Driver:       "sqlite",
			MaxOpenConns: 4,
			MaxIdleConns: 2,
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
			StorageCleanup: WorkerPoolConfig{
				Concurrency:  2,
				PollInterval: time.Minute,
				MaxRetries:   5,
			},
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
			S3Access: LoggingS3AccessConfig{
				Enabled: true,
				Level:   "info",
			},
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
	return u.String()
}

func applyDefaultRuntimePaths(cfg *Config, hasDatabaseDSN, hasCacheDir bool) error {
	hasDatabaseDSN = hasDatabaseDSN || strings.TrimSpace(cfg.Database.DSN) != ""
	hasCacheDir = hasCacheDir || strings.TrimSpace(cfg.Cache.Dir) != ""
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

// Load reads configuration from a TOML file (if it exists) and overlays
// environment variables prefixed with SYNAPS3_.
func Load(path string) (*Config, error) {
	return load(path, true)
}

// LoadFile reads configuration from TOML only, without environment overlays.
func LoadFile(path string) (*Config, error) {
	return load(path, false)
}

func load(path string, includeEnv bool) (*Config, error) {
	cfg, _, err := loadWithOptions(path, includeEnv, true)
	return cfg, err
}

func loadWithOptions(path string, includeEnv, applyRuntimeDefaults bool) (*Config, PersistedFieldPresence, error) {
	k := koanf.New(".")
	cfg := defaultConfig()
	fileLoaded := false

	// Load from TOML file if provided and exists.
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			if err := k.Load(file.Provider(path), toml.Parser()); err != nil {
				return nil, PersistedFieldPresence{}, fmt.Errorf("loading config file %s: %w", path, err)
			}
			fileLoaded = true
		}
	}
	presence := persistedFieldPresence(k, fileLoaded)

	if includeEnv {
		// Overlay environment variables: SYNAPS3_SERVER_PORT → server.port
		if err := k.Load(env.Provider("SYNAPS3_", ".", func(s string) string {
			if field, ok := EnvFieldForName(s); ok {
				return field
			}
			return strings.ReplaceAll(
				strings.ToLower(strings.TrimPrefix(s, "SYNAPS3_")),
				"_", ".",
			)
		}), nil); err != nil {
			return nil, PersistedFieldPresence{}, fmt.Errorf("loading env vars: %w", err)
		}
	}

	if err := k.Unmarshal("", cfg); err != nil {
		return nil, PersistedFieldPresence{}, fmt.Errorf("unmarshalling config: %w", err)
	}
	if applyRuntimeDefaults {
		if err := applyDefaultRuntimePaths(cfg, k.Exists("database.dsn"), k.Exists("cache.dir")); err != nil {
			return nil, PersistedFieldPresence{}, fmt.Errorf("loading default runtime paths: %w", err)
		}
	}

	return cfg, presence, nil
}

func persistedFieldPresence(k *koanf.Koanf, fileLoaded bool) PersistedFieldPresence {
	if !fileLoaded {
		return PersistedFieldPresence{}
	}
	return PersistedFieldPresence{
		FilecoinPrivateKey: k.Exists("filecoin.private_key"),
		DatabaseDriver:     k.Exists("database.driver"),
		DatabaseDSN:        k.Exists("database.dsn"),
		DatabaseMaxOpen:    k.Exists("database.max_open_conns"),
		DatabaseMaxIdle:    k.Exists("database.max_idle_conns"),
		CacheDir:           k.Exists("cache.dir"),
		AdminAddr:          k.Exists("admin.addr"),
	}
}

// FieldValidationErrors returns validation errors tied to dotted config paths.
func (c *Config) FieldValidationErrors() []FieldError {
	var errs []FieldError

	add := func(field, message string) {
		errs = append(errs, FieldError{Field: field, Message: message})
	}

	// S3 listen address.
	if msg := validateListenAddress(c.Server.Port); msg != "" {
		add("server.port", msg)
	}

	// Server concurrency.
	if c.Server.MaxConnections < 1 {
		add("server.max_connections", fmt.Sprintf("must be >= 1, got %d", c.Server.MaxConnections))
	}
	if c.Server.MaxRequests < 1 {
		add("server.max_requests", fmt.Sprintf("must be >= 1, got %d", c.Server.MaxRequests))
	}
	if c.Server.MaxRequests > c.Server.MaxConnections {
		add("server.max_requests", fmt.Sprintf("(%d) must not exceed server.max_connections (%d)", c.Server.MaxRequests, c.Server.MaxConnections))
	}

	// TLS: cert and key required when enabled.
	if c.Server.TLS.Enabled {
		if strings.TrimSpace(c.Server.TLS.CertFile) == "" {
			add("server.tls.cert_file", "must be set when TLS is enabled")
		}
		if strings.TrimSpace(c.Server.TLS.KeyFile) == "" {
			add("server.tls.key_file", "must be set when TLS is enabled")
		}
	}

	// Cache.
	if strings.TrimSpace(c.Cache.Dir) == "" {
		add("cache.dir", "must be non-empty")
	}
	if c.Cache.MaxSizeGB < 1 {
		add("cache.max_size_gb", fmt.Sprintf("must be >= 1, got %d", c.Cache.MaxSizeGB))
	}

	// Eviction policy.
	switch strings.ToLower(c.Cache.EvictionPolicy) {
	case "lru", "manual", "none":
	default:
		add("cache.eviction_policy", fmt.Sprintf("must be one of [lru, manual, none], got %q", c.Cache.EvictionPolicy))
	}

	// Database.
	if strings.TrimSpace(c.Database.DSN) == "" {
		add("database.dsn", "must be non-empty")
	}
	switch c.Database.Driver {
	case "postgres", "sqlite":
	default:
		add("database.driver", fmt.Sprintf("must be postgres or sqlite, got %q", c.Database.Driver))
	}
	if c.Database.MaxOpenConns < 1 {
		add("database.max_open_conns", fmt.Sprintf("must be >= 1, got %d", c.Database.MaxOpenConns))
	}
	if c.Database.MaxIdleConns < 0 {
		add("database.max_idle_conns", fmt.Sprintf("must be >= 0, got %d", c.Database.MaxIdleConns))
	}
	if c.Database.MaxIdleConns > c.Database.MaxOpenConns {
		add("database.max_idle_conns", fmt.Sprintf("(%d) must not exceed max_open_conns (%d)", c.Database.MaxIdleConns, c.Database.MaxOpenConns))
	}

	// Filecoin network.
	switch strings.ToLower(c.Filecoin.Network) {
	case "calibration", "mainnet":
	default:
		add("filecoin.network", fmt.Sprintf("must be calibration or mainnet, got %q", c.Filecoin.Network))
	}
	if msg := validateRPCURL(c.Filecoin.RPCURL); msg != "" {
		add("filecoin.rpc_url", msg)
	}
	if strings.TrimSpace(c.Filecoin.Source) == "" {
		add("filecoin.source", "must be non-empty")
	}
	if strings.TrimSpace(c.Filecoin.PrivateKey) == "" {
		add("filecoin.private_key", "must be non-empty")
	}
	if !model.ValidStorageCopies(c.Filecoin.DefaultCopies) {
		if c.Filecoin.DefaultCopies < model.StorageCopiesMin {
			add("filecoin.default_copies", fmt.Sprintf("must be >= %d, got %d", model.StorageCopiesMin, c.Filecoin.DefaultCopies))
		} else {
			add("filecoin.default_copies", fmt.Sprintf("must be <= %d, got %d", model.StorageCopiesMax, c.Filecoin.DefaultCopies))
		}
	}
	if c.Filecoin.Observability.Interval <= 0 {
		add("filecoin.observability.interval", fmt.Sprintf("must be > 0, got %s", c.Filecoin.Observability.Interval))
	}
	if c.Filecoin.Observability.Timeout <= 0 {
		add("filecoin.observability.timeout", fmt.Sprintf("must be > 0, got %s", c.Filecoin.Observability.Timeout))
	}
	if c.Filecoin.Observability.Concurrency < 1 {
		add("filecoin.observability.concurrency", fmt.Sprintf("must be >= 1, got %d", c.Filecoin.Observability.Concurrency))
	}

	// S3 credentials.
	if strings.TrimSpace(c.S3.Region) == "" {
		add("s3.region", "must be non-empty")
	}

	// Worker pools.
	validatePool := func(name string, p WorkerPoolConfig) {
		if p.Concurrency < 1 {
			add(fmt.Sprintf("worker.%s.concurrency", name), fmt.Sprintf("must be >= 1, got %d", p.Concurrency))
		}
		if p.PollInterval <= 0 {
			add(fmt.Sprintf("worker.%s.poll_interval", name), fmt.Sprintf("must be > 0, got %s", p.PollInterval))
		}
		if p.MaxRetries < 0 {
			add(fmt.Sprintf("worker.%s.max_retries", name), fmt.Sprintf("must be >= 0, got %d", p.MaxRetries))
		}
	}
	validatePool("upload", c.Worker.Upload)
	validatePool("evictor", c.Worker.Evictor)
	validatePool("storage_cleanup", c.Worker.StorageCleanup)

	// Logging.
	switch c.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		add("logging.level", fmt.Sprintf("must be one of [debug, info, warn, error], got %q", c.Logging.Level))
	}
	switch c.Logging.Format {
	case "json", "text":
	default:
		add("logging.format", fmt.Sprintf("must be json or text, got %q", c.Logging.Format))
	}
	switch c.Logging.S3Access.Level {
	case "debug", "info", "warn", "error":
	default:
		add("logging.s3_access.level", fmt.Sprintf("must be one of [debug, info, warn, error], got %q", c.Logging.S3Access.Level))
	}

	// Admin.
	if strings.TrimSpace(c.Admin.Addr) == "" {
		add("admin.addr", "must be non-empty")
	}

	return errs
}

func validateListenAddress(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "must be non-empty"
	}
	_, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "must be a host:port address such as :8080"
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "port must be between 1 and 65535"
	}
	return ""
}

func validateRPCURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "must be non-empty"
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "must be an absolute URL"
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https", "ws", "wss":
	default:
		return "scheme must be one of [http, https, ws, wss]"
	}
	return ""
}

// Validate checks the Config for invalid or missing values and returns all
// validation errors joined together so the operator can fix them in one pass.
func (c *Config) Validate() error {
	fieldErrs := c.FieldValidationErrors()
	errs := make([]error, 0, len(fieldErrs))
	for _, fieldErr := range fieldErrs {
		errs = append(errs, fieldErr)
	}
	return errors.Join(errs...)
}
