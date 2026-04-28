package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.yaml.in/yaml/v3"
)

const generatedConfigFileName = "config.yaml"

// Source describes the config file SynapS3 should read from and persist to.
type Source struct {
	Path             string
	Explicit         bool
	Exists           bool
	GeneratedDefault bool
}

// FieldError is a validation error tied to a dotted config field path.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e FieldError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return e.Field + " " + e.Message
}

// ResolveSource chooses the config source for startup and settings persistence.
func ResolveSource(path string, explicit bool) (Source, error) {
	if explicit {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return Source{}, fmt.Errorf("resolving config path %s: %w", path, err)
		}
		exists, err := fileExists(absPath)
		if err != nil {
			return Source{}, err
		}
		return Source{Path: absPath, Explicit: true, Exists: exists}, nil
	}

	appDataDir, err := defaultAppDataDir()
	if err != nil {
		return Source{}, err
	}
	defaultPath := filepath.Join(appDataDir, generatedConfigFileName)
	exists, err := fileExists(defaultPath)
	if err != nil {
		return Source{}, err
	}
	return Source{Path: defaultPath, Exists: exists, GeneratedDefault: true}, nil
}

// LoadSource reads configuration from a resolved source.
func LoadSource(src Source) (*Config, error) {
	return Load(src.Path)
}

// PersistedFieldPresence records manual/read-only fields that were explicitly
// present in an existing YAML file. Settings writes use it to avoid
// materializing defaults for fields the browser cannot edit.
type PersistedFieldPresence struct {
	FilecoinPrivateKey bool
	DatabaseDriver     bool
	DatabaseDSN        bool
	DatabaseMaxOpen    bool
	DatabaseMaxIdle    bool
	CacheDir           bool
	AdminAddr          bool
}

// Save writes a generated YAML config file. Existing comments and formatting are not preserved.
func Save(path string, cfg *Config) error {
	return save(path, cfg, saveOptions{})
}

// SaveForSettings writes settings YAML while preserving absent manual/read-only fields.
func SaveForSettings(path string, cfg *Config, presence PersistedFieldPresence) error {
	return save(path, cfg, saveOptions{presence: &presence})
}

func save(path string, cfg *Config, opts saveOptions) error {
	data, err := yaml.Marshal(toYAMLConfig(cfg, opts))
	if err != nil {
		return fmt.Errorf("marshalling config yaml: %w", err)
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("creating config directory %s: %w", dir, err)
		}
	}

	tmp, err := os.CreateTemp(dir, ".synaps3-config-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temporary config file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("setting temporary config permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing temporary config file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporary config file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replacing config file %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("setting config permissions: %w", err)
	}
	return nil
}

type saveOptions struct {
	presence *PersistedFieldPresence
}

// LoadFileForSettings reads YAML for settings persistence without environment
// overlays. Runtime defaults are applied for validation and UI display, while
// field presence lets settings writes preserve omitted YAML fields.
func LoadFileForSettings(path string) (*Config, PersistedFieldPresence, error) {
	exists, err := fileExists(path)
	if err != nil {
		return nil, PersistedFieldPresence{}, err
	}
	cfg, presence, err := loadWithOptions(path, false, true)
	if err != nil {
		return nil, PersistedFieldPresence{}, err
	}
	if !exists {
		presence = fullPersistedFieldPresence()
	}
	return cfg, presence, nil
}

func fileExists(path string) (bool, error) {
	if path == "" {
		return false, nil
	}
	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return false, fmt.Errorf("config path %s is a directory", path)
		}
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("checking config path %s: %w", path, err)
}

type yamlConfig struct {
	Server   yamlServerConfig   `yaml:"server"`
	S3       yamlS3Config       `yaml:"s3"`
	Filecoin yamlFilecoinConfig `yaml:"filecoin"`
	Database yamlDatabaseConfig `yaml:"database"`
	Cache    yamlCacheConfig    `yaml:"cache"`
	Worker   yamlWorkerConfig   `yaml:"worker"`
	Logging  yamlLoggingConfig  `yaml:"logging"`
	Admin    yamlAdminConfig    `yaml:"admin"`
}

type yamlServerConfig struct {
	Port           string        `yaml:"port"`
	TLS            yamlTLSConfig `yaml:"tls"`
	MaxConnections int           `yaml:"max_connections"`
	MaxRequests    int           `yaml:"max_requests"`
}

type yamlTLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type yamlS3Config struct {
	Region string `yaml:"region"`
}

type yamlFilecoinConfig struct {
	Network              string  `yaml:"network"`
	RPCURL               string  `yaml:"rpc_url"`
	PrivateKey           *string `yaml:"private_key,omitempty"`
	Source               string  `yaml:"source"`
	WithCDN              bool    `yaml:"with_cdn"`
	AllowPrivateNetworks bool    `yaml:"allow_private_networks"`
}

type yamlDatabaseConfig struct {
	Driver       *string `yaml:"driver,omitempty"`
	DSN          *string `yaml:"dsn,omitempty"`
	MaxOpenConns *int    `yaml:"max_open_conns,omitempty"`
	MaxIdleConns *int    `yaml:"max_idle_conns,omitempty"`
}

type yamlCacheConfig struct {
	Dir            *string `yaml:"dir,omitempty"`
	MaxSizeGB      int     `yaml:"max_size_gb"`
	EvictionPolicy string  `yaml:"eviction_policy"`
}

type yamlWorkerConfig struct {
	Upload  yamlWorkerPoolConfig `yaml:"upload"`
	Evictor yamlWorkerPoolConfig `yaml:"evictor"`
}

type yamlWorkerPoolConfig struct {
	Concurrency  int    `yaml:"concurrency"`
	PollInterval string `yaml:"poll_interval"`
	MaxRetries   int    `yaml:"max_retries"`
}

type yamlLoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type yamlAdminConfig struct {
	Addr *string `yaml:"addr,omitempty"`
}

func toYAMLConfig(cfg *Config, opts saveOptions) yamlConfig {
	presence := fullPersistedFieldPresence()
	if opts.presence != nil {
		presence = *opts.presence
	}

	return yamlConfig{
		Server: yamlServerConfig{
			Port: cfg.Server.Port,
			TLS: yamlTLSConfig{
				Enabled:  cfg.Server.TLS.Enabled,
				CertFile: cfg.Server.TLS.CertFile,
				KeyFile:  cfg.Server.TLS.KeyFile,
			},
			MaxConnections: cfg.Server.MaxConnections,
			MaxRequests:    cfg.Server.MaxRequests,
		},
		S3: yamlS3Config{
			Region: cfg.S3.Region,
		},
		Filecoin: yamlFilecoinConfig{
			Network:              cfg.Filecoin.Network,
			RPCURL:               cfg.Filecoin.RPCURL,
			PrivateKey:           optionalString(cfg.Filecoin.PrivateKey, presence.FilecoinPrivateKey),
			Source:               cfg.Filecoin.Source,
			WithCDN:              cfg.Filecoin.WithCDN,
			AllowPrivateNetworks: cfg.Filecoin.AllowPrivateNetworks,
		},
		Database: yamlDatabaseConfig{
			Driver:       optionalString(cfg.Database.Driver, presence.DatabaseDriver),
			DSN:          optionalString(cfg.Database.DSN, presence.DatabaseDSN),
			MaxOpenConns: optionalInt(cfg.Database.MaxOpenConns, presence.DatabaseMaxOpen),
			MaxIdleConns: optionalInt(cfg.Database.MaxIdleConns, presence.DatabaseMaxIdle),
		},
		Cache: yamlCacheConfig{
			Dir:            optionalString(cfg.Cache.Dir, presence.CacheDir),
			MaxSizeGB:      cfg.Cache.MaxSizeGB,
			EvictionPolicy: cfg.Cache.EvictionPolicy,
		},
		Worker: yamlWorkerConfig{
			Upload:  toYAMLWorkerPool(cfg.Worker.Upload),
			Evictor: toYAMLWorkerPool(cfg.Worker.Evictor),
		},
		Logging: yamlLoggingConfig{
			Level:  cfg.Logging.Level,
			Format: cfg.Logging.Format,
		},
		Admin: yamlAdminConfig{Addr: optionalString(cfg.Admin.Addr, presence.AdminAddr)},
	}
}

func toYAMLWorkerPool(cfg WorkerPoolConfig) yamlWorkerPoolConfig {
	return yamlWorkerPoolConfig{
		Concurrency:  cfg.Concurrency,
		PollInterval: durationString(cfg.PollInterval),
		MaxRetries:   cfg.MaxRetries,
	}
}

func durationString(d time.Duration) string {
	return d.String()
}

func optionalString(value string, include bool) *string {
	if !include {
		return nil
	}
	return &value
}

func optionalInt(value int, include bool) *int {
	if !include {
		return nil
	}
	return &value
}

func fullPersistedFieldPresence() PersistedFieldPresence {
	return PersistedFieldPresence{
		FilecoinPrivateKey: true,
		DatabaseDriver:     true,
		DatabaseDSN:        true,
		DatabaseMaxOpen:    true,
		DatabaseMaxIdle:    true,
		CacheDir:           true,
		AdminAddr:          true,
	}
}
