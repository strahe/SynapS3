package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
		exists, err := fileExists(path)
		if err != nil {
			return Source{}, err
		}
		return Source{Path: path, Explicit: true, Exists: exists}, nil
	}

	if path != "" {
		exists, err := fileExists(path)
		if err != nil {
			return Source{}, err
		}
		if exists {
			return Source{Path: path, Exists: true}, nil
		}
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
	S3AccessKey        bool
	S3SecretKey        bool
	FilecoinPrivateKey bool
	DatabaseDriver     bool
	DatabaseDSN        bool
	DatabaseMaxOpen    bool
	DatabaseMaxIdle    bool
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
// overlays. Existing config files do not get synthesized runtime paths, because
// settings writes must not change absent manual fields such as database.dsn.
func LoadFileForSettings(path string) (*Config, PersistedFieldPresence, error) {
	exists, err := fileExists(path)
	if err != nil {
		return nil, PersistedFieldPresence{}, err
	}
	cfg, presence, err := loadWithOptions(path, false, !exists)
	if err != nil {
		return nil, PersistedFieldPresence{}, err
	}
	if !exists {
		presence = fullPersistedFieldPresence()
	}
	return cfg, presence, nil
}

var envFieldPaths = map[string]string{
	"SYNAPS3_SERVER_PORT":                     "server.port",
	"SYNAPS3_SERVER_MAX_CONNECTIONS":          "server.max_connections",
	"SYNAPS3_SERVER_MAX_REQUESTS":             "server.max_requests",
	"SYNAPS3_SERVER_TLS_ENABLED":              "server.tls.enabled",
	"SYNAPS3_SERVER_TLS_CERT_FILE":            "server.tls.cert_file",
	"SYNAPS3_SERVER_TLS_KEY_FILE":             "server.tls.key_file",
	"SYNAPS3_S3_ACCESS_KEY":                   "s3.access_key",
	"SYNAPS3_S3_SECRET_KEY":                   "s3.secret_key",
	"SYNAPS3_S3_REGION":                       "s3.region",
	"SYNAPS3_FILECOIN_NETWORK":                "filecoin.network",
	"SYNAPS3_FILECOIN_RPC_URL":                "filecoin.rpc_url",
	"SYNAPS3_FILECOIN_PRIVATE_KEY":            "filecoin.private_key",
	"SYNAPS3_FILECOIN_SOURCE":                 "filecoin.source",
	"SYNAPS3_FILECOIN_WITH_CDN":               "filecoin.with_cdn",
	"SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS": "filecoin.allow_private_networks",
	"SYNAPS3_DATABASE_DRIVER":                 "database.driver",
	"SYNAPS3_DATABASE_DSN":                    "database.dsn",
	"SYNAPS3_DATABASE_MAX_OPEN_CONNS":         "database.max_open_conns",
	"SYNAPS3_DATABASE_MAX_IDLE_CONNS":         "database.max_idle_conns",
	"SYNAPS3_CACHE_DIR":                       "cache.dir",
	"SYNAPS3_CACHE_MAX_SIZE_GB":               "cache.max_size_gb",
	"SYNAPS3_CACHE_EVICTION_POLICY":           "cache.eviction_policy",
	"SYNAPS3_WORKER_UPLOAD_CONCURRENCY":       "worker.upload.concurrency",
	"SYNAPS3_WORKER_UPLOAD_POLL_INTERVAL":     "worker.upload.poll_interval",
	"SYNAPS3_WORKER_UPLOAD_MAX_RETRIES":       "worker.upload.max_retries",
	"SYNAPS3_WORKER_EVICTOR_CONCURRENCY":      "worker.evictor.concurrency",
	"SYNAPS3_WORKER_EVICTOR_POLL_INTERVAL":    "worker.evictor.poll_interval",
	"SYNAPS3_WORKER_EVICTOR_MAX_RETRIES":      "worker.evictor.max_retries",
	"SYNAPS3_LOGGING_LEVEL":                   "logging.level",
	"SYNAPS3_LOGGING_FORMAT":                  "logging.format",
	"SYNAPS3_ADMIN_ADDR":                      "admin.addr",
}

// EnvFieldForName returns the config field path for a supported SYNAPS3_ env var.
func EnvFieldForName(envName string) (string, bool) {
	field, ok := envFieldPaths[strings.ToUpper(envName)]
	return field, ok
}

// EnvManagedFieldPaths returns recognized config fields currently controlled by env vars.
func EnvManagedFieldPaths() map[string]string {
	managed := make(map[string]string)
	for envName, field := range envFieldPaths {
		if _, ok := os.LookupEnv(envName); ok {
			managed[field] = envName
		}
	}
	return managed
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
	AccessKey *string `yaml:"access_key,omitempty"`
	SecretKey *string `yaml:"secret_key,omitempty"`
	Region    string  `yaml:"region"`
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
	Dir            string `yaml:"dir"`
	MaxSizeGB      int    `yaml:"max_size_gb"`
	EvictionPolicy string `yaml:"eviction_policy"`
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
			AccessKey: optionalString(cfg.S3.AccessKey, presence.S3AccessKey),
			SecretKey: optionalString(cfg.S3.SecretKey, presence.S3SecretKey),
			Region:    cfg.S3.Region,
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
			Dir:            cfg.Cache.Dir,
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
		S3AccessKey:        true,
		S3SecretKey:        true,
		FilecoinPrivateKey: true,
		DatabaseDriver:     true,
		DatabaseDSN:        true,
		DatabaseMaxOpen:    true,
		DatabaseMaxIdle:    true,
		AdminAddr:          true,
	}
}
