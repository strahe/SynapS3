package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const generatedConfigFileName = "config.toml"

// Source describes the config file SynapS3 should read from and persist to.
type Source struct {
	Path             string
	Explicit         bool
	Exists           bool
	GeneratedDefault bool
}

// InitOptions controls app data directory initialization.
type InitOptions struct {
	Dir string
}

// InitResult describes the paths created by app data directory initialization.
type InitResult struct {
	Dir         string
	ConfigPath  string
	DatabaseDir string
	CacheDir    string
	DefaultDir  bool
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

// InitAppDataDir creates a reference config file and runtime directories.
func InitAppDataDir(opts InitOptions) (InitResult, error) {
	appDir := strings.TrimSpace(opts.Dir)
	defaultDir := appDir == ""
	if defaultDir {
		var err error
		appDir, err = defaultAppDataDir()
		if err != nil {
			return InitResult{}, err
		}
	} else {
		absDir, err := filepath.Abs(appDir)
		if err != nil {
			return InitResult{}, fmt.Errorf("resolving app data directory %s: %w", appDir, err)
		}
		appDir = absDir
	}

	result := InitResult{
		Dir:         appDir,
		ConfigPath:  filepath.Join(appDir, generatedConfigFileName),
		DatabaseDir: filepath.Join(appDir, "db"),
		CacheDir:    filepath.Join(appDir, "cache"),
		DefaultDir:  defaultDir,
	}

	exists, err := fileExists(result.ConfigPath)
	if err != nil {
		return InitResult{}, err
	}
	if exists {
		return InitResult{}, fmt.Errorf("config file %s already exists; back it up or delete it before running init again", result.ConfigPath)
	}

	for _, dir := range []string{result.Dir, result.DatabaseDir, result.CacheDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return InitResult{}, fmt.Errorf("creating directory %s: %w", dir, err)
		}
	}

	if err := writeInitConfig(result.ConfigPath, result.Dir); err != nil {
		return InitResult{}, err
	}

	return result, nil
}

// LoadSource reads configuration from a resolved source.
func LoadSource(src Source) (*Config, error) {
	return Load(src.Path)
}

// PersistedFieldPresence records manual/read-only fields that were explicitly
// present in an existing TOML file. Settings writes use it to avoid
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

// Save writes a generated TOML config file. Existing custom comments and formatting are not preserved.
func Save(path string, cfg *Config) error {
	return save(path, cfg, saveOptions{})
}

// SaveForSettings writes settings TOML while preserving absent manual/read-only fields.
func SaveForSettings(path string, cfg *Config, presence PersistedFieldPresence) error {
	return save(path, cfg, saveOptions{presence: &presence})
}

func writeInitConfig(path, appDir string) error {
	data := []byte(renderInitConfig(appDir))

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("config file %s already exists; back it up or delete it before running init again", path)
		}
		return fmt.Errorf("creating config file %s: %w", path, err)
	}
	created := false
	defer func() {
		if !created {
			_ = f.Close()
			_ = os.Remove(path)
		}
	}()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("writing config file %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing config file %s: %w", path, err)
	}
	created = true
	return nil
}

type initSectionDescriptor struct {
	Name   string
	Fields []initFieldDescriptor
}

type initFieldDescriptor struct {
	Field   string
	Key     string
	Value   string
	Enabled bool
	Notes   []string
}

func renderInitConfig(appDir string) string {
	defaults := defaultConfig()
	defaults.Database.DSN = defaultSQLiteDSN(appDir)
	defaults.Cache.Dir = filepath.Join(appDir, "cache")
	return renderTOMLConfig(defaults, PersistedFieldPresence{
		FilecoinPrivateKey: true,
		DatabaseDriver:     true,
		DatabaseDSN:        true,
		CacheDir:           true,
	}, false)
}

func renderSavedConfig(cfg *Config, presence PersistedFieldPresence) string {
	return renderTOMLConfig(cfg, presence, true)
}

func renderTOMLConfig(cfg *Config, presence PersistedFieldPresence, saveMode bool) string {
	sections := []initSectionDescriptor{
		{
			Name: "server",
			Fields: []initFieldDescriptor{
				{Field: "server.port", Key: "port", Value: quoteTOMLString(cfg.Server.Port), Enabled: saveMode},
				{Field: "server.max_connections", Key: "max_connections", Value: strconv.Itoa(cfg.Server.MaxConnections), Enabled: saveMode},
				{Field: "server.max_requests", Key: "max_requests", Value: strconv.Itoa(cfg.Server.MaxRequests), Enabled: saveMode},
			},
		},
		{
			Name: "server.tls",
			Fields: []initFieldDescriptor{
				{Field: "server.tls.enabled", Key: "enabled", Value: strconv.FormatBool(cfg.Server.TLS.Enabled), Enabled: saveMode},
				{Field: "server.tls.cert_file", Key: "cert_file", Value: quoteTOMLString(cfg.Server.TLS.CertFile), Enabled: saveMode, Notes: []string{"Required when server.tls.enabled is true."}},
				{Field: "server.tls.key_file", Key: "key_file", Value: quoteTOMLString(cfg.Server.TLS.KeyFile), Enabled: saveMode, Notes: []string{"Required when server.tls.enabled is true."}},
			},
		},
		{
			Name: "s3",
			Fields: []initFieldDescriptor{
				{Field: "s3.region", Key: "region", Value: quoteTOMLString(cfg.S3.Region), Enabled: saveMode},
			},
		},
		{
			Name: "filecoin",
			Fields: []initFieldDescriptor{
				{Field: "filecoin.network", Key: "network", Value: quoteTOMLString(cfg.Filecoin.Network), Enabled: saveMode, Notes: []string{"Allowed: calibration, mainnet."}},
				{Field: "filecoin.rpc_url", Key: "rpc_url", Value: quoteTOMLString(cfg.Filecoin.RPCURL), Enabled: saveMode},
				{Field: "filecoin.private_key", Key: "private_key", Value: quoteTOMLString(cfg.Filecoin.PrivateKey), Enabled: !saveMode || presence.FilecoinPrivateKey, Notes: []string{"Required before serving unless SYNAPS3_FILECOIN_PRIVATE_KEY is set."}},
				{Field: "filecoin.source", Key: "source", Value: quoteTOMLString(cfg.Filecoin.Source), Enabled: saveMode},
				{Field: "filecoin.with_cdn", Key: "with_cdn", Value: strconv.FormatBool(cfg.Filecoin.WithCDN), Enabled: saveMode},
				{Field: "filecoin.allow_private_networks", Key: "allow_private_networks", Value: strconv.FormatBool(cfg.Filecoin.AllowPrivateNetworks), Enabled: saveMode, Notes: []string{"Enable only for trusted private deployments."}},
				{Field: "filecoin.default_copies", Key: "default_copies", Value: strconv.Itoa(cfg.Filecoin.DefaultCopies), Enabled: saveMode, Notes: []string{fmt.Sprintf("Allowed range: 1-%d.", MaxFilecoinDefaultCopies)}},
			},
		},
		{
			Name: "database",
			Fields: []initFieldDescriptor{
				{Field: "database.driver", Key: "driver", Value: quoteTOMLString(cfg.Database.Driver), Enabled: !saveMode || presence.DatabaseDriver, Notes: []string{"Enabled with database.dsn so this installation uses SQLite at the initialized path.", "Allowed: sqlite, postgres."}},
				{Field: "database.dsn", Key: "dsn", Value: quoteTOMLString(cfg.Database.DSN), Enabled: !saveMode || presence.DatabaseDSN},
				{Field: "database.max_open_conns", Key: "max_open_conns", Value: strconv.Itoa(cfg.Database.MaxOpenConns), Enabled: saveMode && presence.DatabaseMaxOpen},
				{Field: "database.max_idle_conns", Key: "max_idle_conns", Value: strconv.Itoa(cfg.Database.MaxIdleConns), Enabled: saveMode && presence.DatabaseMaxIdle},
			},
		},
		{
			Name: "cache",
			Fields: []initFieldDescriptor{
				{Field: "cache.dir", Key: "dir", Value: quoteTOMLString(cfg.Cache.Dir), Enabled: !saveMode || presence.CacheDir, Notes: []string{"Enabled so this installation uses the initialized cache directory."}},
				{Field: "cache.max_size_gb", Key: "max_size_gb", Value: strconv.Itoa(cfg.Cache.MaxSizeGB), Enabled: saveMode},
				{Field: "cache.eviction_policy", Key: "eviction_policy", Value: quoteTOMLString(cfg.Cache.EvictionPolicy), Enabled: saveMode, Notes: []string{"Allowed: lru, manual, none."}},
			},
		},
		{
			Name: "worker.upload",
			Fields: []initFieldDescriptor{
				{Field: "worker.upload.concurrency", Key: "concurrency", Value: strconv.Itoa(cfg.Worker.Upload.Concurrency), Enabled: saveMode},
				{Field: "worker.upload.poll_interval", Key: "poll_interval", Value: quoteTOMLString(cfg.Worker.Upload.PollInterval.String()), Enabled: saveMode},
				{Field: "worker.upload.max_retries", Key: "max_retries", Value: strconv.Itoa(cfg.Worker.Upload.MaxRetries), Enabled: saveMode},
			},
		},
		{
			Name: "worker.evictor",
			Fields: []initFieldDescriptor{
				{Field: "worker.evictor.concurrency", Key: "concurrency", Value: strconv.Itoa(cfg.Worker.Evictor.Concurrency), Enabled: saveMode},
				{Field: "worker.evictor.poll_interval", Key: "poll_interval", Value: quoteTOMLString(cfg.Worker.Evictor.PollInterval.String()), Enabled: saveMode},
				{Field: "worker.evictor.max_retries", Key: "max_retries", Value: strconv.Itoa(cfg.Worker.Evictor.MaxRetries), Enabled: saveMode},
			},
		},
		{
			Name: "logging",
			Fields: []initFieldDescriptor{
				{Field: "logging.level", Key: "level", Value: quoteTOMLString(cfg.Logging.Level), Enabled: saveMode, Notes: []string{"Allowed: debug, info, warn, error."}},
				{Field: "logging.format", Key: "format", Value: quoteTOMLString(cfg.Logging.Format), Enabled: saveMode, Notes: []string{"Allowed: json, text."}},
			},
		},
		{
			Name: "admin",
			Fields: []initFieldDescriptor{
				{Field: "admin.addr", Key: "addr", Value: quoteTOMLString(cfg.Admin.Addr), Enabled: saveMode && presence.AdminAddr},
			},
		},
	}

	var b bytes.Buffer
	b.WriteString("# SynapS3 configuration\n")
	if saveMode {
		b.WriteString("# Generated by SynapS3 settings persistence.\n")
	} else {
		b.WriteString("# Generated by synaps3 init.\n")
		b.WriteString("# Commented values show built-in defaults. Uncomment a value to override it.\n")
	}
	b.WriteString("# Environment variables listed below override config values and built-in defaults.\n\n")
	for i, section := range sections {
		if i > 0 {
			b.WriteByte('\n')
		}
		renderTOMLSection(&b, section)
	}
	return b.String()
}

func renderTOMLSection(b *bytes.Buffer, section initSectionDescriptor) {
	writeTOMLValueLine(b, false, "["+section.Name+"]")
	for i, field := range section.Fields {
		if i > 0 {
			b.WriteByte('\n')
		}
		renderTOMLField(b, field)
	}
}

func renderTOMLField(b *bytes.Buffer, field initFieldDescriptor) {
	meta := fieldMetadataByPath[field.Field]
	writeTOMLComment(b, meta.Description)
	if meta.Env != "" {
		writeTOMLComment(b, "Env: "+meta.Env)
	}
	for _, note := range field.Notes {
		writeTOMLComment(b, note)
	}
	value := field.Value
	if !field.Enabled && meta.Secret {
		value = quoteTOMLString("")
	}
	writeTOMLValueLine(b, !field.Enabled, field.Key+" = "+value)
}

func writeTOMLComment(b *bytes.Buffer, text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	b.WriteString("# ")
	b.WriteString(text)
	b.WriteByte('\n')
}

func writeTOMLValueLine(b *bytes.Buffer, commented bool, text string) {
	if commented {
		b.WriteString("# ")
	}
	b.WriteString(text)
	b.WriteByte('\n')
}

func quoteTOMLString(s string) string {
	return strconv.Quote(s)
}

func save(path string, cfg *Config, opts saveOptions) error {
	presence := fullPersistedFieldPresence()
	if opts.presence != nil {
		presence = *opts.presence
	}
	data := []byte(renderSavedConfig(cfg, presence))

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

// LoadFileForSettings reads TOML for settings persistence without environment
// overlays. Runtime defaults are applied for validation and UI display, while
// field presence lets settings writes preserve omitted TOML fields.
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
