package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/strahe/synaps3/internal/config"
	"github.com/strahe/synaps3/internal/securetoken"
)

const (
	settingsWriteHeader      = "X-SynapS3-Settings-Write"
	settingsWriteHeaderValue = "1"
)

type SettingsService struct {
	mu              sync.Mutex
	writeMu         sync.Mutex
	source          config.Source
	effective       *config.Config
	persisted       *config.Config
	restartRequired bool
}

func NewSettingsService(effective *config.Config, source config.Source) (*SettingsService, error) {
	persisted, _, err := config.LoadFileForSettings(source.Path)
	if err != nil {
		return nil, fmt.Errorf("loading persisted config: %w", err)
	}

	return &SettingsService{
		source:    source,
		effective: cloneConfig(effective),
		persisted: cloneConfig(persisted),
	}, nil
}

func (s *SettingsService) Snapshot(writable bool) settingsResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.snapshotLocked(writable)
}

func (s *SettingsService) Update(req settingsUpdateRequest, writable bool) (settingsResponse, []config.FieldError, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	next, fieldPresence, fieldErrs, err := s.settingsDraft(req)
	if err != nil {
		return settingsResponse{}, nil, err
	}
	if len(fieldErrs) > 0 {
		resp := s.snapshotFromConfig(next, writable)
		resp.ValidationErrors = fieldErrs
		return resp, fieldErrs, nil
	}

	if err := config.SaveForSettings(s.source.Path, next, fieldPresence); err != nil {
		return settingsResponse{}, nil, err
	}
	effective, err := config.LoadSource(s.source)
	if err != nil {
		return settingsResponse{}, nil, fmt.Errorf("reloading saved config: %w", err)
	}
	persisted, _, err := config.LoadFileForSettings(s.source.Path)
	if err != nil {
		return settingsResponse{}, nil, fmt.Errorf("reloading persisted config: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.effective = cloneConfig(effective)
	s.persisted = cloneConfig(persisted)
	s.restartRequired = true

	return s.snapshotLocked(writable), nil, nil
}

func (s *SettingsService) Validate(req settingsUpdateRequest) ([]config.FieldError, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	_, _, fieldErrs, err := s.settingsDraft(req)
	if err != nil {
		return nil, err
	}
	return fieldErrs, nil
}

func (s *SettingsService) settingsDraft(req settingsUpdateRequest) (*config.Config, config.PersistedFieldPresence, []config.FieldError, error) {
	persisted, fieldPresence, err := config.LoadFileForSettings(s.source.Path)
	if err != nil {
		return nil, config.PersistedFieldPresence{}, nil, fmt.Errorf("loading current config: %w", err)
	}
	next := cloneConfig(persisted)
	managed := config.EnvManagedFieldPaths()
	var fieldErrs []config.FieldError

	checkField := func(field string) bool {
		if envName, ok := managed[field]; ok {
			fieldErrs = append(fieldErrs, config.FieldError{
				Field:   field,
				Message: "is managed by " + envName,
			})
			return false
		}
		return true
	}
	setString := func(field string, target *string, value *string) bool {
		if value == nil || !checkField(field) {
			return false
		}
		*target = *value
		return true
	}
	setInt := func(field string, target *int, value *int) {
		if value == nil || !checkField(field) {
			return
		}
		*target = *value
	}
	setBool := func(field string, target *bool, value *bool) {
		if value == nil || !checkField(field) {
			return
		}
		*target = *value
	}
	setDuration := func(field string, target *time.Duration, value *string) {
		if value == nil || !checkField(field) {
			return
		}
		parsed, err := time.ParseDuration(*value)
		if err != nil {
			fieldErrs = append(fieldErrs, config.FieldError{Field: field, Message: "must be a valid duration"})
			return
		}
		*target = parsed
	}

	if req.Server != nil {
		setString("server.port", &next.Server.Port, req.Server.Port)
		setInt("server.max_connections", &next.Server.MaxConnections, req.Server.MaxConnections)
		setInt("server.max_requests", &next.Server.MaxRequests, req.Server.MaxRequests)
		if req.Server.TLS != nil {
			setBool("server.tls.enabled", &next.Server.TLS.Enabled, req.Server.TLS.Enabled)
			setString("server.tls.cert_file", &next.Server.TLS.CertFile, req.Server.TLS.CertFile)
			setString("server.tls.key_file", &next.Server.TLS.KeyFile, req.Server.TLS.KeyFile)
		}
	}
	if req.S3 != nil {
		setString("s3.region", &next.S3.Region, req.S3.Region)
	}
	if req.Filecoin != nil {
		setString("filecoin.network", &next.Filecoin.Network, req.Filecoin.Network)
		setString("filecoin.rpc_url", &next.Filecoin.RPCURL, req.Filecoin.RPCURL)
		setString("filecoin.source", &next.Filecoin.Source, req.Filecoin.Source)
		setBool("filecoin.with_cdn", &next.Filecoin.WithCDN, req.Filecoin.WithCDN)
		setBool("filecoin.allow_private_networks", &next.Filecoin.AllowPrivateNetworks, req.Filecoin.AllowPrivateNetworks)
		setInt("filecoin.default_copies", &next.Filecoin.DefaultCopies, req.Filecoin.DefaultCopies)
		if req.Filecoin.Observability != nil {
			setDuration("filecoin.observability.interval", &next.Filecoin.Observability.Interval, req.Filecoin.Observability.Interval)
			setDuration("filecoin.observability.timeout", &next.Filecoin.Observability.Timeout, req.Filecoin.Observability.Timeout)
			setInt("filecoin.observability.concurrency", &next.Filecoin.Observability.Concurrency, req.Filecoin.Observability.Concurrency)
		}
	}
	if req.Cache != nil {
		if setString("cache.dir", &next.Cache.Dir, req.Cache.Dir) {
			fieldPresence.CacheDir = true
		}
		setInt("cache.max_size_gb", &next.Cache.MaxSizeGB, req.Cache.MaxSizeGB)
		setString("cache.eviction_policy", &next.Cache.EvictionPolicy, req.Cache.EvictionPolicy)
	}
	if req.Worker != nil {
		applyWorkerPoolUpdate(req.Worker.Upload, &next.Worker.Upload, "worker.upload", setInt, setDuration)
		applyWorkerPoolUpdate(req.Worker.Evictor, &next.Worker.Evictor, "worker.evictor", setInt, setDuration)
		applyWorkerPoolUpdate(req.Worker.StorageCleanup, &next.Worker.StorageCleanup, "worker.storage_cleanup", setInt, setDuration)
	}
	if req.Logging != nil {
		setString("logging.level", &next.Logging.Level, req.Logging.Level)
		setString("logging.format", &next.Logging.Format, req.Logging.Format)
		if req.Logging.S3Access != nil {
			setBool("logging.s3_access.enabled", &next.Logging.S3Access.Enabled, req.Logging.S3Access.Enabled)
			setString("logging.s3_access.level", &next.Logging.S3Access.Level, req.Logging.S3Access.Level)
		}
	}

	if len(fieldErrs) == 0 {
		fieldErrs = editableValidationErrors(next)
	}
	return next, fieldPresence, fieldErrs, nil
}

func (s *SettingsService) FilecoinDraftConfig(req *settingsFilecoinUpdate) (*config.Config, []config.FieldError) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := cloneConfig(s.effective)
	if next == nil {
		return nil, []config.FieldError{{Field: "filecoin", Message: "settings are unavailable"}}
	}

	var fieldErrs []config.FieldError
	managed := config.EnvManagedFieldPaths()
	checkField := func(field string) bool {
		if envName, ok := managed[field]; ok {
			fieldErrs = append(fieldErrs, config.FieldError{
				Field:   field,
				Message: "is managed by " + envName,
			})
			return false
		}
		return true
	}
	setString := func(field string, target *string, value *string) {
		if value == nil || !checkField(field) {
			return
		}
		*target = *value
	}
	setInt := func(field string, target *int, value *int) {
		if value == nil || !checkField(field) {
			return
		}
		*target = *value
	}
	setBool := func(field string, target *bool, value *bool) {
		if value == nil || !checkField(field) {
			return
		}
		*target = *value
	}
	setDuration := func(field string, target *time.Duration, value *string) {
		if value == nil || !checkField(field) {
			return
		}
		parsed, err := time.ParseDuration(*value)
		if err != nil {
			fieldErrs = append(fieldErrs, config.FieldError{Field: field, Message: "must be a valid duration"})
			return
		}
		*target = parsed
	}

	if req != nil {
		setString("filecoin.network", &next.Filecoin.Network, req.Network)
		setString("filecoin.rpc_url", &next.Filecoin.RPCURL, req.RPCURL)
		setString("filecoin.source", &next.Filecoin.Source, req.Source)
		setBool("filecoin.with_cdn", &next.Filecoin.WithCDN, req.WithCDN)
		setBool("filecoin.allow_private_networks", &next.Filecoin.AllowPrivateNetworks, req.AllowPrivateNetworks)
		setInt("filecoin.default_copies", &next.Filecoin.DefaultCopies, req.DefaultCopies)
		if req.Observability != nil {
			setDuration("filecoin.observability.interval", &next.Filecoin.Observability.Interval, req.Observability.Interval)
			setDuration("filecoin.observability.timeout", &next.Filecoin.Observability.Timeout, req.Observability.Timeout)
			setInt("filecoin.observability.concurrency", &next.Filecoin.Observability.Concurrency, req.Observability.Concurrency)
		}
	}
	if len(fieldErrs) == 0 {
		fieldErrs = filecoinEditableValidationErrors(next)
	}
	if len(fieldErrs) > 0 {
		return next, fieldErrs
	}
	return next, nil
}

func applyWorkerPoolUpdate(
	req *settingsWorkerPoolUpdate,
	target *config.WorkerPoolConfig,
	prefix string,
	setInt func(string, *int, *int),
	setDuration func(string, *time.Duration, *string),
) {
	if req == nil {
		return
	}
	setInt(prefix+".concurrency", &target.Concurrency, req.Concurrency)
	setDuration(prefix+".poll_interval", &target.PollInterval, req.PollInterval)
	setInt(prefix+".max_retries", &target.MaxRetries, req.MaxRetries)
}

func (s *SettingsService) snapshotLocked(writable bool) settingsResponse {
	return s.snapshotFromConfigLocked(s.effective, writable)
}

func (s *SettingsService) snapshotFromConfig(cfg *config.Config, writable bool) settingsResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.snapshotFromConfigLocked(cfg, writable)
}

func (s *SettingsService) snapshotFromConfigLocked(cfg *config.Config, writable bool) settingsResponse {
	validationErrs := cfg.FieldValidationErrors()
	mode := "ready"
	if len(validationErrs) > 0 {
		mode = "setup"
	}

	return settingsResponse{
		Mode:              mode,
		ConfigPath:        s.source.Path,
		Writable:          writable,
		RuntimeAvailable:  false,
		RestartRequired:   s.restartRequired,
		Config:            toSettingsEditableConfig(cfg),
		Manual:            toSettingsManualConfig(cfg),
		Secrets:           toSettingsSecretStatus(cfg),
		Metadata:          config.FieldMetadataByPath(),
		Defaults:          settingsDefaults{FilecoinRPCURLs: config.DefaultFilecoinRPCURLs()},
		EnvManaged:        config.EnvManagedFieldPaths(),
		ValidationErrors:  validationErrs,
		WritableHeader:    settingsWriteHeader,
		WritableHeaderVal: settingsWriteHeaderValue,
	}
}

func cloneConfig(cfg *config.Config) *config.Config {
	if cfg == nil {
		return nil
	}
	clone := *cfg
	return &clone
}

type settingsResponse struct {
	Mode              string                          `json:"mode"`
	ConfigPath        string                          `json:"config_path"`
	Writable          bool                            `json:"writable"`
	RuntimeAvailable  bool                            `json:"runtime_available"`
	RestartRequired   bool                            `json:"restart_required"`
	S3Users           settingsS3UsersStatus           `json:"s3_users"`
	Config            settingsEditableConfig          `json:"config"`
	Manual            settingsManualConfig            `json:"manual"`
	Secrets           settingsSecretStatus            `json:"secrets"`
	Metadata          map[string]config.FieldMetadata `json:"metadata"`
	Defaults          settingsDefaults                `json:"defaults"`
	EnvManaged        map[string]string               `json:"env_managed"`
	ValidationErrors  []config.FieldError             `json:"validation_errors,omitempty"`
	WritableHeader    string                          `json:"write_header"`
	WritableHeaderVal string                          `json:"write_header_value"`
}

type settingsDefaults struct {
	FilecoinRPCURLs map[string]string `json:"filecoin_rpc_urls"`
}

type settingsS3UsersStatus struct {
	Available bool   `json:"available"`
	Reason    string `json:"reason,omitempty"`
}

type settingsEditableConfig struct {
	Server   settingsServerConfig   `json:"server"`
	S3       settingsS3Config       `json:"s3"`
	Filecoin settingsFilecoinConfig `json:"filecoin"`
	Cache    settingsCacheConfig    `json:"cache"`
	Worker   settingsWorkerConfig   `json:"worker"`
	Logging  settingsLoggingConfig  `json:"logging"`
}

type settingsServerConfig struct {
	Port           string            `json:"port"`
	TLS            settingsTLSConfig `json:"tls"`
	MaxConnections int               `json:"max_connections"`
	MaxRequests    int               `json:"max_requests"`
}

type settingsTLSConfig struct {
	Enabled  bool   `json:"enabled"`
	CertFile string `json:"cert_file"`
	KeyFile  string `json:"key_file"`
}

type settingsS3Config struct {
	Region string `json:"region"`
}

type settingsFilecoinConfig struct {
	Network              string                      `json:"network"`
	RPCURL               string                      `json:"rpc_url"`
	Source               string                      `json:"source"`
	WithCDN              bool                        `json:"with_cdn"`
	AllowPrivateNetworks bool                        `json:"allow_private_networks"`
	DefaultCopies        int                         `json:"default_copies"`
	Observability        settingsObservabilityConfig `json:"observability"`
}

type settingsObservabilityConfig struct {
	Interval    string `json:"interval"`
	Timeout     string `json:"timeout"`
	Concurrency int    `json:"concurrency"`
}

type settingsCacheConfig struct {
	Dir            string `json:"dir"`
	MaxSizeGB      int    `json:"max_size_gb"`
	EvictionPolicy string `json:"eviction_policy"`
}

type settingsWorkerConfig struct {
	Upload         settingsWorkerPoolConfig `json:"upload"`
	Evictor        settingsWorkerPoolConfig `json:"evictor"`
	StorageCleanup settingsWorkerPoolConfig `json:"storage_cleanup"`
}

type settingsWorkerPoolConfig struct {
	Concurrency  int    `json:"concurrency"`
	PollInterval string `json:"poll_interval"`
	MaxRetries   int    `json:"max_retries"`
}

type settingsLoggingConfig struct {
	Level    string                        `json:"level"`
	Format   string                        `json:"format"`
	S3Access settingsLoggingS3AccessConfig `json:"s3_access"`
}

type settingsLoggingS3AccessConfig struct {
	Enabled bool   `json:"enabled"`
	Level   string `json:"level"`
}

type settingsManualConfig struct {
	Database  settingsDatabaseConfig `json:"database"`
	Admin     settingsAdminConfig    `json:"admin"`
	Filecoin  settingsManualField    `json:"filecoin_private_key"`
	ConfigDoc string                 `json:"config_doc"`
}

type settingsDatabaseConfig struct {
	Driver        string `json:"driver"`
	DSN           string `json:"dsn"`
	DSNConfigured bool   `json:"dsn_configured"`
	MaxOpenConns  int    `json:"max_open_conns"`
	MaxIdleConns  int    `json:"max_idle_conns"`
}

type settingsAdminConfig struct {
	AddrConfigured bool `json:"addr_configured"`
}

type settingsManualField struct {
	Configured bool   `json:"configured"`
	Field      string `json:"field"`
	Env        string `json:"env,omitempty"`
}

type settingsSecretStatus struct {
	FilecoinPrivateKeyConfigured bool `json:"filecoin_private_key_configured"`
}

type settingsS3Credentials struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type settingsUpdateRequest struct {
	Server   *settingsServerUpdate   `json:"server,omitempty"`
	S3       *settingsS3Update       `json:"s3,omitempty"`
	Filecoin *settingsFilecoinUpdate `json:"filecoin,omitempty"`
	Cache    *settingsCacheUpdate    `json:"cache,omitempty"`
	Worker   *settingsWorkerUpdate   `json:"worker,omitempty"`
	Logging  *settingsLoggingUpdate  `json:"logging,omitempty"`
}

type settingsServerUpdate struct {
	Port           *string            `json:"port,omitempty"`
	TLS            *settingsTLSUpdate `json:"tls,omitempty"`
	MaxConnections *int               `json:"max_connections,omitempty"`
	MaxRequests    *int               `json:"max_requests,omitempty"`
}

type settingsTLSUpdate struct {
	Enabled  *bool   `json:"enabled,omitempty"`
	CertFile *string `json:"cert_file,omitempty"`
	KeyFile  *string `json:"key_file,omitempty"`
}

type settingsS3Update struct {
	Region *string `json:"region,omitempty"`
}

type settingsFilecoinUpdate struct {
	Network              *string                      `json:"network,omitempty"`
	RPCURL               *string                      `json:"rpc_url,omitempty"`
	Source               *string                      `json:"source,omitempty"`
	WithCDN              *bool                        `json:"with_cdn,omitempty"`
	AllowPrivateNetworks *bool                        `json:"allow_private_networks,omitempty"`
	DefaultCopies        *int                         `json:"default_copies,omitempty"`
	Observability        *settingsObservabilityUpdate `json:"observability,omitempty"`
}

type settingsObservabilityUpdate struct {
	Interval    *string `json:"interval,omitempty"`
	Timeout     *string `json:"timeout,omitempty"`
	Concurrency *int    `json:"concurrency,omitempty"`
}

type settingsCacheUpdate struct {
	Dir            *string `json:"dir,omitempty"`
	MaxSizeGB      *int    `json:"max_size_gb,omitempty"`
	EvictionPolicy *string `json:"eviction_policy,omitempty"`
}

type settingsWorkerUpdate struct {
	Upload         *settingsWorkerPoolUpdate `json:"upload,omitempty"`
	Evictor        *settingsWorkerPoolUpdate `json:"evictor,omitempty"`
	StorageCleanup *settingsWorkerPoolUpdate `json:"storage_cleanup,omitempty"`
}

type settingsWorkerPoolUpdate struct {
	Concurrency  *int    `json:"concurrency,omitempty"`
	PollInterval *string `json:"poll_interval,omitempty"`
	MaxRetries   *int    `json:"max_retries,omitempty"`
}

type settingsLoggingUpdate struct {
	Level    *string                        `json:"level,omitempty"`
	Format   *string                        `json:"format,omitempty"`
	S3Access *settingsLoggingS3AccessUpdate `json:"s3_access,omitempty"`
}

type settingsLoggingS3AccessUpdate struct {
	Enabled *bool   `json:"enabled,omitempty"`
	Level   *string `json:"level,omitempty"`
}

func toSettingsEditableConfig(cfg *config.Config) settingsEditableConfig {
	return settingsEditableConfig{
		Server: settingsServerConfig{
			Port: cfg.Server.Port,
			TLS: settingsTLSConfig{
				Enabled:  cfg.Server.TLS.Enabled,
				CertFile: cfg.Server.TLS.CertFile,
				KeyFile:  cfg.Server.TLS.KeyFile,
			},
			MaxConnections: cfg.Server.MaxConnections,
			MaxRequests:    cfg.Server.MaxRequests,
		},
		S3: settingsS3Config{
			Region: cfg.S3.Region,
		},
		Filecoin: settingsFilecoinConfig{
			Network:              cfg.Filecoin.Network,
			RPCURL:               cfg.Filecoin.RPCURL,
			Source:               cfg.Filecoin.Source,
			WithCDN:              cfg.Filecoin.WithCDN,
			AllowPrivateNetworks: cfg.Filecoin.AllowPrivateNetworks,
			DefaultCopies:        cfg.Filecoin.DefaultCopies,
			Observability: settingsObservabilityConfig{
				Interval:    cfg.Filecoin.Observability.Interval.String(),
				Timeout:     cfg.Filecoin.Observability.Timeout.String(),
				Concurrency: cfg.Filecoin.Observability.Concurrency,
			},
		},
		Cache: settingsCacheConfig{
			Dir:            cfg.Cache.Dir,
			MaxSizeGB:      cfg.Cache.MaxSizeGB,
			EvictionPolicy: cfg.Cache.EvictionPolicy,
		},
		Worker: settingsWorkerConfig{
			Upload:         toSettingsWorkerPoolConfig(cfg.Worker.Upload),
			Evictor:        toSettingsWorkerPoolConfig(cfg.Worker.Evictor),
			StorageCleanup: toSettingsWorkerPoolConfig(cfg.Worker.StorageCleanup),
		},
		Logging: settingsLoggingConfig{
			Level:  cfg.Logging.Level,
			Format: cfg.Logging.Format,
			S3Access: settingsLoggingS3AccessConfig{
				Enabled: cfg.Logging.S3Access.Enabled,
				Level:   cfg.Logging.S3Access.Level,
			},
		},
	}
}

func toSettingsWorkerPoolConfig(cfg config.WorkerPoolConfig) settingsWorkerPoolConfig {
	return settingsWorkerPoolConfig{
		Concurrency:  cfg.Concurrency,
		PollInterval: cfg.PollInterval.String(),
		MaxRetries:   cfg.MaxRetries,
	}
}

func toSettingsManualConfig(cfg *config.Config) settingsManualConfig {
	envManaged := config.EnvManagedFieldPaths()
	return settingsManualConfig{
		Database: settingsDatabaseConfig{
			Driver:        cfg.Database.Driver,
			DSN:           configuredLabel(cfg.Database.DSN),
			DSNConfigured: strings.TrimSpace(cfg.Database.DSN) != "",
			MaxOpenConns:  cfg.Database.MaxOpenConns,
			MaxIdleConns:  cfg.Database.MaxIdleConns,
		},
		Admin: settingsAdminConfig{AddrConfigured: strings.TrimSpace(cfg.Admin.Addr) != ""},
		Filecoin: settingsManualField{
			Configured: strings.TrimSpace(cfg.Filecoin.PrivateKey) != "",
			Field:      "filecoin.private_key",
			Env:        envManaged["filecoin.private_key"],
		},
		ConfigDoc: "Edit secrets directly in the config file or deployment environment.",
	}
}

func toSettingsSecretStatus(cfg *config.Config) settingsSecretStatus {
	return settingsSecretStatus{
		FilecoinPrivateKeyConfigured: strings.TrimSpace(cfg.Filecoin.PrivateKey) != "",
	}
}

func editableValidationErrors(cfg *config.Config) []config.FieldError {
	editable := map[string]struct{}{
		"server.port":                          {},
		"server.max_connections":               {},
		"server.max_requests":                  {},
		"server.tls.cert_file":                 {},
		"server.tls.key_file":                  {},
		"s3.region":                            {},
		"cache.dir":                            {},
		"cache.max_size_gb":                    {},
		"cache.eviction_policy":                {},
		"filecoin.network":                     {},
		"filecoin.rpc_url":                     {},
		"filecoin.source":                      {},
		"filecoin.default_copies":              {},
		"filecoin.observability.interval":      {},
		"filecoin.observability.timeout":       {},
		"filecoin.observability.concurrency":   {},
		"worker.upload.concurrency":            {},
		"worker.upload.poll_interval":          {},
		"worker.upload.max_retries":            {},
		"worker.evictor.concurrency":           {},
		"worker.evictor.poll_interval":         {},
		"worker.evictor.max_retries":           {},
		"worker.storage_cleanup.concurrency":   {},
		"worker.storage_cleanup.poll_interval": {},
		"worker.storage_cleanup.max_retries":   {},
		"logging.level":                        {},
		"logging.format":                       {},
		"logging.s3_access.enabled":            {},
		"logging.s3_access.level":              {},
	}

	var out []config.FieldError
	for _, fieldErr := range cfg.FieldValidationErrors() {
		if _, ok := editable[fieldErr.Field]; ok {
			out = append(out, fieldErr)
		}
	}
	return out
}

func filecoinEditableValidationErrors(cfg *config.Config) []config.FieldError {
	editable := map[string]struct{}{
		"filecoin.network":                   {},
		"filecoin.rpc_url":                   {},
		"filecoin.source":                    {},
		"filecoin.default_copies":            {},
		"filecoin.observability.interval":    {},
		"filecoin.observability.timeout":     {},
		"filecoin.observability.concurrency": {},
	}

	var out []config.FieldError
	for _, fieldErr := range cfg.FieldValidationErrors() {
		if _, ok := editable[fieldErr.Field]; ok {
			out = append(out, fieldErr)
		}
	}
	return out
}

func configuredLabel(value string) string {
	if strings.TrimSpace(value) == "" {
		return "missing"
	}
	return "configured"
}

func generateS3Credentials() (settingsS3Credentials, error) {
	accessKey, err := securetoken.URL(16)
	if err != nil {
		return settingsS3Credentials{}, fmt.Errorf("generating S3 access key: %w", err)
	}
	secretKey, err := securetoken.URL(32)
	if err != nil {
		return settingsS3Credentials{}, fmt.Errorf("generating S3 secret key: %w", err)
	}
	return settingsS3Credentials{AccessKey: accessKey, SecretKey: secretKey}, nil
}

type settingsErrorResponse struct {
	Error  string              `json:"error"`
	Fields []config.FieldError `json:"fields,omitempty"`
}

type settingsValidationResponse struct {
	ValidationErrors []config.FieldError `json:"validation_errors"`
}

func (s *Server) handleAPIGetSettings(w http.ResponseWriter, _ *http.Request) {
	if s.settings == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "settings not available"})
		return
	}
	writeJSON(w, http.StatusOK, s.decorateSettingsResponse(s.settings.Snapshot(s.settingsWritable())))
}

func (s *Server) handleAPIUpdateSettings(w http.ResponseWriter, r *http.Request) {
	req, ok := s.readSettingsUpdateRequest(w, r)
	if !ok {
		return
	}

	resp, fieldErrs, err := s.settings.Update(req, true)
	if err != nil {
		s.logger.Error("failed to save settings", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	if len(fieldErrs) > 0 {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid settings", Fields: fieldErrs})
		return
	}
	writeJSON(w, http.StatusOK, s.decorateSettingsResponse(resp))
}

func (s *Server) handleAPIValidateSettings(w http.ResponseWriter, r *http.Request) {
	req, ok := s.readSettingsUpdateRequest(w, r)
	if !ok {
		return
	}

	fieldErrs, err := s.settings.Validate(req)
	if err != nil {
		s.logger.Error("failed to validate settings", "error", err)
		writeJSON(w, http.StatusInternalServerError, settingsErrorResponse{Error: "internal"})
		return
	}
	if fieldErrs == nil {
		fieldErrs = []config.FieldError{}
	}
	writeJSON(w, http.StatusOK, settingsValidationResponse{ValidationErrors: fieldErrs})
}

func (s *Server) readSettingsUpdateRequest(w http.ResponseWriter, r *http.Request) (settingsUpdateRequest, bool) {
	var req settingsUpdateRequest
	if s.settings == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "settings not available"})
		return req, false
	}
	if !s.settingsWritable() {
		writeJSON(w, http.StatusForbidden, settingsErrorResponse{Error: "settings writes require loopback admin binding"})
		return req, false
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "settings writes require application/json"})
		return req, false
	}
	if r.Header.Get(settingsWriteHeader) != settingsWriteHeaderValue {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "missing settings write header"})
		return req, false
	}

	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid settings payload"})
		return req, false
	}
	var extra struct{}
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, settingsErrorResponse{Error: "invalid settings payload"})
		return req, false
	}
	return req, true
}

func (s *Server) decorateSettingsResponse(resp settingsResponse) settingsResponse {
	resp.S3Users = s.s3UsersStatus()
	resp.RuntimeAvailable = !s.setupOnly
	return resp
}

func (s *Server) settingsWritable() bool {
	return isLoopbackAddr(s.addr)
}

func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
