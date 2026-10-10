package config

import (
	"maps"
	"os"
	"strings"
)

// FieldMetadata describes a config field for admin settings clients.
type FieldMetadata struct {
	Label       string `json:"label"`
	Description string `json:"description"`
	Env         string `json:"env,omitempty"`
	Editable    bool   `json:"editable"`
	Secret      bool   `json:"secret"`
}

var fieldMetadataByPath = map[string]FieldMetadata{
	"server.port": {
		Label:       "S3 port",
		Description: "Host and port where the S3-compatible API listens.",
		Env:         "SYNAPS3_SERVER_PORT",
		Editable:    true,
	},
	"server.max_connections": {
		Label:       "Max connections",
		Description: "Maximum concurrent TCP connections accepted by the S3 server. Increase only with matching file descriptor and memory capacity.",
		Env:         "SYNAPS3_SERVER_MAX_CONNECTIONS",
		Editable:    true,
	},
	"server.max_requests": {
		Label:       "Max requests",
		Description: "Maximum in-flight S3 requests before excess requests receive SlowDown responses. Increase only with matching backend capacity.",
		Env:         "SYNAPS3_SERVER_MAX_REQUESTS",
		Editable:    true,
	},
	"server.tls.enabled": {
		Label:       "TLS enabled",
		Description: "Enables TLS for the S3 API listener.",
		Env:         "SYNAPS3_SERVER_TLS_ENABLED",
		Editable:    true,
	},
	"server.tls.cert_file": {
		Label:       "TLS certificate file",
		Description: "Path to the TLS certificate file used by the S3 API listener.",
		Env:         "SYNAPS3_SERVER_TLS_CERT_FILE",
		Editable:    true,
	},
	"server.tls.key_file": {
		Label:       "TLS key file",
		Description: "Path to the TLS private key file used by the S3 API listener.",
		Env:         "SYNAPS3_SERVER_TLS_KEY_FILE",
		Editable:    true,
		Secret:      true,
	},
	"s3.region": {
		Label:       "Region",
		Description: "S3 region reported by the gateway.",
		Env:         "SYNAPS3_S3_REGION",
		Editable:    true,
	},
	"filecoin.network": {
		Label:       "Network",
		Description: "Filecoin network used by synapse-go.",
		Env:         "SYNAPS3_FILECOIN_NETWORK",
		Editable:    true,
	},
	"filecoin.rpc_url": {
		Label:       "RPC URL",
		Description: "Filecoin JSON-RPC endpoint used by synapse-go.",
		Env:         "SYNAPS3_FILECOIN_RPC_URL",
		Editable:    true,
	},
	"filecoin.private_key": {
		Label:       "Filecoin private key",
		Description: "Wallet private key used for Filecoin payments and storage operations. Set it in the config file or environment.",
		Env:         "SYNAPS3_FILECOIN_PRIVATE_KEY",
		Secret:      true,
	},
	"filecoin.with_cdn": {
		Label:       "Use CDN",
		Description: "Requests CDN-backed retrieval hints for eligible uploads.",
		Env:         "SYNAPS3_FILECOIN_WITH_CDN",
		Editable:    true,
	},
	"filecoin.allow_private_networks": {
		Label:       "Allow private networks",
		Description: "Allows private-network URLs used for storage provider operations, retrieval, and diagnostics; enable only for trusted infrastructure.",
		Env:         "SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS",
		Editable:    true,
	},
	"filecoin.anchor_provider_tier": {
		Label:       "Provider requirement",
		Description: "Requires at least one provider from the selected list, without fallback; none removes the list requirement. Restart required.",
		Env:         "SYNAPS3_FILECOIN_ANCHOR_PROVIDER_TIER",
		Editable:    true,
	},
	"filecoin.default_copies": {
		Label:       "Default replicas",
		Description: "Default Filecoin replica target for buckets without their own replica policy, from 1 to 8.",
		Env:         "SYNAPS3_FILECOIN_DEFAULT_COPIES",
		Editable:    true,
	},
	"filecoin.observability.interval": {
		Label:       "Observability interval",
		Description: "Interval between background provider and local data set health checks. Restart required.",
		Env:         "SYNAPS3_FILECOIN_OBSERVABILITY_INTERVAL",
		Editable:    true,
	},
	"filecoin.observability.timeout": {
		Label:       "Observability timeout",
		Description: "Base timeout used to derive bounded registry, wallet scan, provider lookup, and health check deadlines. Restart required.",
		Env:         "SYNAPS3_FILECOIN_OBSERVABILITY_TIMEOUT",
		Editable:    true,
	},
	"filecoin.observability.concurrency": {
		Label:       "Observability concurrency",
		Description: "Maximum concurrent provider health checks. Restart required.",
		Env:         "SYNAPS3_FILECOIN_OBSERVABILITY_CONCURRENCY",
		Editable:    true,
	},
	"database.driver": {
		Label:       "Database driver",
		Description: "Database backend used for metadata persistence.",
		Env:         "SYNAPS3_DATABASE_DRIVER",
	},
	"database.dsn": {
		Label:       "Database DSN",
		Description: "PostgreSQL connection URL for the metadata database.",
		Env:         "SYNAPS3_DATABASE_DSN",
		Secret:      true,
	},
	"database.max_open_conns": {
		Label:       "Database max open connections",
		Description: "Maximum number of open database connections.",
		Env:         "SYNAPS3_DATABASE_MAX_OPEN_CONNS",
	},
	"database.max_idle_conns": {
		Label:       "Database max idle connections",
		Description: "Maximum number of idle database connections.",
		Env:         "SYNAPS3_DATABASE_MAX_IDLE_CONNS",
	},
	"cache.dir": {
		Label:       "Directory",
		Description: "Filesystem directory used for cached object data.",
		Env:         "SYNAPS3_CACHE_DIR",
		Editable:    true,
	},
	"cache.max_size_gb": {
		Label:       "Max cache size (GiB)",
		Description: "Maximum cache capacity in gibibytes (GiB).",
		Env:         "SYNAPS3_CACHE_MAX_SIZE_GB",
		Editable:    true,
	},
	"cache.eviction_policy": {
		Label:       "Eviction policy",
		Description: "Controls automatic cache cleanup. LRU keeps recently read objects, After Upload removes objects once remote storage is complete, and None disables automatic cleanup. Restart required.",
		Env:         "SYNAPS3_CACHE_EVICTION_POLICY",
		Editable:    true,
	},
	"cache.lru_high_watermark_percent": {
		Label:       "LRU high watermark (%)",
		Description: "Starts LRU cache cleanup when usage reaches this percentage of the configured maximum. Restart required.",
		Env:         "SYNAPS3_CACHE_LRU_HIGH_WATERMARK_PERCENT",
		Editable:    true,
	},
	"cache.lru_low_watermark_percent": {
		Label:       "LRU low watermark (%)",
		Description: "Sets the LRU cleanup target as a percentage of cache capacity. Cleanup may go lower to leave room for the largest supported object. Restart required.",
		Env:         "SYNAPS3_CACHE_LRU_LOW_WATERMARK_PERCENT",
		Editable:    true,
	},
	"worker.tasks.concurrency": {
		Label: "Task concurrency", Description: "Maximum background operations that may run at once. Restart required.",
		Env: "SYNAPS3_WORKER_TASKS_CONCURRENCY", Editable: true,
	},
	"worker.tasks.poll_interval": {
		Label: "Task poll interval", Description: "Interval between checks for ready background operations. Restart required.",
		Env: "SYNAPS3_WORKER_TASKS_POLL_INTERVAL", Editable: true,
	},
	"worker.tasks.lease_duration": {
		Label: "Task lease duration", Description: "Time another process waits before recovering interrupted background work. Restart required.",
		Env: "SYNAPS3_WORKER_TASKS_LEASE_DURATION", Editable: true,
	},
	"worker.tasks.upload_concurrency": {
		Label: "Upload concurrency", Description: "Maximum uploads that may run at once. Restart required.",
		Env: "SYNAPS3_WORKER_TASKS_UPLOAD_CONCURRENCY", Editable: true,
	},
	"worker.tasks.commit_max_pieces": {
		Label: "Batch size", Description: "Maximum pieces a batch submits to its data set in one transaction. Restart required.",
		Env: "SYNAPS3_WORKER_TASKS_COMMIT_MAX_PIECES", Editable: true,
	},
	"worker.tasks.commit_max_wait": {
		Label: "Batch wait", Description: "Time to collect pieces for a batch; 0s skips the collection delay. Restart required.",
		Env: "SYNAPS3_WORKER_TASKS_COMMIT_MAX_WAIT", Editable: true,
	},
	"worker.tasks.commit_seal_on_cache_pressure": {
		Label: "Submit batches early to free cache space", Description: "Submit batches early when automatic cache cleanup cannot free enough space. May increase transaction costs. Restart required.",
		Env: "SYNAPS3_WORKER_TASKS_COMMIT_SEAL_ON_CACHE_PRESSURE", Editable: true,
	},
	"worker.tasks.commit_max_backlog": {
		Label: "Batch backlog", Description: "Transferred pieces per data set that may wait for an unsubmitted batch before new uploads and replicas to that data set wait. Restart required.",
		Env: "SYNAPS3_WORKER_TASKS_COMMIT_MAX_BACKLOG", Editable: true,
	},
	"logging.level": {
		Label:       "Level",
		Description: "Minimum log level emitted by SynapS3.",
		Env:         "SYNAPS3_LOGGING_LEVEL",
		Editable:    true,
	},
	"logging.format": {
		Label:       "Format",
		Description: "Log output format.",
		Env:         "SYNAPS3_LOGGING_FORMAT",
		Editable:    true,
	},
	"logging.s3_access.enabled": {
		Label:       "S3 access log",
		Description: "Whether S3 request access logs are emitted.",
		Env:         "SYNAPS3_LOGGING_S3_ACCESS_ENABLED",
		Editable:    true,
	},
	"logging.s3_access.level": {
		Label:       "S3 access log level",
		Description: "Log level used for S3 request access logs.",
		Env:         "SYNAPS3_LOGGING_S3_ACCESS_LEVEL",
		Editable:    true,
	},
	"admin.addr": {
		Label:       "Admin address",
		Description: "Address where the admin dashboard and API listen.",
		Env:         "SYNAPS3_ADMIN_ADDR",
	},
	"admin.trusted_proxies": {
		Label:       "Admin trusted proxies",
		Description: "Trusted reverse proxy IPs or CIDRs whose forwarded client and proto headers may be used by the Admin API.",
		Env:         "SYNAPS3_ADMIN_TRUSTED_PROXIES",
	},
	"admin.auth.enabled": {
		Label:       "Admin auth enabled",
		Description: "Requires login for the Admin UI and Admin API.",
		Env:         "SYNAPS3_ADMIN_AUTH_ENABLED",
	},
	"admin.auth.username": {
		Label:       "Admin username",
		Description: "Single administrator username for the Admin UI and Admin API.",
		Env:         "SYNAPS3_ADMIN_AUTH_USERNAME",
	},
	"admin.auth.session_secret": {
		Label:       "Admin session secret",
		Description: "Secret used to sign Admin UI session cookies.",
		Env:         "SYNAPS3_ADMIN_AUTH_SESSION_SECRET",
		Secret:      true,
	},
	"admin.auth.session_ttl": {
		Label:       "Admin session TTL",
		Description: "Lifetime of each standard Admin UI token. This is not a server-enforced idle timeout.",
		Env:         "SYNAPS3_ADMIN_AUTH_SESSION_TTL",
	},
}

var envFieldByName = buildEnvFieldByName()

func buildEnvFieldByName() map[string]string {
	out := make(map[string]string)
	for field, meta := range fieldMetadataByPath {
		if meta.Env != "" {
			out[strings.ToUpper(meta.Env)] = field
		}
	}
	out["SYNAPS3_ADMIN_AUTH_PASSWORD_HASH"] = "admin.auth.password_hash"
	return out
}

// FieldMetadataByPath returns metadata keyed by dotted config field path.
func FieldMetadataByPath() map[string]FieldMetadata {
	out := make(map[string]FieldMetadata, len(fieldMetadataByPath))
	maps.Copy(out, fieldMetadataByPath)
	return out
}

// EnvFieldForName returns the config field path for a supported SYNAPS3_ env var.
func EnvFieldForName(envName string) (string, bool) {
	field, ok := envFieldByName[strings.ToUpper(envName)]
	return field, ok
}

// EnvManagedFieldPaths returns recognized config fields currently controlled by env vars.
func EnvManagedFieldPaths() map[string]string {
	managed := make(map[string]string)
	for field, meta := range fieldMetadataByPath {
		if meta.Env == "" {
			continue
		}
		if _, ok := os.LookupEnv(meta.Env); ok {
			managed[field] = meta.Env
		}
	}
	return managed
}
