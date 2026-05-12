package config

import (
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
		Label:       "S3 Port",
		Description: "Host and port where the S3-compatible API listens.",
		Env:         "SYNAPS3_SERVER_PORT",
		Editable:    true,
	},
	"server.max_connections": {
		Label:       "Max Connections",
		Description: "Maximum concurrent TCP connections accepted by the S3 server. Increase only with matching file descriptor and memory capacity.",
		Env:         "SYNAPS3_SERVER_MAX_CONNECTIONS",
		Editable:    true,
	},
	"server.max_requests": {
		Label:       "Max Requests",
		Description: "Maximum in-flight S3 requests before excess requests receive SlowDown responses. Increase only with matching backend capacity.",
		Env:         "SYNAPS3_SERVER_MAX_REQUESTS",
		Editable:    true,
	},
	"server.tls.enabled": {
		Label:       "TLS Enabled",
		Description: "Enables TLS for the S3 API listener.",
		Env:         "SYNAPS3_SERVER_TLS_ENABLED",
		Editable:    true,
	},
	"server.tls.cert_file": {
		Label:       "TLS Cert File",
		Description: "Path to the TLS certificate file used by the S3 API listener.",
		Env:         "SYNAPS3_SERVER_TLS_CERT_FILE",
		Editable:    true,
	},
	"server.tls.key_file": {
		Label:       "TLS Key File",
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
		Label:       "Filecoin Private Key",
		Description: "Wallet private key used for Filecoin payments and storage operations. Set it in the config file or environment.",
		Env:         "SYNAPS3_FILECOIN_PRIVATE_KEY",
		Secret:      true,
	},
	"filecoin.source": {
		Label:       "Source",
		Description: "Source identifier sent to synapse-go.",
		Env:         "SYNAPS3_FILECOIN_SOURCE",
		Editable:    true,
	},
	"filecoin.with_cdn": {
		Label:       "Use CDN",
		Description: "Requests CDN-backed retrieval hints for eligible uploads.",
		Env:         "SYNAPS3_FILECOIN_WITH_CDN",
		Editable:    true,
	},
	"filecoin.allow_private_networks": {
		Label:       "Allow Private Networks",
		Description: "Allows retrieval URLs on private networks; enable only in trusted environments.",
		Env:         "SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS",
		Editable:    true,
	},
	"filecoin.default_copies": {
		Label:       "Default Copies",
		Description: "Default target Filecoin copies for buckets without an explicit copy policy, from 1 to 8.",
		Env:         "SYNAPS3_FILECOIN_DEFAULT_COPIES",
		Editable:    true,
	},
	"database.driver": {
		Label:       "Database Driver",
		Description: "Database backend used for metadata persistence.",
		Env:         "SYNAPS3_DATABASE_DRIVER",
	},
	"database.dsn": {
		Label:       "Database DSN",
		Description: "Database connection string. SQLite only needs a file URL; SynapS3 manages SQLite runtime parameters.",
		Env:         "SYNAPS3_DATABASE_DSN",
		Secret:      true,
	},
	"database.max_open_conns": {
		Label:       "Database Max Open Conns",
		Description: "Maximum number of open database connections.",
		Env:         "SYNAPS3_DATABASE_MAX_OPEN_CONNS",
	},
	"database.max_idle_conns": {
		Label:       "Database Max Idle Conns",
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
		Label:       "Max Size GB",
		Description: "Maximum cache capacity in gigabytes.",
		Env:         "SYNAPS3_CACHE_MAX_SIZE_GB",
		Editable:    true,
	},
	"cache.eviction_policy": {
		Label:       "Eviction Policy",
		Description: "Cache eviction mode: lru automatically evicts local cache after remote storage; manual and none do not.",
		Env:         "SYNAPS3_CACHE_EVICTION_POLICY",
		Editable:    true,
	},
	"worker.upload.concurrency": {
		Label:       "Upload Concurrency",
		Description: "Number of upload worker jobs that may run concurrently.",
		Env:         "SYNAPS3_WORKER_UPLOAD_CONCURRENCY",
		Editable:    true,
	},
	"worker.upload.poll_interval": {
		Label:       "Upload Poll Interval",
		Description: "Interval between upload worker polling cycles.",
		Env:         "SYNAPS3_WORKER_UPLOAD_POLL_INTERVAL",
		Editable:    true,
	},
	"worker.upload.max_retries": {
		Label:       "Upload Max Retries",
		Description: "Maximum retry attempts for failed upload work.",
		Env:         "SYNAPS3_WORKER_UPLOAD_MAX_RETRIES",
		Editable:    true,
	},
	"worker.evictor.concurrency": {
		Label:       "Evictor Concurrency",
		Description: "Number of cache eviction jobs that may run concurrently.",
		Env:         "SYNAPS3_WORKER_EVICTOR_CONCURRENCY",
		Editable:    true,
	},
	"worker.evictor.poll_interval": {
		Label:       "Evictor Poll Interval",
		Description: "Interval between cache evictor polling cycles.",
		Env:         "SYNAPS3_WORKER_EVICTOR_POLL_INTERVAL",
		Editable:    true,
	},
	"worker.evictor.max_retries": {
		Label:       "Evictor Max Retries",
		Description: "Maximum retry attempts for failed eviction work.",
		Env:         "SYNAPS3_WORKER_EVICTOR_MAX_RETRIES",
		Editable:    true,
	},
	"worker.storage_cleanup.concurrency": {
		Label:       "Replica Cleanup Concurrency",
		Description: "Number of remote replica cleanup jobs that may run concurrently.",
		Env:         "SYNAPS3_WORKER_STORAGE_CLEANUP_CONCURRENCY",
		Editable:    true,
	},
	"worker.storage_cleanup.poll_interval": {
		Label:       "Replica Cleanup Poll Interval",
		Description: "Interval between remote replica cleanup polling cycles.",
		Env:         "SYNAPS3_WORKER_STORAGE_CLEANUP_POLL_INTERVAL",
		Editable:    true,
	},
	"worker.storage_cleanup.max_retries": {
		Label:       "Replica Cleanup Max Retries",
		Description: "Maximum retry attempts for failed remote replica cleanup work.",
		Env:         "SYNAPS3_WORKER_STORAGE_CLEANUP_MAX_RETRIES",
		Editable:    true,
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
		Label:       "S3 Access Log Enabled",
		Description: "Whether S3 request access logs are emitted.",
		Env:         "SYNAPS3_LOGGING_S3_ACCESS_ENABLED",
		Editable:    true,
	},
	"logging.s3_access.level": {
		Label:       "S3 Access Log Level",
		Description: "Log level used for S3 request access logs.",
		Env:         "SYNAPS3_LOGGING_S3_ACCESS_LEVEL",
		Editable:    true,
	},
	"admin.addr": {
		Label:       "Admin Address",
		Description: "Address where the admin dashboard and API listen.",
		Env:         "SYNAPS3_ADMIN_ADDR",
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
	return out
}

// FieldMetadataByPath returns metadata keyed by dotted config field path.
func FieldMetadataByPath() map[string]FieldMetadata {
	out := make(map[string]FieldMetadata, len(fieldMetadataByPath))
	for field, meta := range fieldMetadataByPath {
		out[field] = meta
	}
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
