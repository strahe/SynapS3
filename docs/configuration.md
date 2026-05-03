# Configuration

SynapS3 loads configuration from YAML and supports a limited set of environment-variable overrides. Start with [`config.example.yaml`](../config.example.yaml), then override individual values as needed.

## Loading Rules

- Without `--config`, SynapS3 reads and writes `~/.synaps3/config.yaml`
- Pass `--config <path>` to use a different config file
- A `config.yaml` in the current directory is ignored unless you pass `--config config.yaml`
- Environment variables use the `SYNAPS3_` prefix
- Common underscore-containing keys such as `filecoin.rpc_url`, `s3.secret_key`, `cache.max_size_gb`, and worker `max_retries` have explicit env mappings
- Unknown `SYNAPS3_` variables still use the legacy fallback mapping that lowercases the name and replaces `_` with `.`

Examples:

```text
SYNAPS3_DATABASE_DSN      -> database.dsn
SYNAPS3_SERVER_PORT       -> server.port
SYNAPS3_FILECOIN_RPC_URL  -> filecoin.rpc_url
SYNAPS3_S3_SECRET_KEY     -> s3.secret_key
SYNAPS3_S3_IAM_DIR        -> s3.iam_dir
SYNAPS3_FILECOIN_NETWORK  -> filecoin.network
SYNAPS3_FILECOIN_DEFAULT_COPIES -> filecoin.default_copies
SYNAPS3_WORKER_UPLOAD_CONCURRENCY -> worker.upload.concurrency
```

## Default Runtime Data

When `database.dsn`, `cache.dir`, and `s3.iam_dir` are omitted, SynapS3 stores local runtime data under the current user's home directory:

```text
~/.synaps3/
  db/
    synaps3.db
    synaps3.db-shm
    synaps3.db-wal
  cache/
  iam/
    users.json
```

The application creates the default database, cache, and IAM directories automatically. SQLite WAL and SHM files are expected and live beside the database file. Explicit `database.dsn`, `cache.dir`, and `s3.iam_dir` values still take precedence, including relative paths, which remain relative to the process working directory.

SynapS3 does not automatically migrate old local `./synaps3.db*` or `./cache` data. Move those files manually if you want to keep existing local state.

## Main Sections

| Section | Key Fields | Purpose |
| --- | --- | --- |
| `database` | `driver`, `dsn`, `max_open_conns`, `max_idle_conns` | Database connection settings |
| `cache` | `dir`, `max_size_gb`, `eviction_policy` | Local disk cache behavior |
| `s3` | `access_key`, `secret_key`, `region`, `iam_dir` | S3 authentication and VersityGW users.json settings |
| `server` | `port`, `tls.enabled`, `tls.cert_file`, `tls.key_file` | S3 server bind address and TLS |
| `filecoin` | `network`, `rpc_url`, `private_key`, `source`, `with_cdn`, `allow_private_networks`, `default_copies` | synapse-go client settings and storage policy defaults; `default_copies` accepts 1-8 |
| `worker.upload` | `concurrency`, `poll_interval`, `max_retries` | Upload worker tuning |
| `worker.evictor` | `concurrency`, `poll_interval`, `max_retries` | Cache eviction worker tuning |
| `logging` | `level`, `format` | Log output settings |
| `admin` | `addr` | Admin server bind address |

## Example Environment Overrides

```bash
export SYNAPS3_DATABASE_DRIVER=postgres
export SYNAPS3_DATABASE_DSN="postgres://user:pass@localhost:5432/synaps3?sslmode=disable"
export SYNAPS3_FILECOIN_NETWORK=calibration
export SYNAPS3_SERVER_PORT=:8080
```

## Recommended Workflow

1. Start SynapS3 without `--config` to use `~/.synaps3/config.yaml`, or copy `config.example.yaml` and pass it with `--config`
2. Fill in S3 and Filecoin credentials
3. Use environment variables for deployment-specific overrides and keep long-lived local settings in YAML

For production deployment, monitoring, and admin endpoints, see [Operations](operations.md).
