# Configuration

SynapS3 loads configuration from YAML and supports a limited set of environment-variable overrides. Start with [`config.example.yaml`](../config.example.yaml), then override individual values as needed.

## Loading Rules

- Pass the config file with `--config`
- Environment variables use the `SYNAPS3_` prefix and replace every `_` with `.`
- Because of that mapping, env overrides work reliably only for paths where every segment has no underscore
- Any path segment containing `_` becomes ambiguous after the mapping, so keys such as `filecoin.rpc_url`, `filecoin.private_key`, `filecoin.with_cdn`, `filecoin.allow_private_networks`, `s3.access_key`, `s3.secret_key`, `database.max_open_conns`, and `worker.upload.max_retries` should stay in YAML for now

Examples:

```text
SYNAPS3_DATABASE_DSN      -> database.dsn
SYNAPS3_SERVER_PORT       -> server.port
SYNAPS3_FILECOIN_NETWORK  -> filecoin.network
SYNAPS3_WORKER_UPLOAD_CONCURRENCY -> worker.upload.concurrency
```

## Default Runtime Data

When `database.dsn` and `cache.dir` are omitted, SynapS3 stores local runtime data under the current user's home directory:

```text
~/.synaps3/
  db/
    synaps3.db
    synaps3.db-shm
    synaps3.db-wal
  cache/
```

The application creates the default database and cache directories automatically. SQLite WAL and SHM files are expected and live beside the database file. Explicit `database.dsn` and `cache.dir` values still take precedence, including relative paths, which remain relative to the process working directory.

SynapS3 does not automatically migrate old local `./synaps3.db*` or `./cache` data. Move those files manually if you want to keep existing local state.

## Main Sections

| Section | Key Fields | Purpose |
| --- | --- | --- |
| `database` | `driver`, `dsn`, `max_open_conns`, `max_idle_conns` | Database connection settings |
| `cache` | `dir`, `max_size_gb`, `eviction_policy` | Local disk cache behavior |
| `s3` | `access_key`, `secret_key`, `region` | S3 authentication settings |
| `server` | `port`, `tls.enabled`, `tls.cert_file`, `tls.key_file` | S3 server bind address and TLS |
| `filecoin` | `network`, `rpc_url`, `private_key`, `source`, `with_cdn`, `allow_private_networks` | synapse-go client settings |
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

1. Copy `config.example.yaml` to `config.yaml`
2. Fill in S3 and Filecoin credentials
3. Use environment variables only for deployment-specific overrides that do not rely on underscore-containing leaf keys

For production deployment, monitoring, and admin endpoints, see [Operations](operations.md).
