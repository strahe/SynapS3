# Configuration

SynapS3 loads TOML configuration from a config file, then applies recognized `SYNAPS3_` environment overrides. Built-in defaults live in code; generated config files only store values that should be explicit for that installation.

## Initialize

Default local setup:

```bash
synaps3 init
synaps3 serve
```

Custom app data directory:

```bash
synaps3 init --dir /var/lib/synaps3
synaps3 serve --config /var/lib/synaps3/config.toml
```

`synaps3 init` creates `config.toml`, `db/`, and `cache/`. It fails if `config.toml` already exists; back up or delete the file before running init again.

The generated config is a full reference file. Section headers stay active so fields can be uncommented in place. Commented values show built-in defaults and do not override runtime defaults until you uncomment them. Only installation-specific values are enabled by default:

```toml
[server]
# Host and port where the S3-compatible API listens.
# Env: SYNAPS3_SERVER_PORT
# port = ":8080"

[filecoin]
# Required before serving unless SYNAPS3_FILECOIN_PRIVATE_KEY is set.
private_key = ""

[database]
# Enabled with database.dsn so this installation uses SQLite at the initialized path.
driver = "sqlite"
dsn = "file:/path/to/synaps3/db/synaps3.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"

[cache]
# Enabled so this installation uses the initialized cache directory.
dir = "/path/to/synaps3/cache"
```

Set `filecoin.private_key` in the config file or use `SYNAPS3_FILECOIN_PRIVATE_KEY`. `database.driver`, `database.dsn`, and `cache.dir` are enabled so a custom `--dir` remains the active runtime data directory.

If settings are saved through the admin API, SynapS3 rewrites `config.toml` in its standard format with built-in comments. User-written comments, order, and spacing are not preserved.

## Loading Rules

- Without `--config`, SynapS3 reads and writes `~/.synaps3/config.toml`
- Pass `--config <path>` to use a different config file
- A `config.toml` in the current directory is ignored unless you pass `--config config.toml`
- `synaps3 init --dir <path>` creates a config file but does not change the default config source
- Environment variables use the `SYNAPS3_` prefix and override file values
- Unknown `SYNAPS3_` names use a fallback mapping that lowercases the name and replaces `_` with `.`

Examples:

```text
SYNAPS3_SERVER_PORT                  -> server.port
SYNAPS3_S3_REGION                    -> s3.region
SYNAPS3_FILECOIN_RPC_URL             -> filecoin.rpc_url
SYNAPS3_FILECOIN_PRIVATE_KEY         -> filecoin.private_key
SYNAPS3_FILECOIN_DEFAULT_COPIES      -> filecoin.default_copies
SYNAPS3_DATABASE_DSN                 -> database.dsn
SYNAPS3_CACHE_DIR                    -> cache.dir
SYNAPS3_WORKER_UPLOAD_CONCURRENCY    -> worker.upload.concurrency
SYNAPS3_WORKER_EVICTOR_POLL_INTERVAL -> worker.evictor.poll_interval
```

## Runtime Data

Default local paths:

```text
~/.synaps3/
  config.toml
  db/
    synaps3.db
    synaps3.db-shm
    synaps3.db-wal
  cache/
```

SQLite WAL and SHM files are expected and live beside the database file. Explicit `database.dsn` and `cache.dir` values take precedence.

## Main Sections

| Section | Key Fields | Purpose |
| --- | --- | --- |
| `server` | `port`, `max_connections`, `max_requests`, `tls.enabled`, `tls.cert_file`, `tls.key_file` | S3 API listener and concurrency limits |
| `s3` | `region` | Region reported by the S3-compatible gateway |
| `filecoin` | `network`, `rpc_url`, `private_key`, `source`, `with_cdn`, `allow_private_networks`, `default_copies` | Filecoin client and upload defaults |
| `database` | `driver`, `dsn`, `max_open_conns`, `max_idle_conns` | Metadata database |
| `cache` | `dir`, `max_size_gb`, `eviction_policy` | Local object cache |
| `worker.upload` | `concurrency`, `poll_interval`, `max_retries` | Upload worker tuning |
| `worker.evictor` | `concurrency`, `poll_interval`, `max_retries` | Cache eviction worker tuning |
| `logging` | `level`, `format` | Log output |
| `admin` | `addr` | Admin dashboard and API address |

`filecoin.network` supports `calibration` and `mainnet`. `filecoin.default_copies` accepts `1` through `8`. `cache.eviction_policy` supports `lru`, `manual`, and `none`.

## Production Example

```toml
[database]
driver = "postgres"
dsn = "postgres://synaps3:password@db:5432/synaps3?sslmode=require"
max_open_conns = 25
max_idle_conns = 10

[cache]
dir = "/var/lib/synaps3/cache"
max_size_gb = 500
eviction_policy = "lru"

[filecoin]
network = "calibration"
rpc_url = "https://api.calibration.node.glif.io/rpc/v1"
private_key = "0x..."
source = "synaps3"
with_cdn = false
allow_private_networks = false
default_copies = 2

[admin]
addr = "127.0.0.1:9090"
```

Keep secrets out of commits. For production, prefer environment variables or secret-managed config files.
