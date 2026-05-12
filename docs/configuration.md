# Configuration

SynapS3 reads TOML config and applies `SYNAPS3_` environment overrides. Environment values win.

## Initialize

Default setup:

```bash
synaps3 init
synaps3 serve
```

Custom app data directory:

```bash
synaps3 init --dir /var/lib/synaps3
synaps3 serve --config /var/lib/synaps3/config.toml
```

`synaps3 init` creates `config.toml`, `db/`, and `cache/`. It fails if `config.toml` already exists.

## Config Source

- Without `--config`, SynapS3 uses `~/.synaps3/config.toml`
- Pass `--config <path>` to use a different file
- A `config.toml` in the current directory is ignored unless passed explicitly
- `synaps3 init --dir <path>` creates files but does not change the default config source
- Admin settings writes rewrite `config.toml`; custom comments and ordering are not preserved

## Required Secret

Edit `~/.synaps3/config.toml` before normal serving:

```toml
[filecoin]
private_key = "0x..."
```

Use an environment variable only when a deployment system manages secrets outside the config file:

```bash
export SYNAPS3_FILECOIN_PRIVATE_KEY='0x...'
```

Keep secrets out of commits. In production, prefer environment variables or secret-managed config files.

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

SQLite WAL and SHM files are expected. Explicit `database.dsn` and `cache.dir` values take precedence.
For SQLite, `database.dsn` only needs the database file URL. SynapS3 manages SQLite runtime pragmas.

## Environment Overrides

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
SYNAPS3_WORKER_STORAGE_CLEANUP_POLL_INTERVAL -> worker.storage_cleanup.poll_interval
SYNAPS3_ADMIN_ADDR                   -> admin.addr
```

Unknown `SYNAPS3_` names fall back to lowercase with `_` replaced by `.`.

## Main Sections

| Section | Key Fields | Purpose |
| --- | --- | --- |
| `server` | `port`, `max_connections`, `max_requests`, `tls.*` | S3 API listener |
| `s3` | `region` | Region reported to S3 clients |
| `filecoin` | `network`, `rpc_url`, `private_key`, `source`, `with_cdn`, `allow_private_networks`, `default_copies` | Filecoin uploads and admin provider identity resolution |
| `database` | `driver`, `dsn`, `max_open_conns`, `max_idle_conns` | Metadata database |
| `cache` | `dir`, `max_size_gb`, `eviction_policy` | Local object cache |
| `worker.upload` | `concurrency`, `poll_interval`, `max_retries` | Upload worker |
| `worker.evictor` | `concurrency`, `poll_interval`, `max_retries` | Cache eviction worker |
| `worker.storage_cleanup` | `concurrency`, `poll_interval`, `max_retries` | Remote replica cleanup worker |
| `logging` | `level`, `format`, `s3_access.*` | Runtime logs |
| `admin` | `addr` | Dashboard and admin API |

Allowed values:

- `filecoin.network`: `calibration`, `mainnet`
- `filecoin.default_copies`: `1` through `8`
- `cache.eviction_policy`: `lru`, `manual`, `none`
- `logging.level`: `debug`, `info`, `warn`, `error`
- `logging.format`: `json`, `text`
- `logging.s3_access.level`: `debug`, `info`, `warn`, `error`

S3 access logs are emitted through the SynapS3 runtime logger. Set `logging.s3_access.enabled = false` to disable them.

Default capacity settings are conservative for a single-node installation:

```toml
[server]
max_connections = 4096
max_requests = 512

[database]
max_open_conns = 4
max_idle_conns = 2
```

`server.max_connections` limits concurrent TCP connections. `server.max_requests` limits in-flight S3 requests before SlowDown responses. Increase them only with matching file descriptor, memory, database, disk, and backend capacity.

SQLite DSNs should stay simple:

```toml
[database]
driver = "sqlite"
dsn = "file:///var/lib/synaps3/db/synaps3.db"
```

SynapS3 adds SQLite runtime pragmas. File-backed SQLite gets `journal_mode(WAL)`, `busy_timeout(5000)`, and `foreign_keys(1)`. In-memory SQLite skips WAL and still gets `busy_timeout(5000)` and `foreign_keys(1)`. Postgres DSNs are passed through unchanged.

`cache.eviction_policy = "lru"` queues local cache eviction after remote storage succeeds. It is not a general least-recently-used cache scanner.

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

[worker.upload]
concurrency = 4
poll_interval = "5s"
max_retries = 5

[worker.evictor]
concurrency = 2
poll_interval = "1m"
max_retries = 3

[worker.storage_cleanup]
concurrency = 2
poll_interval = "1m"
max_retries = 5

[logging]
level = "info"
format = "text"

[logging.s3_access]
enabled = true
level = "info"

[admin]
addr = "127.0.0.1:9090"
```
