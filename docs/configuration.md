# Configuration

SynapS3 reads TOML config. `SYNAPS3_` environment overrides win.

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
- Admin settings writes rewrite `config.toml`; comments and ordering are not preserved

## Required Secret

Set the Filecoin wallet private key before normal serving:

```toml
[filecoin]
private_key = "0x..."
```

Or use an environment variable:

```bash
export SYNAPS3_FILECOIN_PRIVATE_KEY='0x...'
```

Keep private keys out of commits.

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

## Common Environment Overrides

```text
SYNAPS3_SERVER_PORT                  -> server.port
SYNAPS3_S3_REGION                    -> s3.region
SYNAPS3_FILECOIN_NETWORK             -> filecoin.network
SYNAPS3_FILECOIN_RPC_URL             -> filecoin.rpc_url
SYNAPS3_FILECOIN_PRIVATE_KEY         -> filecoin.private_key
SYNAPS3_FILECOIN_DEFAULT_COPIES      -> filecoin.default_copies
SYNAPS3_DATABASE_DRIVER              -> database.driver
SYNAPS3_DATABASE_DSN                 -> database.dsn
SYNAPS3_CACHE_DIR                    -> cache.dir
SYNAPS3_CACHE_MAX_SIZE_GB            -> cache.max_size_gb
SYNAPS3_CACHE_EVICTION_POLICY        -> cache.eviction_policy
SYNAPS3_WORKER_UPLOAD_CONCURRENCY    -> worker.upload.concurrency
SYNAPS3_WORKER_UPLOAD_POLL_INTERVAL  -> worker.upload.poll_interval
SYNAPS3_WORKER_UPLOAD_MAX_RETRIES    -> worker.upload.max_retries
SYNAPS3_ADMIN_ADDR                   -> admin.addr
```

## Main Sections

| Section | Key fields | Purpose |
| --- | --- | --- |
| `server` | `port`, `max_connections`, `max_requests`, `tls.*` | S3 API listener |
| `s3` | `region` | Region reported to S3 clients |
| `filecoin` | `network`, `rpc_url`, `private_key`, `source`, `with_cdn`, `allow_private_networks`, `default_copies` | Filecoin uploads |
| `database` | `driver`, `dsn`, `max_open_conns`, `max_idle_conns` | Metadata database |
| `cache` | `dir`, `max_size_gb`, `eviction_policy` | Local object cache |
| `worker.*` | `concurrency`, `poll_interval`, `max_retries` | Background work |
| `logging` | `level`, `format`, `s3_access.*` | Runtime logs |
| `admin` | `addr` | Dashboard and admin API |

Allowed values:

- `filecoin.network`: `calibration`, `mainnet`
- `filecoin.default_copies`: `1` through `8`
- `database.driver`: `sqlite`, `postgres`
- `cache.eviction_policy`: `lru`, `manual`, `none`
- `logging.level`: `debug`, `info`, `warn`, `error`
- `logging.format`: `json`, `text`

## Notes

SQLite DSNs should stay simple:

```toml
[database]
driver = "sqlite"
dsn = "file:///var/lib/synaps3/db/synaps3.db"
```

SynapS3 manages SQLite runtime pragmas. Postgres DSNs are passed through unchanged.

`cache.eviction_policy = "lru"` queues local cache eviction after remote storage succeeds. It is not a general LRU scanner.

Default capacity settings are conservative for one node:

```toml
[server]
max_connections = 4096
max_requests = 512

[database]
max_open_conns = 4
max_idle_conns = 2
```
