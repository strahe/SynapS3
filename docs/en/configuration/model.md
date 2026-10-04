---
title: Configuration Model
description: Understand SynapS3 configuration sources, defaults, editable settings, and high-risk fields.
---

# Configuration Model

SynapS3 reads TOML configuration first, then applies `SYNAPS3_` environment overrides. Use a config file for stable settings and environment variables for secrets or deployment-specific overrides.

## Source Rules

- Without `--config`, SynapS3 reads `~/.synaps3/config.toml`.
- Pass `--config <path>` to use another file.
- A `config.toml` in the current directory is ignored unless passed explicitly.
- `synaps3 init --dir <path>` creates files but does not change the default config source.
- Admin settings writes rewrite `config.toml`; comments and ordering are not preserved.

Check the effective settings:

```bash
synaps3 admin settings get
```

The output shows the config path, whether writes are allowed, and whether restart is required.

After saving settings, restart SynapS3, check `/healthz`, and run `synaps3 admin settings get` again to confirm the effective values.

## Required Secrets

Set the Filecoin wallet private key before normal serving:

```toml
[filecoin]
private_key = "0x..."
```

Or manage the value through `SYNAPS3_FILECOIN_PRIVATE_KEY`; see [Environment Variables](./environment.md) for supported overrides.

Keep private keys out of commits, container images, and shell history.

Admin auth also requires a password hash and `admin.auth.session_secret` when `admin.auth.enabled = true`. `synaps3 init` creates both for new configs; use `synaps3 admin-auth reset-password --config <path>` when a password is missing or must be rotated. Password reset also rotates the session secret.

Keep configuration, `.env`, and credential files at permission mode `0600`.

## S3 Server

The S3 API supports native TLS through these fields:

```toml
[server.tls]
enabled = true
cert_file = "/path/to/tls.crt"
key_file = "/path/to/tls.key"
```

The certificate and private key must be readable by the SynapS3 process. In a container deployment, their configured paths must exist inside the container, typically through read-only mounts. Production S3 traffic must use native TLS or a controlled TLS reverse proxy.

The Admin endpoint has separate exposure controls. Keep `admin.addr` on loopback, use an SSH tunnel, or place it behind an access-controlled HTTPS reverse proxy.

## Database Choice

SQLite is the default and recommended database for SynapS3 single-node deployments. PostgreSQL remains available when a deployment already operates an external PostgreSQL service or needs an external metadata database. Keep its DSN in protected configuration or secret storage.

`database.max_open_conns` sizes the connection pool. SQLite still writes through one connection at a time; the other connections serve reads, and a write that finds the database busy waits up to five seconds (`busy_timeout`) before it fails with `SQLITE_BUSY`.

## Main Sections

| Section | Purpose |
| --- | --- |
| `server` | S3 API listener, concurrency limits, and TLS fields. |
| `s3` | Region reported to S3 clients. |
| `filecoin` | Network, RPC, wallet, provider URL policy, CDN hints, and copy policy. |
| `filecoin.observability` | Provider and local data set health checks. |
| `database` | SQLite or PostgreSQL metadata database. |
| `cache` | Local object cache directory, capacity, and eviction policy. |
| `worker.tasks` | Shared background task execution, recovery, retention, and provider mutation limits. |
| `logging` | Runtime log level, format, and S3 access logs. |
| `admin` | Dashboard, Admin API listener, and Admin auth settings. |

## Important Defaults

| Field | Default |
| --- | --- |
| `server.port` | `:8080` |
| `server.max_connections` | `4096` |
| `server.max_requests` | `512` |
| `s3.region` | `us-east-1` |
| `filecoin.network` | `calibration` |
| `filecoin.default_copies` | `3` |
| `database.driver` | `sqlite` |
| `database.max_open_conns` | `32` |
| `database.max_idle_conns` | `2` |
| `cache.max_size_gb` | `100` |
| `cache.eviction_policy` | `lru` |
| `cache.lru_high_watermark_percent` | `80` |
| `cache.lru_low_watermark_percent` | `50` |
| `worker.tasks.concurrency` | `12` |
| `worker.tasks.poll_interval` | `5s` |
| `worker.tasks.lease_duration` | `5m` |
| `worker.tasks.max_retries` | `5` |
| `worker.tasks.retention` | `168h` |
| `worker.tasks.provider_mutation_concurrency` | `4` |
| `worker.tasks.destructive_mutation_concurrency` | `2` |
| `worker.tasks.commit_max_pieces` | `32` |
| `worker.tasks.commit_max_wait` | `30m` |
| `worker.tasks.commit_seal_on_cache_pressure` | `false` |
| `worker.tasks.commit_max_backlog` | `256` |
| `admin.addr` | `127.0.0.1:9090` |
| `admin.trusted_proxies` | `[]` |
| `admin.auth.enabled` | `true` |
| `admin.auth.username` | `admin` |
| `admin.auth.session_ttl` | `12h` |

`worker.tasks.concurrency` limits all background operations. Remote storage creation, Store, Pull, and commit submission additionally share `provider_mutation_concurrency`; remote cleanup and service retirement share `destructive_mutation_concurrency`. Status and confirmation checks do not consume either mutation limit. Wallet mutations are serialized. An operation that finds its limit full steps aside and tries again shortly instead of holding a `concurrency` slot, so other background work keeps running. Task settings require a SynapS3 restart, and existing tasks retain the retry limit recorded when they were created.

Uploads and replicas copied to the same storage service share on-chain batch submissions, so one signed request and one transaction cover many pieces. `commit_max_pieces` (1–200) caps the pieces in one batch; storage services with a data set ID below 1,559 on Mainnet or 32,331 on Calibration take at most 80. A batch is signed as soon as it is full or its storage service stops taking new data. Otherwise, `commit_max_wait` (0–30m) gives later uploads and replicas time to join, starting when the oldest piece became ready; `0s` signs immediately. New pieces and restarts do not reset that time. Once signed, membership stays fixed while the batch waits for an earlier submission or one of the storage service's four in-flight slots. Upload data awaiting submission stays in the local cache. Longer windows delay submission, replica copies that need a committed source, and local cache eviction.

Once `commit_max_backlog` transferred pieces (at least `commit_max_pieces`), from uploads or replicas, wait on one storage service for an unsent batch, new uploads and replica copies to it wait. A batch that does not fit one add-pieces message is split and signed again with fewer pieces.

Enable **Submit batches early to free cache space** in Settings (`worker.tasks.commit_seal_on_cache_pressure`) to submit batches early when safe automatic cleanup cannot free enough space. It is off by default and requires restart. With `lru`, it follows the effective cleanup target; with `after_upload`, a refused write supplies the required space. It has no automatic effect under `none`. Smaller batches can increase transaction costs. Confirmation and the bucket's durability requirements still apply before cache removal.

Open **Batches** and choose **Submit batch** for a batch waiting to submit. This works independently of the automatic option and cache policy. Open **Details** to inspect members, submission times, transaction ID, and errors; stopped tasks use **Recover** when available. Each member shows a related object and the number of other versions sharing its data. Missing historical object information and sizes are shown as unavailable.

## Admin Session Lifetime

`admin.auth.session_ttl` controls the lifetime of each standard Admin UI session token. It is neither a server-enforced idle timeout nor an absolute cap on a login. After the earlier of five minutes or half the token lifetime, the server permits renewal. The official dashboard requests renewal only after a trusted pointer, click, keyboard, or wheel interaction; background polling and returning to a visible tab do not trigger it. Any client holding the valid session cookie and matching CSRF token can call the refresh endpoint after `refresh_after`. If no client requests renewal, the token expires at `expires_at`.

The login page uses a browser-session cookie by default. Selecting **Keep me signed in** creates a persistent cookie and uses the greater of 30 days or `admin.auth.session_ttl`. While the dashboard continues to receive user activity, it can keep requesting renewal; the server applies no absolute login lifetime cap.

## Allowed Values

- `filecoin.network`: `calibration`, `mainnet`.
- `filecoin.default_copies`: `1` through `8`.
- `database.driver`: `sqlite`, `postgres`.
- `cache.eviction_policy`: `lru`, `after_upload`, `none`.
- `logging.level`: `debug`, `info`, `warn`, `error`.
- `logging.format`: `json`, `text`.
- `admin.trusted_proxies`: IP or CIDR entries. Keep empty unless a trusted reverse proxy strips untrusted forwarded headers.

Cache eviction policies have these user-visible results:

- `lru`: when cache usage reaches the high watermark, or a write is refused because the cache is full, SynapS3 removes the least recently accessed remotely safe entries until usage reaches the low watermark. The effective low watermark never leaves less than one largest object (`1,065,353,216` bytes) of free space, which matters only for small caches.
- `after_upload`: after a version meets its bucket's minimum durable copies, SynapS3 queues it for asynchronous removal. A later remote read can restore the cache, and that restored entry is not immediately removed again.
- `none`: SynapS3 does not automatically remove local cache data.

The LRU watermarks must always satisfy `0 <= low < high <= 100`. They remain saved but have no effect under `after_upload` or `none`.

```toml
[cache]
eviction_policy = "lru"
lru_high_watermark_percent = 80
lru_low_watermark_percent = 50
```

Eviction settings take effect after restart. Cache cleanup is asynchronous: a write does not wait for an LRU pass, and a refused write only requests the next one.

## High-Risk Fields

| Field | Risk |
| --- | --- |
| `admin.addr` | Exposing Admin API allows operational writes. Keep loopback unless protected by HTTPS and access control. |
| `admin.trusted_proxies` | Enables `X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, and `X-Forwarded-Host` trust for matching proxies. Configure only proxies you control. |
| Admin password hash | Controls Admin login. Do not configure it manually; generate it with `synaps3 init` or `synaps3 admin-auth reset-password`. |
| `admin.auth.session_secret` | Signs Admin browser sessions. Treat as secret. |
| `filecoin.private_key` | Controls wallet spending and storage operations. Treat as a secret. |
| `database.dsn` | May contain database credentials. Treat it as a secret. |
| `filecoin.network` | Moving to `mainnet` changes payment and storage environment. |
| `filecoin.allow_private_networks` | Allows private-network provider URLs. Enable only for trusted private deployments. |
| `cache.max_size_gb` | Too small blocks writes; too large can consume the host disk. |
| `cache.lru_high_watermark_percent` | A high value leaves less headroom for writes while eviction catches up. |
| `cache.lru_low_watermark_percent` | A low value removes more cached data during each LRU cycle. |

High-risk settings may require explicit confirmation:

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
