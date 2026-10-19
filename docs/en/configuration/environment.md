---
title: Environment Variables
description: Look up the SYNAPS3_ environment variables that override configuration fields or control the CLI and container.
---

# Environment Variables

Environment variables override values in the configuration file. A field set this way cannot be changed from the **Settings** page or `synaps3 admin settings set`. Use them for secrets such as the private key and database DSN, and for host-specific values.

## Configuration Overrides

| Environment variable | Config field |
| --- | --- |
| `SYNAPS3_SERVER_PORT` | `server.port` |
| `SYNAPS3_SERVER_MAX_CONNECTIONS` | `server.max_connections` |
| `SYNAPS3_SERVER_MAX_REQUESTS` | `server.max_requests` |
| `SYNAPS3_SERVER_TLS_ENABLED` | `server.tls.enabled` |
| `SYNAPS3_SERVER_TLS_CERT_FILE` | `server.tls.cert_file` |
| `SYNAPS3_SERVER_TLS_KEY_FILE` | `server.tls.key_file` |
| `SYNAPS3_S3_REGION` | `s3.region` |
| `SYNAPS3_FILECOIN_NETWORK` | `filecoin.network` |
| `SYNAPS3_FILECOIN_RPC_URL` | `filecoin.rpc_url` |
| `SYNAPS3_FILECOIN_PRIVATE_KEY` | `filecoin.private_key` |
| `SYNAPS3_FILECOIN_WITH_CDN` | `filecoin.with_cdn` |
| `SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS` | `filecoin.allow_private_networks` |
| `SYNAPS3_FILECOIN_DEFAULT_COPIES` | `filecoin.default_copies` |
| `SYNAPS3_FILECOIN_ANCHOR_PROVIDER_TIER` | `filecoin.anchor_provider_tier` |
| `SYNAPS3_FILECOIN_UPLOAD_SPEED_TEST_AUTO_SCOPE` | `filecoin.upload_speed_test.auto_scope` |
| `SYNAPS3_FILECOIN_UPLOAD_SPEED_TEST_RETEST_INTERVAL` | `filecoin.upload_speed_test.retest_interval` |
| `SYNAPS3_FILECOIN_OBSERVABILITY_INTERVAL` | `filecoin.observability.interval` |
| `SYNAPS3_FILECOIN_OBSERVABILITY_TIMEOUT` | `filecoin.observability.timeout` |
| `SYNAPS3_FILECOIN_OBSERVABILITY_CONCURRENCY` | `filecoin.observability.concurrency` |
| `SYNAPS3_DATABASE_DRIVER` | `database.driver` |
| `SYNAPS3_DATABASE_DSN` | `database.dsn` |
| `SYNAPS3_DATABASE_MAX_OPEN_CONNS` | `database.max_open_conns` |
| `SYNAPS3_DATABASE_MAX_IDLE_CONNS` | `database.max_idle_conns` |
| `SYNAPS3_CACHE_DIR` | `cache.dir` |
| `SYNAPS3_CACHE_MAX_SIZE_GB` | `cache.max_size_gb` |
| `SYNAPS3_CACHE_EVICTION_POLICY` | `cache.eviction_policy` |
| `SYNAPS3_CACHE_LRU_HIGH_WATERMARK_PERCENT` | `cache.lru_high_watermark_percent` |
| `SYNAPS3_CACHE_LRU_LOW_WATERMARK_PERCENT` | `cache.lru_low_watermark_percent` |
| `SYNAPS3_WORKER_TASKS_CONCURRENCY` | `worker.tasks.concurrency` |
| `SYNAPS3_WORKER_TASKS_POLL_INTERVAL` | `worker.tasks.poll_interval` |
| `SYNAPS3_WORKER_TASKS_LEASE_DURATION` | `worker.tasks.lease_duration` |
| `SYNAPS3_WORKER_TASKS_UPLOAD_CONCURRENCY` | `worker.tasks.upload_concurrency` |
| `SYNAPS3_WORKER_TASKS_COMMIT_MAX_PIECES` | `worker.tasks.commit_max_pieces` |
| `SYNAPS3_WORKER_TASKS_COMMIT_MAX_WAIT` | `worker.tasks.commit_max_wait` |
| `SYNAPS3_WORKER_TASKS_COMMIT_SEAL_ON_CACHE_PRESSURE` | `worker.tasks.commit_seal_on_cache_pressure` |
| `SYNAPS3_WORKER_TASKS_COMMIT_MAX_BACKLOG` | `worker.tasks.commit_max_backlog` |
| `SYNAPS3_LOGGING_LEVEL` | `logging.level` |
| `SYNAPS3_LOGGING_FORMAT` | `logging.format` |
| `SYNAPS3_LOGGING_S3_ACCESS_ENABLED` | `logging.s3_access.enabled` |
| `SYNAPS3_LOGGING_S3_ACCESS_LEVEL` | `logging.s3_access.level` |
| `SYNAPS3_ADMIN_ADDR` | `admin.addr` |
| `SYNAPS3_ADMIN_TRUSTED_PROXIES` | `admin.trusted_proxies` (comma-separated IPs or CIDRs) |
| `SYNAPS3_ADMIN_AUTH_ENABLED` | `admin.auth.enabled` |
| `SYNAPS3_ADMIN_AUTH_USERNAME` | `admin.auth.username` |
| `SYNAPS3_ADMIN_AUTH_PASSWORD_HASH` | `admin.auth.password_hash` |
| `SYNAPS3_ADMIN_AUTH_SESSION_SECRET` | `admin.auth.session_secret` |
| `SYNAPS3_ADMIN_AUTH_SESSION_TTL` | `admin.auth.session_ttl` |

`synaps3 init` and `synaps3 admin-auth reset-password` write `admin.auth.password_hash` and `admin.auth.session_secret` to the configuration file. Set them through the environment only when an external secret store manages Admin credentials.

## CLI and Container Variables

These variables do not map to configuration fields.

| Environment variable | Purpose |
| --- | --- |
| `SYNAPS3_CONFIG` | Configuration file for commands run without `--config`. The Docker deployment sets it to `/var/lib/synaps3/config.toml`. |
| `SYNAPS3_DATA_DIR` | Container only. Data directory that the container initializes with `synaps3 init` when the configuration file does not exist. Default: `/var/lib/synaps3`. |
| `SYNAPS3_ADMIN_PASSWORD` | Admin password for `synaps3 admin` commands. Prefer the password prompt so the value stays out of shell history. |

## Handling Secrets

- Treat `SYNAPS3_FILECOIN_PRIVATE_KEY`, `SYNAPS3_DATABASE_DSN`, `SYNAPS3_ADMIN_PASSWORD`, and `SYNAPS3_ADMIN_AUTH_SESSION_SECRET` as secrets. Keep them in `.env`, the host environment, or a secret manager, never in git or container images.
- Keep `.env` at mode `0600`.
- Keep `SYNAPS3_ADMIN_TRUSTED_PROXIES` empty unless your proxy strips forwarded headers sent by clients.
- Keep `SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS` unset or `false` unless you trust storage providers on private networks.
