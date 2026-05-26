---
title: Environment Variables
description: Configure SynapS3 with SYNAPS3 environment overrides and understand when to use them.
---

# Environment Variables

Environment variables use the `SYNAPS3_` prefix and map underscores to config paths. They override file values and are the preferred place for deployment secrets and host-specific settings.

## Common Overrides

| Environment variable | Config path |
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
| `SYNAPS3_FILECOIN_SOURCE` | `filecoin.source` |
| `SYNAPS3_FILECOIN_WITH_CDN` | `filecoin.with_cdn` |
| `SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS` | `filecoin.allow_private_networks` |
| `SYNAPS3_FILECOIN_DEFAULT_COPIES` | `filecoin.default_copies` |
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
| `SYNAPS3_WORKER_UPLOAD_CONCURRENCY` | `worker.upload.concurrency` |
| `SYNAPS3_WORKER_UPLOAD_POLL_INTERVAL` | `worker.upload.poll_interval` |
| `SYNAPS3_WORKER_UPLOAD_MAX_RETRIES` | `worker.upload.max_retries` |
| `SYNAPS3_WORKER_EVICTOR_CONCURRENCY` | `worker.evictor.concurrency` |
| `SYNAPS3_WORKER_EVICTOR_POLL_INTERVAL` | `worker.evictor.poll_interval` |
| `SYNAPS3_WORKER_EVICTOR_MAX_RETRIES` | `worker.evictor.max_retries` |
| `SYNAPS3_WORKER_STORAGE_CLEANUP_CONCURRENCY` | `worker.storage_cleanup.concurrency` |
| `SYNAPS3_WORKER_STORAGE_CLEANUP_POLL_INTERVAL` | `worker.storage_cleanup.poll_interval` |
| `SYNAPS3_WORKER_STORAGE_CLEANUP_MAX_RETRIES` | `worker.storage_cleanup.max_retries` |
| `SYNAPS3_LOGGING_LEVEL` | `logging.level` |
| `SYNAPS3_LOGGING_FORMAT` | `logging.format` |
| `SYNAPS3_LOGGING_S3_ACCESS_ENABLED` | `logging.s3_access.enabled` |
| `SYNAPS3_LOGGING_S3_ACCESS_LEVEL` | `logging.s3_access.level` |
| `SYNAPS3_ADMIN_ADDR` | `admin.addr` |

## When to Use Environment Variables

Use environment variables for:

- wallet private keys,
- container-only paths,
- network-specific RPC URLs,
- deployment-specific logging format,
- temporary overrides during troubleshooting.

Use the TOML config file for stable settings that should survive process restarts and be visible in `synaps3 admin settings get`.

## Security Guidance

- Keep `SYNAPS3_FILECOIN_PRIVATE_KEY` in a secret manager, `.env`, or host environment.
- Do not commit `.env`, `config.toml`, local databases, cache data, or wallet material.
- Keep `filecoin.allow_private_networks = false` unless private provider URLs are explicitly trusted.
- Remember that environment-managed fields override file values; changing the file will not affect them until the environment changes.

## Verify Effective Settings

```bash
synaps3 admin settings get
```

Expected result: the output shows current values and whether settings writes are available.
