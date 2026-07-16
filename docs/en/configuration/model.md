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

## Main Sections

| Section | Purpose |
| --- | --- |
| `server` | S3 API listener, concurrency limits, and TLS fields. |
| `s3` | Region reported to S3 clients. |
| `filecoin` | Network, RPC, wallet, provider URL policy, CDN hints, and copy policy. |
| `filecoin.observability` | Provider and local data set health checks. |
| `database` | SQLite or PostgreSQL metadata database. |
| `cache` | Local object cache directory, capacity, and eviction policy. |
| `worker.upload` | Background Filecoin storage concurrency, polling, and retries. |
| `worker.evictor` | Local cache eviction tasks. |
| `worker.storage_cleanup` | Remote copy cleanup tasks. |
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
| `database.max_open_conns` | `4` |
| `database.max_idle_conns` | `2` |
| `cache.max_size_gb` | `100` |
| `cache.eviction_policy` | `lru` |
| `worker.upload.concurrency` | `4` |
| `worker.upload.max_retries` | `5` |
| `admin.addr` | `127.0.0.1:9090` |
| `admin.trusted_proxies` | `[]` |
| `admin.auth.enabled` | `true` |
| `admin.auth.username` | `admin` |
| `admin.auth.session_ttl` | `12h` |

## Allowed Values

- `filecoin.network`: `calibration`, `mainnet`.
- `filecoin.default_copies`: `1` through `8`.
- `database.driver`: `sqlite`, `postgres`.
- `cache.eviction_policy`: `lru`, `manual`, `none`.
- `logging.level`: `debug`, `info`, `warn`, `error`.
- `logging.format`: `json`, `text`.
- `admin.trusted_proxies`: IP or CIDR entries. Keep empty unless a trusted reverse proxy strips untrusted forwarded headers.

Cache eviction policies have these user-visible results:

- `lru`: after remote storage satisfies the configured copy policy, SynapS3 automatically queues eligible local cache data for removal.
- `manual`: SynapS3 does not automatically remove local cache data.
- `none`: SynapS3 does not automatically remove local cache data.

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

High-risk settings may require explicit confirmation:

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
