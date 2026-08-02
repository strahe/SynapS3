---
title: Production Checklist
description: Prepare a SynapS3 deployment.
---

# Production Checklist

Before serving traffic, verify local disk, database health, background tasks, transport security, and recovery paths.

## Network Exposure

| Surface | Recommended exposure |
| --- | --- |
| S3 API | Require native TLS or a controlled TLS reverse proxy; expose only to trusted clients or an authenticated edge. |
| Dashboard and Admin API | Keep the backend on `127.0.0.1:9090`; publish only through the Docker deployment's authenticated Caddy HTTPS endpoint. |
| Metrics | Keep Admin auth enabled and restrict scraping to the host, a private network, or an authenticated HTTPS client. |

Never bind port 9090 to a public interface. Settings, wallet, task retry, and S3 user endpoints can change the node. For the public Admin hostname, confirm `make docker-verify` validates its certificate and HTTP redirect.

## Runtime Data

- Put `/var/lib/synaps3` or `~/.synaps3` on durable storage.
- Use the default SQLite database unless deployment requirements call for an external PostgreSQL service.
- Stop SynapS3 before backup. For SQLite, back up the complete runtime data volume. For PostgreSQL, use a database-native backup plus matching configuration and cache data.
- Keep the database and cache at the same recovery point, verify backup archives, and test the documented restore order.
- Watch free space on the database volume and cache volume.
- Keep `config.toml`, `.env`, databases, cache data, and wallet material out of git. Protect configuration, secret, and credential files with `0600` permissions.
- Preserve `synaps3-caddy-data` and `synaps3-caddy-config`; they contain the Admin certificate, private key, ACME account, and Caddy state.

## Secrets and Wallet

- Store `SYNAPS3_FILECOIN_PRIVATE_KEY` in a host environment, `.env`, or secret manager.
- Store the Admin password securely. Rotate it offline with `synaps3 admin-auth reset-password --config <path>` when it is lost or exposed; this also invalidates existing browser sessions.
- Confirm the Admin dashboard or the following command reports a healthy wallet after startup:

```bash
make docker-admin ADMIN_ARGS='status'
```

- Deposit USDFC and approve FWSS from the dashboard before expected uploads. Confirm each transaction reaches `confirmed` before relying on it.

## Configuration Review

Check the effective settings:

```bash
make docker-admin ADMIN_ARGS='settings get'
```

Review these values first:

| Field | Check |
| --- | --- |
| `admin.addr` | Keep `127.0.0.1:9090` unless protected by HTTPS and access control. |
| `admin.trusted_proxies` | The Admin HTTPS deployment must set only `127.0.0.1/32` for host-local Caddy. |
| `admin.auth.enabled` | Keep `true` for production. |
| Admin password hash and `admin.auth.session_secret` | Must be present; generate the hash with init/reset and manage the session secret as a secret. |
| `filecoin.network` | `calibration` until you intentionally move to `mainnet` |
| `filecoin.allow_private_networks` | `false` unless provider URLs are trusted private endpoints |
| `cache.max_size_gb` | Size it for expected upload backlog |
| `logging.format` | Compose sets `json`; built-in default is `text`. |

After saving settings, run `make docker-stop`, `make docker-up`, `make docker-verify`, and the settings command again.

High-risk settings require explicit confirmation:

```bash
make docker-admin ADMIN_ARGS='settings set filecoin.network=mainnet --yes'
```

## Monitoring

At minimum, monitor:

- `GET /healthz`
- `GET /metrics`
- cache usage
- task queue depth
- exhausted task count
- background task activity
- provider and data set health

Treat `{"status":"unhealthy"}` as a problem to investigate. It means database, cache, or background task checks failed.

## Upgrade Readiness

Before upgrading:

```bash
make docker-verify
make docker-admin ADMIN_ARGS='task stats'
make docker-admin ADMIN_ARGS='task list --status exhausted --limit 50'
```

Expected result: health is `ok`, task queues are understood, and every exhausted task has a clear handling decision before the process is replaced.

## Recovery Entry Points

- Health issue: start with [Health and Metrics](./health-metrics.md).
- Failed background work: use [Troubleshooting](./troubleshooting.md).
- Version change: follow [Upgrade and Recovery](./upgrade-recovery.md).
