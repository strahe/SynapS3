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
| Dashboard and Admin API | Keep on `127.0.0.1:9090`; use SSH tunneling or HTTPS reverse proxy for remote access. |
| Metrics | Scrape with Admin auth from the private network or host-local agent only. |

Do not publish the dashboard or Admin API directly to the internet. Settings, wallet, task retry, and S3 user endpoints can change the node.

## Runtime Data

- Put `/var/lib/synaps3` or `~/.synaps3` on durable storage.
- Use the default SQLite database unless deployment requirements call for an external PostgreSQL service.
- Stop SynapS3 before backup. For SQLite, back up the complete runtime data volume. For PostgreSQL, use a database-native backup plus matching configuration and cache data.
- Keep the database and cache at the same recovery point, verify backup archives, and test the documented restore order.
- Watch free space on the database volume and cache volume.
- Keep `config.toml`, `.env`, databases, cache data, and wallet material out of git. Protect configuration, secret, and credential files with `0600` permissions.

## Secrets and Wallet

- Store `SYNAPS3_FILECOIN_PRIVATE_KEY` in a host environment, `.env`, or secret manager.
- Store the Admin password securely. Rotate it offline with `synaps3 admin-auth reset-password --config <path>` when it is lost or exposed; this also invalidates existing browser sessions.
- Confirm `synaps3 admin status` reports a healthy wallet after startup.
- Deposit USDFC and approve FWSS before expected uploads. This example deposits `2 USDFC`:

```bash
synaps3 wallet deposit 2 # 2 USDFC
synaps3 wallet approve
```

A new deposit or approval should print `Transaction: <hash>` and `Status: confirmed`; an existing approval prints `FWSS approval: already approved`.

## Configuration Review

Check the effective settings:

```bash
synaps3 admin settings get
```

Review these values first:

| Field | Check |
| --- | --- |
| `admin.addr` | Keep `127.0.0.1:9090` unless protected by HTTPS and access control. |
| `admin.trusted_proxies` | Keep empty unless trusted proxies strip untrusted forwarded headers. |
| `admin.auth.enabled` | Keep `true` for production. |
| Admin password hash and `admin.auth.session_secret` | Must be present; generate the hash with init/reset and manage the session secret as a secret. |
| `filecoin.network` | `calibration` until you intentionally move to `mainnet` |
| `filecoin.allow_private_networks` | `false` unless provider URLs are trusted private endpoints |
| `cache.max_size_gb` | Size it for expected upload backlog |
| `logging.format` | Compose sets `json`; built-in default is `text`. |

After saving settings, restart SynapS3, check `/healthz`, and run `synaps3 admin settings get` again to verify the effective values.

High-risk settings require explicit confirmation:

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
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
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 50
```

Expected result: health is `ok`, task queues are understood, and every exhausted task has a clear handling decision before the process is replaced.

## Recovery Entry Points

- Health issue: start with [Health and Metrics](./health-metrics.md).
- Failed background work: use [Troubleshooting](./troubleshooting.md).
- Version change: follow [Upgrade and Recovery](./upgrade-recovery.md).
