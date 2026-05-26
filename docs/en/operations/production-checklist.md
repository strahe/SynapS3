---
title: Production Checklist
description: Prepare a long-running single-host SynapS3 deployment.
---

# Production Checklist

Before running SynapS3 as a long-lived single-host service, verify local disk, database health, background workers, and recovery paths.

## Network Exposure

| Surface | Recommended exposure |
| --- | --- |
| S3 API | Expose only to trusted clients or an authenticated edge. |
| Dashboard and Admin API | Keep on `127.0.0.1:9090`; use SSH tunneling for remote access. |
| Metrics | Scrape from the private network or host-local agent only. |

Do not publish the dashboard or Admin API directly to the internet. Settings, wallet, task retry, and S3 user endpoints are operational control surfaces.

## Runtime Data

- Put `/var/lib/synaps3` or `~/.synaps3` on durable storage.
- Back up `config.toml`, `db/`, and cache metadata before upgrades.
- Watch free space on the database volume and cache volume.
- Keep `config.toml`, `.env`, databases, cache data, and wallet material out of git.

## Secrets and Wallet

- Store `SYNAPS3_FILECOIN_PRIVATE_KEY` in a host environment, `.env`, or secret manager.
- Confirm `synaps3 admin status` reports a healthy wallet after startup.
- Deposit USDFC before expected uploads. This example deposits `2 USDFC`:

```bash
synaps3 wallet deposit 2 # 2 USDFC
```

Expected result: the wallet operation is accepted and later appears in the dashboard or `GET /api/v1/wallet/operations`.

## Configuration Review

Check the effective settings:

```bash
synaps3 admin settings get
```

Review these values first:

| Field | Check |
| --- | --- |
| `admin.addr` | Keep `127.0.0.1:9090` unless protected by private access. |
| `filecoin.network` | `calibration` until you intentionally move to `mainnet` |
| `filecoin.allow_private_networks` | `false` unless provider URLs are trusted private endpoints |
| `cache.max_size_gb` | Size it for expected upload backlog |
| `logging.format` | Compose sets `json`; built-in default is `text`. |

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
- worker liveness
- provider and data set health

Treat `{"status":"unhealthy"}` as actionable. It means database, cache, or worker checks failed.

## Upgrade Readiness

Before upgrading:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 50
```

Expected result: health is `ok`, task queues are understood, and any exhausted task has an owner decision before the process is replaced.

## Recovery Entry Points

- Health issue: start with [Health and Metrics](./health-metrics.md).
- Failed background work: use [Troubleshooting](./troubleshooting.md).
- Version change: follow [Upgrade and Recovery](./upgrade-recovery.md).
