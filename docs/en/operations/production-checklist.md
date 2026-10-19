---
title: Production Checklist
description: Check network exposure, data, secrets, wallet, configuration, and monitoring before serving production traffic.
---

# Production Checklist

Complete these checks before you serve production traffic.

## Network Exposure

| Surface | Exposure |
| --- | --- |
| S3 API (`server.port`, default `:8080`) | Serve it over TLS, natively or through your own proxy. Expose it only to the clients that need it. |
| Dashboard and Admin API (`admin.addr`, default `127.0.0.1:9090`) | Keep it on loopback. For remote access, use an SSH tunnel, the Docker Admin HTTPS option, or an HTTPS reverse proxy with access control. Never bind it to a public interface. |
| `/metrics` | Requires Admin authentication. Scrape it from the same host or a private network. |
| `/healthz` | Requires no authentication. An `unhealthy` response names the failing check. |

The Admin API can change data, S3 credentials, and wallet funds. Keep `admin.auth.enabled = true`. Behind a reverse proxy, set `admin.trusted_proxies` to the proxy address only if the proxy strips forwarded headers sent by clients.

SynapS3 needs outbound access to `filecoin.rpc_url` and to the storage providers' service URLs.

## Data and Backups

- Put the SynapS3 data directory (`~/.synaps3` or `/var/lib/synaps3`) and PostgreSQL on durable storage, and monitor free space on both.
- The database and its backups contain S3 secret keys in plaintext. Restrict access to them as you would to S3 credentials.
- Back up and test a restore as described in [Runtime Data](../configuration/runtime-data.md).
- Keep `config.toml`, `.env`, `.postgres-admin-password`, database dumps, cache data, and wallet keys out of git, with mode `0600` for credential files.

## Secrets and Wallet

- Keep `SYNAPS3_FILECOIN_PRIVATE_KEY` in the host environment, `.env`, or a secret manager.
- Keep the Admin password in a password manager. If it is lost or exposed, run `synaps3 admin-auth reset-password --config <path>` and restart SynapS3; this also signs out all browser sessions.
- Fund the wallet before uploads: keep FIL for gas, deposit USDFC, and approve FWSS on the **Wallet** page or with the CLI:

  ```bash
  synaps3 wallet deposit 2 # 2 USDFC
  synaps3 wallet approve
  ```

## Configuration Review

```bash
synaps3 admin settings get
```

| Field | Check |
| --- | --- |
| `admin.addr` | `127.0.0.1:9090`, unless it is behind HTTPS and access control. |
| `admin.trusted_proxies` | Empty, unless a trusted proxy strips client forwarded headers. |
| `admin.auth.enabled` | `true`. |
| `filecoin.network`, `filecoin.rpc_url` | The intended network; one database per network. |
| `filecoin.allow_private_networks` | `false`, unless you trust storage providers on private networks. |
| `cache.max_size_gb` | Large enough for the upload backlog that is not yet on Filecoin. |
| `logging.format` | `json` for log collection. The built-in default is `text`; the Docker deployment sets `json`. |

## Monitoring

Monitor at least:

- the `status` field of `/healthz`, not only the HTTP status;
- `synaps3_cache_used_bytes` against `cache.max_size_gb`;
- `synaps3_task_queue_depth` and the failed task count from `synaps3 admin task stats`;
- storage provider and data set health on the dashboard.

See [What to Alert On](./health-metrics.md#what-to-alert-on).

## Before Go-Live

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status failed --limit 50
```

Go live when health is `ok`, every failed task has a known cause, and you have a verified backup.
