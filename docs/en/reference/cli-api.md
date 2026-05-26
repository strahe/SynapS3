---
title: CLI Reference
description: Common SynapS3 CLI commands for setup, serving, wallet operations, S3 users, settings, and tasks.
---

# CLI Reference

SynapS3 exposes the S3 API, an Admin API, and CLI commands for local operations.

## Endpoints

| Surface | Default |
| --- | --- |
| S3 API | `http://localhost:8080` |
| Dashboard and Admin API | `http://127.0.0.1:9090` |
| Health | `GET http://127.0.0.1:9090/healthz` |
| Metrics | `GET http://127.0.0.1:9090/metrics` |

## Runtime Commands

| Command | Purpose |
| --- | --- |
| `synaps3 init` | Initialize `~/.synaps3` runtime data. |
| `synaps3 init --dir /var/lib/synaps3` | Initialize a custom app data directory. |
| `synaps3 serve` | Start the S3 gateway, dashboard, Admin API, and workers. |
| `synaps3 migrate` | Run database migrations and exit. |
| `synaps3 version` | Print version information. |

## Wallet Commands

```bash
synaps3 wallet generate
synaps3 wallet fund-testnet 0x...
synaps3 wallet deposit 2 # 2 USDFC
```

Expected result: `generate` prints wallet material, `fund-testnet` claims Calibration assets, and `deposit` submits a `2 USDFC` deposit using the configured private key.

## Admin Commands

```bash
synaps3 admin status
synaps3 admin s3-user create
synaps3 admin s3-user list
synaps3 admin settings get
synaps3 admin settings set cache.max_size_gb=200
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task retry 42
```

Use `--json` on admin commands when scripting successful responses.

## Settings Safety

The Admin API contains write endpoints for settings, wallet operations, task retries, and S3 user management. Keep it bound to loopback or place it behind authenticated private access.

High-risk changes require confirmation:

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
synaps3 admin s3-user create --role admin --yes
synaps3 admin s3-user delete <access-key> --yes
```

## Remote Admin Access

If SynapS3 runs on another host, keep `admin.addr` on `127.0.0.1:9090` and tunnel it:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

Then run local admin commands with the default admin URL, or pass `--admin-url` explicitly.
