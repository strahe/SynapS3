---
title: Docker Deployment
description: Deploy SynapS3 with Docker Compose.
---

# Docker Deployment

The container stores runtime data under `/var/lib/synaps3`, exposes the S3 API on port `8080`, and keeps the dashboard and Admin API bound to loopback by default.

## Prerequisites

- Docker Engine with Docker Compose v2.24 or later.
- Durable local disk for the `synaps3-data` volume.
- SSH access if the dashboard is reached from another machine.
- A Calibration wallet you can fund, or use the wallet steps below.

## Prepare Configuration

Create a deployment directory with the Compose file:

```bash
mkdir synaps3-deploy
cd synaps3-deploy
curl -fsSLO https://raw.githubusercontent.com/strahe/SynapS3/main/compose.yaml
```

Generate a wallet:

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

The command prints a wallet address and private key. Create a protected `.env` file:

```bash
touch .env
chmod 600 .env
```

Then edit `.env` and add the generated private key:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

Keep `.env` at permission mode `0600`. Do not put the real private key directly in a shell command because it may be saved in shell history. See [Environment Variables](../configuration/environment.md) for other supported overrides.

Fund the generated address on Calibration:

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

If faucet funding is unreliable, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/) before serving.

Successful faucet claims print `CalibnetUSDFC: <hash>` and `CalibnetFIL: <hash>`.

## Start SynapS3

```bash
docker compose up -d
docker compose logs --tail=50 synaps3
```

The logs should show the service starting without config validation errors.

Default endpoints:

| Endpoint | Address |
| --- | --- |
| S3 API | `http://<host>:8080` |
| Dashboard and Admin API | `http://127.0.0.1:9090` |
| Runtime data | Docker volume `synaps3-data` |

The Compose file uses host networking. This lets the S3 API listen on the host while the admin server stays on loopback.

The HTTP addresses above are suitable for local evaluation. For production S3 traffic, either configure native TLS with `SYNAPS3_SERVER_TLS_ENABLED=true`, `SYNAPS3_SERVER_TLS_CERT_FILE`, and `SYNAPS3_SERVER_TLS_KEY_FILE`, or place the S3 API behind a controlled TLS reverse proxy. Certificate and key paths must be visible inside the container, typically through read-only mounts. Keep the Admin endpoint on loopback, through an SSH tunnel, or behind an access-controlled HTTPS reverse proxy.

For dashboard login, read the generated Admin password:

```bash
docker compose exec synaps3 cat /var/lib/synaps3/admin-initial-password
```

The username is `admin`. Container-local `synaps3 admin` commands read this password file automatically.

Read the password only in a private terminal, store it in a password manager, and keep the password file at `0600`. Do not place the password directly in shell history.

## Verify the Node

```bash
curl http://127.0.0.1:9090/healthz
docker compose exec synaps3 synaps3 admin status
docker compose exec synaps3 synaps3 wallet deposit 2 # 2 USDFC
docker compose exec synaps3 synaps3 wallet approve
```

Expected result: health returns `{"status":"ok"}`, and `admin status` shows runtime, background task, and cache status. A new deposit or approval prints `Transaction: <hash>` and `Status: confirmed`; an existing approval prints `FWSS approval: already approved`.

Access a remote dashboard through SSH:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

> [!CAUTION]
> Do not publish the dashboard or Admin API directly to an untrusted network. Use SSH tunneling or an HTTPS reverse proxy with explicit access control for remote access.

## Operate the Deployment

Before putting traffic on the node, complete the [Production Checklist](../operations/production-checklist.md).

Useful commands:

```bash
docker compose ps
docker compose logs --tail=100 synaps3
docker compose exec synaps3 synaps3 admin task stats
```

## Upgrade

Follow [Upgrade and Recovery](../operations/upgrade-recovery.md). It includes preflight checks, a consistent backup, upgrade verification, and rollback order.

## Back Up Runtime Data

Do not archive a live data volume. Check health and the task queue, stop SynapS3, then follow the driver-specific backup and restore steps in [Runtime Data](../configuration/runtime-data.md). Keep the database and cache at the same recovery point.
