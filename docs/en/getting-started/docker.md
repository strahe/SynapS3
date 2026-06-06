---
title: Docker Deployment
description: Deploy SynapS3 as a long-running single-host service with Docker Compose.
---

# Docker Deployment

Use Docker Compose for a long-running single-host deployment. The container stores runtime data under `/var/lib/synaps3`, exposes the S3 API on port `8080`, and keeps the dashboard and Admin API bound to loopback by default.

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

The command prints a wallet address and private key. Edit `.env` and add the generated private key:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

Do not put the real private key directly in a shell command because it may be saved in shell history. See [Environment Variables](../configuration/environment.md) for other supported overrides.

Fund the generated address on Calibration:

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

If faucet funding is unreliable, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/) before serving.

## Start SynapS3

```bash
docker compose up -d
docker compose logs --tail=50 synaps3
```

The logs should show the service starting without config validation errors.

Read the generated Admin password from the runtime volume:

```bash
ADMIN_PASSWORD=$(docker compose exec synaps3 cat /var/lib/synaps3/admin-initial-password)
```

Default endpoints:

| Endpoint | Address |
| --- | --- |
| S3 API | `http://<host>:8080` |
| Dashboard and Admin API | `http://127.0.0.1:9090` |
| Runtime data | Docker volume `synaps3-data` |

The Compose file uses host networking. This lets the S3 API listen on the host while the admin server stays on loopback.

## Verify the Node

```bash
curl http://127.0.0.1:9090/healthz
docker compose exec -e SYNAPS3_ADMIN_PASSWORD="$ADMIN_PASSWORD" synaps3 \
  synaps3 --config /var/lib/synaps3/config.toml admin status
docker compose exec synaps3 synaps3 --config /var/lib/synaps3/config.toml wallet deposit 2 # 2 USDFC
```

Expected result: health returns `{"status":"ok"}`, and `admin status` shows runtime, worker, and cache status. The deposit command submits a wallet operation.

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
docker compose exec -e SYNAPS3_ADMIN_PASSWORD="$ADMIN_PASSWORD" synaps3 \
  synaps3 --config /var/lib/synaps3/config.toml admin task stats
```

## Upgrade

```bash
docker compose pull
docker compose up -d
```

Compose replaces the container and keeps `synaps3-data` mounted.

## Back Up Runtime Data

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
```

The archive should contain `config.toml`, `db/`, and cache data from the mounted volume.
