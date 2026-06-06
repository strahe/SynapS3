---
title: Quick Start
description: Start a temporary SynapS3 node, fund it on Calibration, and upload the first object.
---

# Quick Start

For a quick evaluation, start a temporary Docker container, create a Calibration wallet, and upload one S3 object.

For a long-lived node, use [Docker Deployment](./docker.md).

## Prerequisites

- Docker Engine or Docker Desktop.
- Host networking enabled. Docker Desktop users need host networking for the full flow because the Admin API stays bound to loopback by default.
- A shell on the machine where the node will run.

## Create Configuration and Wallet

Generate a wallet:

```bash
docker run --rm ghcr.io/strahe/synaps3:edge synaps3 wallet generate
```

The command prints a wallet address and private key. Edit `.env` and add the generated private key:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

Do not put the real private key directly in a shell command because it may be saved in shell history. See [Environment Variables](../configuration/environment.md) for other supported overrides.

Fund the generated address on Calibration:

```bash
docker run --rm --env-file .env ghcr.io/strahe/synaps3:edge synaps3 wallet fund-testnet 0x...
```

If the faucet command fails, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/), then continue after the wallet has funds.

## Start the Temporary Node

```bash
docker volume create synaps3-test-data
docker run -d --name synaps3-test \
  --network host \
  --env-file .env \
  -v synaps3-test-data:/var/lib/synaps3 \
  ghcr.io/strahe/synaps3:edge
```

Docker prints a container ID when the node starts.

Check health and deposit USDFC:

```bash
curl http://127.0.0.1:9090/healthz
docker exec synaps3-test synaps3 --config /var/lib/synaps3/config.toml wallet deposit 2 # 2 USDFC
```

Expected result: health returns `{"status":"ok"}` and the deposit command accepts the wallet operation. If health returns `setup` or `unhealthy`, use [Troubleshooting](../operations/troubleshooting.md).

## Open the Dashboard

Read the generated Admin password:

```bash
ADMIN_PASSWORD=$(docker exec synaps3-test cat /var/lib/synaps3/admin-initial-password)
```

Open:

```text
http://127.0.0.1:9090
```

If the node runs on a remote host, keep the Admin API on loopback and tunnel it:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

The dashboard asks for the Admin username `admin` and the generated password. After login, it shows buckets, wallet status, tasks, topology, settings, and health.

## Upload the First Object

Create an S3 user:

```bash
docker exec -e SYNAPS3_ADMIN_PASSWORD="$ADMIN_PASSWORD" synaps3-test \
  synaps3 --config /var/lib/synaps3/config.toml admin s3-user create
```

Use the access key and secret with a path-style S3 client. This example uses MinIO Client:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
mc alias set synaps3 http://localhost:8080 replace-with-access-key replace-with-secret-key
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

`mc cat` prints the uploaded content. The sample file is padded because the Filecoin upload path requires objects of at least 127 bytes.

See [S3 Clients](./s3-clients.md) for AWS CLI and rclone examples.

## Clean Up

```bash
docker rm -f synaps3-test
docker volume rm synaps3-test-data
```

Do not run these cleanup commands against a long-lived deployment.
