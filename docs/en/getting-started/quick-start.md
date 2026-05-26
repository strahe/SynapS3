---
title: Quick Start
description: Start a temporary SynapS3 node, fund it on Calibration, and upload the first object.
---

# Quick Start

Use this page to evaluate SynapS3 with a disposable Docker container, a Calibration wallet, and one S3 object upload.

For a long-lived node, use [Docker Deployment](./docker.md) after you understand the flow.

## Goal

Outcome:

- `GET /healthz` returns `{"status":"ok"}`.
- The dashboard opens on `http://127.0.0.1:9090`.
- A test object can be uploaded and read back through an S3 client.

## Prerequisites

- Docker Engine or Docker Desktop.
- Host networking enabled. Docker Desktop users must enable host networking for the full flow because the Admin API stays bound to loopback and Admin write operations require loopback binding.
- A shell on the machine where the node will run.

## Create Configuration and Wallet

Generate a wallet:

```bash
docker run --rm ghcr.io/strahe/synaps3:edge synaps3 wallet generate
```

Expected result: the command prints a wallet address and private key. Edit `.env` and add the generated private key:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

Do not put the real private key directly in a shell command because it may be saved in shell history. See [Environment Variables](../configuration/environment.md) for other supported overrides.

Fund the generated address on Calibration:

```bash
docker run --rm --env-file .env ghcr.io/strahe/synaps3:edge synaps3 wallet fund-testnet 0x...
```

Expected result: the command claims test assets. If it fails, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/), then continue after the wallet has funds.

## Start the Temporary Node

```bash
docker volume create synaps3-test-data
docker run -d --name synaps3-test \
  --network host \
  --env-file .env \
  -v synaps3-test-data:/var/lib/synaps3 \
  ghcr.io/strahe/synaps3:edge
```

Expected result: Docker prints a container ID.

Check health and deposit USDFC:

```bash
curl http://127.0.0.1:9090/healthz
docker exec synaps3-test synaps3 --config /var/lib/synaps3/config.toml wallet deposit 2 # 2 USDFC
```

Expected result: health returns `{"status":"ok"}` and the deposit command accepts a wallet operation. If health returns `setup` or `unhealthy`, use [Troubleshooting](../operations/troubleshooting.md).

## Open the Dashboard

Open:

```text
http://127.0.0.1:9090
```

If the node runs on a remote host, keep the Admin API on loopback and tunnel it:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

Expected result: the dashboard shows buckets, wallet status, tasks, topology, settings, and health signals.

## Upload the First Object

Create an S3 user:

```bash
docker exec synaps3-test synaps3 --config /var/lib/synaps3/config.toml admin s3-user create
```

Use the access key and secret with a path-style S3 client. This example uses MinIO Client:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
mc alias set synaps3 http://localhost:8080 replace-with-access-key replace-with-secret-key
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

Expected result: `mc cat` prints the uploaded content. The sample file is padded because the Filecoin upload path requires objects of at least 127 bytes.

See [S3 Clients](./s3-clients.md) for AWS CLI and rclone examples.

## Clean Up

```bash
docker rm -f synaps3-test
docker volume rm synaps3-test-data
```

Expected result: the temporary container and volume are removed. Do not use this cleanup command for a long-lived deployment.
