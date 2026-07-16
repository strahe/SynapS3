---
title: Quick Start
description: Start a temporary SynapS3 node, fund it on Calibration, and upload the first object.
---

# Quick Start

For a quick evaluation, start a temporary Docker container, create a Calibration wallet, and upload one S3 object.

For deployment, use [Docker Deployment](./docker.md).

## Prerequisites

- Docker Engine or Docker Desktop.
- Host networking enabled. Docker Desktop users need host networking for the full flow because the Admin API stays bound to loopback by default.
- A shell on the machine where the node will run.

## Create Configuration and Wallet

Generate a wallet:

```bash
docker run --rm ghcr.io/strahe/synaps3:edge synaps3 wallet generate
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
docker run --rm --env-file .env ghcr.io/strahe/synaps3:edge synaps3 wallet fund-testnet 0x...
```

If the faucet command fails, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/), then continue after the wallet has funds.

Successful faucet claims print `CalibnetUSDFC: <hash>` and `CalibnetFIL: <hash>`.

## Start the Temporary Node

```bash
docker volume create synaps3-test-data
docker run -d --name synaps3-test \
  --network host \
  --env-file .env \
  -e SYNAPS3_CONFIG=/var/lib/synaps3/config.toml \
  -v synaps3-test-data:/var/lib/synaps3 \
  ghcr.io/strahe/synaps3:edge
```

Docker prints a container ID when the node starts.

Check health, deposit USDFC, and approve FWSS:

```bash
curl http://127.0.0.1:9090/healthz
docker exec synaps3-test synaps3 wallet deposit 2 # 2 USDFC
docker exec synaps3-test synaps3 wallet approve
```

Expected result: health returns `{"status":"ok"}`. A new deposit or approval prints `Transaction: <hash>` and `Status: confirmed`; an existing approval prints `FWSS approval: already approved`. If health returns `setup` or `unhealthy`, use [Troubleshooting](../operations/troubleshooting.md).

The HTTP endpoints in this quick start are only for local evaluation. For production S3 traffic, enable [native TLS](../configuration/model.md#s3-server) or use a controlled TLS reverse proxy.

## Open the Dashboard

Browser login needs the generated Admin password:

```bash
docker exec synaps3-test cat /var/lib/synaps3/admin-initial-password
```

Open:

```text
http://127.0.0.1:9090
```

If the node runs on a remote host, keep the Admin API on loopback and tunnel it:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

The dashboard asks for the Admin username `admin` and the generated password. Read the password only in a private terminal, store it in a password manager, and keep the password file at `0600`. Do not include the password in a command that will be saved to shell history. After login, the dashboard shows buckets, wallet status, tasks, topology, settings, and health.

## Upload the First Object

Create an S3 user:

```bash
docker exec synaps3-test synaps3 admin s3-user create
```

Use the access key and secret key with a path-style S3 client. The secret key is shown only once: save it in a client credential file protected with `0600`, and rotate it immediately if it is exposed. This MinIO Client example reads the credentials interactively, without placing the secret key in shell history:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
printf 'S3 access key: '
read -r S3_ACCESS_KEY
printf 'S3 secret key: '
read -rs S3_SECRET_KEY
printf '\n'
mc alias set synaps3 http://localhost:8080 "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}"
unset S3_ACCESS_KEY S3_SECRET_KEY
chmod 600 ~/.mc/config.json
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

Do not run these cleanup commands against a real deployment. The `.env` file contains a wallet private key: if the temporary wallet is no longer needed, remove the file securely; if the wallet must be retained, move the wallet material to protected storage before removing the evaluation directory.
