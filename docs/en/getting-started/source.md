---
title: Build from Source
description: Build the SynapS3 binary, prepare PostgreSQL and the wallet, and start the node.
---

# Build from Source

This page builds the `synaps3` binary and runs it directly. For containers, see [Docker Deployment](./docker.md).

## Prerequisites

- Go 1.27.2 or later.
- `make`.
- A C toolchain for cgo, such as `gcc` or `clang`.
- Node.js 22.12 or later and pnpm 11.
- PostgreSQL 18, or Docker to run it locally.

## Build

```bash
git clone https://github.com/strahe/SynapS3.git
cd SynapS3
make build
```

`make build` builds the dashboard, embeds it, and writes `bin/synaps3`.

## Prepare PostgreSQL

Use a database owned by a non-superuser role. To run PostgreSQL locally with Docker, with a database and role both named `synaps3`:

```bash
printf 'PostgreSQL administrator password: '
read -rs POSTGRES_PASSWORD
printf '\n'
printf 'SynapS3 database password: '
read -rs POSTGRES_APP_PASSWORD
printf '\n'
export POSTGRES_PASSWORD POSTGRES_APP_PASSWORD
docker run -d --name synaps3-postgres --restart unless-stopped \
  -e POSTGRES_DB=synaps3 -e POSTGRES_PASSWORD -e POSTGRES_APP_PASSWORD \
  -p 127.0.0.1:5432:5432 -v synaps3-postgres-local:/var/lib/postgresql \
  -v "$PWD/docker/postgres-init.sql:/docker-entrypoint-initdb.d/synaps3.sql:ro" \
  postgres:18
unset POSTGRES_PASSWORD POSTGRES_APP_PASSWORD
```

PostgreSQL creates the role and passwords only when the volume is empty.

## Initialize Runtime Data

```bash
./bin/synaps3 init
./bin/synaps3 wallet generate
```

`synaps3 init` creates `~/.synaps3/config.toml`, the `cache/` directory, and the Admin credentials. In a terminal, it prints the Admin password once; otherwise it writes the password to `~/.synaps3/admin-initial-password`. Store the password in a password manager.

Add the database connection URL and the wallet private key to `~/.synaps3/config.toml`:

```toml
[database]
dsn = "postgres://synaps3:PASSWORD@127.0.0.1:5432/synaps3?sslmode=disable"

[filecoin]
private_key = "0x..."
```

The file holds credentials and wallet material. Keep it at mode `0600`, readable only by the account that runs SynapS3.

On Calibration, claim test FIL and USDFC:

```bash
./bin/synaps3 wallet fund-testnet 0x...
```

A successful claim prints `CalibnetUSDFC: <hash>` and `CalibnetFIL: <hash>`.

## Serve

```bash
./bin/synaps3 serve
```

| Endpoint | Default |
| --- | --- |
| S3 API | `http://localhost:8080` |
| Dashboard and Admin API | `http://127.0.0.1:9090` |
| Runtime data | `~/.synaps3/` |

In another terminal, check health, deposit USDFC, and approve FWSS:

```bash
curl http://127.0.0.1:9090/healthz
./bin/synaps3 wallet deposit 2 # 2 USDFC
./bin/synaps3 wallet approve
```

Health returns `{"status":"ok"}`. Each transaction prints `Transaction: <hash>` and `Status: confirmed`; if FWSS is already approved, `approve` prints `FWSS approval: already approved`.

Plain HTTP is for local evaluation. For production, see [Network Exposure](../operations/production-checklist.md#network-exposure).

Continue with [Quick Start](./quick-start.md) to create S3 credentials and upload a first object.

## Common Build and Startup Issues

| Symptom | Fix |
| --- | --- |
| Dashboard build fails | Install Node.js 22.12 or later and pnpm 11. |
| Go build fails on cgo | Install a C toolchain and make sure it is in `PATH`. |
| `serve` fails Admin auth validation | If `config.toml` exists, run `./bin/synaps3 admin-auth reset-password --config ~/.synaps3/config.toml`; otherwise run `./bin/synaps3 init`. |
| `serve` reports `database.dsn is not set` | Set `database.dsn` in the config file or `SYNAPS3_DATABASE_DSN`. |
| `/healthz` returns `setup` | Set `filecoin.private_key` in the config file or `SYNAPS3_FILECOIN_PRIVATE_KEY`, then restart. |
