---
title: Build from Source
description: Build SynapS3 locally, initialize runtime data, and verify the embedded dashboard and S3 API.
---

# Build from Source

Build from source when you are developing SynapS3, need a custom binary, or want to inspect the embedded dashboard build.

For deployment, prefer [Docker Deployment](./docker.md).

## Prerequisites

- Go 1.26.3 or later.
- `make`.
- A C toolchain for cgo, such as `gcc` or `clang`.
- Node.js 22.12 or later.
- pnpm 11.

## Build

```bash
git clone https://github.com/strahe/SynapS3.git
cd SynapS3
make build
```

The command builds the React dashboard, embeds it, and writes `bin/synaps3`.

## Initialize Runtime Data

```bash
./bin/synaps3 init
./bin/synaps3 wallet generate
```

`synaps3 init` creates `~/.synaps3/config.toml`, `db/`, `cache/`, and Admin auth. Save the printed Admin password in a password manager. If init is non-interactive, read it from `~/.synaps3/admin-initial-password` in a private terminal. Keep the configuration and password file at permission mode `0600`.

Add the generated wallet private key to `~/.synaps3/config.toml`:

```toml
[filecoin]
private_key = "0x..."
```

Do not place the private key in shell history. The configuration file contains wallet material and must remain readable only by the SynapS3 account.

For Calibration testing, fund the wallet:

```bash
./bin/synaps3 wallet fund-testnet 0x...
```

Successful faucet claims print `CalibnetUSDFC: <hash>` and `CalibnetFIL: <hash>`.

## Serve

```bash
./bin/synaps3 serve
```

Default endpoints:

| Endpoint | Address |
| --- | --- |
| S3 API | `http://localhost:8080` |
| Dashboard and Admin API | `http://127.0.0.1:9090` |
| Runtime data | `~/.synaps3/` |

In another terminal, verify health, deposit USDFC, and approve FWSS:

```bash
curl http://127.0.0.1:9090/healthz
./bin/synaps3 wallet deposit 2 # 2 USDFC
./bin/synaps3 wallet approve
```

Expected result: health returns `{"status":"ok"}`. A new deposit or approval prints `Transaction: <hash>` and `Status: confirmed`; an existing approval prints `FWSS approval: already approved`.

These HTTP endpoints are for local evaluation. Production S3 traffic must use native TLS or a controlled TLS reverse proxy; keep the Admin endpoint on loopback, through an SSH tunnel, or behind an access-controlled HTTPS reverse proxy.

## Verify with an S3 Client

Create an S3 user:

```bash
./bin/synaps3 admin s3-user create
```

Enter the Admin password at the no-echo prompt. The command prints the S3 secret only once; save it to a client credential file protected with `0600` and rotate it immediately if it is exposed.

Then use a path-style S3 client:

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

`mc cat` prints the uploaded object. The alias stores the credentials in `~/.mc/config.json`; keep that file at permission mode `0600`. See [S3 Clients](./s3-clients.md) for more client examples.

## Common Build Issues

| Symptom | Check |
| --- | --- |
| UI build fails | Confirm Node.js 22.12 or later and pnpm 11 are installed. |
| Go build fails on cgo | Confirm a C toolchain is installed and visible in `PATH`. |
| `serve` fails with Admin auth validation | Run `./bin/synaps3 init` for a fresh config, or `./bin/synaps3 admin-auth reset-password --config ~/.synaps3/config.toml` for an existing config. |
| `serve` starts in setup mode | Set `filecoin.private_key` in config or `SYNAPS3_FILECOIN_PRIVATE_KEY`. |
