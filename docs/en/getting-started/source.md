---
title: Build from Source
description: Build SynapS3 locally, initialize runtime data, and verify the embedded dashboard and S3 API.
---

# Build from Source

Build from source when you are developing SynapS3, need a custom binary, or want to inspect the embedded dashboard build.

For ordinary long-running operation, prefer [Docker Deployment](./docker.md).

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

Expected result: the command builds the React dashboard first, embeds it, and writes `bin/synaps3`.

## Initialize Runtime Data

```bash
./bin/synaps3 init
./bin/synaps3 wallet generate
```

Expected result: `~/.synaps3/config.toml`, `db/`, and `cache/` are created. Add the generated private key to `~/.synaps3/config.toml`:

`synaps3 init` also creates Admin auth. Save the printed Admin password. If init is non-interactive, read it from `~/.synaps3/admin-initial-password`.

```toml
[filecoin]
private_key = "0x..."
```

For Calibration testing, fund the wallet:

```bash
./bin/synaps3 wallet fund-testnet 0x...
```

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

In another terminal, verify health and deposit USDFC:

```bash
curl http://127.0.0.1:9090/healthz
./bin/synaps3 wallet deposit 2 # 2 USDFC
```

Expected result: health returns `{"status":"ok"}` and the deposit operation is accepted.

## Verify with an S3 Client

Create an S3 user:

```bash
export SYNAPS3_ADMIN_PASSWORD='replace-with-admin-password'
./bin/synaps3 admin s3-user create
```

Then use a path-style S3 client:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
mc alias set synaps3 http://localhost:8080 replace-with-access-key replace-with-secret-key
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

Expected result: `mc cat` prints the uploaded object. See [S3 Clients](./s3-clients.md) for more client examples.

## Common Build Issues

| Symptom | Check |
| --- | --- |
| UI build fails | Confirm Node.js 22.12 or later and pnpm 11 are installed. |
| Go build fails on cgo | Confirm a C toolchain is installed and visible in `PATH`. |
| Serve fails with admin auth validation | Run `./bin/synaps3 init` for a fresh config, or `./bin/synaps3 admin-auth reset-password --config ~/.synaps3/config.toml` for an existing config. |
| Serve starts in setup mode | Set `filecoin.private_key` in config or `SYNAPS3_FILECOIN_PRIVATE_KEY`. |
