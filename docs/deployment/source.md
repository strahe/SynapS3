# Build from Source

Use this flow for local development or when you need to build the binary yourself. For quick evaluation, use the [README Quick Start](../../README.md#quick-start). For long-running Docker Compose deployment, use the [Docker deployment guide](docker.md).

## Prerequisites

- [Go](https://go.dev/doc/install) 1.26.3 or later
- C toolchain for cgo, such as [gcc](https://gcc.gnu.org/install/) or [clang](https://clang.llvm.org/get_started.html)
- [Node.js](https://nodejs.org/en/download) 22.12 or later
- [pnpm](https://pnpm.io/installation) 11

If pnpm 11 is not already installed:

```bash
npm install --global pnpm@11
```

## Build

Clone and build SynapS3 with the embedded dashboard:

```bash
git clone https://github.com/strahe/SynapS3.git
cd SynapS3
make build
```

Initialize the local app data directory:

```bash
./bin/synaps3 init
```

Generate a wallet if you do not already have one:

```bash
./bin/synaps3 wallet generate
```

Set `filecoin.private_key` in `~/.synaps3/config.toml`:

```toml
[filecoin]
private_key = "0x..."
```

For Calibration testing, fund the wallet with testnet tFIL and USDFC:

```bash
# Use the generated wallet address, not the private key.
./bin/synaps3 wallet fund-testnet 0x...
```

The command claims tFIL and USDFC. It may wait up to 120 seconds per faucet and automatically tries a fallback faucet when the primary faucet is unavailable or missing a token.

Start SynapS3:

```bash
./bin/synaps3 serve
```

Default endpoints:

- S3 API: `http://localhost:8080`
- Dashboard and admin API: `http://localhost:9090`
- Runtime data: `~/.synaps3/`

Do not expose the dashboard and admin API directly to an untrusted network.

## First Upload

Deposit USDFC into the payment account before uploading:

```bash
./bin/synaps3 wallet deposit 2
```

Upload a file from the dashboard, or use an S3 client for API verification. To test with an S3 client, install [rc](https://github.com/rustfs/cli) or [mc](https://min.io/docs/minio/linux/reference/minio-mc.html), then create an S3 user from the dashboard or CLI. Secrets are shown only when created or rotated.

```bash
./bin/synaps3 admin s3-user create
```

Example with rc:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
rc alias set synaps3 http://localhost:8080 'replace-with-access-key' 'replace-with-secret-key'
rc mb synaps3/demo
rc cp ./hello.txt synaps3/demo/hello.txt
rc cat synaps3/demo/hello.txt
```

The sample file is padded because FOC uploads require at least 127 bytes. Any path-style S3 client can be used.

Check health and task recovery:

```bash
./bin/synaps3 admin status
curl http://localhost:9090/healthz
./bin/synaps3 admin task list --status exhausted --limit 100
```
