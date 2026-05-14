# Docker Compose Deployment

This guide covers long-running Docker Compose deployment on a single Linux host. For quick evaluation with `docker run`, use the [README Quick Start](../../README.md#quick-start). To compile locally, use the [source build guide](source.md).

SynapS3 is still a developer preview, and the default examples use Filecoin Calibration.

Set the Filecoin wallet private key through `.env`, environment variables, or `config.toml`.

Prerequisites:

- [Docker Engine](https://docs.docker.com/engine/install/) with [Docker Compose v2.24 or later](https://docs.docker.com/compose/install/)
- A durable local disk for the Docker volume
- SSH access if the dashboard is reached from another machine

Prepare core environment overrides:

```bash
cp .env.example .env
```

Generate a wallet:

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

Copy the generated private key into `.env`:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

Fund the generated address on Calibration:

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

The command claims tFIL and USDFC. It may wait up to 120 seconds per faucet and automatically tries a fallback faucet when the primary faucet is unavailable or missing a token.

Start SynapS3:

```bash
docker compose up -d
docker compose logs --tail=50 synaps3
```

Default endpoints on the host:

- S3 API: `http://<host>:8080`
- Admin and dashboard: `http://127.0.0.1:9090`
- Runtime data: Docker volume `synaps3-data` mounted at `/var/lib/synaps3`

The container defaults to `SYNAPS3_CONFIG=/var/lib/synaps3/config.toml`. If you set `SYNAPS3_CONFIG` to another path, mount that file before starting the container.

Check health and deposit USDFC:

```bash
curl http://127.0.0.1:9090/healthz
docker compose exec synaps3 synaps3 --config /var/lib/synaps3/config.toml admin status
docker compose exec synaps3 synaps3 --config /var/lib/synaps3/config.toml wallet deposit 2
```

Access the dashboard from your workstation with an SSH tunnel:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

Then open `http://127.0.0.1:9090` and upload a file. Do not publish the admin port to the public internet. The Compose file uses [Linux host networking](https://docs.docker.com/engine/network/tutorials/host/) so the admin server can stay bound to loopback while the S3 API listens on port 8080.

Create an S3 user only if you want to verify with an S3 client:

```bash
docker compose exec synaps3 synaps3 --config /var/lib/synaps3/config.toml admin s3-user create
```

Install [rc](https://github.com/rustfs/cli) or [mc](https://min.io/docs/minio/linux/reference/minio-mc.html) for optional S3 CLI checks. Example with rc:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
rc alias set synaps3 http://<host>:8080 'replace-with-access-key' 'replace-with-secret-key'
rc mb synaps3/demo
rc cp ./hello.txt synaps3/demo/hello.txt
rc cat synaps3/demo/hello.txt
```

The sample file is padded because FOC uploads require at least 127 bytes.

Restart:

```bash
docker compose restart synaps3
```

Upgrade:

```bash
docker compose pull
docker compose up -d
```

Back up runtime data:

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
```
