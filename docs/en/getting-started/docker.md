---
title: Docker Deployment
description: Deploy SynapS3 with Docker and optionally enable automatic Admin HTTPS.
---

# Docker Deployment

Docker keeps the dashboard and Admin API on `127.0.0.1:9090` by default. When a public Admin hostname is needed, select Caddy HTTPS during initialization. Caddy does not proxy the S3 API.

## Prerequisites

- A Linux host with Git, Make, curl, Docker Engine, and Docker Compose v2.24 or later.
- Durable local disk for the `synaps3-data` volume.
- SSH access or a usable public HTTPS hostname when Admin is accessed from another machine.
- A fundable Calibration wallet. You can also start in `setup` state and configure the wallet later.

## Initialize the Deployment

Clone the repository:

```bash
git clone --depth 1 https://github.com/strahe/SynapS3.git
cd SynapS3
```

### Default: Local Admin

Without a hostname, Admin remains bound to loopback:

```bash
make docker-init
```

### Optional: Automatic Admin HTTPS

Public HTTPS also requires:

- An A record pointing the Admin hostname to the host's public IPv4 address.
- An AAAA record only when the host is also reachable over IPv6.
- Inbound `80/TCP` and `443/TCP` through host and cloud firewalls.
- A CAA policy, when present, that permits a public CA available to Caddy.

Pass the hostname during initialization:

```bash
make docker-init ADMIN_DOMAIN=admin.example.com
```

Use only the hostname for `ADMIN_DOMAIN`; do not include a scheme, port, path, or wildcard. This mode loads Caddy, manages certificate issuance and renewal, and redirects HTTP to HTTPS. SynapS3 Admin remains bound to `127.0.0.1:9090`.

### Build the Docker Image from This Checkout

To build the container image from the current checkout:

```bash
make docker-init IMAGE_SOURCE=local
```

Pass `ADMIN_DOMAIN` in the same command when Admin HTTPS is also needed. `make docker-up` builds the local image when required.

`docker-init` creates `.env` at permission mode `0600` and refuses to overwrite an existing file. Review `.env` after selecting the deployment mode and keep it at `0600`.

## Prepare a Wallet

Generate a wallet in a private terminal when needed:

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

Store the printed private key securely, then edit `.env`:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

Do not put the real private key in a shell command. See [Environment Variables](../configuration/environment.md) for other supported overrides.

Fund the wallet address on Calibration:

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

If faucet funding is unavailable, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/). You can also leave the wallet unset, start SynapS3 in `setup` state, and finish configuration in the dashboard.

## Start and Verify

```bash
make docker-up
make docker-verify
```

`docker-verify` always checks the local `/healthz` endpoint. When Admin HTTPS is enabled, it also verifies the trusted certificate and the HTTP-to-HTTPS redirect.

| Status | Meaning |
| --- | --- |
| `setup` | Admin is reachable, but required settings are missing. |
| `ok` | Admin is reachable and runtime health checks pass. |
| `unhealthy` | The database, cache, or workers need attention. |

Only `ok` is ready for normal S3 traffic.

| Surface or data | Address or location |
| --- | --- |
| S3 API | `http://<host>:8080` until S3 TLS is configured separately |
| Local Admin | `http://127.0.0.1:9090` |
| Public Admin | HTTPS mode only: `https://admin.example.com` |
| SynapS3 runtime data | Docker volume `synaps3-data` |
| Caddy state | HTTPS mode only: `synaps3-caddy-data` and `synaps3-caddy-config` |

Read the initial Admin password:

```bash
make docker-password
```

The username is `admin`. Read the password only in a private terminal and store it in a password manager.

Reach local Admin remotely through an SSH tunnel:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

When HTTPS is enabled, use the configured hostname directly. Never bind port 9090 to a public interface.

Container-local Admin CLI operations continue to use the native command:

```bash
docker compose exec -T synaps3 synaps3 admin status
```

## S3 Transport Security

Caddy protects only the dashboard and Admin API. It does not handle the S3 API.

For production S3 traffic, either configure native TLS with `SYNAPS3_SERVER_TLS_ENABLED=true`, `SYNAPS3_SERVER_TLS_CERT_FILE`, and `SYNAPS3_SERVER_TLS_KEY_FILE`, or place the S3 API behind a separate controlled TLS proxy. Certificate and key paths must be visible inside the container, typically through read-only mounts.

## Daily Operations

```bash
make docker-status
make docker-logs
make docker-logs DOCKER_SERVICE=synaps3
make docker-logs DOCKER_SERVICE=caddy DOCKER_LOG_FOLLOW=1 # HTTPS mode only
make docker-down
```

Logs show the latest 100 lines by default. `docker-down` removes containers but preserves `.env`, `synaps3-data`, and any existing Caddy certificate volumes.

## HTTPS Troubleshooting

This section applies only when Admin HTTPS is enabled. If `make docker-verify` reports that HTTPS is not ready:

1. Check the A record and optional AAAA record.
2. Confirm that no other service occupies ports 80 or 443.
3. Check host firewall, cloud firewall, and CAA policy.
4. Read the Caddy logs:

```bash
make docker-logs DOCKER_SERVICE=caddy
```

Caddy continues retrying after the problem is fixed. Do not delete `synaps3-caddy-data`; it contains certificate private keys and ACME account state.

## Back Up Docker Data

Check health and stop the containers first:

```bash
make docker-verify
make docker-down
```

For the default SQLite deployment, archive the complete runtime data volume:

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
docker run --rm \
  -v "$PWD":/backup \
  alpine:3 \
  sh -c 'cd /backup && tar tzf synaps3-data.tgz >/dev/null && sha256sum synaps3-data.tgz > synaps3-data.tgz.sha256 && sha256sum -c synaps3-data.tgz.sha256'
```

PostgreSQL deployments require a database-native backup plus configuration and cache data from the same recovery point. See [Runtime Data](../configuration/runtime-data.md) for consistency and restore order. Caddy volumes are outside the database-and-cache recovery point, but retain them to avoid replacing certificates and the ACME account.

After the backup, run `make docker-up` and `make docker-verify`.

## Upgrade

Create and verify a consistent backup first. For a published-image deployment:

```bash
git pull --ff-only
docker compose pull
make docker-up
make docker-verify
```

For a local-image deployment, omit `docker compose pull`; after updating the checkout, `make docker-up` rebuilds the image. The current release channel is the movable `edge` tag and has no automatic rollback. Complete the [Production Checklist](../operations/production-checklist.md) before restoring normal traffic.
