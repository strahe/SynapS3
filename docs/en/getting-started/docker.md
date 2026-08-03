---
title: Docker Deployment
description: Deploy SynapS3 with Docker and optionally enable automatic Admin HTTPS.
---

# Docker Deployment

Docker keeps the dashboard and Admin API on `127.0.0.1:9090` by default. When a public Admin hostname is needed, select Caddy HTTPS. Caddy does not proxy the S3 API.

## Prerequisites

- A Linux host with Git, Make, curl, Docker Engine, and Docker Compose v2.24 or later.
- Durable local disk for the `synaps3-data` volume.
- SSH access or a usable public HTTPS hostname when Admin is accessed from another machine.
- A fundable Calibration wallet. If you do not have one, follow the wallet generation steps below.

## Initialize the Deployment

Clone the repository:

```bash
git clone --depth 1 https://github.com/strahe/SynapS3.git
cd SynapS3
```

`docker-init` creates `.env` with permission mode `0600`. Before the first initialization, choose one of these four combinations:

| Image source | Admin access | Initialization command |
| --- | --- | --- |
| Published image (default) | Local `127.0.0.1:9090` | `make docker-init` |
| Published image (default) | Public HTTPS | `make docker-init ADMIN_DOMAIN=admin.example.com` |
| Current checkout | Local `127.0.0.1:9090` | `make docker-init IMAGE_SOURCE=local` |
| Current checkout | Public HTTPS | `make docker-init IMAGE_SOURCE=local ADMIN_DOMAIN=admin.example.com` |

The default uses the published image and keeps Admin on `127.0.0.1:9090`. `IMAGE_SOURCE=local` makes `make docker-up` build from the current checkout. `ADMIN_DOMAIN` loads Caddy, manages certificate issuance and renewal, and redirects HTTP to HTTPS. Both options can be used together.

`docker-init` refuses to overwrite an existing `.env`. Review the file after creation and keep its permission mode at `0600`.

### Admin HTTPS Requirements

Public HTTPS also requires:

- An A record pointing the Admin hostname to the host's public IPv4 address.
- An AAAA record only when the host is also reachable over IPv6.
- Inbound `80/TCP` and `443/TCP` through host and cloud firewalls.
- A CAA policy, when present, that permits a public CA available to Caddy.

Use only the hostname for `ADMIN_DOMAIN`; do not include a scheme, port, path, or wildcard. SynapS3 Admin remains bound to `127.0.0.1:9090`.

`docker-init` checks hostname syntax only. Certificate issuance still requires a publicly resolvable hostname accepted by a public CA; internal and reserved names will fail `docker-verify`. HSTS applies only to the configured Admin hostname, without subdomain inheritance or preload.

## Configure the Wallet Private Key

If you already have a wallet, add its private key to `.env`. Otherwise, generate one in a private terminal:

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

Store the printed private key securely, then uncomment or add this entry in `.env`:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

Do not put the real private key in a shell command. The dashboard reports whether the key is configured, but it does not accept or store the key; Docker deployments should set it through `.env`. See [Environment Variables](../configuration/environment.md) for other supported overrides.

Fund the wallet address on Calibration:

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

If faucet funding is unavailable, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/).

Without the private key, the container starts but SynapS3 exposes only Admin setup mode and cannot serve normal S3 traffic. After setting the key, start or restart the service, then run `make docker-verify` and confirm that the status is `ok`.

## Start and Verify

```bash
make docker-up
make docker-verify
```

`docker-verify` always checks the local `/healthz` endpoint. When Admin HTTPS is enabled, it also verifies the trusted certificate and the HTTP-to-HTTPS redirect.

With Admin HTTPS enabled, `https://admin.example.com/healthz` remains available without Admin credentials for external health checks. An `unhealthy` response can identify the affected database, cache, or worker. Use local Admin through an SSH tunnel instead if this status must not be public.

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

For a local-image deployment, omit `docker compose pull`; after updating the checkout, `make docker-up` rebuilds the image. The current published channel is the movable `edge` tag and has no automatic rollback. Complete the [Production Checklist](../operations/production-checklist.md) before restoring normal traffic.
