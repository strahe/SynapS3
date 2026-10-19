---
title: Docker Deployment
description: Deploy SynapS3 with Docker Compose, optionally with public Admin HTTPS, and back it up or upgrade it.
---

# Docker Deployment

The Docker deployment runs SynapS3 and, by default, PostgreSQL 18 with Docker Compose. The dashboard and Admin API listen on `127.0.0.1:9090`. An optional Caddy proxy serves them on a public HTTPS hostname; Caddy does not proxy the S3 API.

## Prerequisites

- A Linux host with Git, Make, curl, Docker Engine, and Docker Compose v2.24 or later.
- Durable local disk for the `synaps3-data` and `synaps3-postgres-data` volumes.
- For public Admin HTTPS: a public hostname and inbound `80/TCP` and `443/TCP`.
- A Filecoin wallet with FIL and USDFC. The deployment uses Calibration unless you set `SYNAPS3_FILECOIN_NETWORK`.

## Initialize the Deployment

```bash
git clone --depth 1 https://github.com/strahe/SynapS3.git
cd SynapS3
```

Choose an initialization command:

| Image source | Admin access | Command |
| --- | --- | --- |
| Published image | Local `127.0.0.1:9090` | `make docker-init` |
| Published image | Public HTTPS | `make docker-init ADMIN_DOMAIN=admin.example.com` |
| Built from this checkout | Local `127.0.0.1:9090` | `make docker-init IMAGE_SOURCE=local` |
| Built from this checkout | Public HTTPS | `make docker-init IMAGE_SOURCE=local ADMIN_DOMAIN=admin.example.com` |

`docker-init` creates `.env` and `.postgres-admin-password` with mode `0600` and refuses to overwrite either file. It generates both PostgreSQL passwords: the `synaps3` application role password goes to `.env`, and the administrator password goes to `.postgres-admin-password`. The `synaps3` role owns the database but is not a superuser.

The managed PostgreSQL listens on `127.0.0.1:15432`. Add `POSTGRES_PORT=<port>` to the command to use another port.

To use an external PostgreSQL server, add `DATABASE_SOURCE=external` to the command, then set `SYNAPS3_DATABASE_DSN` in `.env`.

### Admin HTTPS Requirements

With `ADMIN_DOMAIN`, Caddy obtains and renews the certificate, redirects HTTP to HTTPS, and sends HSTS for that hostname only. It requires:

- An A record for the hostname that points to the host's public IPv4 address, and an AAAA record only if the host is reachable over IPv6.
- Inbound `80/TCP` and `443/TCP` open in host and cloud firewalls.
- No CAA record that blocks the public CAs Caddy uses.

Set `ADMIN_DOMAIN` to the hostname only, without a scheme, port, path, or wildcard. `docker-init` checks only the syntax; internal or reserved names fail certificate issuance and `make docker-verify`.

## Configure the Wallet

To generate a new wallet, run this in a private terminal:

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

Store the printed private key securely. Add it to `.env`; do not pass it on a command line:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

On Calibration, claim test FIL and USDFC from the faucet:

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

If the faucet claim fails, use [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/).

Without a private key, SynapS3 starts in setup mode: only the Admin settings are available and the S3 API is not served. The dashboard shows whether a key is set but cannot set it.

## Start and Verify

```bash
make docker-up
make docker-verify
```

`docker-verify` checks the local `/healthz` endpoint. With Admin HTTPS, it also checks the certificate, the public `/healthz`, and the HTTP-to-HTTPS redirect. Serve S3 traffic only when the status is `ok`; see [Health and Metrics](../operations/health-metrics.md#health) for other statuses.

With Admin HTTPS, `https://admin.example.com/healthz` answers without credentials, and an `unhealthy` response names the failing check. If that must not be public, use local Admin through an SSH tunnel instead.

Print the initial Admin password in a private terminal and store it in a password manager. The username is `admin`.

```bash
make docker-password
```

To reach local Admin from another machine, forward the port and browse to `http://127.0.0.1:9090`:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

In the dashboard, open **Wallet**, deposit USDFC with **Fund**, then select **Approve FWSS**. Uploads need both.

Run Admin CLI commands inside the container:

```bash
docker compose exec -T synaps3 synaps3 admin status
```

| Surface or data | Address or location |
| --- | --- |
| S3 API | `http://<host>:8080`, or HTTPS after you configure S3 TLS |
| Local Admin | `http://127.0.0.1:9090` |
| Public Admin | `https://admin.example.com` (HTTPS mode only) |
| SynapS3 runtime data | Docker volume `synaps3-data` |
| PostgreSQL data | Docker volume `synaps3-postgres-data` (managed database only) |
| Database passwords | `.env` and `.postgres-admin-password` |
| Caddy state | Docker volumes `synaps3-caddy-data` and `synaps3-caddy-config` (HTTPS mode only) |

## S3 Transport Security

Caddy serves only the dashboard and Admin API. For production S3 traffic, either enable native TLS with `SYNAPS3_SERVER_TLS_ENABLED=true`, `SYNAPS3_SERVER_TLS_CERT_FILE`, and `SYNAPS3_SERVER_TLS_KEY_FILE`, or put the S3 API behind your own TLS proxy. The certificate and key paths must exist inside the container, usually as read-only mounts.

## Daily Operations

```bash
make docker-status
make docker-logs
make docker-logs DOCKER_SERVICE=synaps3
make docker-logs DOCKER_SERVICE=postgres
make docker-logs DOCKER_SERVICE=caddy DOCKER_LOG_FOLLOW=1 # HTTPS mode only
make docker-down
```

`docker-logs` prints the last 100 lines. `docker-down` removes the containers and keeps `.env`, `.postgres-admin-password`, and all volumes.

Passwords are never rotated automatically. To change the application password, change it for the `synaps3` role in PostgreSQL, update `POSTGRES_APP_PASSWORD` in `.env`, then run `make docker-up`. To change the PostgreSQL port, edit `POSTGRES_PORT` in `.env` and run `make docker-up`.

PostgreSQL creates the database and the `synaps3` role only when its volume is empty. If that first start fails, read `make docker-logs DOCKER_SERVICE=postgres`, fix the cause, remove the `synaps3-postgres-data` volume, and run `make docker-up` again.

## HTTPS Troubleshooting

If `make docker-verify` reports that Admin HTTPS is not ready:

1. Check the A record and the optional AAAA record.
2. Confirm that no other service uses ports 80 or 443.
3. Check host and cloud firewalls and any CAA record.
4. Read the Caddy logs:

```bash
make docker-logs DOCKER_SERVICE=caddy
```

Caddy keeps retrying after you fix the cause. Do not delete `synaps3-caddy-data`; it holds the certificate private keys and the ACME account.

## Back Up Docker Data

1. Check health, then stop SynapS3 and leave PostgreSQL running:

   ```bash
   make docker-verify
   docker compose stop synaps3
   ```

2. Dump the managed database, archive the `synaps3-data` volume (configuration and cache), and record checksums:

   ```bash
   docker compose exec -T postgres pg_dump --username=synaps3 --dbname=synaps3 --format=custom > synaps3-db.dump
   docker run --rm \
     -v synaps3-data:/data:ro \
     -v "$PWD":/backup \
     alpine:3 \
     tar czf /backup/synaps3-data.tgz -C /data .
   docker run --rm \
     -v "$PWD":/backup \
     alpine:3 \
     sh -c 'cd /backup && tar tzf synaps3-data.tgz >/dev/null && sha256sum synaps3-db.dump synaps3-data.tgz > synaps3-backup.sha256 && sha256sum -c synaps3-backup.sha256'
   ```

3. Keep `.env` and `.postgres-admin-password` with the backup.
4. Restart and verify:

   ```bash
   make docker-up
   make docker-verify
   ```

With `DATABASE_SOURCE=external`, back up the database with that server's own tools; the `pg_dump` command above covers only the managed database. The Caddy volumes are not part of the recovery point; keep them to avoid reissuing certificates.

To restore, start from the backed-up `.env` and `.postgres-admin-password` and empty volumes. Restore the database before the runtime data:

```bash
docker compose up -d --wait postgres
docker compose exec -T postgres pg_restore --username=synaps3 --dbname=synaps3 --no-owner --exit-on-error < synaps3-db.dump
docker run --rm \
  -v synaps3-data:/data \
  -v "$PWD":/backup \
  alpine:3 \
  tar xzf /backup/synaps3-data.tgz -C /data
make docker-up
make docker-verify
```

See [Runtime Data](../configuration/runtime-data.md) for the consistency rules.

## Upgrade

Back up first, then for a published-image deployment:

```bash
git pull --ff-only
docker compose pull
make docker-up
make docker-verify
```

For a deployment built from the checkout, skip `docker compose pull`; `make docker-up` rebuilds the image. The published image uses the movable `edge` tag, and the upgrade has no automatic rollback. Complete the [Production Checklist](../operations/production-checklist.md) before restoring normal traffic.
