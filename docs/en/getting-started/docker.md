---
title: Docker Deployment
description: Deploy SynapS3 with automatic Admin HTTPS.
---

# Docker Deployment

This deployment runs SynapS3 and Caddy on one Linux host. SynapS3 keeps the Admin server on `127.0.0.1:9090`; Caddy publishes the dashboard at your HTTPS hostname and manages its certificate automatically.

## Prerequisites

- A Linux host with Git, Make, curl, Docker Engine, and Docker Compose v2.24 or later.
- Durable local disk for the `synaps3-data` volume.
- A public hostname such as `admin.example.com`.
- Public DNS and firewall access:
  - Point the hostname's A record to the host's public IPv4 address.
  - Add an AAAA record only when IPv6 reaches the same host. Remove a broken AAAA record before starting.
  - Allow inbound `80/TCP` and `443/TCP`. Allow `443/UDP` when HTTP/3 is wanted.
  - If the domain uses CAA records, allow a public CA that Caddy can use.
- A Calibration wallet you can fund, or use the wallet commands below.

Caddy's default certificate challenges require ports 80 or 443 to be reachable from the public internet. This deployment does not include DNS-provider plugins or wildcard certificate support.

## Create the Deployment

Clone the repository and initialize the protected environment file:

```bash
git clone --depth 1 https://github.com/strahe/SynapS3.git
cd SynapS3
make docker-init ADMIN_DOMAIN=admin.example.com
```

Use a bare hostname for `ADMIN_DOMAIN`; do not include `https://`, a port, path, or wildcard. The command creates `.env` with permission mode `0600` and selects the published SynapS3 image plus Admin HTTPS.

Review `.env` before starting. Keep it at `0600`, and never put a private key directly in a shell command because shell history may retain it. See [Environment Variables](../configuration/environment.md) for supported SynapS3 overrides.

### Build the Image Locally

To build from the checkout instead of using the published image, choose the local source during initialization:

```bash
make docker-init ADMIN_DOMAIN=admin.example.com IMAGE_SOURCE=local
make docker-build
```

Choose the image source before `.env` exists. `docker-init` refuses to overwrite an existing file.

## Prepare a Calibration Wallet

If you do not already have a wallet, generate one:

```bash
make docker-wallet
```

The command prints a wallet address and private key. Run it only in a private terminal, save the private key in a protected location, and add it to `.env`:

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

Request Calibration assets for the generated address:

```bash
make docker-fund WALLET_ADDRESS=0x...
```

If faucet funding is unavailable, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/). Successful claims print `CalibnetUSDFC: <hash>` and `CalibnetFIL: <hash>`.

SynapS3 can start in `setup` state without a wallet key. Complete the remaining wallet settings in the dashboard before sending S3 traffic.

## Start and Verify

Start the deployment and verify both local health and public HTTPS:

```bash
make docker-up
make docker-verify
```

`docker-up` waits for the containers to run and for the SynapS3 container health check. It does not claim that public certificate issuance has finished. `docker-verify` checks the loopback Admin endpoint, the trusted HTTPS certificate, and HTTP-to-HTTPS redirection.

The health response has three relevant states:

| Status | Meaning |
| --- | --- |
| `setup` | HTTPS is ready, but required SynapS3 settings are still missing. |
| `ok` | Admin HTTPS is ready and runtime health checks pass. |
| `unhealthy` | HTTPS may be working, but the database, cache, or workers need attention. |

Only `ok` is ready for normal S3 traffic.

Deployment endpoints and data:

| Surface | Address or location |
| --- | --- |
| S3 API | `http://<host>:8080` until S3 TLS is configured separately |
| Dashboard and Admin API | `https://admin.example.com` |
| Admin backend | `http://127.0.0.1:9090`, not publicly reachable |
| SynapS3 runtime data | Docker volume `synaps3-data` |
| Caddy certificates and state | Docker volumes `synaps3-caddy-data` and `synaps3-caddy-config` |

Read the generated Admin password in a private terminal:

```bash
make docker-password
```

The username is `admin`. Save the password in a password manager. Container-local Admin commands read the same protected password file automatically:

```bash
make docker-admin ADMIN_ARGS='status'
```

Deposit USDFC and approve FWSS from the dashboard before expected uploads.

## S3 Transport Security

Caddy in this deployment protects the dashboard and Admin API only. It does not proxy the S3 API.

For production S3 traffic, configure native TLS with `SYNAPS3_SERVER_TLS_ENABLED=true`, `SYNAPS3_SERVER_TLS_CERT_FILE`, and `SYNAPS3_SERVER_TLS_KEY_FILE`, or place the S3 API behind a separately controlled TLS proxy. Certificate and key paths must be visible inside the container, normally through read-only mounts.

## Operate the Deployment

Use `make docker-help` to list the supported commands. Common operations are:

```bash
make docker-status
make docker-logs DOCKER_SERVICE=synaps3
make docker-logs DOCKER_SERVICE=caddy DOCKER_LOG_FOLLOW=1
make docker-admin ADMIN_ARGS='task stats'
```

`make docker-stop` stops both services but retains their containers and data. `make docker-down` removes the containers, but preserves `.env`, SynapS3 runtime data, and Caddy certificate volumes. No provided Make target deletes volumes.

Before serving traffic, complete the [Production Checklist](../operations/production-checklist.md).

## Certificate Troubleshooting

If `make docker-verify` reports that HTTPS is not ready:

1. Confirm the A and optional AAAA records resolve to this host.
2. Confirm no other service is using ports 80 or 443.
3. Confirm the host firewall and cloud firewall allow inbound 80/TCP and 443/TCP.
4. Check whether CAA records restrict certificate issuance.
5. Inspect Caddy's certificate logs:

```bash
make docker-logs DOCKER_SERVICE=caddy
```

Caddy retries certificate issuance after DNS or firewall problems are corrected. Do not delete `synaps3-caddy-data`; it contains certificate keys and ACME account state.

## Upgrade and Back Up

Do not archive a live data volume. Check health and task queues, stop the deployment, then follow the driver-specific steps in [Runtime Data](../configuration/runtime-data.md). Keep the database and cache at the same recovery point.

After creating and verifying that backup, follow [Upgrade and Recovery](../operations/upgrade-recovery.md). The current published channel is the moving `edge` image, so an upgrade does not provide automatic rollback.
