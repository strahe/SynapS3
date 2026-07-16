---
title: CLI Reference
description: Common SynapS3 CLI commands for setup, serving, wallet operations, S3 users, settings, and tasks.
---

# CLI Reference

SynapS3 provides the S3 API, the Admin API, and CLI commands for local operations.

## Endpoints

| Surface | Default |
| --- | --- |
| S3 API | `http://localhost:8080` |
| Dashboard and Admin API | `http://127.0.0.1:9090` |
| Health | `GET http://127.0.0.1:9090/healthz` |
| Metrics | `GET http://127.0.0.1:9090/metrics` |

The HTTP S3 endpoint is for local evaluation. Use native TLS or a controlled TLS reverse proxy for production S3 traffic.

## Config File Source

Commands that need a config file use `--config <path>` first, then non-empty `SYNAPS3_CONFIG`, then `~/.synaps3/config.toml`. `synaps3 init` uses `--dir` for the runtime data directory and does not read `SYNAPS3_CONFIG`. `admin-auth reset-password` requires `--config` or `SYNAPS3_CONFIG`.

The root `--config <path>` flag also has the short form `-c <path>`.

## Runtime Commands

| Command | Purpose |
| --- | --- |
| `synaps3 init` | Initialize `~/.synaps3` runtime data. |
| `synaps3 init --dir /var/lib/synaps3` | Initialize a custom runtime data directory. |
| `synaps3 serve` | Start the S3 gateway, dashboard, Admin API, and background tasks. |
| `synaps3 migrate` | Run database migrations and exit. |
| `synaps3 admin-auth reset-password --config <path>` | Reset the Admin password offline, rotate the session secret, and write a new `admin-initial-password` file. |
| `synaps3 version` | Print version information. |

## Wallet Commands

```bash
synaps3 wallet generate
synaps3 wallet fund-testnet 0x...
synaps3 wallet deposit 2 # 2 USDFC
synaps3 wallet approve
```

`fund-testnet` requires `<address>` and `deposit` requires `<amount>`. `generate` prints wallet material, `fund-testnet` claims Calibration assets, `deposit` submits the requested USDFC amount, and `approve` approves FWSS spending using the configured private key. Successful faucet claims print `CalibnetUSDFC: <hash>` and `CalibnetFIL: <hash>`.

A confirmed deposit or approval prints `Transaction: <hash>` and `Status: confirmed`. If FWSS approval already exists, `approve` prints `FWSS approval: already approved`.

## Provider Commands

List PDP storage providers registered on the selected Filecoin network:

```bash
synaps3 provider list
synaps3 provider list --active --no-health
synaps3 provider list --network mainnet --json
```

| Flag | Purpose |
| --- | --- |
| `--json` | Return provider results as JSON. |
| `--active` | Show only active providers. |
| `--rpc-url <url>` | Override the configured RPC endpoint. |
| `--network <calibration\|mainnet>` | Select the Filecoin network. |
| `--timeout <duration>` | Set the health-check timeout per provider; default `5s`. |
| `--no-health` | Skip provider health checks. |

## Admin Commands

Admin commands use HTTP Basic auth. The username comes from `admin.auth.username`; the password comes from `SYNAPS3_ADMIN_PASSWORD`, the config-adjacent `admin-initial-password`, or the terminal prompt.

```bash
synaps3 admin status
synaps3 admin s3-user create
synaps3 admin s3-user list
synaps3 admin s3-user update <access-key> --role userplus
synaps3 admin s3-user rotate-secret <access-key>
synaps3 admin settings get
synaps3 admin settings set cache.max_size_gb=200
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task retry 42
```

If no protected password file is available, enter the Admin password at the no-echo prompt. Do not place the password directly in shell history. S3 user creation and secret rotation show the secret only once; store it in a client credential file protected with `0600`.

Admin global flags must appear after `admin` and before the subcommand:

| Flag | Purpose |
| --- | --- |
| `--admin-url <url>` | Override the Admin API base URL. |
| `--json` | Return successful responses as JSON. |
| `--timeout <duration>` | Set the Admin API request timeout. |

Task listing supports `--type`, `--stage`, `--status`, `--limit`, and `--offset`. `--stage` requires `--type`.

## Settings Safety

The Admin API contains write endpoints for settings, wallet operations, task retries, and S3 user management. Admin auth is required by default. Keep the listener on loopback, or place remote access behind HTTPS and explicit access control.

High-risk changes require confirmation:

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
synaps3 admin s3-user create --role admin --yes
synaps3 admin s3-user update <access-key> --role admin --yes
synaps3 admin s3-user delete <access-key> --yes
```

After saving settings, restart SynapS3, check `/healthz`, and use `synaps3 admin settings get` to confirm the effective values.

## Remote Admin Access

If SynapS3 runs on another host, keep `admin.addr` on `127.0.0.1:9090` and tunnel it:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

Then run local admin commands with the default admin URL, or pass `--admin-url` explicitly. Commands use `SYNAPS3_ADMIN_PASSWORD` first, then `admin-initial-password` next to the config file, then the prompt. For browser access, sign in with the Admin username and password from init or `admin-auth reset-password`.
