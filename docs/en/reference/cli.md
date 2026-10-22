---
title: CLI Reference
description: Look up synaps3 commands, flags, and output for setup, wallet, storage providers, and administration.
---

# CLI Reference

## Configuration File

Commands that read configuration use `--config <path>` (short form `-c`), then `SYNAPS3_CONFIG`, then `~/.synaps3/config.toml`.

## Runtime Commands

| Command | Purpose |
| --- | --- |
| `synaps3 init [--dir <path>]` | Create `config.toml`, the `cache/` directory, and Admin credentials in `<path>` (default `~/.synaps3`). Fails if `config.toml` exists. Does not accept `--config`. |
| `synaps3 serve` | Apply database migrations, then start the S3 API, the dashboard and Admin API, and background tasks. |
| `synaps3 migrate` | Apply database migrations and exit. |
| `synaps3 migrate --force-unlock` | Release a migration lock left by a killed run. Use it only when no migration is running. |
| `synaps3 admin-auth reset-password` | Set a new Admin password, rotate the session secret, and write the password to `admin-initial-password` next to the configuration file. Requires `--config` or `SYNAPS3_CONFIG`. Restart SynapS3 to apply it. |
| `synaps3 version` | Print version information. |

## Wallet Commands

Wallet commands use the private key and RPC endpoint from the configuration and do not need a running server.

```bash
synaps3 wallet generate
synaps3 wallet fund-testnet <address>
synaps3 wallet deposit <amount>   # amount in USDFC, for example 2
synaps3 wallet approve
```

| Command | Output |
| --- | --- |
| `generate` | `Address: <address>` and `Private key: <key>`. Run it in a private terminal. |
| `fund-testnet` | Claims Calibration test FIL and USDFC. Prints `CalibnetUSDFC: <hash>` and `CalibnetFIL: <hash>` on success. |
| `deposit` | Deposits USDFC into the payment account. Prints `Transaction: <hash>` and `Status: confirmed`. |
| `approve` | Approves FWSS to spend from the payment account. Prints `Transaction: <hash>` and `Status: confirmed`, or `FWSS approval: already approved`. |

All wallet commands accept `--json`. `deposit` and `approve` wait up to `--timeout` (default `5m`) for the receipt; if it does not arrive in time, they print `Status: submitted`. A failed broadcast also prints the transaction hash when available and exits with an error; check that transaction on chain before submitting another payment.

## Provider Commands

List PDP storage providers registered on the network:

```bash
synaps3 provider list
synaps3 provider list --active --no-health
synaps3 provider list --network mainnet --json
```

| Flag | Purpose |
| --- | --- |
| `--json` | Print JSON. |
| `--active` | Show only active storage providers. |
| `--rpc-url <url>` | Override the configured RPC endpoint. |
| `--network <calibration\|mainnet>` | Select the network. |
| `--timeout <duration>` | Health check timeout per storage provider. Default `5s`. |
| `--no-health` | Skip health checks. |

## Admin Commands

`synaps3 admin` commands call the Admin API at `admin.addr` from the configuration. They authenticate as `admin.auth.username` with the password from `SYNAPS3_ADMIN_PASSWORD`, then `admin-initial-password` next to the configuration file, then a no-echo prompt.

These flags go after `admin` and before the subcommand:

| Flag | Purpose |
| --- | --- |
| `--admin-url <url>` | Admin API base URL. |
| `--json` | Print successful responses as JSON. |
| `--timeout <duration>` | Request timeout. Default `10s`. |

```bash
synaps3 admin status
synaps3 admin s3-user list
synaps3 admin s3-user create --name "Backup client"
synaps3 admin s3-user create --role admin --yes
synaps3 admin s3-user update <access-key> --name "Archive client"
synaps3 admin s3-user update <access-key> --role userplus
synaps3 admin s3-user rotate-secret <access-key>
synaps3 admin s3-user delete <access-key> --yes
synaps3 admin settings get
synaps3 admin settings get cache.max_size_gb
synaps3 admin settings set cache.eviction_policy=lru cache.lru_high_watermark_percent=80
synaps3 admin task stats
synaps3 admin task list --status failed --limit 100
synaps3 admin task retry 42
synaps3 admin task acknowledge 42
synaps3 admin task acknowledge --type storage_store --yes
synaps3 admin storage-confirmation list
```

### S3 Users

- Roles are `user`, `userplus` (default), and `admin`; see [Create Credentials](../getting-started/s3-clients.md#create-credentials).
- `create` and `rotate-secret` print the secret key once.
- Creating an `admin` user, assigning the `admin` role, and deleting a user require `--yes`.
- Names are optional and unique. `update <access-key> --name ''` clears a name.

### Settings

- `settings get` prints all settings, or one field when you name it.
- `settings set` saves `field=value` pairs. It refuses fields set by environment variables. Raising `server.max_connections` or `server.max_requests`, changing `filecoin.network`, and enabling `filecoin.allow_private_networks` require `--yes`.
- Saved settings apply after a restart.

### Tasks

- `task list` and `task stats` take `--scope work` (default) or `--scope history`. `work` holds pending, running, and unacknowledged failed tasks; `history` holds completed, cancelled, acknowledged, and superseded executions.
- `task list` filters with `--type`, `--status`, `--limit` (default `20`), and `--cursor <task-id>`. `--status` accepts `pending`, `running`, or `failed` with `work`, and `completed`, `cancelled`, or `failed` with `history`.
- `task retry <id>` starts a new execution of a retryable failed task and prints `Retry task <new-id>`. Repeating it returns the same new execution.
- `task acknowledge <id>` moves a reviewed failure to `history` without changing its result or whether it can be retried. `task acknowledge --yes` acknowledges all failures, optionally filtered with `--type` and `--before <RFC 3339 time>`.
- `storage-confirmation list` shows batches that need attention, with the owning task ID, storage provider, data set, piece count, and the storage provider's response. Retry one with `task retry <task-id>`. `--limit` accepts 1 to 1,000 (default `100`).

| Task type | Dashboard operation |
| --- | --- |
| `bucket_provision` | **Prepare bucket** |
| `upload_plan` | **Prepare upload** |
| `storage_dataset_ensure` | **Create data set** |
| `storage_transfer_plan` | **Prepare replica** |
| `storage_store` | **Upload data** |
| `storage_pull` | **Replicate data** |
| `storage_commit` | **Commit batch** |
| `provider_replacement_coordinate` | **Replace provider** |
| `cache_evict` | **Release cache** |
| `cache_reconcile_durability` | **Recheck cache release** |
| `cache_capacity_reconcile` | **Manage cache capacity** |
| `storage_cleanup` | **Clean up deleted data** |
| `storage_removal` | **Remove batch** |
| `storage_dataset_retire` | **Terminate data set** |
| `wallet_operation` | **Wallet operation** |
| `provider_upload_speed_test` | **Test upload speed** |
| `observability_refresh` | **Refresh storage health** |
| `approved_provider_refresh` | **Refresh approved providers** |
| `endorsed_provider_refresh` | **Refresh endorsed providers** |

## Remote Admin Access

To manage a remote host from your machine, forward the Admin port:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

`synaps3 admin` commands on your machine then reach the default `http://127.0.0.1:9090` and prompt for the Admin password. Alternatively, run the commands on the host itself.
