---
title: Configuration Model
description: Find where SynapS3 reads settings, how changes apply, and what each setting group controls.
---

# Configuration Model

SynapS3 reads a TOML configuration file, then applies `SYNAPS3_` [environment variables](./environment.md) on top. Keep stable settings in the file and secrets or host-specific values in the environment.

## Sources and Changes

- Commands read the file given by `--config`, then `SYNAPS3_CONFIG`, then `~/.synaps3/config.toml`. A `config.toml` in the current directory is not read unless you pass it.
- After `synaps3 init --dir <path>`, pass `--config <path>/config.toml` to later commands.
- The **Settings** page and `synaps3 admin settings set` rewrite `config.toml`; comments and ordering are not kept.
- A field set by an environment variable cannot be changed from **Settings** or the CLI. Change the environment instead.
- Saved settings apply after a restart. `synaps3 admin settings get` shows the saved values and **Restart required**.

```bash
synaps3 admin settings get
```

## Required Settings

| Field | Notes |
| --- | --- |
| `database.dsn` | PostgreSQL connection URL, for example `postgres://synaps3:PASSWORD@db.example.com:5432/synaps3?sslmode=require`. No default. The role must be able to create tables; SynapS3 applies its schema migrations at startup. |
| `filecoin.private_key` | Wallet private key. Without it, SynapS3 starts in setup mode. Set it in the file or `SYNAPS3_FILECOIN_PRIVATE_KEY`; the dashboard cannot set it. |
| `admin.auth.password_hash`, `admin.auth.session_secret` | Required while `admin.auth.enabled = true`. `synaps3 init` creates both. `synaps3 admin-auth reset-password --config <path>` sets a new password and rotates the session secret, which signs out all browser sessions. |

A database is bound to the first wallet address and Filecoin network that use it. Starting with another wallet or network fails with `database belongs to another Filecoin wallet or network`. You can change `filecoin.rpc_url` to another endpoint on the same network.

Keep the configuration file, `.env`, and other credential files at mode `0600`, and keep private keys out of commits, images, and shell history.

## Sections

| Section | Controls |
| --- | --- |
| `server` | S3 API listener, connection and request limits, and TLS. |
| `s3` | Region reported to S3 clients. |
| `filecoin` | Network, RPC endpoint, wallet, private-network storage provider URLs, CDN retrieval, default replica target, and storage provider selection. |
| `filecoin.observability` | Storage provider and data set health checks. |
| `filecoin.upload_speed_test` | Automatic upload speed tests. |
| `database` | PostgreSQL connection and pool size. |
| `cache` | Local cache directory, capacity, and eviction policy. |
| `worker.tasks` | Background task concurrency and batch submission. |
| `logging` | Log level, format, and S3 access logs. |
| `admin` | Dashboard and Admin API listener, trusted proxies, and authentication. |

## Defaults

| Field | Default |
| --- | --- |
| `server.port` | `:8080` |
| `server.max_connections` | `4096` |
| `server.max_requests` | `512` |
| `s3.region` | `us-east-1` |
| `filecoin.network` | `calibration` |
| `filecoin.rpc_url` | `https://api.calibration.node.glif.io/rpc/v1` |
| `filecoin.default_copies` | `3` |
| `filecoin.anchor_provider_tier` | `approved` |
| `filecoin.upload_speed_test.auto_scope` | `trusted` |
| `filecoin.upload_speed_test.retest_interval` | `0s` |
| `filecoin.observability.interval` | `5m` |
| `filecoin.observability.timeout` | `5s` |
| `filecoin.observability.concurrency` | `8` |
| `database.max_open_conns` | `32` |
| `database.max_idle_conns` | `2` |
| `cache.dir` | `~/.synaps3/cache` |
| `cache.max_size_gb` | `100` |
| `cache.eviction_policy` | `lru` |
| `cache.lru_high_watermark_percent` | `80` |
| `cache.lru_low_watermark_percent` | `50` |
| `worker.tasks.concurrency` | `12` |
| `worker.tasks.poll_interval` | `5s` |
| `worker.tasks.lease_duration` | `5m` |
| `worker.tasks.upload_concurrency` | `4` |
| `worker.tasks.commit_max_pieces` | `32` |
| `worker.tasks.commit_max_wait` | `30m` |
| `worker.tasks.removal_max_pieces` | `100` |
| `worker.tasks.removal_max_wait` | `30m` |
| `worker.tasks.commit_max_backlog` | `256` |
| `worker.tasks.commit_seal_on_cache_pressure` | `false` |
| `logging.level` | `info` |
| `logging.format` | `text` (the Docker deployment sets `json`) |
| `admin.addr` | `127.0.0.1:9090` |
| `admin.trusted_proxies` | `[]` |
| `admin.auth.enabled` | `true` |
| `admin.auth.username` | `admin` |
| `admin.auth.session_ttl` | `12h` |

## Allowed Values

- `filecoin.network`: `calibration`, `mainnet`. `filecoin.rpc_url` must serve the same network; the Mainnet default is `https://api.node.glif.io/rpc/v1`. When you change the network on the **Settings** page, a default RPC URL switches with it.
- `filecoin.default_copies`: `1` through `8`.
- `filecoin.anchor_provider_tier`: `approved`, `endorsed`, `none`.
- `filecoin.upload_speed_test.auto_scope`: `trusted`, `all`. `retest_interval` must be `0s` or more.
- `cache.eviction_policy`: `lru`, `after_upload`, `none`.
- `cache.lru_low_watermark_percent` and `cache.lru_high_watermark_percent`: `0 <= low < high <= 100`.
- `worker.tasks.commit_max_pieces`: `1` through `200`. `commit_max_wait`: `0s` through `30m`. `commit_max_backlog`: at least `commit_max_pieces`.
- `worker.tasks.removal_max_pieces`: `1` through `200`. `removal_max_wait`: `0s` through `30m`.
- `server.max_requests`: at most `server.max_connections`. `database.max_idle_conns`: at most `database.max_open_conns`.
- `logging.level`: `debug`, `info`, `warn`, `error`. `logging.format`: `json`, `text`.
- `admin.trusted_proxies`: IP addresses or CIDR ranges.

## S3 Listener

Enable native TLS for the S3 API:

```toml
[server.tls]
enabled = true
cert_file = "/path/to/tls.crt"
key_file = "/path/to/tls.key"
```

The SynapS3 process must be able to read both files. Without native TLS, put the S3 API behind your own TLS proxy for production traffic.

When more than `server.max_requests` S3 requests are in flight, extra requests receive `503 SlowDown`.

## Cache

`cache.max_size_gb` caps local cache usage in GiB. A write reserves its full size before SynapS3 reads the body; if it does not fit, the write fails with `503 SlowDown`. See [Cache Full](../operations/troubleshooting.md#cache-full).

| `cache.eviction_policy` | Behavior |
| --- | --- |
| `lru` | When usage reaches the high watermark, or a write is refused, SynapS3 removes the least recently read versions that are safe to remove until usage reaches the low watermark. The target always leaves room for one maximum-size object (1,065,353,216 bytes). |
| `after_upload` | SynapS3 removes each version once its bucket's minimum durable replicas are committed. |
| `none` | SynapS3 never removes cached data automatically. |

Watermarks apply only to `lru`. Removal runs in the background; a write does not wait for it. Which versions are safe to remove is described in [Write Path and Cache](../concepts/write-path-cache.md#cache-eviction).

## Background Tasks

`worker.tasks.concurrency` (**Task concurrency**) limits all background tasks. `worker.tasks.upload_concurrency` (**Upload concurrency**) also limits uploads to storage providers, including reading and hashing cached data. When a limit is reached, tasks wait in the queue. Failed tasks retry a fixed number of times; task history is kept indefinitely.

## Batches

SynapS3 adds uploaded pieces to their data set on chain in batches, one transaction per batch. The **Batches** page lists them. Removal batches appear on **Tasks**.

| Field | Settings label | Effect |
| --- | --- | --- |
| `worker.tasks.commit_max_pieces` | **Batch size** | Maximum pieces per transaction. |
| `worker.tasks.commit_max_wait` | **Batch wait** | Maximum time the oldest piece waits for its batch to fill. `0s` submits without waiting. |
| `worker.tasks.removal_max_pieces` | **Removal batch size** | Maximum pieces per removal transaction. |
| `worker.tasks.removal_max_wait` | **Removal batch wait** | Maximum time to collect pieces for removal. `0s` skips the collection delay. |
| `worker.tasks.commit_max_backlog` | **Batch backlog** | When this many transferred pieces wait for one data set, new uploads and replicas to that data set wait. |
| `worker.tasks.commit_seal_on_cache_pressure` | **Submit batches early to free cache space** | Submits batches early when automatic cache cleanup cannot free enough space. Has no effect with `cache.eviction_policy = none`. |

A commit batch is submitted when it reaches the batch size, when its oldest piece reaches the batch wait, when the data set stops accepting data, or when you select **Submit next batch** on the **Batches** page. Each data set has at most four submitted batches awaiting confirmation; later batches wait.

Pieces waiting for submission stay in the local cache. A longer batch wait delays cache release and replicas that copy from the first replica. Smaller batches mean more transactions and higher fees.

Open a batch to see its pieces, transaction, and errors. A stopped batch shows **Retry**.

## Provider Selection

SynapS3 chooses storage providers when it prepares a bucket, adds replicas, or replaces a storage provider automatically. A candidate needs a recent healthy check and a successful upload speed test for its current service URL. Assigned storage providers stay assigned when their tier membership or speed results change, and uploads to them do not wait for speed tests.

`filecoin.anchor_provider_tier` (**Provider requirement**) requires at least one selected storage provider from the FWSS approved list (`approved`) or the endorsed list (`endorsed`); `none` drops the requirement. If the required list is empty, bucket preparation stops; fix the cause shown on the bucket or task, then select **Retry**.

Speed tests:

- Approved, endorsed, and assigned storage providers are tested automatically when they first become eligible and after their service URL changes.
- `auto_scope = "trusted"` (**Trusted providers**) tests only those. Other storage providers become candidates after you test them with **Test remaining providers** or from their details on the **Storage** page. `all` (**All providers**) tests every eligible storage provider.
- `retest_interval` greater than `0s` retests storage providers periodically. With `0s`, a failed or timed-out test stays failed until you select **Retry**.

A bucket is created immediately. Its storage preparation waits until eligible approved and endorsed storage providers finish speed tests, then checks the replica target and the provider requirement. Waiting does not use up retries.

**Provider preference** is chosen at bucket creation and cannot be changed:

| Option | API value | Ranking |
| --- | --- | --- |
| **Distribution first** (default) | `distribution` | Storage providers with fewer active data sets first, then faster uploads. |
| **Speed first** | `speed` | Faster uploads first, then fewer active data sets. |

Ties are broken randomly. Buckets created through the S3 API use `distribution`. The preference applies to additional replicas and automatic replacements. A storage provider can serve a bucket again only after all its data sets for that bucket are retired, and automatic replacement never picks a storage provider the bucket has used before.

## Admin Sessions

`admin.auth.session_ttl` sets the lifetime of a dashboard session. While you use the dashboard (pointer, click, keyboard, or wheel input), it renews the session; background polling does not. A session that is not renewed expires after `session_ttl`. There is no maximum login lifetime.

By default the login cookie lasts for the browser session. **Keep me signed in** sets a persistent cookie that lasts 30 days or `session_ttl`, whichever is longer.

## High-Risk Fields

| Field | Risk |
| --- | --- |
| `admin.addr` | The Admin API can change data, credentials, and wallet funds. Keep it on loopback unless it is behind HTTPS and access control. |
| `admin.trusted_proxies` | SynapS3 trusts `X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, and `X-Forwarded-Host` from these addresses. List only proxies you control that strip these headers from client requests. |
| `admin.auth.password_hash` | Controls Admin login. Generate it with `synaps3 init` or `synaps3 admin-auth reset-password`. |
| `admin.auth.session_secret` | Signs Admin sessions. Treat it as a secret. |
| `filecoin.private_key` | Spends wallet funds. Treat it as a secret. |
| `database.dsn` | Usually contains the database password. Treat it as a secret. |
| `filecoin.network` | Must match the database and `filecoin.rpc_url`. Use a separate database per network. |
| `filecoin.allow_private_networks` | Allows storage provider URLs on private networks. Enable only when you trust those storage providers. |
| `cache.max_size_gb` | Too small refuses writes; too large can fill the disk. |
| `cache.lru_high_watermark_percent` | A high value leaves less room for writes while cleanup runs. |
| `cache.lru_low_watermark_percent` | A low value removes more cached data in each cleanup. |
