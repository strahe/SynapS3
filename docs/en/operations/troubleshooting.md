---
title: Troubleshooting
description: Diagnose common SynapS3 setup, health, wallet, cache, task, and provider issues.
---

# Troubleshooting

Start here when an upload, download, login, or background storage operation fails. Check health first, then narrow the problem by wallet, cache, task, or storage provider signals.

## First Checks

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin status
synaps3 admin task stats
```

Expected healthy baseline:

```json
{"status":"ok"}
```

If health is not `ok`, use the error text as the next branch.

## Setup Mode

Health may return:

```json
{"status":"setup"}
```

This means SynapS3 is missing required settings. Review the configuration validation details before changing a value.

Check the reported fields:

```bash
synaps3 admin status
synaps3 admin settings get
```

If the wallet private key is missing, generate one:

```bash
synaps3 wallet generate
```

If the missing value is the wallet key, set the generated private key in `SYNAPS3_FILECOIN_PRIVATE_KEY` or `filecoin.private_key` in the config file. Restart SynapS3, check `/healthz`, and verify the effective settings.

Expected result: health changes from `setup` to `ok` after restart.

## Unhealthy Background Tasks

Example:

```json
{"status":"unhealthy","errors":["worker/tasks: not responding"]}
```

Check task pressure:

```bash
synaps3 admin task stats
synaps3 admin task list --status running --limit 20
```

After a restart, unfinished tasks become eligible to continue. If background task processing stays unhealthy, record the current task state, inspect logs, and then restart the service.

## Wallet Funding or Deposit Fails

Check wallet status:

```bash
synaps3 admin status
```

For Calibration, fund the wallet address again:

```bash
synaps3 wallet fund-testnet 0x...
```

Then retry deposit and FWSS approval:

```bash
synaps3 wallet deposit 2 # 2 USDFC
synaps3 wallet approve
```

If faucet funding fails, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/), then rerun `synaps3 admin status`.

Successful faucet claims print `CalibnetUSDFC: <hash>` and `CalibnetFIL: <hash>`. A confirmed deposit or approval prints `Transaction: <hash>` and `Status: confirmed`; an existing approval prints `FWSS approval: already approved`. If these results do not appear, verify RPC connectivity, wallet funds, and the reported error before retrying.

## Cache Full

When the local cache cannot hold a write, S3 clients and dashboard uploads receive `503 SlowDown`. Uploads in progress hold cache capacity for their full size, so a new upload can be refused while reported usage is still below the limit. Completing a multipart upload needs free capacity for the assembled object in addition to its parts. Check usage:

```bash
synaps3 admin status
synaps3 admin settings get cache.max_size_gb
synaps3 admin settings get cache.eviction_policy
synaps3 admin settings get cache.lru_high_watermark_percent
synaps3 admin settings get cache.lru_low_watermark_percent
```

Recovery options:

- Confirm the host has free disk space, then increase `cache.max_size_gb` if capacity allows.
- Restore storage provider connectivity and background task progress so queued uploads can complete and cache eviction can run.
- If batches are waiting to fill, open **Batches** and choose **Submit next batch**, or enable [automatic early submission](../configuration/model.md) in Settings and restart.
- Use the default `lru` policy for capacity-based cleanup. Lower the high watermark to leave more write headroom, and keep `0 <= low < high <= 100`.
- Use `after_upload` only when each version should be removed asynchronously after its bucket's minimum durable copies commit.
- Use `none` when automatic removal must be disabled.

LRU cannot remove multipart staging data, versions below their bucket's minimum durable copies, or versions without a readable committed remote copy. With `lru`, a refused write requests background cleanup when usage exceeds the effective low watermark. Writes do not wait for cleanup, so `503 SlowDown` can continue until cleanup catches up or safe candidates become available. Default S3 client retries may give up sooner; retry the upload later.

Failed LRU deletion tasks remain visible as failed work. Fix the reported filesystem or database problem first; use `synaps3 admin task retry <id>` when the task is marked retryable.

After changing a cache setting, restart SynapS3, check `/healthz`, and verify the effective cache values with `synaps3 admin settings get`.

## Failed Tasks

List failed work:

```bash
synaps3 admin task list --status failed --limit 100
```

Retry only after RPC connectivity, storage provider availability, wallet funds, FWSS approval, and cache disk capacity are ready.

```bash
synaps3 admin task retry 42
```

The API indicates whether Retry is available for each failed task. Provider replacement work is recovered from **Details** → **Storage** → **Data Sets**. A wallet operation can be recovered from Tasks when no broadcast started, or when it stopped because of an internal error; recovery sends the transaction only if it was never broadcast, and an uncertain broadcast remains non-retryable. A failed Store offers **Retry**; it checks the provider first and resends the piece only if missing. For remote-copy removal that remains unconfirmed after 24 hours, **Retry** checks the chain first and may submit another paid request if the copy is still present and not queued; the earlier request may still succeed. If the data set is no longer active, the chain cannot confirm this individual removal, so the task keeps checking instead of claiming the copy is gone. Use **Acknowledge** or `synaps3 admin task acknowledge <id>` to mark a reviewed failure as viewed; history is retained indefinitely. When failures have piled up, **Acknowledge all** on the Tasks page clears the ones the current Operation filter selects, and `synaps3 admin task acknowledge --type <operation> --yes` does the same from the CLI; failures recorded after you confirm stay in the list.

For a failed replica, use **Retry** in **Tasks** or the object's **Provenance → Replicas** table. It creates a new execution and preserves earlier failed executions for that replica. Check Provenance for **Stored** to confirm completion. Confirmed peer sync failures use retained local cache automatically when available; otherwise replica recovery needs a readable remote source or local cache. If recovery is unavailable, follow the reason shown beside the replica.

## Provider or RPC Issues

Check this node's storage health on Overview and Filecoin readiness in Settings, or inspect the Admin API:

```bash
curl -u admin http://127.0.0.1:9090/api/v1/filecoin/readiness
curl -u admin http://127.0.0.1:9090/api/v1/observability/providers
```

Enter the Admin password at curl's no-echo prompt.

Recovery:

- Restore the configured `filecoin.rpc_url`.
- Confirm provider URLs are reachable from the SynapS3 host. The provider details in **Storage Topology** show why the last health check failed.
- Keep `filecoin.allow_private_networks = false` unless private provider URLs are expected and trusted.

## S3 Client Cannot Upload

Check these in order:

1. S3 client uses path-style addressing.
2. Access key and secret came from `synaps3 admin s3-user create`.
3. Endpoint is `http://localhost:8080` for local evaluation or the correct HTTPS address for production.
4. Object size is between `127` and `1,065,353,216` bytes, and the object key meets the [S3 compatibility limits](../reference/s3-compatibility.md#stable-limits).
5. Dashboard task view shows whether Filecoin storage is queued, running, waiting, or failed.
