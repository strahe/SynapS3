---
title: Troubleshooting
description: Diagnose and fix SynapS3 startup, health, S3, cache, task, wallet, and storage provider problems.
---

# Troubleshooting

## First Checks

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin status
synaps3 admin task stats
```

If the status is not `ok`, go to the matching section below. Status meanings are listed in [Health](./health-metrics.md#health).

## Setup Mode

`{"status":"setup"}` means required settings are missing or invalid. `synaps3 admin status` and the **Settings** page list them.

The most common cause is a missing wallet private key. Set `SYNAPS3_FILECOIN_PRIVATE_KEY` or `filecoin.private_key` (generate one with `synaps3 wallet generate` if needed), restart SynapS3, and check that `/healthz` returns `ok`.

## Startup Fails

| Error | Fix |
| --- | --- |
| `database.dsn is not set` | Set `database.dsn` or `SYNAPS3_DATABASE_DSN`. |
| `database belongs to another Filecoin wallet or network` | The private key or `filecoin.network` differs from the one the database was created with. Use the original private key and network, or a separate database. |
| `database contains an incompatible SynapS3 schema` | See [If the Database Is Incompatible](./backup-recovery.md#if-the-database-is-incompatible). |
| `locking migrator` | A previous migration was killed. If no other SynapS3 process is running, run `synaps3 migrate --force-unlock`, then start again. |
| A validation error for `admin.auth.*` | Run `synaps3 admin-auth reset-password --config <path>`. |

## Unhealthy Background Tasks

`{"status":"unhealthy","errors":["worker/tasks: not responding"]}` means background task processing has stopped.

1. Record the current state:

   ```bash
   synaps3 admin task stats
   synaps3 admin task list --status running --limit 20
   ```

2. Check the SynapS3 logs for errors.
3. Restart SynapS3. Unfinished tasks resume after the restart.

## S3 Requests Return SlowDown

SynapS3 returns `503 SlowDown` for several conditions. The error message names the cause:

| Message | Cause | Action |
| --- | --- | --- |
| `The bucket is still being prepared. Please retry shortly.` | The bucket's data sets are not ready yet. | Check the bucket's status and preparation message in the dashboard, and the speed-test notice on the **Storage** page. Retry when the bucket is **Ready**. |
| `The local cache is full. Please retry later.` | The write does not fit in the cache. | See [Cache Full](#cache-full). |
| `The same data is still being removed after an earlier delete. Please retry shortly.` | You deleted the last version holding these bytes, and SynapS3 is still removing the remote replicas. | Retry after cleanup finishes. The wait includes **Removal batch wait**. |
| Any other message | More than `server.max_requests` requests are in flight. | Retry with backoff, or raise `server.max_requests`. |

S3 clients retry `SlowDown` a few times by default and may give up before the condition clears; retry the operation later.

## Cache Full

A write that does not fit in the cache fails with `503 SlowDown`. Writes in progress reserve their full size, so a write can be refused while reported usage is below the limit.

```bash
synaps3 admin status
synaps3 admin settings get cache.max_size_gb
synaps3 admin settings get cache.eviction_policy
```

The cache frees space only after versions reach their bucket's minimum durable replicas; see [Cache Eviction](../concepts/write-path-cache.md#cache-eviction). To recover:

- Restore storage provider connectivity and background task progress, so pending uploads finish and cache can be released.
- If batches are waiting to fill, select **Submit next batch** on the **Batches** page, or enable **Submit batches early to free cache space** in **Settings** and restart.
- If the host disk has room, raise `cache.max_size_gb` and restart.
- With `lru`, lower `cache.lru_high_watermark_percent` to start cleanup earlier.
- Lower the bucket's **Release cache after** to release cache sooner.

If the **Manage cache capacity** task waits with `Waiting for reliable cache access records`, LRU cleanup is paused until restart. Fix the database errors in the logs, then restart SynapS3.

A failed **Release cache** task stays in the failed list. Fix the reported filesystem or database problem, then retry it.

## Failed Tasks

```bash
synaps3 admin task list --status failed --limit 100
synaps3 admin task retry 42
```

Fix the cause first: RPC connectivity, storage provider reachability, wallet funds, FWSS approval, or cache space. Then select **Retry** on the **Tasks** page, or run `synaps3 admin task retry <id>`. **Retry** is offered only when it is safe; otherwise the task shows why.

| Operation | Recovery |
| --- | --- |
| **Create data set** | **Retry** continues with the same storage provider. To use another one instead, open the bucket's **Storage** tab and replace the provider. |
| **Upload data** | **Retry** checks whether the storage provider already has the data and uploads it again only if it is missing. |
| **Replace provider** | Recover from the bucket's **Storage** tab. |
| **Commit batch** | Restore the dependency, then **Retry**. SynapS3 checks the chain before it resubmits the batch. `synaps3 admin storage-confirmation list` lists batches that need attention. |
| **Wallet operation** | **Retry** continues the recorded transaction or starts an operation that was never broadcast. If a broadcast may have gone out without a recorded transaction, check the wallet on chain; the operation cannot be retried. |
| **Remove batch** | **Retry** checks the chain and may submit another paid removal request. An earlier request can still succeed. |
| **Clean up deleted data** | **Retry** retries failed replicas and continues cleanup. |

For a failed replica, use **Retry** on the **Tasks** page or in the object's **Provenance** dialog. The replica is complete when it shows **Stored**.

After you review a failure, select **Acknowledge** or run `synaps3 admin task acknowledge <id>` to move it to **Closed**. Acknowledging does not change its result or whether it can be retried. To clear many failures, open **Open**, filter by **Failed** and optionally **Operation**, and select **Acknowledge all**; or run `synaps3 admin task acknowledge --type <type> --yes`. Failures recorded after you confirm stay open.

## Wallet Operations

**Awaiting confirmation** means the transaction is recorded but its outcome is unresolved, even if the RPC rejected the broadcast. SynapS3 checks every 30 seconds; five minutes without a receipt consumes one retry. It can resend the same signed transaction when available, without creating another payment. If automatic confirmation stops, use **Retry** on the **Tasks** page to continue the same transaction. A stopped task does not cancel a transaction on chain.

When a transaction can never be included, its operation stops on its own and cannot be retried. If the transaction's permit expired before it was included, the operation is marked failed and no funds moved; start the operation again. If another transaction from this wallet used the same nonce, the outcome is marked unknown; check the wallet's transactions on chain to see whether the operation happened.

Check the balances and FWSS approval on the **Wallet** page, or with:

```bash
curl -u admin http://127.0.0.1:9090/api/v1/wallet
```

On Calibration, claim test funds again, then deposit and approve:

```bash
synaps3 wallet fund-testnet 0x...
synaps3 wallet deposit 2 # 2 USDFC
synaps3 wallet approve
```

If the faucet claim fails, use [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/). If a command fails, check RPC connectivity and the wallet's FIL balance for gas. Expected output is listed in [Wallet Commands](../reference/cli.md#wallet-commands).

## Provider or RPC Issues

The **Overview** page shows storage health, and the **Settings** page shows Filecoin readiness. The same data is available from the Admin API:

```bash
curl -u admin http://127.0.0.1:9090/api/v1/filecoin/readiness
curl -u admin http://127.0.0.1:9090/api/v1/observability/providers
```

To recover:

- Fix `filecoin.rpc_url`, or the network path to it.
- Confirm that storage provider URLs are reachable from the SynapS3 host. The storage provider's details on the **Storage** page show why the last health check failed.
- Keep `filecoin.allow_private_networks = false` unless you trust storage providers on private networks.

Buckets wait for speed tests before they choose storage providers. Use **Test remaining providers** to test eligible storage providers without a result, and **Retry** for failed tests. See [Provider Selection](../configuration/model.md#provider-selection).

## S3 Client Cannot Upload

Check in order:

1. The client uses path-style addressing.
2. The access key and secret key are correct, and the user's role allows the operation; see [Create Credentials](../getting-started/s3-clients.md#create-credentials).
3. The endpoint is correct: `http://localhost:8080` for local evaluation, or your HTTPS endpoint.
4. The object is 127 to 1,065,353,216 bytes, and its key is within the [Stable Limits](../reference/s3-compatibility.md#stable-limits).
5. For `503 SlowDown`, see [S3 Requests Return SlowDown](#s3-requests-return-slowdown).
