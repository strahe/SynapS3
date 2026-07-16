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
{"status":"unhealthy","errors":["worker/uploader: not responding"]}
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

Upload endpoints can fail when cache capacity is exhausted. Check usage:

```bash
synaps3 admin status
synaps3 admin settings get cache.max_size_gb
```

Recovery options:

- Confirm the host has free disk space, then increase `cache.max_size_gb` if capacity allows.
- Restore storage provider connectivity and background task progress so queued uploads can complete and cache eviction can run.
- Keep `cache.eviction_policy = "lru"` if local cache cleanup should be queued after remote storage succeeds.

After changing the cache setting, restart SynapS3, check `/healthz`, and verify the effective value with `synaps3 admin settings get cache.max_size_gb`.

## Exhausted Tasks

List exhausted work:

```bash
synaps3 admin task list --status exhausted --limit 100
```

Retry only after RPC connectivity, storage provider availability, wallet funds, FWSS approval, and cache disk capacity are ready.

```bash
synaps3 admin task retry 42
```

## Provider or RPC Issues

Check provider health and Filecoin readiness in the dashboard, or inspect the Admin API:

```bash
curl -u admin http://127.0.0.1:9090/api/v1/filecoin/readiness
curl -u admin http://127.0.0.1:9090/api/v1/observability/providers
```

Enter the Admin password at curl's no-echo prompt.

Recovery:

- Restore the configured `filecoin.rpc_url`.
- Confirm provider URLs are reachable from the SynapS3 host.
- Keep `filecoin.allow_private_networks = false` unless private provider URLs are expected and trusted.

## S3 Client Cannot Upload

Check these in order:

1. S3 client uses path-style addressing.
2. Access key and secret came from `synaps3 admin s3-user create`.
3. Endpoint is `http://localhost:8080` for local evaluation or the correct HTTPS address for production.
4. Object size is between `127` and `1,065,353,216` bytes, and the object key meets the [S3 compatibility limits](../reference/s3-compatibility.md#stable-limits).
5. Dashboard task view shows whether Filecoin storage is queued, running, or exhausted.
