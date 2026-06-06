---
title: Troubleshooting
description: Diagnose common SynapS3 setup, health, wallet, cache, task, and provider issues.
---

# Troubleshooting

Start here when a user-visible workflow fails. Check health first, then narrow the problem by wallet, cache, task, or provider signals.

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

This means SynapS3 is missing required settings, usually `filecoin.private_key`.

Fix:

```bash
synaps3 wallet generate
```

Set the generated private key in `SYNAPS3_FILECOIN_PRIVATE_KEY` or `filecoin.private_key` in the config file, then restart SynapS3.

Expected result: health changes from `setup` to `ok` after restart.

## Unhealthy Worker

Example:

```json
{"status":"unhealthy","errors":["worker/uploader: not responding"]}
```

Check task pressure:

```bash
synaps3 admin task stats
synaps3 admin task list --status running --limit 20
```

If the process recently restarted, startup recovery should release stale leases. If the worker stays unhealthy, capture the current task state, inspect logs, and then restart the service.

## Wallet Funding or Deposit Fails

Check wallet status:

```bash
synaps3 admin status
```

For Calibration, fund the wallet address again:

```bash
synaps3 wallet fund-testnet 0x...
```

Then retry deposit:

```bash
synaps3 wallet deposit 2 # 2 USDFC
```

If faucet funding fails, claim manually from [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) or [Plumbline](https://faucet.reiers.io/), then rerun `synaps3 admin status`.

## Cache Full

Upload endpoints can fail when cache capacity is exhausted. Check usage:

```bash
synaps3 admin status
synaps3 admin settings get cache.max_size_gb
```

Recovery options:

- Increase `cache.max_size_gb` if disk capacity allows.
- Restore provider or worker connectivity so queued uploads can complete and cache eviction can run.
- Keep `cache.eviction_policy = "lru"` if local cache cleanup should be queued after remote storage succeeds.

## Exhausted Tasks

List exhausted work:

```bash
synaps3 admin task list --status exhausted --limit 100
```

Retry only after the underlying dependency is fixed. Typical dependencies are RPC connectivity, provider availability, wallet balance, or cache disk capacity.

```bash
synaps3 admin task retry 42
```

## Provider or RPC Issues

Check provider health and Filecoin readiness in the dashboard, or inspect the Admin API:

```bash
curl -u "admin:${SYNAPS3_ADMIN_PASSWORD}" http://127.0.0.1:9090/api/v1/filecoin/readiness
curl -u "admin:${SYNAPS3_ADMIN_PASSWORD}" http://127.0.0.1:9090/api/v1/observability/providers
```

Recovery:

- Restore the configured `filecoin.rpc_url`.
- Confirm provider URLs are reachable from the SynapS3 host.
- Keep `filecoin.allow_private_networks = false` unless private provider URLs are expected and trusted.

## S3 Client Cannot Upload

Check these in order:

1. S3 client uses path-style addressing.
2. Access key and secret came from `synaps3 admin s3-user create`.
3. Endpoint is `http://localhost:8080` or the correct remote S3 address.
4. Test object is at least 127 bytes.
5. Dashboard task view shows whether Filecoin storage is queued, running, or exhausted.
