---
title: Filecoin Storage Flow
description: Understand how background workers move locally durable objects to Filecoin-backed storage.
---

# Filecoin Storage Flow

Filecoin storage happens after the S3 write is accepted. Background workers move objects from local durability to provider-backed storage.

## Task Chain

```text
PutObject
  -> cached object + upload task
  -> uploading
  -> committing
  -> replicating
  -> stored
  -> evict_cache task
  -> cache_evicted
```

## Object States

| State | Meaning |
| --- | --- |
| `cached` | Object is durable locally and queued for upload. |
| `uploading` | A worker is preparing provider storage or uploading bytes. |
| `committing` | Provider storage has a piece ready and the commit step is in progress. |
| `replicating` | At least one readable copy exists while target copies are still being completed. |
| `stored` | Target remote copy policy is satisfied and metadata is available. |
| `failed` | The active lifecycle step failed and may be retried. |
| `cache_evicted` | Local cache has been removed after remote durability. |

## Retries and Leases

Workers claim tasks with lease semantics. If a process crashes, startup recovery releases expired leases and resets stale upload states so work can continue.

Retries are bounded by worker settings. Tasks that exhaust retries need operator action:

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task retry 42
```

Retry after fixing the cause, such as RPC availability, provider reachability, wallet balance, or cache capacity.

## Provider Health

Observability checks record provider and local data set health. These signals power dashboard storage-health views and help operators identify unavailable, degraded, or unknown storage copies.

## What Users See

- S3 upload can succeed before Filecoin storage finishes.
- Dashboard task and topology views show storage progress.
- Reads prefer local cache, then provider retrieval when metadata is available.
- Cache eviction is an operational optimization, not the write acceptance point.
