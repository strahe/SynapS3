---
title: Health and Metrics
description: Monitor SynapS3 health checks, background task activity, cache usage, task queues, and Prometheus metrics.
---

# Health and Metrics

The admin server exposes health checks, Prometheus metrics, and operational views for the dashboard. `/healthz` is unauthenticated; `/metrics` and dashboard API endpoints require Admin auth.

## Health

During normal operation, `GET /healthz` checks database connectivity, cache directory availability, and background task activity.

```bash
synaps3 admin status
curl http://127.0.0.1:9090/healthz
```

Healthy response:

```json
{"status":"ok"}
```

Missing required configuration:

```json
{"status":"setup"}
```

Failed check:

```json
{"status":"unhealthy","errors":["worker/tasks: not responding"]}
```

`setup` means required configuration is missing. `unhealthy` means a database, cache, or background task check failed; check the returned errors first.

## Background Task Activity

SynapS3 reports an unhealthy task processor when it stops reporting activity for longer than its configured health window. This detects stalled background storage even when no upload is active.

Check background task state:

```bash
synaps3 admin status
synaps3 admin task stats
```

Status should show background task processing as healthy. Task stats report acknowledged failures separately as dismissed, and the dashboard presents pending work as queued, scheduled, or waiting.

## Prometheus Metrics

Metrics are exposed on the admin port:

```yaml
scrape_configs:
  - job_name: synaps3
    static_configs:
      - targets: ["127.0.0.1:9090"]
    metrics_path: /metrics
    basic_auth:
      username: admin
      password_file: /run/secrets/synaps3-admin-password
```

This target is for Prometheus running on the SynapS3 host. A containerized Prometheus cannot use the host loopback address: place both services on an explicitly configured private network, bind the Admin endpoint only to the required private interface, keep Admin authentication enabled, and do not publish the Admin port publicly.

Key metrics:

| Metric | Meaning |
| --- | --- |
| `synaps3_backend_object_operations_total` | S3 operations by type and status. |
| `synaps3_cache_used_bytes` | Current cache disk usage. |
| `synaps3_cache_hits_total` / `synaps3_cache_misses_total` | Cache read behavior. |
| `synaps3_cache_lru_eviction_paused` | `1` when LRU eviction is paused because recent cache access could not be retained safely. Resolve the persistence error and restart SynapS3. |
| `synaps3_task_queue_depth` | Active tasks by type and status. |
| `synaps3_object_state_distribution` | Object counts by lifecycle state. |

## Operator Signals

| Signal | Action |
| --- | --- |
| `/healthz` returns `setup` | Run `synaps3 admin status` or `synaps3 admin settings get`, set the reported missing configuration, restart, and check again. |
| `/healthz` returns `unhealthy` | Check database, cache directory, and background task error messages. |
| Cache usage approaches capacity | Increase capacity or restore upload and eviction progress. |
| Failed task count increases | Fix the dependency, then retry only tasks marked retryable. |
| Provider health is degraded | Check RPC, provider URLs, and network reachability. |

See [Troubleshooting](./troubleshooting.md) for recovery steps.
