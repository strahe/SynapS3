---
title: Health and Metrics
description: Monitor SynapS3 health, worker liveness, cache usage, task queues, and Prometheus metrics.
---

# Health and Metrics

The admin server exposes health checks, Prometheus metrics, and read-only operational views for the dashboard.

## Health

In normal serve mode, `GET /healthz` checks database connectivity, cache directory availability, and worker liveness.

```bash
synaps3 admin status
curl http://localhost:9090/healthz
```

Healthy response:

```json
{"status":"ok"}
```

Setup response:

```json
{"status":"setup"}
```

Unhealthy response:

```json
{"status":"unhealthy","errors":["worker/uploader: not responding"]}
```

Use `setup` to complete missing configuration. Treat `unhealthy` as an operational incident and check the error list first.

## Worker Liveness

Workers are unhealthy when they have no active work and no recent tick for longer than `3 * poll_interval`. This catches stopped workers without requiring active uploads.

Check worker state:

```bash
synaps3 admin status
synaps3 admin task stats
```

Expected result: status shows workers as healthy, and task stats show whether work is queued, running, failed, or exhausted.

## Prometheus Metrics

Metrics are exposed on the admin port:

```yaml
scrape_configs:
  - job_name: synaps3
    static_configs:
      - targets: ["synaps3:9090"]
    metrics_path: /metrics
```

Key metrics:

| Metric | Meaning |
| --- | --- |
| `synaps3_backend_object_operations_total` | S3 operations by type and status. |
| `synaps3_cache_used_bytes` | Current cache disk usage. |
| `synaps3_cache_hits_total` / `synaps3_cache_misses_total` | Cache read behavior. |
| `synaps3_worker_tasks_processed_total` | Worker throughput by result. |
| `synaps3_worker_tasks_exhausted_total` | Tasks that exhausted retries. |
| `synaps3_task_queue_depth` | Active tasks by type and status. |
| `synaps3_object_state_distribution` | Object counts by lifecycle state. |

## Operator Signals

| Signal | Action |
| --- | --- |
| Health is `setup` | Set missing required configuration, usually `filecoin.private_key`. |
| Health is `unhealthy` | Check database, cache directory, and worker error messages. |
| Cache usage approaches capacity | Increase capacity or restore upload and eviction progress. |
| Exhausted task count increases | Fix the dependency, then retry tasks. |
| Provider health is degraded | Check RPC, provider URLs, and network reachability. |

See [Troubleshooting](./troubleshooting.md) for recovery steps.
