---
title: Health and Metrics
description: Read the SynapS3 health endpoint, dashboard monitoring, and Prometheus metrics.
---

# Health and Metrics

The Admin listener serves `/healthz` without authentication and `/metrics` with Admin authentication.

## Health

```bash
curl http://127.0.0.1:9090/healthz
```

| Response | HTTP status | Meaning |
| --- | --- | --- |
| `{"status":"ok"}` | `200` | The database is reachable, the cache directory exists, and background tasks are running. |
| `{"status":"setup"}` | `200` | Required settings are missing or invalid. Only Admin settings are available; the S3 API is not served. |
| `{"status":"unhealthy","errors":[...]}` | `503` | A check failed. `errors` contains `db: unreachable`, `cache: directory not found`, or `worker/tasks: not responding`. |

`setup` also returns `200`, so a probe that checks only the HTTP status treats it as healthy. Check the `status` field.

`worker/tasks: not responding` means background task processing has stopped reporting activity, even if no upload is in progress.

`synaps3 admin status` shows the health status, version, uptime, cache usage, and background task status. In setup mode, it lists the settings that need fixing.

## Dashboard Monitoring

The **Overview** page shows:

- **Process resources**: SynapS3 CPU usage in cores and resident memory.
- **Application traffic**: upload and download rates. **Client** covers S3 and dashboard object transfers; **Storage** covers transfers between SynapS3 and storage providers. Speed tests and copies between storage providers are not counted. The information button shows sample times and bytes since startup.

Charts sample every 15 seconds and keep one hour. A restart clears the history; use Prometheus for longer retention.

## Prometheus Metrics

Prometheus on the same host can scrape the Admin listener with Basic authentication:

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

A Prometheus container cannot reach the host loopback address. Bind `admin.addr` to a private interface that only Prometheus can reach, keep Admin authentication enabled, and do not publish the port.

| Metric | Meaning |
| --- | --- |
| `synaps3_backend_object_operations_total` | S3 object operations by type and status. |
| `synaps3_transfer_bytes_total` | Object bytes by `scope` (`client`, `storage`) and `direction` (`upload`, `download`). Resets on restart. |
| `synaps3_cache_used_bytes` | Cache disk usage. |
| `synaps3_cache_hits_total`, `synaps3_cache_misses_total` | Reads served from the cache, and reads that missed it. |
| `synaps3_task_queue_depth` | Pending and running tasks by type and status. Failed tasks are not included. |
| `synaps3_object_state_distribution` | Objects by state (`cached`, `uploading`, `committing`, `replicating`, `stored`, `failed`). |
| `synaps3_database_metrics_collection_success` | `1` if the last collection of database-backed metrics succeeded, otherwise `0`. |
| `synaps3_database_metrics_last_success_timestamp_seconds` | Time of the last successful collection, in Unix seconds. |

Database-backed metrics (`synaps3_task_queue_depth` and `synaps3_object_state_distribution`) are collected every 30 seconds. A failed collection keeps the previous values; check the two collection metrics before you trust them.

## What to Alert On

| Signal | Action |
| --- | --- |
| `/healthz` status is not `ok` | See [Troubleshooting](./troubleshooting.md#first-checks). |
| Cache usage nears `cache.max_size_gb` | See [Cache Full](./troubleshooting.md#cache-full). |
| Failed tasks increase (`synaps3 admin task stats` or `GET /api/v1/tasks/stats`) | See [Failed Tasks](./troubleshooting.md#failed-tasks). |
| Storage providers or data sets fail health checks | See [Provider or RPC Issues](./troubleshooting.md#provider-or-rpc-issues). |
