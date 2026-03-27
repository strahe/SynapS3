# Operations

This document covers the runtime behavior of SynapS3: how data moves through the system, how to run it in production, what to monitor, and which admin endpoints are available.

## Storage Pipeline

```text
PutObject -> cache write + DB commit -> upload worker -> on-chain worker -> cache eviction
```

### Object flow

1. `PutObject` writes the payload to the local filesystem cache, then commits metadata and an upload task to the database.
2. The uploader worker sends the cached file to the Storage Provider and records the resulting PieceCID.
3. The on-chain worker submits the data root to the bucket ProofSet lifecycle.
4. The evictor removes the local cache entry after on-chain storage is confirmed.
5. `GetObject` serves cached data first and can download from the provider on eligible cache misses once the object has a `PieceCID` and the size is within `max_sp_download_size`.

### Bucket flow

- `CreateBucket` enqueues async ProofSet creation
- `DeleteBucket` enqueues async ProofSet deletion

## Production Deployment

For production, prefer PostgreSQL and keep the admin server bound to localhost unless it is protected by an authenticated reverse proxy.

```yaml
database:
  driver: postgres
  dsn: "postgres://synaps3:password@db:5432/synaps3?sslmode=require"
  max_open_conns: 25
  max_idle_conns: 10

cache:
  dir: /var/lib/synaps3/cache
  max_size_gb: 500
  eviction_policy: lru
  evict_after_onchain: true
  max_sp_download_size: 1073741824  # 1 GiB (0 = unlimited)

worker:
  upload:
    concurrency: 4
    poll_interval: 5s
  onchain:
    concurrency: 2
    poll_interval: 30s
  evictor:
    concurrency: 2
    poll_interval: 1m
  proofset:
    concurrency: 1
    poll_interval: 30s

logging:
  level: info
  format: json

admin:
  addr: "127.0.0.1:9090"
```

## Docker

> The admin server exposes unauthenticated write endpoints such as `POST /admin/dead-letters/{id}/retry`.
> Keep it on `127.0.0.1` or place it behind an authenticated reverse proxy.

```bash
docker build -t synaps3 .
docker run -d \
  --name synaps3 \
  -p 8080:8080 \
  -v /etc/synaps3/config.yaml:/etc/synaps3/config.yaml:ro \
  -v /data/synaps3/cache:/var/lib/synaps3/cache \
  synaps3
```

The image includes a health check against `/healthz` on the admin port. If you need to reach `/metrics` or the admin endpoints from outside the container, publish port `9090` and override `admin.addr` accordingly only inside a trusted network or behind an authenticated proxy.

## Monitoring

SynapS3 exposes Prometheus metrics on the admin port. The scrape target below assumes that the admin endpoint is reachable from Prometheus; if you keep `admin.addr` on `127.0.0.1`, scrape it locally or expose it through a protected proxy first.

| Metric | Type | Description |
| --- | --- | --- |
| `synaps3_backend_object_operations_total` | Counter | S3 operations by type and status |
| `synaps3_cache_used_bytes` | Gauge | Current cache disk usage |
| `synaps3_cache_hits_total` / `synaps3_cache_misses_total` | Counter | Cache hit and miss counts |
| `synaps3_worker_tasks_processed_total` | Counter | Tasks processed by worker and result |
| `synaps3_worker_task_duration_seconds` | Histogram | Per-task processing latency |
| `synaps3_worker_dead_letter_total` | Counter | Tasks that exceeded max retries |
| `synaps3_task_queue_depth` | Gauge | Pending tasks by type and status |
| `synaps3_object_state_distribution` | Gauge | Object counts by pipeline state |

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: synaps3
    static_configs:
      - targets: ["synaps3:9090"]
    metrics_path: /metrics
```

## Failure Modes

| Scenario | Behavior | Recovery |
| --- | --- | --- |
| Storage Provider unreachable | Upload tasks retry with backoff and can end in dead-letter after max retries; eligible cache-miss `GetObject` requests can also fail | Restore provider connectivity, then retry via admin API |
| RPC node down | On-chain tasks retry with backoff | Restore RPC connectivity |
| Database full | Writes fail and worker claims stop progressing | Free space or scale the database |
| Cache disk full | `PutObject` fails while cached reads continue | Increase disk, lower cache size, or evict more aggressively |
| Process crash | Startup recovery reconciles stale states and orphaned tasks | Automatic on restart |

## Admin API

### `GET /healthz`

Returns service health. It checks database connectivity, cache directory availability, and worker liveness.

Workers are treated as unhealthy if they stop completing poll cycles for longer than `3 * poll_interval`.

Healthy:

```json
{"status":"ok"}
```

Unhealthy:

```json
{"status":"unhealthy","errors":["worker/onchain: not responding"]}
```

### `GET /metrics`

Prometheus-format metrics endpoint.

### `GET /admin/dead-letters?limit=100`

Lists tasks that exhausted retries and entered dead-letter status.

```json
[
  {
    "ID": 42,
    "Type": "upload_to_sp",
    "RefType": "object",
    "RefID": 7,
    "RefGeneration": 1,
    "IdempotencyKey": "upload:7:1",
    "Payload": null,
    "Status": "dead_letter",
    "RetryCount": 5,
    "MaxRetries": 5,
    "LastError": "SP upload: connection refused (max retries reached)",
    "ScheduledAt": "2025-01-15T10:30:00Z",
    "ClaimedAt": null,
    "LeaseUntil": null,
    "StartedAt": null,
    "CompletedAt": null
  }
]
```

### `POST /admin/dead-letters/{id}/retry`

Requeues a dead-letter task for another attempt.

```json
{"status":"requeued"}
```
