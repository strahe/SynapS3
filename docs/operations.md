# Operations

Run SynapS3, monitor health, and recover failed tasks.

## Runtime Flow

```text
PutObject -> cache + DB -> worker -> storage provider + Filecoin
```

- Writes commit to local cache and metadata before provider upload
- Upload tasks retry with backoff and move to exhausted after max retries
- `GetObject` reads from cache first and can retrieve from the provider when metadata is available
- `DeleteBucket` is disabled; object deletes are soft deletes

## Deployment Notes

- Use the local build flow in the [README](../README.md) for the current developer preview
- Docker deployment guidance is coming soon
- Keep the admin server bound to `127.0.0.1` unless it is behind an authenticated reverse proxy
- Keep `filecoin.private_key` out of committed config files
- Store cache data on durable local disk for long-running nodes
- Monitor cache usage, task queue depth, worker health, and exhausted tasks

The admin server exposes unauthenticated write endpoints. Do not publish it directly to an untrusted network.

## Health

In normal serve mode, `GET /healthz` checks database connectivity, cache directory availability, and worker liveness.

```bash
synaps3 admin status
curl http://localhost:9090/healthz
```

Healthy:

```json
{"status":"ok"}
```

Unhealthy:

```json
{"status":"unhealthy","errors":["worker/uploader: not responding"]}
```

Setup mode returns `{"status":"setup"}`. Workers are unhealthy when they have no active work and no recent tick for longer than `3 * poll_interval`.

## Metrics

Prometheus metrics are exposed on the admin port:

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
| `synaps3_backend_object_operations_total` | S3 operations by type and status |
| `synaps3_cache_used_bytes` | Current cache disk usage |
| `synaps3_cache_hits_total` / `synaps3_cache_misses_total` | Cache read behavior |
| `synaps3_worker_tasks_processed_total` | Worker throughput by result |
| `synaps3_worker_tasks_exhausted_total` | Tasks that exhausted retries |
| `synaps3_task_queue_depth` | Active tasks by type and status |
| `synaps3_object_state_distribution` | Object counts by lifecycle state |

## Recovery

| Scenario | Recovery |
| --- | --- |
| Storage provider unreachable | Restore connectivity, then retry exhausted tasks |
| RPC node down | Restore RPC connectivity, then retry exhausted tasks |
| Private retrieval URL blocked | Keep blocked by default; enable `filecoin.allow_private_networks` only for trusted private deployments |
| Database full | Free space or scale the database |
| Cache disk full | Increase disk, lower `cache.max_size_gb`, or evict data |
| Process crash | Restart; startup recovery reconciles stale states and orphaned tasks |

Useful commands:

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin s3-user list
synaps3 admin settings get
synaps3 admin settings set logging.level=debug
```

High-risk settings, such as switching Filecoin networks or allowing private retrieval URLs, require `--yes`.

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
