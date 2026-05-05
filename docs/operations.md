# Operations

Run SynapS3 safely, monitor it, and recover failed tasks.

## Runtime Flow

```text
PutObject -> cache + DB -> worker -> storage provider + Filecoin
```

- Writes commit to local cache and metadata before provider upload
- Upload tasks retry with backoff and move to dead-letter after max retries
- `GetObject` reads from cache first and can retrieve from the provider when metadata is available
- `DeleteBucket` is disabled; object deletes are soft deletes

## Production

Use PostgreSQL for shared or durable deployments. Keep the admin server bound to `127.0.0.1` unless it is behind an authenticated reverse proxy.

Minimum production posture:

- Store cache data on durable local disk
- Keep `filecoin.private_key` out of committed config files
- Keep `filecoin.allow_private_networks = false` unless provider retrieval URLs are trusted
- Monitor cache usage, task queue depth, worker health, and dead-letter tasks

See [Configuration](configuration.md) for the full production config example.

## Docker

The admin server exposes unauthenticated write endpoints. Do not publish it directly to an untrusted network.

```bash
docker build -t synaps3 .
docker run -d \
  --name synaps3 \
  -p 8080:8080 \
  -v /etc/synaps3/config.toml:/etc/synaps3/config.toml:ro \
  -v /data/synaps3/cache:/var/lib/synaps3/cache \
  synaps3
```

The image health check calls `/healthz` on the admin port. Publish `9090` only on a trusted network or behind an authenticated proxy.

## Monitoring

SynapS3 exposes Prometheus metrics on the admin port:

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
| `synaps3_worker_task_duration_seconds` | Worker task latency |
| `synaps3_worker_dead_letter_total` | Tasks that exhausted retries |
| `synaps3_task_queue_depth` | Pending tasks by type and status |
| `synaps3_object_state_distribution` | Object counts by lifecycle state |

## Health

`GET /healthz` checks database connectivity, cache directory availability, and worker liveness.

The CLI wraps the same admin API:

```bash
synaps3 admin status
```

Healthy:

```json
{"status":"ok"}
```

Unhealthy:

```json
{"status":"unhealthy","errors":["worker/uploader: not responding"]}
```

Workers are unhealthy if they stop completing poll cycles for longer than `3 * poll_interval`.

## Failure Recovery

| Scenario | Recovery |
| --- | --- |
| Storage provider unreachable | Restore connectivity, then retry dead-letter tasks |
| RPC node down | Restore RPC connectivity, then retry dead-letter tasks |
| Private retrieval URL blocked | Keep blocked by default; enable `filecoin.allow_private_networks` only for trusted private deployments |
| Database full | Free space or scale the database |
| Cache disk full | Increase disk, lower `cache.max_size_gb`, or evict data |
| Process crash | Restart; startup recovery reconciles stale states and orphaned tasks |

List dead-letter tasks:

```bash
synaps3 admin task list --status dead_letter --limit 100
```

Show task queue counts:

```bash
synaps3 admin task stats
```

Retry one task:

```bash
synaps3 admin task retry 42
```

List S3 users:

```bash
synaps3 admin s3-user list
```

Update or delete an S3 user:

```bash
synaps3 admin s3-user update <access-key> --role userplus
synaps3 admin s3-user delete <access-key> --yes
```

Show editable settings:

```bash
synaps3 admin settings get
synaps3 admin settings get logging.level
```

Update settings:

```bash
synaps3 admin settings set logging.level=debug
```

High-risk settings, such as switching Filecoin networks or allowing private retrieval URLs, require `--yes`.

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
