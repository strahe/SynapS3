---
title: Upgrade and Recovery
description: Upgrade SynapS3 safely and recover from common single-node failure scenarios.
---

# Upgrade and Recovery

SynapS3 is a single-node gateway. During recovery, protect durable local data first, then handle background tasks. When a dependency fails, restore it before retrying tasks.

## Before Upgrading

Run:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 50
```

Expected result: health is `ok`, and every exhausted task has a clear handling decision before the process is replaced.

Back up runtime data before major changes:

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
```

## Upgrade Docker Compose

```bash
docker compose pull
docker compose up -d
docker compose logs --tail=100 synaps3
```

Expected result: the service starts with the same `synaps3-data` volume and health returns `ok`.

## Runtime Flow

```text
PutObject -> cache + DB -> worker -> storage provider + Filecoin
```

- Writes commit to local cache and metadata before provider upload.
- Upload tasks retry with backoff and move to exhausted after max retries.
- `GetObject` reads from cache first and can retrieve from the provider when metadata is available.
- Bucket deletion is disabled; object deletes are soft deletes.

## Recovery Matrix

| Scenario | Recovery |
| --- | --- |
| Storage provider unreachable | Restore connectivity, then retry exhausted tasks. |
| RPC node down | Restore RPC connectivity, then retry exhausted tasks. |
| Private provider URL blocked | Keep blocked by default; enable `filecoin.allow_private_networks` only for trusted private deployments. |
| Database full | Free space or scale the database. |
| Cache disk full | Increase disk, raise `cache.max_size_gb`, or restore upload and eviction progress. |
| Process crash | Restart; startup recovery releases expired leases and resets stalled upload states. |

Useful commands:

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin s3-user list
synaps3 admin settings get
```

High-risk settings require `--yes`:

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
