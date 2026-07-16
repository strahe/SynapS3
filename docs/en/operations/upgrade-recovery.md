---
title: Upgrade and Recovery
description: Upgrade SynapS3 safely and recover from common single-node failure scenarios.
---

# Upgrade and Recovery

SynapS3 is a single-node gateway. During an upgrade or recovery, protect locally durable object data and metadata as one consistent set, then resume background tasks. Restore failed dependencies before retrying work.

## Before Upgrading

Run:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 50
```

Expected result: health is `ok`, and every exhausted task has a clear handling decision before the process is replaced.

Stop S3 traffic and SynapS3 before creating a backup:

```bash
docker compose stop synaps3
```

- SQLite deployments: archive the complete runtime data volume, then verify the archive and its checksum.
- PostgreSQL deployments: create a database-native backup and archive the matching configuration and cache data.

Keep every backup artifact at the same recovery point. Follow [Runtime Data](../configuration/runtime-data.md) for exact backup, verification, and restart steps.

## Upgrade Docker Compose

```bash
docker compose pull
docker compose up -d
docker compose logs --tail=100 synaps3
curl http://127.0.0.1:9090/healthz
docker compose exec synaps3 synaps3 admin settings get
docker compose exec synaps3 synaps3 admin task stats
```

Expected result: the service starts with the intended runtime data, health returns `ok`, effective settings match the deployment, and task queues resume without unexpected exhausted work. Read a known object through the S3 API before restoring normal traffic.

## Runtime Flow

```text
Receive write -> save object -> record metadata -> return success -> continue background storage
```

- Writes commit to local cache and metadata before provider upload.
- Failed storage tasks retry and move to `exhausted` after the configured retry limit.
- `GetObject` reads from cache first and can retrieve from the provider when metadata is available.
- Bucket deletion is not supported and returns `501`; object deletion removes the object from S3 visibility while cleanup continues safely.

## Recovery Matrix

| Scenario | Recovery |
| --- | --- |
| Background storage task cannot reach a provider | Restore connectivity, then retry exhausted storage tasks. |
| RPC node down | Restore RPC connectivity, then retry exhausted tasks. |
| Private provider URL blocked | Keep blocked by default; enable `filecoin.allow_private_networks` only for trusted private deployments. |
| Database full | Free space or scale the database. |
| Cache disk full | Increase disk, raise `cache.max_size_gb`, or restore upload and eviction progress. |
| Process crash | Restart the service, then verify health and task statistics; unfinished tasks become eligible to continue. |

A provider becoming unavailable after a copy has already been stored does not necessarily create a retryable task. Use storage-health views to identify affected copies. Restoring the target copy count is part of the [Replica Repair Vision](../concepts/filecoin-storage-flow.md#replica-repair-vision).

## Restore or Roll Back

1. Stop S3 traffic and SynapS3.
2. Verify archive checksums and select database and cache artifacts from the same recovery point.
3. For SQLite, restore the complete runtime data volume. For PostgreSQL, restore the database-native backup first, then the matching configuration and cache data.
4. If rolling back the application, start the pinned previous image only with data that is compatible with that version. When compatibility is uncertain, restore the pre-upgrade recovery point.
5. Start SynapS3 and verify `/healthz`, effective settings, task statistics, exhausted tasks, wallet readiness, and a known S3 object.

Do not resume normal traffic until these checks pass.

Useful commands:

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin s3-user list
synaps3 admin settings get
```

After changing a recovery-related setting, restart SynapS3 and verify both `/healthz` and `synaps3 admin settings get`.
