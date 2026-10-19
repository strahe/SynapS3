---
title: Backup and Recovery
description: Restore SynapS3 from a backup, handle an incompatible database, and recover from common failures.
---

# Backup and Recovery

A recovery point is the database backup, the matching configuration and cache archive, and the SynapS3 version that wrote them. Back up and restore as described in [Runtime Data](../configuration/runtime-data.md); Docker commands are in [Back Up Docker Data](../getting-started/docker.md#back-up-docker-data).

## Verify After a Restart

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin settings get
synaps3 admin task stats
```

Health is `ok`, the settings match the deployment, and tasks resume without new failures. Read a known object through the S3 API before you restore normal traffic.

## If the Database Is Incompatible

When startup reports `database contains an incompatible SynapS3 schema`, SynapS3 leaves the database unchanged.

1. Stop SynapS3 and confirm that `database.dsn` points to the intended database.
2. Keep that database and its matching cache unchanged. Do not edit them to get past the check.
3. Restore a verified backup into a new empty database and data directory with the version saved for that recovery point. Follow [Restore Order](../configuration/runtime-data.md#restore-order).

Starting with an empty database does not import buckets, objects, users, or tasks from the old one. The data sets created from the old database stay on chain and keep charging the wallet. Never run two SynapS3 databases with the same wallet at the same time.

## Recovery Matrix

| Situation | Recovery |
| --- | --- |
| A storage provider or the RPC endpoint is temporarily unavailable | Restore connectivity. Waiting work resumes on its own; retry only failed tasks. See [Failed Tasks](./troubleshooting.md#failed-tasks). |
| A storage provider is permanently gone | Replace it from the bucket's **Storage** tab. See [Replace a Storage Provider](../concepts/filecoin-storage-flow.md#replace-a-storage-provider). |
| The cache disk is full | See [Cache Full](./troubleshooting.md#cache-full). |
| The database volume is full | Stop S3 traffic, free space or grow the volume, then check health. |
| SynapS3 crashed | Restart it. Check health and task statistics, retry stopped batches, and review the result of any wallet operation that was running. |
| Startup reports an incompatible database | See [If the Database Is Incompatible](#if-the-database-is-incompatible). |

Recover through the dashboard, Admin API, or CLI. Do not edit the SynapS3 database directly.
