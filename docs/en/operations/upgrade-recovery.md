---
title: Upgrade and Recovery
description: Upgrade SynapS3 safely and recover background work.
---

# Upgrade and Recovery

Before changing versions, protect the database and cache as one recovery point. Restore failed dependencies before retrying background work.

## Before Upgrading

Run:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status failed --limit 50
```

Expected result: health is `ok`, and every failed task has a clear handling decision before the process is replaced.

Stop incoming S3 traffic and SynapS3 before creating a backup. Keep the database, cache, configuration, and credentials at the same recovery point. Follow [Runtime Data](../configuration/runtime-data.md) for backup and verification steps.

## Upgrade SynapS3

Replace the executable, package, or container image through the same installation method used for the deployment. Docker-specific commands are documented on the [Docker Deployment](../getting-started/docker.md) page.

Start SynapS3 with the intended database and cache. If startup reports that the database is incompatible, stop the process, leave the database unchanged, and follow [If the Database Is Incompatible](#if-the-database-is-incompatible).

After startup, run:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin settings get
synaps3 admin task stats
```

Expected result: health is `ok`, effective settings match the deployment, and background work resumes without unexpected failures. Read a known object through the S3 API before restoring normal traffic.

## If the Database Is Incompatible

1. Stop incoming S3 traffic and every SynapS3 process using the deployment.
2. Back up the reported database and verify the backup.
3. Keep the database and matching cache read-only. Do not edit either location to bypass the compatibility check.
4. Configure an empty database and cache directory for the replacement installation.
5. Start SynapS3 and verify health, effective settings, and background task processing before restoring traffic.

For SQLite, create a consistent backup after the process stops and verify that it opens:

```bash
sqlite3 /old/path/synaps3.db ".backup '/backup/path/synaps3-pre-upgrade.db'"
sqlite3 -readonly /backup/path/synaps3-pre-upgrade.db "PRAGMA integrity_check;"
```

The integrity check must print `ok`. Protect the backup, its WAL/SHM files when retained, the matching cache, and the configuration as one recovery set. PostgreSQL deployments should use `pg_dump` or the deployment's approved database snapshot and verify that artifact separately.

SynapS3 leaves an incompatible database unchanged. Deprecated `worker.upload`, `worker.provider_replacement`, `worker.evictor`, and `worker.storage_cleanup` configuration sections are also rejected; replace them with `worker.tasks` settings.

Starting with an empty database does not import existing buckets, objects, users, storage data sets, wallet operations, provider replacements, or tasks. Existing paid remote storage services remain active. Keep the verified backup so those services and records can be reviewed and handled separately.

Do not run the preserved installation and its replacement against the same database, cache, wallet workflow, or S3 traffic. After starting the replacement, create an S3 user and test bucket, then write and read a test object before restoring normal traffic.

## Recover Background Work

After a restart, unfinished work becomes eligible to continue automatically.

- Retry a failed task only when the dashboard or API marks it retryable.
- Recover provider replacements from **Details** → **Storage** → **Data Sets**.
- A wallet operation can be retried from Tasks only when no broadcast started. An uncertain broadcast remains non-retryable.
- **Retry upload** checks whether the provider has the piece, then uploads it again if missing. A repeat can use more bandwidth or open another upload session.
- `status=failed` lists unacknowledged failures. Use `status=dismissed` to list acknowledged failures.
- Review unresolved storage confirmations with `synaps3 admin storage-confirmation list`.

Useful commands:

```bash
synaps3 admin task list --status failed --limit 100
synaps3 admin task list --status dismissed --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin task acknowledge 42
synaps3 admin storage-confirmation list
synaps3 admin settings get
```

Restore failed dependencies before retrying work. Use the dashboard, Admin API, or CLI instead of editing the application database.

## Recovery Matrix

| Scenario | Recovery |
| --- | --- |
| Storage provider or RPC is temporarily unavailable | Restore connectivity. Waiting work resumes automatically; retry only failed tasks marked retryable. |
| Database full | Stop traffic, free space or scale the database, then verify health. |
| Cache disk full | Increase disk or `cache.max_size_gb`, or restore remote storage and cache-cleanup progress. |
| Provider must be evacuated | Open the bucket and use **Details** → **Storage** → **Data Sets**. Do not retry the replacement from Tasks. |
| Process crash | Restart SynapS3, verify health and task statistics, then review any unresolved storage confirmation or wallet outcome. |
| Startup reports an incompatible database | Stop the process, verify that the configured database is the intended one, and preserve it unchanged before using an empty replacement database. |

## Restore or Roll Back

1. Stop S3 traffic and SynapS3.
2. Verify backup checksums and select database and cache artifacts from the same recovery point.
3. For SQLite, restore the complete runtime data volume. For PostgreSQL, restore the database-native backup first, then the matching configuration and cache data.
4. If rolling back the application, use only data compatible with the selected version. When compatibility is uncertain, restore the pre-upgrade recovery point.
5. Start SynapS3, then check `/healthz`, effective settings, task statistics, failed tasks, wallet readiness, and a known S3 object.

Do not resume normal traffic until these checks pass.
