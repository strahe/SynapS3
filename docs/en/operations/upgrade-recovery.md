---
title: Upgrade and Recovery
description: Move to the current database baseline safely and recover background work without repeating external effects.
---

# Upgrade and Recovery

The current SynapS3 release starts from a new database baseline. It does not migrate or take ownership of data from an earlier database. Plan the cutover as a new installation with a preserved, read-only copy of the old runtime data.

## Required Fresh-Baseline Cutover

1. Stop incoming S3 traffic and stop every old SynapS3 process.
2. Back up the old database and verify the backup before continuing.
3. Keep the old database and cache read-only. Do not configure the new release to use either location.
4. Configure a new, empty database path and a new, empty cache directory.
5. Start the new release and verify health, effective settings, and task processing before restoring traffic.

For SQLite, create a consistent backup after the process stops and verify that it opens:

```bash
sqlite3 /old/path/synaps3.db ".backup '/backup/path/synaps3-pre-baseline.db'"
sqlite3 -readonly /backup/path/synaps3-pre-baseline.db "PRAGMA integrity_check;"
```

The integrity check must print `ok`. Protect the backup, its WAL/SHM files when retained, the old cache, and the matching configuration as one recovery set. PostgreSQL deployments should use `pg_dump` or the deployment's approved database snapshot and verify that artifact separately.

The new release refuses a database that contains an earlier SynapS3 migration marker or any application table. It does not drop or modify that database. Old `worker.upload`, `worker.provider_replacement`, `worker.evictor`, and `worker.storage_cleanup` configuration sections are rejected; replace them with `worker.tasks`.

## What Is Not Carried Forward

The fresh installation does not take over old buckets, objects, users, storage data sets, pieces, wallet operations, replacement records, or task state. Existing paid remote storage services are not ended by resetting the local database. Keep the verified old backup so those services and records can be reviewed and handled manually.

Do not run old and new SynapS3 versions against the same database, cache, wallet workflow, or S3 traffic. The new release must never connect to the preserved old database.

## Verify the New Installation

Run:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin settings get
synaps3 admin task stats
```

Expected result: health is `ok`, the configured database and cache are the new locations, and the task engine reports activity. Create a test bucket, write and read a test object, then confirm its background storage task before restoring normal traffic.

## Runtime Recovery

After the cutover, unfinished work uses one task engine with five stored states: `pending`, `running`, `completed`, `failed`, and `cancelled`.

- An interrupted running task is reclaimed after its lease expires and starts in recovery mode.
- Recovery checks its checkpoint and domain evidence before starting another external effect.
- A failed task can be retried only when the API marks it retryable; retry always starts in recovery mode.
- Provider replacement recovery remains in the bucket Data Sets view.
- A wallet operation that stopped before broadcasting can be recovered from Tasks. An uncertain broadcast remains non-retryable and is retained as an unknown wallet outcome rather than replayed blindly.
- A failed Store whose provider outcome is uncertain offers **Check again**. This action checks the provider for the intended parked piece and never uploads the bytes again.
- `status=failed` lists unacknowledged failures. Use `status=dismissed` to list acknowledged failures; `dismissed` is a filter and presentation value, not a sixth stored task status.
- A storage confirmation whose provider outcome cannot be proved appears in `synaps3 admin storage-confirmation list` for explicit review.

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

Restore failed dependencies before retrying work. Do not edit task rows, clear checkpoints, or shorten leases manually.

If the last object that references an uncertain, uncommitted Store is permanently deleted, SynapS3 releases the terminal task binding. A piece that reached the provider but was never committed has no local provider piece ID to delete; the provider's parked-piece garbage collection remains responsible for reclaiming it.

## Recovery Matrix

| Scenario | Recovery |
| --- | --- |
| Storage provider or RPC is temporarily unavailable | Restore connectivity. Waiting work resumes automatically; retry only failed tasks marked retryable. |
| Database full | Stop traffic, free space or scale the database, then verify health. |
| Cache disk full | Increase disk or `cache.max_size_gb`, or restore remote storage and cache-cleanup progress. |
| Provider must be evacuated | Open the bucket and use **Details** → **Storage** → **Data Sets**. Do not retry the replacement from Tasks. |
| Process crash | Restart SynapS3. Expired claims are recovered without changing the task identity. Review any storage-confirmation or wallet outcome that remains uncertain. |
| Fresh-baseline start reports an incompatible database | Stop the process, verify that the configured DSN is the intended new empty database, and preserve the reported database unchanged. |

## Back Up the New Installation

After the cutover, future backups again treat configuration, database, and cache as one recovery point. Stop SynapS3 before a filesystem backup, or use a database-native consistent snapshot and coordinate it with the cache. Follow [Runtime Data](../configuration/runtime-data.md) for the current layout and verification steps.
