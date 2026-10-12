---
title: Backup and Recovery
description: Back up runtime data and recover background work.
---

# Backup and Recovery

Protect the database, cache, and executable as one recovery point. Restore failed dependencies before retrying background work.

## Back Up Runtime Data

Stop incoming S3 traffic and SynapS3 before creating a backup. Keep the database, cache, configuration, credentials, and matching executable together. Follow [Runtime Data](../configuration/runtime-data.md) for backup and verification steps.

## Verify After a Restart

After startup, run:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin settings get
synaps3 admin task stats
```

Expected result: health is `ok`, effective settings match the deployment, and background work resumes without unexpected failures. Read a known object through the S3 API before restoring normal traffic.

## If the Database Is Incompatible

Stop SynapS3 and confirm that the configured database is the intended one. Preserve the database and matching cache unchanged. Restore a verified backup into a separate empty database and runtime directory, using the executable saved with that recovery point. Follow [Restore Order](../configuration/runtime-data.md#restore-order). Do not edit the preserved data to bypass the compatibility check.

SynapS3 leaves an incompatible database unchanged.

Starting with an empty database initializes separate records; it does not import buckets, objects, users, tasks, or wallet operations from a backup. It does not delete remote data sets or stop their storage charges. Do not let independent operation ledgers control the same wallet concurrently.

## Recover Background Work

After a restart, unfinished work becomes eligible to continue automatically.

- Use **Retry** for a stopped task. The task list shows whether its result permits retry; opening details checks the current conditions. If another operation has taken over or recovery is unsafe, Retry explains why and refreshes the list.
- Recover provider replacements from the bucket's **Storage** tab.
- When data set setup is **Failed**, use **Retry** from **Tasks** to continue with the same provider or choose another provider from **Storage**. This also applies to a replacement provider whose data set is not ready. Replacement is unavailable while setup is running. A request that completes after replacement can leave an unused data set and incur creation charges.
- A storage transfer task that stopped because of an internal error stays retryable. Retry checks what the earlier attempt already did before continuing.
- A wallet operation can be retried from Tasks when no broadcast started, or when it stopped because of an internal error. Retry checks the operation first: it sends the transaction only if none was ever broadcast, and otherwise checks the transaction already sent. If a broadcast may have gone out without a recorded transaction, the operation is marked unknown and cannot be retried.
- **Retry** checks whether the provider has the piece, then uploads it again if missing. A repeat can use more bandwidth or open another upload session.
- Acknowledgement moves a failure to **Closed**; use `scope=history&status=failed` to view it.
- Batch recovery preserves the signed request and checks the chain before resubmitting. Missing pieces are transferred again when safe. Provider errors and failed transfers use a finite budget. If a batch was conclusively refused and the chain confirms it did not land, the remaining pieces can continue when one cannot be recovered. When recovery stops, the batch keeps the saved evidence. Find it in **Tasks** (Submit batch, Failed) or with `synaps3 admin storage-confirmation list`, restore its dependencies, and use **Retry**. An accepted request with conflicting chain evidence remains stopped for review.
- A provider with a full Pull queue is tried again after its requested delay without using the retry budget. Other Pull failures use up to 12 opportunities. Restarting does not reset the used opportunities; manual Retry starts with a fresh budget.

Useful commands:

```bash
synaps3 admin task list --status failed --limit 100
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
| Provider must be evacuated | Open the bucket's **Storage** tab and replace the provider from **Data sets**. |
| Process crash | Restart SynapS3, verify health and task statistics, then retry any stopped batch and review any wallet outcome. |
| Startup reports an incompatible database | Stop the process, confirm the configured database, and preserve the database and matching cache. Follow [If the Database Is Incompatible](#if-the-database-is-incompatible). |

## Restore a Backup

1. Stop S3 traffic and SynapS3.
2. Verify backup checksums and select database and cache artifacts from the same recovery point.
3. Restore the database-native backup into a separate empty database, then restore the matching configuration and cache data into a separate empty runtime directory.
4. Use the executable saved with that recovery point.
5. Start SynapS3, then check `/healthz`, effective settings, task statistics, failed tasks, wallet readiness, and a known S3 object.

Do not resume normal traffic until these checks pass.
