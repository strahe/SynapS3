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

Before upgrading, stop incoming S3 writes but leave the current SynapS3 process running until uploads and provider replacements have finished. If the new version refuses to start because storage work is still in progress, run the previous version against the unchanged database, restore the affected provider if necessary, and let that work finish before retrying the upgrade. Do not alter the database to bypass this check.

Stop S3 traffic and SynapS3 with the service manager used by your deployment before creating a backup.

- SQLite deployments: archive the complete runtime data volume, then verify the archive and its checksum.
- PostgreSQL deployments: create a database-native backup and archive the matching configuration and cache data.

Keep every backup artifact at the same recovery point. Follow [Runtime Data](../configuration/runtime-data.md) for exact backup, verification, and restart steps.

## Upgrade SynapS3

Replace the executable, package, or container image through the same installation method used for the current deployment. Docker-specific commands are documented on the [Docker Deployment](../getting-started/docker.md) page.

Start SynapS3 with the service manager used by your deployment, then run:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin settings get
synaps3 admin task stats
```

Expected result: health is `ok`, effective settings match the deployment, and task queues resume without unexpected exhausted work. Read a known object through the S3 API before restoring normal traffic.

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
| Established provider is temporarily unavailable during initial storage | Restore the original provider. Other assigned writable copies continue, while the unfinished copy waits without consuming retries and resumes automatically. SynapS3 does not select a replacement provider. |
| Background storage task cannot reach a provider | Restore connectivity, then retry exhausted storage tasks. |
| RPC node down | Restore RPC connectivity, then retry exhausted tasks. |
| Private provider URL blocked | Keep blocked by default; enable `filecoin.allow_private_networks` only for trusted private deployments. |
| Database full | Free space or scale the database. |
| Cache disk full | Increase disk, raise `cache.max_size_gb`, or restore upload and eviction progress. |
| Provider is permanently unavailable, or must be evacuated | Open the bucket, choose **Details**, then **Storage** → **Data Sets**, and replace the provider. New uploads move to the new provider once it is ready. Existing objects copy from another replica or from local cache; an object with neither cannot be copied, and the old provider is not shut down. If the selected target is already in use, choose another provider rather than retrying it. |
| Process crash | Restart the service, then verify health and task statistics. Most unfinished storage work resumes automatically without submitting the same piece again. Work whose outcome cannot be determined safely appears in `synaps3 admin storage-confirmation list`; inspect the listed provider and transaction details before taking action. Recorded service-shutdown transactions are checked before another shutdown is submitted. |

A provider becoming unavailable after a copy has already been stored does not necessarily create a retryable task. Use storage-health views to identify affected copies. Restoring the target copy count is part of [Planned Replica Repair](../concepts/filecoin-storage-flow.md#planned-replica-repair).

## Restore or Roll Back

1. Stop S3 traffic and SynapS3.
2. Verify archive checksums and select database and cache artifacts from the same recovery point.
3. For SQLite, restore the complete runtime data volume. For PostgreSQL, restore the database-native backup first, then the matching configuration and cache data.
4. If rolling back the application, start the previous release only with data that is compatible with that version. When compatibility is uncertain, restore the pre-upgrade recovery point.
5. Start SynapS3, then check `/healthz`, effective settings, task statistics, exhausted tasks, wallet readiness, and a known S3 object.

Do not resume normal traffic until these checks pass.

Useful commands:

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin storage-confirmation list
synaps3 admin s3-user list
synaps3 admin settings get
```

After changing a recovery-related setting, restart SynapS3 and verify both `/healthz` and `synaps3 admin settings get`.
