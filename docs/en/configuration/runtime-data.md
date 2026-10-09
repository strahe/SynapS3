---
title: Runtime Data
description: Understand where SynapS3 stores configuration, metadata, cache data, and what to back up.
---

# Runtime Data

SynapS3 stores configuration and cached object data on local disk, and metadata in PostgreSQL. Place all of them on durable storage. A usable backup must keep the database and cache at the same recovery point.

## Default Local Layout

```text
~/.synaps3/
  config.toml
  admin-initial-password
  cache/
```

An explicit `cache.dir` value takes precedence over the default.

## Docker Layout

The container uses `/var/lib/synaps3`:

```text
/var/lib/synaps3/
  config.toml
  admin-initial-password
  cache/
```

The Docker deployment mounts this path through the `synaps3-data` volume, and keeps the managed PostgreSQL database in the `synaps3-postgres-data` volume. Docker-specific lifecycle and backup commands are documented on the [Docker Deployment](../getting-started/docker.md) page.

## What Must Be Durable

| Data | Why it matters |
| --- | --- |
| `config.toml` | Holds stable runtime settings when they are not environment-managed. |
| `admin-initial-password` | Stores the generated Admin password for non-interactive init and password reset. Keep it at `0600`; after saving the password securely, retain it only if local CLI commands still need it. |
| PostgreSQL database | Stores buckets, objects, versions, tasks, users, and storage metadata. |
| `cache/` | Holds locally durable object bytes for Filecoin upload and read rehydration. |
| Environment secrets | May hold the Filecoin private key and deployment-specific overrides. |

Keep `config.toml`, `.env`, credential files, and exported secrets at permission mode `0600`. Do not commit or copy wallet private keys into unprotected archives.

## Before a Backup

1. Check `curl http://127.0.0.1:9090/healthz` and record any non-`ok` result.
2. Review active and failed work with `synaps3 admin task stats` and `synaps3 admin task list --status failed`.
3. Stop SynapS3 with the service manager used by your deployment so object data, metadata, and task state cannot change during the backup.

Do not create a filesystem archive while SynapS3 is still running.

## Back Up

With SynapS3 stopped:

1. Create a database-native backup with `pg_dump`, a managed-database snapshot, or the approved PostgreSQL backup tool for your deployment.
2. Back up the SynapS3 configuration and cache directory.
3. Label the database backup and the configuration and cache archive with the same recovery point.
4. Verify both artifacts before restarting the service.

The [Docker Deployment](../getting-started/docker.md#back-up-docker-data) page has the commands for the managed database.

## Restart and Verify

After a successful backup, start SynapS3 with the service manager used by your deployment, then run:

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
```

`/healthz` should return `{"status":"ok"}`. Investigate `setup` or `unhealthy` before resuming S3 traffic.

## Restore Order

1. Stop SynapS3 and keep S3 traffic disabled.
2. Verify the backup checksums and confirm the database and cache have the same recovery-point label.
3. Restore the database-native backup into an empty database, then restore the matching configuration and cache data into an empty location.
4. Confirm the restored configuration and credential files are `0600` and readable by the SynapS3 account.
5. Start SynapS3, check `/healthz`, review task statistics and failed tasks, then read a known object through the S3 API.

Do not combine a database backup with cache data from another point in time.

Restore a backup only with a compatible SynapS3 version. If startup reports that the database is incompatible, leave the backup unchanged and follow [Upgrade and Recovery](../operations/upgrade-recovery.md).
