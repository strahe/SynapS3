---
title: Runtime Data
description: Understand where SynapS3 stores configuration, metadata, cache data, and what to back up.
---

# Runtime Data

SynapS3 stores configuration, metadata, and cached object data on local disk. Place this data on durable storage. A usable backup must keep the database and cache at the same recovery point.

## Default Local Layout

```text
~/.synaps3/
  config.toml
  admin-initial-password
  db/
    synaps3.db
    synaps3.db-shm
    synaps3.db-wal
  cache/
```

SQLite WAL and SHM files are expected. Explicit `database.dsn` and `cache.dir` values take precedence over defaults.

## Docker Layout

The container uses `/var/lib/synaps3`:

```text
/var/lib/synaps3/
  config.toml
  admin-initial-password
  db/
  cache/
```

The Compose deployment mounts this path through the `synaps3-data` Docker volume.

## What Must Be Durable

| Data | Why it matters |
| --- | --- |
| `config.toml` | Holds stable runtime settings when they are not environment-managed. |
| `admin-initial-password` | Stores the generated Admin password for non-interactive init and password reset. Keep it at `0600`; after saving the password securely, retain it only if local CLI commands still need it. |
| `db/` | Stores buckets, objects, versions, tasks, users, and storage metadata. |
| `cache/` | Holds locally durable object bytes for Filecoin upload and read rehydration. |
| Environment secrets | May hold the Filecoin private key and deployment-specific overrides. |

Keep `config.toml`, `.env`, credential files, and exported secrets at permission mode `0600`. Do not commit or copy wallet private keys into unprotected archives.

## Before a Backup

1. Check `curl http://127.0.0.1:9090/healthz` and record any non-`ok` result.
2. Review active and exhausted work with `synaps3 admin task stats` and `synaps3 admin task list --status exhausted`.
3. Stop SynapS3 so object data, metadata, and task state cannot change during the backup. With Compose, run `docker compose stop synaps3`.

Do not create a filesystem archive while SynapS3 is still running.

## SQLite Backup

SQLite is the default database. With SynapS3 stopped, back up the entire runtime data volume so the database, WAL/SHM files, configuration, and cache share one recovery point:

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
docker run --rm \
  -v "$PWD":/backup \
  alpine:3 \
  sh -c 'cd /backup && tar tzf synaps3-data.tgz >/dev/null && sha256sum synaps3-data.tgz > synaps3-data.tgz.sha256 && sha256sum -c synaps3-data.tgz.sha256'
```

The archive listing and checksum verification must exit successfully. Store `synaps3-data.tgz` and `synaps3-data.tgz.sha256` together in protected backup storage.

## PostgreSQL Backup

If the deployment uses PostgreSQL, stop SynapS3 and then:

1. Create a database-native backup with `pg_dump`, a managed-database snapshot, or the approved PostgreSQL backup tool for your deployment.
2. Back up the SynapS3 configuration and cache volume separately.
3. Label the database backup and volume archive with the same recovery point.
4. Verify both artifacts before restarting the service.

The PostgreSQL backup replaces copying a SQLite database directory; it does not replace the configuration and cache backup.

## Restart and Verify

After a successful backup:

```bash
docker compose start synaps3
curl http://127.0.0.1:9090/healthz
docker compose exec synaps3 synaps3 admin task stats
```

`/healthz` should return `{"status":"ok"}`. Investigate `setup` or `unhealthy` before resuming S3 traffic.

## Restore Order

Before restoring a SQLite archive, verify the stored copy:

```bash
docker run --rm \
  -v "$PWD":/backup:ro \
  alpine:3 \
  sh -c 'cd /backup && sha256sum -c synaps3-data.tgz.sha256'
```

1. Stop SynapS3 and keep S3 traffic disabled.
2. Verify the archive checksum and confirm the database and cache have the same recovery-point label.
3. Restore the runtime volume into an empty replacement location. For PostgreSQL, restore the database-native backup before attaching the matching configuration and cache data.
4. Confirm the restored configuration and credential files are `0600` and readable by the SynapS3 account.
5. Start SynapS3, check `/healthz`, review task statistics and exhausted tasks, then read a known object through the S3 API.

Do not combine a database backup with cache data from another point in time.
