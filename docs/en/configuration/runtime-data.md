---
title: Runtime Data
description: Find where SynapS3 keeps its data, and back it up and restore it as one recovery point.
---

# Runtime Data

SynapS3 keeps its configuration and cached objects on local disk and its metadata in PostgreSQL. The cache can hold objects that are not on Filecoin yet, and the database records where every object is stored. Put both on durable storage, and back them up and restore them together.

## Layout

| Data | Default location | Contents |
| --- | --- | --- |
| `config.toml` | `~/.synaps3/` | Settings and Admin credentials; often the database DSN and wallet private key. |
| `admin-initial-password` | `~/.synaps3/` | Admin password written by a non-interactive `synaps3 init` or by `synaps3 admin-auth reset-password`. |
| `cache/` | `~/.synaps3/cache/`, or `cache.dir` | Cached object data, including objects not yet stored on Filecoin. |
| PostgreSQL database | `database.dsn` | Buckets, objects, versions, replicas, tasks, wallet operations, and S3 users. S3 secret keys are stored in plaintext. |
| Environment secrets | `.env` or host environment | Private key, DSN, and other overrides. |

In the Docker deployment, the files are in `/var/lib/synaps3/` on the `synaps3-data` volume, and the managed database is on the `synaps3-postgres-data` volume.

Keep `config.toml`, `admin-initial-password`, `.env`, and backups readable only by the account that runs SynapS3. Delete `admin-initial-password` once the password is in a password manager, unless local `synaps3 admin` commands read it.

## Back Up

Docker deployments can use the commands in [Back Up Docker Data](../getting-started/docker.md#back-up-docker-data).

1. Record the current state:

   ```bash
   curl http://127.0.0.1:9090/healthz
   synaps3 admin task stats
   synaps3 admin task list --status failed
   ```

2. Stop SynapS3 with your service manager. An archive taken while SynapS3 runs is not consistent.
3. Back up the database with `pg_dump` or a database snapshot.
4. Archive the configuration file and the cache directory.
5. Label the database backup and the archive with the same recovery point, record checksums, and verify both.
6. Start SynapS3 and confirm that `/healthz` returns `{"status":"ok"}`.

## Restore Order

1. Stop SynapS3 and keep S3 traffic off.
2. Verify the checksums, and confirm that the database backup and the archive carry the same recovery-point label.
3. Restore the database into an empty database, then restore the configuration and cache into an empty directory.
4. Confirm that the restored configuration and credential files have mode `0600` and belong to the SynapS3 account.
5. Start the SynapS3 version that wrote the backup.
6. Check `/healthz`, `synaps3 admin task stats`, and failed tasks, then read a known object through the S3 API.

Never combine a database backup with cache data from another recovery point. If startup reports an incompatible database, see [If the Database Is Incompatible](../operations/backup-recovery.md#if-the-database-is-incompatible).
