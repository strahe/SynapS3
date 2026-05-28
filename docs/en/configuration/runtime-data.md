---
title: Runtime Data
description: Understand where SynapS3 stores configuration, metadata, cache data, and what to back up.
---

# Runtime Data

SynapS3 stores configuration, metadata, and cached object data on local disk. For long-running nodes, place this data on durable storage and back it up before upgrades.

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
| `admin-initial-password` | Stores the generated Admin password for non-interactive init and password reset. File mode is `0600`; remove or rotate it after storing the password securely. |
| `db/` | Stores buckets, objects, versions, tasks, users, and storage metadata. |
| `cache/` | Holds locally durable object bytes before and after Filecoin upload. |
| Environment secrets | May hold the Filecoin private key and deployment-specific overrides. |

## Backup Example

For Docker Compose:

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
```

Expected result: the archive contains config, database, and cache data from the volume.
