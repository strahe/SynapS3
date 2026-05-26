---
title: Write Path and Cache
description: Learn how SynapS3 accepts S3 writes, persists bytes locally, and uses cache during reads.
---

# Write Path and Cache

SynapS3 uses a cache-first durability model. A successful S3 write means object bytes are durable on local disk and metadata is committed to the database.

## PutObject Flow

```text
Client PUT /bucket/key
  -> SynapseBackend.PutObject
  -> cache.Put(bucket, key, body)
  -> repositories transaction
  -> return 200 OK with ETag
```

The cache write:

- writes to a temporary file,
- computes MD5 ETag and SHA-256 checksum,
- fsyncs the file,
- atomically renames the file into place,
- fsyncs the parent directory.

The database transaction upserts the object, bumps its generation, and creates an upload task.

## Durability Invariant

::: tip
SynapS3 returns success only after both local cache persistence and database commit succeed.
:::

This keeps the S3 response independent from Filecoin provider latency. Provider upload happens after the write is accepted.

## Read Path

`GetObject` reads local cache first. If the cache entry is missing and committed provider metadata is available, SynapS3 can retrieve the object from provider storage, verify the checksum, serve the response, and best-effort rehydrate cache.

## Multipart Uploads

Multipart uploads stage parts in the cache. Completion validates requested parts, computes the S3 multipart ETag, assembles the final object, commits metadata, and then cleans up the upload staging directory.

## Operational Impact

| Condition | Meaning |
| --- | --- |
| Cache disk is full | New writes can fail before Filecoin storage is involved. |
| Upload worker is down | Confirmed writes remain local, but remote storage will not progress. |
| Cache entry is evicted | Reads can still succeed when provider metadata and retrieval are available. |
| Database commit fails | The S3 write does not return success. |

For capacity and recovery steps, see [Runtime Data](../configuration/runtime-data.md) and [Troubleshooting](../operations/troubleshooting.md).
