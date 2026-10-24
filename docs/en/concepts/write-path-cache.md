---
title: Write Path and Cache
description: Learn when SynapS3 reports a write as successful, how reads use the cache, and when cached data can be removed.
---

# Write Path and Cache

SynapS3 returns success for an S3 write only after the object data is stored in the local cache and its metadata is committed to PostgreSQL. If either step fails, the client receives an error. Filecoin storage starts after the response, so write latency does not depend on storage providers.

## Writes

- A write reserves cache capacity for its declared size before SynapS3 reads the body. If the cache cannot hold it, the write fails with `503 SlowDown` and nothing is stored.
- The response carries an S3-compatible ETag; multipart uploads return the S3 multipart ETag.
- Writing bytes that the bucket already holds creates a new version without another Filecoin upload.
- Multipart parts stay in the local cache until the upload is completed or aborted. Completing an upload needs free cache capacity for the assembled object in addition to its parts.

## Reads

`GetObject` reads from the local cache. On a cache miss, SynapS3 reads a committed replica from a storage provider, verifies the complete object before delivering the last bytes, and after a complete read attempts to restore it to the cache unless `cache.eviction_policy` is `after_upload`. A range request on a cache miss still downloads the whole object for verification. A read that the client closes early leaves no partial cache entry.

These reads mark a cached version as recently used for `lru` eviction: S3 reads, including range reads; `CopyObject` sources read from the cache; dashboard downloads; and version restores. `HeadObject` and background uploads do not.

## Cache Eviction

SynapS3 removes a version from the cache only when all of these hold:

- The version has reached its bucket's [minimum durable replicas](./filecoin-storage-flow.md#target-and-minimum-replicas), and those replicas are committed and readable.
- No upload or batch submission in progress still needs the cached data.

Eviction never removes multipart parts, and it waits for open reads of the version to finish. Which eligible versions are removed, and when, depends on `cache.eviction_policy`; see [Cache](../configuration/model.md#cache). Removal runs in the background. When it cannot keep up or no version is eligible, writes fail with `503 SlowDown`; see [Cache Full](../operations/troubleshooting.md#cache-full).

After removal, reads of that version come from a storage provider.
