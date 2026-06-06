---
title: S3 Compatibility
description: Supported SynapS3 bucket, object, versioning, and multipart S3 operations and limitations.
---

# S3 Compatibility

SynapS3 mainly supports path-style S3 access for writing bucket and object data to Filecoin.

## Client Requirements

- Use path-style addressing.
- Point clients at the SynapS3 S3 endpoint, usually `http://localhost:8080`.
- Use credentials created by `synaps3 admin s3-user create`.
- Use objects of at least 127 bytes when testing the Filecoin storage path.

## Operation Matrix

| Area | Operation | Status | Notes |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | Supported | Creates a bucket. |
| Bucket | `HeadBucket` | Supported | Checks bucket metadata. |
| Bucket | `ListBuckets` | Supported | Lists active buckets. |
| Bucket | `DeleteBucket` | Not supported | Bucket deletion is not part of the SynapS3 lifecycle. |
| Bucket | `GetBucketVersioning` | Supported | Buckets are always versioning-enabled. |
| Bucket | `PutBucketVersioning` | Partial | Accepts `Enabled`; rejects `Suspended`. |
| Object | `PutObject` | Supported | Stores an object through the cache-first write model. |
| Object | `GetObject` | Supported | Reads from cache or committed remote storage. |
| Object | `HeadObject` | Supported | Reads object metadata. |
| Object | `DeleteObject` | Supported | Creates a delete marker, or deletes a specific `versionId`. |
| Object | `DeleteObjects` | Supported | Creates delete markers, or deletes specific `versionId` entries. |
| Object | `CopyObject` | Supported | Source object must be readable from cache or committed remote storage. |
| Object | `ListObjects` | Supported | Marker pagination. |
| Object | `ListObjectsV2` | Supported | Continuation-token pagination. |
| Object | `ListObjectVersions` | Supported | Lists object versions and delete markers. |
| Object | `GetObjectAttributes` | Supported | Reports metadata and multipart `ObjectParts`; `TotalPartsCount` is not emitted. |
| Multipart | `CreateMultipartUpload` | Supported | Starts an upload. |
| Multipart | `UploadPart` | Supported | Uploads one part. |
| Multipart | `UploadPartCopy` | Partial | Whole-object copy only; range copy is not supported. |
| Multipart | `CompleteMultipartUpload` | Supported | Assembles parts. |
| Multipart | `AbortMultipartUpload` | Supported | Cancels an upload. |
| Multipart | `ListMultipartUploads` | Supported | Lists open uploads. |
| Multipart | `ListParts` | Supported | Lists uploaded parts. |

## Versioning Behavior

Buckets behave as versioning-enabled. A normal object delete creates a delete marker. A delete request with `versionId` deletes that specific version. Version listing returns object versions and delete markers.

## What Is Intentionally Out of Scope

- Bucket deletion.
- Suspending bucket versioning.
- `TotalPartsCount` in `GetObjectAttributes.ObjectParts`; the current VersityGW response type has no field for it.
- Multipart range copy for `UploadPartCopy`.
- Distributed coordination across multiple SynapS3 nodes.

## Verification

Use [S3 Clients](../getting-started/s3-clients.md) for AWS CLI, rclone, and MinIO Client examples.
