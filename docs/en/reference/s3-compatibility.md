---
title: S3 Compatibility
description: Supported SynapS3 bucket, object, versioning, and multipart S3 operations and limitations.
---

# S3 Compatibility

SynapS3 mainly supports path-style S3 access for writing bucket and object data to Filecoin.

## Client Requirements

- Use path-style addressing.
- For local evaluation, point clients at `http://localhost:8080`. Production S3 traffic must use native TLS or a controlled TLS reverse proxy.
- Use credentials created by `synaps3 admin s3-user create`.
- Keep objects and object keys within the stable limits below.

## Stable Limits

- Object size: `127` through `1,065,353,216` bytes.
- Object keys: valid UTF-8, no NUL, and at most `1024` bytes.
- Multipart uploads: at most `10,000` parts; the completed object remains subject to the object size limit.

## Operation Matrix

| Area | Operation | Status | Notes |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | Supported | Creates a bucket. |
| Bucket | `HeadBucket` | Supported | Checks bucket metadata. |
| Bucket | `ListBuckets` | Supported | Lists active buckets. |
| Bucket | `DeleteBucket` | Not supported | Returns `501`; bucket deletion is not part of the SynapS3 lifecycle. |
| Bucket | `GetBucketVersioning` | Supported | Buckets are always versioning-enabled. |
| Bucket | `PutBucketVersioning` | Partial | Accepts `Enabled`; rejects `Suspended`. |
| Bucket ACL | `GetBucketAcl` | Supported | Returns the persisted bucket ACL. |
| Bucket ACL | `PutBucketAcl` | Supported | Persists bucket ownership and ACL changes used for access control. |
| Object ACL | `GetObjectAcl`, `PutObjectAcl` | Not supported | Do not rely on object-level ACL state or authorization changes. |
| Bucket policy | `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy` | Not supported | Bucket policies are not stored or enforced. |
| Bucket tagging | `GetBucketTagging`, `PutBucketTagging`, `DeleteBucketTagging` | Not supported | Bucket tags are not stored. |
| Object tagging | `GetObjectTagging`, `PutObjectTagging`, `DeleteObjectTagging` | Not supported | Object tags are not stored. |
| Ownership controls | `GetBucketOwnershipControls` | Partial | Reports `BucketOwnerPreferred`. |
| Ownership controls | `PutBucketOwnershipControls` | Partial | Accepts only `BucketOwnerPreferred`; rejects other ownership modes. |
| Ownership controls | `DeleteBucketOwnershipControls` | Partial | Keeps the ACL-compatible `BucketOwnerPreferred` behavior. |
| Object | `PutObject` | Supported | Stores an object through the cache-first write model. |
| Object | `GetObject` | Supported | Reads from cache or committed remote storage. |
| Object | `HeadObject` | Supported | Reads object metadata. |
| Object | `DeleteObject` | Supported | Creates a delete marker, or deletes a specific `versionId`. |
| Object | `DeleteObjects` | Supported | Creates delete markers, or deletes specific `versionId` entries. |
| Object | `CopyObject` | Supported | Source object must be readable from cache or committed remote storage. |
| Object | `ListObjects` | Supported | Marker pagination. |
| Object | `ListObjectsV2` | Supported | Continuation-token pagination. |
| Object | `ListObjectVersions` | Supported | Lists object versions and delete markers. |
| Object | `GetObjectAttributes` | Partial | Reports metadata and multipart `ObjectParts`, but does not include `TotalPartsCount`. |
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
- Object ACL authorization, bucket policies, and bucket or object tagging.
- Ownership modes other than `BucketOwnerPreferred`.
- `TotalPartsCount` in `GetObjectAttributes.ObjectParts`.
- Multipart range copy for `UploadPartCopy`.
- Distributed coordination across multiple SynapS3 nodes.

## Verification

Use [S3 Clients](../getting-started/s3-clients.md) for AWS CLI, rclone, and MinIO Client examples.
