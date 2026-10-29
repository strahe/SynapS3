---
title: S3 Compatibility
description: Look up which S3 operations SynapS3 supports, its object limits, and how versioning and deletion behave.
---

# S3 Compatibility

## Client Requirements

- Use path-style addressing.
- Use credentials created on the **S3 users** page or with `synaps3 admin s3-user create`.
- Use TLS for production traffic, natively or through your own proxy.

## Stable Limits

- Object size: 127 to 1,065,353,216 bytes. Smaller and larger objects are rejected with `EntityTooSmall` or `EntityTooLarge`.
- Object keys: valid UTF-8, no NUL character, at most 1,024 bytes.
- Multipart uploads: at most 10,000 parts. The completed object must also be within the object size limit.

## Operation Matrix

| Area | Operation | Status | Notes |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | Supported | |
| Bucket | `HeadBucket` | Supported | |
| Bucket | `ListBuckets` | Supported | |
| Bucket | `DeleteBucket` | Not supported | Returns `501 NotImplemented`. |
| Bucket | `GetBucketVersioning` | Supported | Buckets are always versioning-enabled. |
| Bucket | `PutBucketVersioning` | Partial | Accepts `Enabled`; rejects `Suspended`. |
| Bucket ACL | `GetBucketAcl` | Supported | |
| Bucket ACL | `PutBucketAcl` | Supported | Grants are enforced; see [Access Control](#access-control). |
| Object ACL | `GetObjectAcl`, `PutObjectAcl` | Not supported | |
| Bucket policy | `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy` | Not supported | Policies are not stored or enforced. |
| Bucket tagging | `GetBucketTagging`, `PutBucketTagging`, `DeleteBucketTagging` | Not supported | |
| Object tagging | `GetObjectTagging`, `PutObjectTagging`, `DeleteObjectTagging` | Not supported | |
| Ownership controls | `GetBucketOwnershipControls` | Partial | Reports `BucketOwnerPreferred`. |
| Ownership controls | `PutBucketOwnershipControls` | Partial | Accepts only `BucketOwnerPreferred`. |
| Ownership controls | `DeleteBucketOwnershipControls` | Partial | The bucket keeps `BucketOwnerPreferred`. |
| Object | `PutObject` | Supported | |
| Object | `GetObject` | Supported | Supports a single byte range. A range read of an object that is not cached downloads the whole object from the storage provider. |
| Object | `HeadObject` | Supported | Returns custom metadata of the requested version. |
| Object | `DeleteObject` | Supported | See [Versioning and Deletion](#versioning-and-deletion). |
| Object | `DeleteObjects` | Supported | 1 to 1,000 keys. Reports failures per key; `Quiet=true` omits successful keys. |
| Object | `CopyObject` | Supported | The source must be in the cache or have a committed replica. |
| Object | `ListObjects` | Supported | Marker pagination. |
| Object | `ListObjectsV2` | Supported | Continuation-token pagination. |
| Object | `ListObjectVersions` | Supported | Lists versions and delete markers. |
| Object | `GetObjectAttributes` | Partial | For multipart objects, omits `TotalPartsCount` from `ObjectParts` and the overall `Checksum`. |
| Multipart | `CreateMultipartUpload` | Supported | |
| Multipart | `UploadPart` | Supported | |
| Multipart | `UploadPartCopy` | Partial | Whole-object copy only; a source range returns `NotImplemented`. |
| Multipart | `CompleteMultipartUpload` | Supported | |
| Multipart | `AbortMultipartUpload` | Supported | |
| Multipart | `ListMultipartUploads` | Supported | |
| Multipart | `ListParts` | Supported | |

Server-side encryption is not supported; requests with customer-provided keys (SSE-C) return `501 NotImplemented`.

## Access Control

Bucket ACL grants allow other S3 users to read, write, and list according to the granted permission. Even `FULL_CONTROL` does not allow `GetObjectAttributes`, deleting a specific version, `ListParts`, `AbortMultipartUpload`, or changing bucket configuration; these need the bucket owner or an S3 user with the `admin` role. A user with write permission can create and complete multipart uploads, but needs the owner or an admin to list parts or abort them.

## Versioning and Deletion

Buckets always keep versions.

- `DeleteObject` without `versionId` adds a delete marker.
- `DeleteObject` with `versionId` deletes that version or delete marker immediately, or fails; it is never queued.
- Deleting a data version fails with `400 InvalidRequest` while its storage work is active or a submitted Filecoin transaction awaits confirmation. Failed storage work that never submitted a transaction does not block it, and storage work never blocks deleting a delete marker. In `DeleteObjects`, the error applies only to that key.
- In `DeleteObjects`, a key that resolves outside the bucket, such as `../file.txt`, rejects the whole request before anything is deleted.

After a data version is deleted, SynapS3 removes the remote replicas that no other version uses, and asks each storage provider to delete its copy. A storage provider that does not support deletion keeps its copy. Until the removal finishes, usually within minutes, uploading the same bytes to that bucket returns `503 SlowDown`; a later upload stores them as new data.

Client setup examples are in [S3 Clients](../getting-started/s3-clients.md).
