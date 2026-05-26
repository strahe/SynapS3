![SynapS3 dashboard](docs/assets/readme-dashboard.png)

# SynapS3

[![CI](https://github.com/strahe/SynapS3/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/strahe/SynapS3/actions/workflows/ci.yml)
[![Package](https://img.shields.io/badge/package-GHCR-blue?logo=github)](https://github.com/strahe/SynapS3/pkgs/container/synaps3)
[![Go Report](https://goreportcard.com/badge/github.com/strahe/synaps3)](https://goreportcard.com/report/github.com/strahe/synaps3)
[![Go Version](https://img.shields.io/github/go-mod/go-version/strahe/SynapS3?filename=go.mod)](go.mod)

SynapS3 is an S3-compatible gateway for storing objects on Filecoin.

## Documentation

- [Documentation](https://synaps3.strahe.com/)
- [中文文档](https://synaps3.strahe.com/zh/)

## Highlights

- S3-compatible bucket and object APIs.
- Object storage backed by Filecoin storage providers.
- Web dashboard for buckets, objects, wallet, tasks, topology, settings, and health.
- Multipart uploads for large objects.
- Wallet funding, USDFC deposit, and background task controls.

## Core S3 Compatibility

| Area | Operation | Status | Notes |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | ✅ | Creates a bucket |
| Bucket | `HeadBucket` | ✅ | Checks bucket metadata |
| Bucket | `ListBuckets` | ✅ | Lists active buckets |
| Bucket | `DeleteBucket` | ❌ | Bucket deletion is not supported |
| Bucket | `GetBucketVersioning` | ✅ | Buckets are always versioning-enabled |
| Bucket | `PutBucketVersioning` | ⚠️ | Accepts `Enabled`; `Suspended` is rejected |
| Object | `PutObject` | ✅ | Stores an object |
| Object | `GetObject` | ✅ | Reads an object |
| Object | `HeadObject` | ✅ | Reads object metadata |
| Object | `DeleteObject` | ✅ | Soft-deletes one object |
| Object | `DeleteObjects` | ✅ | Soft-deletes multiple objects |
| Object | `CopyObject` | ✅ | Source object must be readable from cache or committed provider storage |
| Object | `ListObjects` | ✅ | Marker pagination |
| Object | `ListObjectsV2` | ✅ | Continuation-token pagination |
| Object | `ListObjectVersions` | ✅ | Lists object versions and delete markers |
| Object | `GetObjectAttributes` | ✅ | Reports ETag, checksum, size, and storage class |
| Multipart | `CreateMultipartUpload` | ✅ | Starts an upload |
| Multipart | `UploadPart` | ✅ | Uploads one part |
| Multipart | `UploadPartCopy` | ⚠️ | Whole-object copy only; range copy is not supported |
| Multipart | `CompleteMultipartUpload` | ✅ | Assembles parts |
| Multipart | `AbortMultipartUpload` | ✅ | Cancels an upload |
| Multipart | `ListMultipartUploads` | ✅ | Lists open uploads |
| Multipart | `ListParts` | ✅ | Lists uploaded parts |

## License

See [LICENSE](LICENSE).
