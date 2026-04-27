# SynapS3

SynapS3 exposes an S3-compatible endpoint backed by Filecoin. Clients use standard S3 APIs, while SynapS3 lands data in local cache first and moves it through provider upload and proof-related workflows in the background.

## Architecture

```text
S3 Client
   |
   v
SynapS3
   |
   +--> Local cache + metadata database
   |
   +--> Storage Provider + Filecoin proof flow
```

Writes land in the local cache and metadata database first, then background workers handle provider upload and the on-chain lifecycle.

## Quick Start

1. Copy the example config and fill in your database, S3, and Filecoin settings.
2. Start SynapS3 with the config file.
3. Point your S3 client at the SynapS3 endpoint.

```bash
cp config.example.yaml config.yaml
go run ./cmd/synaps3 serve --config config.yaml
```

The default S3 endpoint is `http://localhost:8080`.

The built-in dashboard is available at `http://localhost:9090` (admin port) and provides an overview of system health, bucket/object browsing, and task monitoring.

By default, local runtime data is stored under `~/.synaps3/`: SQLite files in `db/` and cached objects in `cache/`. Set `database.dsn` or `cache.dir` if you need explicit paths.

Start with [`config.example.yaml`](config.example.yaml) and see [`docs/configuration.md`](docs/configuration.md) for the main settings.

## S3 API Compatibility

### Bucket Operations

| Operation | Status | Notes |
| --- | --- | --- |
| `CreateBucket` | Supported | Creates an active bucket and cache namespace |
| `HeadBucket` | Supported | Returns bucket metadata |
| `DeleteBucket` | Not supported | Bucket deletion is intentionally disabled in the current lifecycle |
| `ListBuckets` | Supported | Lists active buckets |

### Object Operations

| Operation | Status | Notes |
| --- | --- | --- |
| `PutObject` | Supported | Writes to local cache first, then enqueues provider upload |
| `GetObject` | Supported | Serves from cache and can fall back on eligible cache misses when provider metadata is available and size limits allow |
| `HeadObject` | Supported | Returns object metadata without body |
| `DeleteObject` | Supported | Soft-deletes the object |
| `DeleteObjects` | Supported | Batch soft-delete |
| `CopyObject` | Supported | Copies within or across buckets while the source object is still available in local cache |
| `ListObjects` | Supported | Marker-based pagination |
| `ListObjectsV2` | Supported | Continuation-token pagination |

### Multipart Upload Operations

| Operation | Status | Notes |
| --- | --- | --- |
| `CreateMultipartUpload` | Supported | Starts a multipart upload |
| `UploadPart` | Supported | Uploads a single part |
| `UploadPartCopy` | Supported | Copies a full existing object from local cache into a part (`CopySourceRange` is not yet supported) |
| `CompleteMultipartUpload` | Supported | Assembles parts into the final object and enqueues upload |
| `AbortMultipartUpload` | Supported | Cancels the upload and cleans up staged parts |
| `ListMultipartUploads` | Supported | Lists in-progress multipart uploads |
| `ListParts` | Supported | Lists uploaded parts for an upload ID |

## Documentation

- [Configuration](docs/configuration.md)
- [Operations](docs/operations.md)
- [Development](docs/development.md)

## License

See [LICENSE](LICENSE).
