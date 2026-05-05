# SynapS3

SynapS3 lets S3 clients use Filecoin storage.

## Why SynapS3

- Use existing S3 clients, SDKs, and tools.
- Store object data through Filecoin providers.
- Manage buckets, objects, settings, tasks, and health from one dashboard.

## Quick Start

```bash
synaps3 init
```

Set `filecoin.private_key` in `~/.synaps3/config.toml`:

```toml
[filecoin]
private_key = "0x..."
```

Start SynapS3:

```bash
synaps3 serve
```

Default endpoints:

- S3 API: `http://localhost:8080`
- Dashboard and admin API: `http://localhost:9090`
- Runtime data: `~/.synaps3/`

Open the dashboard, create an S3 user, then configure your S3 client with the generated keys. Secrets are shown only when created or rotated.

Example with AWS CLI:

```bash
export AWS_ACCESS_KEY_ID='replace-with-access-key'
export AWS_SECRET_ACCESS_KEY='replace-with-secret-key'
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://localhost:8080 s3api create-bucket --bucket demo
aws --endpoint-url http://localhost:8080 s3api put-object --bucket demo --key hello.txt --body ./hello.txt
aws --endpoint-url http://localhost:8080 s3api get-object --bucket demo --key hello.txt ./hello.out
```

## Documentation

- [Configuration](docs/configuration.md)
- [Operations](docs/operations.md)

## S3 Compatibility

| Area | Operation | Status | Notes |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | ✅ | Creates a bucket |
| Bucket | `HeadBucket` | ✅ | Checks bucket metadata |
| Bucket | `ListBuckets` | ✅ | Lists active buckets |
| Bucket | `DeleteBucket` | ❌ | Bucket deletion is not part of the current lifecycle |
| Object | `PutObject` | ✅ | Stores an object |
| Object | `GetObject` | ✅ | Reads an object |
| Object | `HeadObject` | ✅ | Reads object metadata |
| Object | `DeleteObject` | ✅ | Soft-deletes one object |
| Object | `DeleteObjects` | ✅ | Soft-deletes multiple objects |
| Object | `CopyObject` | ✅ | Source object must be in local cache |
| Object | `ListObjects` | ✅ | Marker pagination |
| Object | `ListObjectsV2` | ✅ | Continuation-token pagination |
| Multipart | `CreateMultipartUpload` | ✅ | Starts an upload |
| Multipart | `UploadPart` | ✅ | Uploads one part |
| Multipart | `UploadPartCopy` | ⚠️ | Whole-object copy only; range copy is not supported |
| Multipart | `CompleteMultipartUpload` | ✅ | Assembles parts |
| Multipart | `AbortMultipartUpload` | ✅ | Cancels an upload |
| Multipart | `ListMultipartUploads` | ✅ | Lists open uploads |
| Multipart | `ListParts` | ✅ | Lists uploaded parts |

## License

See [LICENSE](LICENSE).
