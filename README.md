# SynapS3

SynapS3 lets S3 clients use Filecoin storage.

> SynapS3 is a developer preview and is not ready for production use. Test with Filecoin Calibration first, and feedback is welcome.

## Why SynapS3

- Use existing S3 clients, SDKs, and tools.
- Store object data through Filecoin providers.
- Manage buckets, objects, settings, tasks, and health from one dashboard.

## Core Features

| Feature | Status | Note |
| --- | --- | --- |
| S3-compatible API | ✅ | Works with standard S3 clients and tools |
| Bucket and object operations | ✅ | Create buckets; upload, list, read, and delete objects |
| Multipart uploads | ✅ | S3 multipart flow for large objects |
| Object versioning | ✅ | Version IDs, current versions, and delete markers |
| Web dashboard | ✅ | Buckets, objects, tasks, settings, and health views |
| S3 user management | ✅ | Access keys for S3 client authentication |
| Filecoin storage backend | ✅ | Stores object data through Synapse providers |
| Automatic provider selection | ✅ | Selects provider contexts through Synapse |
| Configurable storage copies | ✅ | Global and per-bucket copy targets |
| Provider-backed reads | ✅ | Reads from cache first, then provider storage |
| Wallet and payment tools | ✅ | Wallet setup, Calibration funding, and USDFC deposit |
| Background task management | ✅ | Task monitoring, retry, and recovery controls |
| Managed provider policy | 📝 | Provider allow/deny and placement controls |
| Automatic repair | 📝 | Background replica reconciliation |
| One-click deployment | 📝 | Packaged deployment automation |
| Production readiness | 📝 | Security and operations hardening |

## Quick Start

This Quick Start uses `docker run` for quick evaluation. For Docker Compose deployment, use the [Docker deployment guide](docs/deployment/docker.md). To compile locally, use the [source build guide](docs/deployment/source.md).

Prerequisites:

- [Docker Engine](https://docs.docker.com/engine/install/) or [Docker Desktop](https://docs.docker.com/get-started/get-docker/)
- [Host networking](https://docs.docker.com/engine/network/tutorials/host/) enabled for Docker Desktop

The commands use Docker host networking so the admin server can stay bound to `127.0.0.1`.

```bash
cp .env.example .env
docker run --rm ghcr.io/strahe/synaps3:edge synaps3 wallet generate
```

Copy the generated private key into `.env`, then fund the generated address on Calibration:

```bash
docker run --rm --env-file .env ghcr.io/strahe/synaps3:edge synaps3 wallet fund-testnet 0x...
```

Start a temporary service:

```bash
docker volume create synaps3-test-data
docker run -d --name synaps3-test \
  --network host \
  --env-file .env \
  -v synaps3-test-data:/var/lib/synaps3 \
  ghcr.io/strahe/synaps3:edge
```

Check health and deposit USDFC:

```bash
curl http://127.0.0.1:9090/healthz
docker exec synaps3-test synaps3 --config /var/lib/synaps3/config.toml wallet deposit 2
```

Open the dashboard at `http://127.0.0.1:9090` and upload a file. If the host is remote, use an SSH tunnel:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

Clean up the testing container when done:

```bash
docker rm -f synaps3-test
docker volume rm synaps3-test-data
```

## Documentation

- [Docker deployment](docs/deployment/docker.md)
- [Build from source](docs/deployment/source.md)
- [Configuration](docs/configuration.md)
- [Operations](docs/operations.md)

## Core S3 Compatibility

| Area | Operation | Status | Notes |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | ✅ | Creates a bucket |
| Bucket | `HeadBucket` | ✅ | Checks bucket metadata |
| Bucket | `ListBuckets` | ✅ | Lists active buckets |
| Bucket | `DeleteBucket` | ❌ | Bucket deletion is not part of the current lifecycle |
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
