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

Prerequisites:

- Go 1.26.3 or later
- C toolchain for cgo, such as gcc or clang
- Node.js 22.12 or later
- pnpm 11, installed directly or managed by [Corepack](https://github.com/nodejs/corepack)
- [rc](https://github.com/rustfs/cli) or [mc](https://github.com/minio/mc) for S3 testing

If pnpm 11 is not already installed, enable [Corepack](https://github.com/nodejs/corepack) before building:

```bash
corepack enable
```

If `corepack` is missing, install it first with `npm install --global corepack@latest`.

Clone and build SynapS3 with the embedded dashboard:

```bash
git clone https://github.com/strahe/SynapS3.git
cd SynapS3
make build
```

Initialize the local app data directory:

```bash
./bin/synaps3 init
```

Generate a wallet if you do not already have one:

```bash
./bin/synaps3 wallet generate
```

Set `filecoin.private_key` in `~/.synaps3/config.toml`:

```toml
[filecoin]
private_key = "0x..."
```

For Calibration testing, fund the wallet with testnet tFIL and USDFC:

```bash
# Use the generated wallet address, not the private key.
./bin/synaps3 wallet fund-testnet 0x...
```

Start SynapS3:

```bash
./bin/synaps3 serve
```

Default endpoints:

- S3 API: `http://localhost:8080`
- Dashboard and admin API: `http://localhost:9090`
- Runtime data: `~/.synaps3/`

Do not expose the dashboard and admin API directly to an untrusted network.

Deposit USDFC into the payment account before uploading. This command signs with the Filecoin private key from your SynapS3 config:

```bash
./bin/synaps3 wallet deposit 2
```

In another terminal, create an S3 user from the dashboard or CLI, then configure your S3 client with the generated keys. Secrets are shown only when created or rotated.

```bash
./bin/synaps3 admin s3-user create
```

Example with rc:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
rc alias set synaps3 http://localhost:8080 'replace-with-access-key' 'replace-with-secret-key'
rc mb synaps3/demo
rc cp ./hello.txt synaps3/demo/hello.txt
rc cat synaps3/demo/hello.txt
```

The sample file is padded because FOC uploads require at least 127 bytes. Any path-style S3 client can be used.

You can also upload files from the dashboard.

Check health and task recovery:

```bash
./bin/synaps3 admin status
curl http://localhost:9090/healthz
./bin/synaps3 admin task list --status exhausted --limit 100
```

## Documentation

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
