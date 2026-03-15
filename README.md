# SynapS3

Industrial-grade S3-compatible gateway for Filecoin. Upload data via standard S3 APIs; SynapS3 handles local caching, asynchronous upload to Storage Providers, and on-chain Proof-of-Data-Possession (PDP) proof set management.

## Architecture Overview

```
                         ┌────────────┐
                         │  S3 Client │
                         └─────┬──────┘
                               │ S3 API (HTTP)
                         ┌─────▼──────┐
                         │ VersityGW  │   S3-compatible HTTP server
                         └─────┬──────┘
                               │ backend.Backend interface
                    ┌──────────▼──────────┐
                    │   SynapseBackend    │
                    │  (internal/backend) │
                    └──┬──────┬───────┬───┘
                       │      │       │
              ┌────────▼┐ ┌──▼────┐ ┌▼──────────┐
              │  Cache   │ │  DB   │ │  State    │
              │  (disk)  │ │ (Bun) │ │  Machine  │
              └──────────┘ └──┬────┘ └───────────┘
                              │
               ┌──────────────▼──────────────────┐
               │       Background Workers        │
               │  ┌──────────┐  ┌─────────────┐  │
               │  │ Uploader │  │  OnChain    │  │
               │  │ (→ SP)   │  │  (→ chain)  │  │
               │  ├──────────┤  ├─────────────┤  │
               │  │ Evictor  │  │  ProofSet   │  │
               │  │ (cleanup)│  │  (lifecycle)│  │
               │  └──────────┘  └─────────────┘  │
               └──────────────┬──────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │   go-synapse SDK  │
                    └────────┬──────────┘
                             │
                  ┌──────────▼──────────┐
                  │  Filecoin SP + Chain │
                  └─────────────────────┘
```

**Core dependencies:**

| Component | Role |
|-----------|------|
| [VersityGW](https://github.com/versity/versitygw) | S3-compatible HTTP server and request routing |
| [go-synapse](https://github.com/data-preservation-programs/go-synapse) | Filecoin PDP SDK for SP upload and on-chain proofs |
| [Bun ORM](https://bun.uptrace.dev) | Database layer (PostgreSQL or SQLite) |

## Getting Started

### Prerequisites

- **Go 1.24+**
- **PostgreSQL** or **SQLite** (SQLite works out of the box for development)
- **golangci-lint** (optional, for linting)

### Build and Run

```bash
# Clone and build
git clone https://github.com/strahe/synaps3.git
cd synaps3
make build

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml — set database DSN, S3 credentials, Filecoin keys

# Run
./bin/synaps3 serve --config config.yaml
```

### Docker

```bash
docker build -t synaps3 .
docker run -p 8080:8080 \
  -v /path/to/config.yaml:/etc/synaps3/config.yaml \
  -v /path/to/cache:/var/lib/synaps3/cache \
  synaps3
```

## Configuration

All configuration lives in a YAML file (see [`config.example.yaml`](config.example.yaml)). Every value can be overridden with environment variables using the `SYNAPS3_` prefix and underscore-to-dot mapping:

```
SYNAPS3_DATABASE_DSN      → database.dsn
SYNAPS3_SERVER_PORT       → server.port
SYNAPS3_FILECOIN_RPC_URL  → filecoin.rpc_url
```

### Key Sections

| Section | Key Fields | Description |
|---------|------------|-------------|
| `database` | `driver` (`postgres` / `sqlite`), `dsn`, `max_open_conns` | Database connection. SQLite uses WAL mode by default. |
| `cache` | `dir`, `max_size_gb`, `eviction_policy`, `evict_after_onchain` | Local disk cache for object data. |
| `s3` | `access_key`, `secret_key`, `region` | S3 authentication credentials. |
| `server` | `port`, `tls.enabled`, `tls.cert_file`, `tls.key_file` | HTTP server binding and optional TLS. |
| `filecoin` | `network`, `rpc_url`, `private_key`, `provider_url` | Filecoin network, RPC endpoint, and SP connection. |
| `worker.upload` | `concurrency`, `poll_interval`, `max_retries` | Uploader worker tuning. |
| `worker.onchain` | `concurrency`, `poll_interval`, `max_retries` | OnChain worker tuning. |
| `worker.proofset` | `concurrency`, `poll_interval`, `max_retries` | ProofSet lifecycle worker tuning. |
| `worker.evictor` | `concurrency`, `poll_interval`, `max_retries` | Cache evictor worker tuning. |
| `logging` | `level` (`debug`/`info`/`warn`/`error`), `format` (`json`/`text`) | Log output configuration. |

### Example Environment Overrides

```bash
export SYNAPS3_DATABASE_DRIVER=postgres
export SYNAPS3_DATABASE_DSN="postgres://user:pass@localhost:5432/synaps3?sslmode=disable"
export SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
export SYNAPS3_SERVER_PORT=:9090
```

## Storage Pipeline

Objects flow through an asynchronous pipeline from S3 ingestion to Filecoin on-chain proof:

```
PutObject             Uploader           OnChain            Evictor
   │                    │                  │                  │
   ▼                    ▼                  ▼                  ▼
Write to cache    Upload to SP      Add roots to       Delete from
+ DB commit       → get PieceCID    ProofSet on-chain  local cache
   │                    │                  │                  │
   ▼                    ▼                  ▼                  ▼
 [cached]         [uploading→uploaded] [onchaining→onchained] [cache_evicted]
```

**Step by step:**

1. **PutObject** — the object body is written to the local filesystem cache (with fsync durability), then metadata and an upload task are committed atomically to the database. Success is returned to the client only after both succeed.

2. **Uploader worker** — claims pending `upload_to_sp` tasks, uploads the cached file to the Storage Provider via go-synapse, records the resulting PieceCID, and enqueues an `add_roots` task.

3. **OnChain worker** — claims `add_roots` tasks, submits the data root to the bucket's ProofSet contract on Filecoin, and enqueues an `evict_cache` task.

4. **Evictor worker** — claims `evict_cache` tasks and removes the local cache entry after confirming on-chain storage.

5. **Cold reads** — when a `GetObject` request arrives for an object not in local cache, SynapS3 downloads it from the Storage Provider using the stored PieceCID, verifies the SHA-256 checksum, and serves it to the client.

**Bucket lifecycle** is also managed asynchronously: `CreateBucket` enqueues a `create_proof_set` task (handled by the ProofSet worker), and `DeleteBucket` enqueues `delete_proof_set` for on-chain cleanup.

## S3 Compatibility

SynapS3 implements the following S3 operations:

### Bucket Operations

| Operation | Description |
|-----------|-------------|
| `CreateBucket` | Creates a bucket and initiates async ProofSet creation on Filecoin |
| `HeadBucket` | Returns bucket metadata |
| `DeleteBucket` | Initiates async ProofSet deletion and marks bucket for removal |
| `ListBuckets` | Lists all active buckets |

### Object Operations

| Operation | Description |
|-----------|-------------|
| `PutObject` | Writes object to local cache and enqueues SP upload |
| `GetObject` | Serves from cache; falls back to SP download on cache miss |
| `HeadObject` | Returns object metadata without body |
| `DeleteObject` | Soft-deletes the object |
| `DeleteObjects` | Batch soft-delete (multi-object delete) |
| `CopyObject` | Copies an object within or across buckets |
| `ListObjectsV1` | Lists objects with marker-based pagination |
| `ListObjectsV2` | Lists objects with continuation-token pagination |

### Multipart Upload Operations

| Operation | Description |
|-----------|-------------|
| `CreateMultipartUpload` | Initiates a multipart upload, returns an UploadID |
| `UploadPart` | Uploads a single part to the staging area |
| `UploadPartCopy` | Copies data from an existing object as a part |
| `CompleteMultipartUpload` | Assembles parts into a final object and enqueues SP upload |
| `AbortMultipartUpload` | Cancels the upload and cleans up staged parts |
| `ListMultipartUploads` | Lists in-progress multipart uploads for a bucket |
| `ListParts` | Lists uploaded parts for a given UploadID |

## Development

### Build Commands

```bash
make build    # Build binary to ./bin/synaps3
make test     # Run all tests with race detector
make lint     # Run golangci-lint
make fmt      # Format code (gofmt + goimports)
make run      # Build and run with config.example.yaml
make clean    # Remove build artifacts
make migrate  # Run database migrations only
```

### Running Tests

```bash
# Full test suite
go test -race -count=1 ./...

# Single package
go test ./internal/db/repository -count=1

# Single test
go test ./internal/db/repository -run '^TestObjectRepo_UpsertAndBumpGeneration_Overwrite$' -count=1
```

### Project Structure

```
cmd/synaps3/           CLI entrypoint (serve, migrate, version subcommands)
internal/
  backend/             SynapseBackend — VersityGW Backend implementation
  cache/               Filesystem cache with durability guarantees
  config/              Configuration loading (YAML + env overlay)
  db/                  Database bootstrap and migrations
  db/repository/       Repository interfaces and implementations
  model/               Domain models (Bucket, Object, Task, Multipart)
  state/               Finite state machine for object lifecycle
  synapse/             Thin wrappers around go-synapse SDK
  worker/              Background workers (Uploader, OnChain, Evictor, ProofSet)
  buildinfo/           Version injection via ldflags
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
