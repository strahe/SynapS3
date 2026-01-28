# SynapS3

Industrial-grade Filecoin on-chain S3 gateway. Upload data via standard S3 APIs; SynapS3 handles local caching, async upload to Storage Providers, and on-chain Proof-of-Data-Possession (PDP) proof set management.

## Architecture

```
S3 Client → VersityGW (S3 HTTP) → SynapS3 Backend → Local Cache + DB → Background Workers → go-synapse → Filecoin
```

**Core components:**

| Component | Role |
|-----------|------|
| [VersityGW](https://github.com/versity/versitygw) | S3-compatible HTTP server |
| [go-synapse](https://github.com/data-preservation-programs/go-synapse) | Filecoin PDP SDK |
| [Bun ORM](https://bun.uptrace.dev) | Database layer (PostgreSQL / SQLite) |

## Quick Start

```bash
# Clone and build
git clone https://github.com/strahe/synaps3.git
cd synaps3
make build

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml with your Filecoin credentials

# Run
./bin/synaps3 --config config.yaml
```

## Configuration

See [`config.example.yaml`](config.example.yaml) for all options. Environment variables override file config with prefix `SYNAPS3_`:

```bash
SYNAPS3_SERVER_PORT=:9090
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
SYNAPS3_DATABASE_DRIVER=postgres
SYNAPS3_DATABASE_DSN="postgres://user:pass@localhost:5432/synaps3?sslmode=disable"
```

## Data Flow

1. **PutObject** → write to local cache (fsync) + atomic DB commit → return 200 OK
2. **Upload Worker** → async upload to Storage Provider via go-synapse
3. **OnChain Worker** → create/update ProofSet on Filecoin chain
4. **Evictor** → clean up local cache after successful remote storage

## Development

```bash
make build    # Build binary
make test     # Run tests
make lint     # Run linter
make fmt      # Format code
make run      # Build and run with example config
```

## License

See [LICENSE](LICENSE).
