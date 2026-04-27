# Development

## Prerequisites

- Go 1.26.1 or later
- `goimports` if you want to run `make fmt`
- `golangci-lint` if you want to run `make lint`

Dependency source clones are optional reference material only and are not required for builds.

## Common Commands

```bash
make build    # Build binary to ./bin/synaps3
make test     # Run the full test suite
make lint     # Run golangci-lint
make fmt      # Format code (requires goimports)
make run      # Build and run with config.example.yaml
make clean    # Remove build artifacts
make migrate  # Build binary and run database migrations with config.example.yaml
```

`config.example.yaml` leaves the default SQLite and cache paths unset, so `make run` and `make migrate` use `~/.synaps3/db/` and `~/.synaps3/cache/` unless you explicitly configure different paths.

## Running Tests

```bash
go test -race -count=1 ./...
go test ./internal/db/repository -count=1
go test ./internal/db/repository -run '^TestObjectRepo_UpsertAndBumpGeneration_Overwrite$' -count=1
```

## Project Structure

```text
cmd/synaps3/           CLI entrypoint
internal/backend/      S3 backend implementation
internal/cache/        Filesystem cache
internal/config/       Config loading and validation
internal/db/           Database bootstrap and migrations
internal/db/repository/Repository interfaces and implementations
internal/model/        Domain models
internal/state/        Object lifecycle state machine
internal/synapse/      synapse-go SDK boundary
internal/worker/       Background workers
internal/buildinfo/    Version metadata
```
