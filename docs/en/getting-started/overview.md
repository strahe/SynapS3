---
title: Overview
description: Learn what SynapS3 does, how a node is put together, and choose an installation path.
---

# Overview

SynapS3 is a self-hosted S3-compatible gateway for Filecoin storage. S3 clients use the S3 API; SynapS3 selects storage providers, creates data sets, and pays for storage from its wallet.

## What SynapS3 Does

- Serves common S3 bucket, object, versioning, and multipart operations. See [S3 Compatibility](../reference/s3-compatibility.md).
- Stores each object as replicas with Filecoin storage providers, and keeps a local cache in front of them.
- Provides a dashboard, Admin API, and CLI for buckets, objects, wallet, tasks, storage providers, settings, and health.

Coming soon: replica repair for storage provider outages. See [Planned Replica Repair](../concepts/filecoin-storage-flow.md#planned-replica-repair).

## Architecture

<img class="architecture-overview architecture-overview--light" src="/architecture-overview-light.svg" alt="SynapS3 architecture">
<img class="architecture-overview architecture-overview--dark" src="/architecture-overview.svg" alt="SynapS3 architecture">

SynapS3 runs as a single process. Run exactly one SynapS3 instance per database; several instances sharing a database or wallet are not supported.

| Component | Address or location | Role |
| --- | --- | --- |
| S3 API | `server.port`, default `:8080` | Serves S3 requests. |
| Dashboard and Admin API | `admin.addr`, default `127.0.0.1:9090` | Serves the dashboard, Admin API, `/healthz`, and `/metrics`. |
| Local cache | `cache.dir` | Holds object data until enough replicas are committed and the eviction policy removes it. |
| PostgreSQL | `database.dsn` | Holds metadata, tasks, and storage records. |
| Background tasks | In process | Upload and replicate objects, submit batches, release cache, clean up deleted data, run wallet operations, and check storage provider health. |
| Filecoin RPC | `filecoin.rpc_url` (outbound) | Reads chain state and sends transactions signed by the wallet. |
| Storage providers | Storage provider service URLs (outbound) | Store, copy, and serve the pieces of each replica. |

Data moves through a node like this:

1. An S3 write is stored in the local cache and committed to PostgreSQL before SynapS3 responds. See [Write Path and Cache](../concepts/write-path-cache.md).
2. Background tasks upload the object to one of the bucket's storage providers and add it to that storage provider's data set on chain. The bucket's other storage providers then copy it from that replica. See [Filecoin Storage Flow](../concepts/filecoin-storage-flow.md).
3. Reads use the local cache, or a committed replica when the cache does not hold the object.
4. Once the bucket's minimum durable replicas are committed, the cache eviction policy can remove the local copy.

The local cache and the database hold data that may not be on Filecoin yet. Keep both on durable storage and back them up together; see [Runtime Data](../configuration/runtime-data.md). For which ports to expose, see [Network Exposure](../operations/production-checklist.md#network-exposure).

## Start Here

1. Install with [Docker Deployment](./docker.md) or [Build from Source](./source.md).
2. Follow [Quick Start](./quick-start.md) to check the node and upload a first object.
3. Configure clients with [S3 Clients](./s3-clients.md).
4. Complete the [Production Checklist](../operations/production-checklist.md) before serving production traffic.
