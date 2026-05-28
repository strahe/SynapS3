---
title: Architecture
description: Understand the SynapS3 single-node gateway architecture and data flow boundaries.
---

# Architecture

SynapS3 bridges S3 clients to Filecoin storage through a cache-first gateway, repository-backed metadata, and background workers.

## System Shape

```text
S3 client
  -> VersityGW
  -> SynapseBackend
  -> local cache + repositories + state machine
  -> background workers
  -> synapse-go SDK
  -> Filecoin storage providers
```

The important operational boundary is between the S3 response and Filecoin upload. A confirmed write means local durability is complete. Filecoin storage happens after the response.

## Main Layers

| Layer | Responsibility |
| --- | --- |
| `cmd/synaps3` | CLI entrypoint, config loading, DB migrations, runtime wiring. |
| `internal/backend` | S3 behavior and VersityGW backend implementation. |
| `internal/cache` | Durable local filesystem cache. |
| `internal/db/repository` | Persistence boundary for backend and workers. |
| `internal/state` | Object lifecycle transition validation. |
| `internal/worker` | Async upload, eviction, leases, retries, recovery. |
| `internal/admin` and `ui/` | Dashboard, Admin API, Admin auth, health, metrics. |
| `internal/synapse` | Narrow wrapper around Synapse SDK behavior. |

## Design Principles

- Confirmed S3 writes must survive async upload failures.
- Raw database access stays behind repositories.
- Object visibility and object storage state are separate concerns.
- Generation values protect newer writes from stale workers.
- Cache eviction only happens after sufficient remote durability metadata exists.
- The current design is single-node first and does not assume distributed coordination.

## What This Means for Operators

| Behavior | Operator impact |
| --- | --- |
| S3 write success is local-first | Provider outages do not make accepted writes disappear. |
| Background tasks handle Filecoin storage | Watch task queues and exhausted tasks. |
| Cache is part of durability | Treat cache disk as runtime data, not disposable scratch space. |
| Admin API controls operations | Use Admin auth; keep it on loopback or behind HTTPS and access control. |

## Dashboard Role

The embedded React dashboard is an operational surface. It shows buckets, objects, wallet state, background tasks, storage topology, settings, and health signals. It shares the admin server, uses the Admin auth session, and must not be exposed directly to untrusted networks.

## Admin Auth Boundary

Admin API requests are classified by canonical path before they reach the Go `ServeMux`. `/healthz` stays public; `/api/v1/*`, `/metrics`, and `/admin/exhausted-tasks*` require Admin auth. Browser sessions use an HttpOnly cookie plus `X-SynapS3-CSRF` for unsafe methods. CLI and scripts may use HTTP Basic auth, with browser-origin checks for unsafe Basic-authenticated requests.

Failed password checks are limited by resolved client IP. Forwarded client, scheme, and host headers are trusted only when `admin.trusted_proxies` matches the direct peer. Logout clears the cookie and revokes the current session token in memory until it expires.
