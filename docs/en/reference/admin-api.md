---
title: Admin API
description: Reference for SynapS3 health, metrics, dashboard, settings, wallet, task, and S3 user endpoints.
---

# Admin API

The Admin API powers the dashboard and CLI. Keep it bound to loopback unless it is protected by an authenticated private access layer.

Default base URL:

```text
http://127.0.0.1:9090
```

## Safety Model

Keep the Admin API on loopback. Write endpoints reject unsafe exposure. Most JSON writes also require an explicit confirmation header.

| Endpoint group | Required protection |
| --- | --- |
| Settings, wallet, bucket/object writes, S3 user writes, Filecoin preflight | Loopback admin binding, `Content-Type: application/json`, and `X-SynapS3-Settings-Write: 1`. |
| Observability refresh | Loopback admin binding and `X-SynapS3-Observability-Refresh: 1`. |
| Task retry and diagnostic refresh | Private admin access; retry changes background work state. |
| S3 user list and object download | Private admin access; these expose access metadata or object data. |

If the admin server is not loopback-bound, protected operations return `403`.

## High-Risk Operations

Treat these endpoints as change-window operations. Some use the settings write header; all can change data, credentials, wallet balance, or background state.

| Area | Endpoints | Risk |
| --- | --- | --- |
| Settings | `PUT /api/v1/settings` | Changes can require restart or move the node to a different Filecoin network. Validate settings and Filecoin readiness before saving. |
| Wallet | `POST /api/v1/wallet/fund`, `POST /api/v1/wallet/withdraw` | Creates on-chain payment operations. |
| S3 users | `POST /api/v1/s3-users`, `PUT /api/v1/s3-users/{accessKey}`, `POST /api/v1/s3-users/{accessKey}/secret`, `DELETE /api/v1/s3-users/{accessKey}` | Changes client access or invalidates credentials. |
| Buckets and objects | bucket create, owner/copy-policy updates, object upload/download/delete/restore/permanent-delete | Changes or exposes user-visible S3 data and metadata. |
| Tasks and observability | task retry, diagnostic refresh, provider/data-set refresh | Requeues work or refreshes operational state. |

## Health and Metrics

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Health status for database, cache, and workers. |
| `GET` | `/metrics` | Prometheus metrics. |
| `GET` | `/api/v1/system/info` | Version and runtime information. |
| `GET` | `/api/v1/workers` | Worker liveness map. |
| `GET` | `/api/v1/cache/stats` | Cache usage and capacity. |

## Dashboard Data

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/overview` | Dashboard summary. |
| `GET` | `/api/v1/events` | Dashboard event stream. |
| `GET` | `/api/v1/buckets` | List buckets. |
| `POST` | `/api/v1/buckets` | Create a bucket. |
| `GET` | `/api/v1/buckets/{name}/objects` | List objects. |
| `POST` | `/api/v1/buckets/{name}/objects/upload` | Upload an object through the dashboard. |
| `GET` | `/api/v1/buckets/{name}/objects/download` | Download an object through the dashboard. |
| `GET` | `/api/v1/buckets/{name}/objects/versions` | List object versions. |
| `GET` | `/api/v1/buckets/{name}/objects/provenance` | Inspect object storage provenance. |

## Tasks

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | List background tasks. Supports filters such as `type`, `stage`, `status`, `limit`, and `offset`. |
| `GET` | `/api/v1/tasks/stats` | Count tasks by status. |
| `GET` | `/api/v1/tasks/{id}/ref-detail` | Resolve the object or storage upload behind a task. |
| `GET` | `/api/v1/tasks/{id}/diagnostic` | Read task diagnostics. |
| `POST` | `/api/v1/tasks/{id}/diagnostic/refresh` | Refresh diagnostics. |
| `POST` | `/api/v1/tasks/{id}/retry` | Retry an exhausted task. |

## Wallet and Filecoin

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/wallet` | Wallet identity, balances, contract state, and business counters. |
| `POST` | `/api/v1/wallet/fund` | Create a wallet funding operation. |
| `POST` | `/api/v1/wallet/withdraw` | Create a wallet withdrawal operation. |
| `GET` | `/api/v1/wallet/operations` | List wallet operations. |
| `GET` | `/api/v1/filecoin/readiness` | Check Filecoin readiness. |
| `POST` | `/api/v1/filecoin/readiness/preflight` | Validate pending Filecoin settings. |
| `GET` | `/api/v1/observability/providers` | Provider health data. |
| `POST` | `/api/v1/observability/providers/refresh` | Refresh provider health. |
| `GET` | `/api/v1/observability/data-sets` | Local data set health data. |
| `POST` | `/api/v1/observability/data-sets/refresh` | Refresh data set health. |

## Settings and S3 Users

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/settings` | Read effective settings and metadata. |
| `PUT` | `/api/v1/settings` | Persist settings changes. |
| `POST` | `/api/v1/settings/validate` | Validate a settings payload without saving. |
| `GET` | `/api/v1/s3-users` | List S3 users. |
| `POST` | `/api/v1/s3-users` | Create an S3 user. |
| `PUT` | `/api/v1/s3-users/{accessKey}` | Update an S3 user role. |
| `POST` | `/api/v1/s3-users/{accessKey}/secret` | Rotate an S3 secret key. |
| `DELETE` | `/api/v1/s3-users/{accessKey}` | Delete an S3 user. |

## Write Example

```bash
curl -X POST http://127.0.0.1:9090/api/v1/s3-users \
  -H 'Content-Type: application/json' \
  -H 'X-SynapS3-Settings-Write: 1' \
  -d '{"role":"user"}'
```

Expected result: the response contains an access key, secret key, and role.
