---
title: Admin API
description: Reference for SynapS3 health, metrics, dashboard, settings, wallet, task, and S3 user endpoints.
---

# Admin API

The Admin API powers the dashboard and CLI. Admin authentication is enabled by default. Keep the listener on loopback for local access; use an HTTPS reverse proxy or SSH tunnel for remote access.

Default base URL:

```text
http://127.0.0.1:9090
```

## Auth Model

`/healthz` is public so process health checks can run without credentials. The dashboard shell and static assets can load before login, but dashboard data and mutations are gated by Admin auth.

| Surface | Required auth |
| --- | --- |
| `/healthz` | None. |
| `/api/v1/auth/login`, `/api/v1/auth/session` | Login/session endpoints. Session returns `401` when no valid browser session exists. |
| `/api/v1/auth/logout` | Requires a valid browser session and CSRF header; HTTP Basic auth is not accepted. |
| `/api/v1/*` | Browser session cookie with CSRF for unsafe methods, or HTTP Basic auth. |
| `/metrics` | Browser session cookie or HTTP Basic auth. |
| `/admin/exhausted-tasks*` | Browser session cookie or HTTP Basic auth. |

Browser login sets the `synaps3_admin_session` HttpOnly cookie and returns a CSRF token. Cookie-authenticated `POST`, `PUT`, `PATCH`, and `DELETE` requests must include `X-SynapS3-CSRF`. CLI and script calls can use HTTP Basic auth and do not need a CSRF header; logout is the exception and only accepts browser sessions. Basic-authenticated browser requests are rejected when `Sec-Fetch-Site`, `Origin`, or `Referer` show a cross-site origin. Requests without browser-origin headers continue to work for CLI and scripts. Failed password checks are rate-limited by resolved client IP and fail closed when the limiter is full. Successful Basic auth credentials are cached briefly per client IP to avoid repeated bcrypt work.

When SynapS3 is behind a reverse proxy, forwarded client, scheme, and host headers are used only when `admin.trusted_proxies` contains the proxy IP or CIDR. Keep it empty unless the proxy removes untrusted `X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, and `X-Forwarded-Host` headers.

Admin credentials are created by `synaps3 init`. Interactive init prints the password once. Non-interactive and Docker init write it to `admin-initial-password` in the app data directory with file mode `0600`. Local `synaps3 admin` commands use `SYNAPS3_ADMIN_PASSWORD` first, then `admin-initial-password` next to the config file, then the prompt. Resetting the password also rotates `admin.auth.session_secret`, invalidating existing browser sessions. Reset it offline with:

```bash
synaps3 admin-auth reset-password --config /var/lib/synaps3/config.toml
```

## Auth Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/v1/auth/login` | Validate username and password, set the browser cookie, and return a CSRF token. |
| `GET` | `/api/v1/auth/session` | Return the current browser session and CSRF token. |
| `POST` | `/api/v1/auth/logout` | Require session and CSRF, revoke the current token in memory until it expires, then clear the browser cookie. |

The dashboard also clears local auth state after any logout attempt or `401` API response, so the UI returns to login even when the server-side logout request fails.

## Security Headers

Admin responses include `Content-Security-Policy`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and `Referrer-Policy: strict-origin-when-cross-origin`. The CSP is same-origin by default and permits current inline script/style required by the embedded dashboard.

## High-Risk Operations

Treat these endpoints as change-window operations. They can change data, credentials, wallet balance, or background state.

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
| `GET` | `/metrics` | Prometheus metrics. Requires Admin auth. |
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
| `GET` | `/api/v1/buckets/{name}` | Read bucket detail. |
| `PUT` | `/api/v1/buckets/{name}/owner` | Update bucket owner. |
| `PUT` | `/api/v1/buckets/{name}/copy-policy` | Update default copy policy. |
| `DELETE` | `/api/v1/buckets/{name}` | Not implemented. Returns `501 Not Implemented`. |
| `GET` | `/api/v1/buckets/{name}/objects` | List objects. |
| `DELETE` | `/api/v1/buckets/{name}/objects` | Create an object delete marker. |
| `POST` | `/api/v1/buckets/{name}/objects/upload` | Upload an object through the dashboard. |
| `GET` | `/api/v1/buckets/{name}/objects/download` | Download an object through the dashboard. |
| `GET` | `/api/v1/buckets/{name}/objects/versions` | List object versions. |
| `GET` | `/api/v1/buckets/{name}/objects/provenance` | Inspect object storage provenance. |
| `GET` | `/api/v1/buckets/{name}/objects/status-detail` | Read detailed object state. |
| `GET` | `/api/v1/buckets/{name}/objects/deleted` | List deleted objects. |
| `GET` | `/api/v1/buckets/{name}/objects/deletions` | List object delete markers. |
| `POST` | `/api/v1/buckets/{name}/objects/restore` | Restore an object from a delete marker. |
| `POST` | `/api/v1/buckets/{name}/objects/permanent-delete` | Permanently delete an object version. |
| `POST` | `/api/v1/buckets/{name}/objects/deleted/permanent-delete` | Permanently delete a deleted object version. |
| `GET` | `/api/v1/buckets/{name}/storage-health/affected-versions` | List versions affected by storage health issues. |

For object upload, the HTTP `Content-Type` is the uploaded object's content type. It is not a JSON request marker.

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
export SYNAPS3_ADMIN_PASSWORD='replace-with-admin-password'

curl -X POST http://127.0.0.1:9090/api/v1/s3-users \
  -u "admin:${SYNAPS3_ADMIN_PASSWORD}" \
  -H 'Content-Type: application/json' \
  -d '{"role":"user"}'
```

Expected result: the response contains an access key, secret key, and role.
