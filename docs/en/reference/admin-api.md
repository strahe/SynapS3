---
title: Admin API
description: Reference for SynapS3 health, metrics, dashboard, settings, wallet, task, and S3 user endpoints.
---

# Admin API

The Admin API is used by the dashboard and CLI. Admin authentication is enabled by default. Keep the listener on loopback for local access; use an HTTPS reverse proxy or SSH tunnel for remote access.

Default base URL:

```text
http://127.0.0.1:9090
```

## Setup Mode

When `/healthz` returns `{"status":"setup"}`, the Admin endpoint exposes only the surfaces needed to finish configuration:

- `/healthz`, Admin login, session, refresh, and logout;
- the dashboard shell;
- `GET /api/v1/settings`, `PUT /api/v1/settings`, and `POST /api/v1/settings/validate`;
- `POST /api/v1/filecoin/readiness/preflight`.

Runtime metrics, buckets, objects, tasks, wallet operations, storage health, and S3 user management are unavailable in setup mode. Save valid settings, restart SynapS3, and verify that `/healthz` returns `{"status":"ok"}` before using the full API.

## Auth Model

`/healthz` is public so process health checks can run without credentials. The dashboard shell and static assets can load before login, but dashboard data and write operations require Admin auth.

| Surface | Required auth |
| --- | --- |
| `/healthz` | None. |
| `/api/v1/auth/login`, `/api/v1/auth/session` | Login and session endpoints. Session returns `401` when no valid browser session exists. |
| `/api/v1/auth/refresh`, `/api/v1/auth/logout` | Require a valid browser session and CSRF header; HTTP Basic auth is not accepted. |
| `/api/v1/*` | Browser session cookie with CSRF for unsafe methods, or HTTP Basic auth. |
| `/metrics` | Browser session cookie or HTTP Basic auth. |
| `/admin/exhausted-tasks*` | Browser session cookie with CSRF for unsafe methods, or HTTP Basic auth. |

### Browser Sessions

Browser login sets the `synaps3_admin_session` HttpOnly cookie and returns a CSRF token. Cookie-authenticated `POST`, `PUT`, `PATCH`, and `DELETE` requests must include `X-SynapS3-CSRF`. Refresh and logout only accept browser sessions.

The optional `remember` boolean in the login request defaults to `false`. A standard login uses a browser-session cookie and the configured `admin.auth.session_ttl`. When `remember` is `true`, the cookie persists for the greater of 30 days or the configured session TTL. Some browsers can restore browser-session cookies when restoring a previous browsing session.

Login, session, and refresh responses contain `username`, `csrf_token`, `expires_at`, and `refresh_after`. Any client holding the valid session cookie and matching CSRF token can request renewal after `refresh_after`. The login has no absolute lifetime cap. If no client requests renewal, the token expires at `expires_at`.

Refresh preserves the session lifetime, CSRF token, and login family. Logout revokes the whole family, including tokens issued before the latest refresh. Revocations are kept in memory; restarting SynapS3 clears them, although the browser cookie is still removed during a normal logout.

### CLI and Basic Auth

CLI and script calls can use HTTP Basic auth and do not need a CSRF header. Browser Basic-auth requests are rejected when `Sec-Fetch-Site`, `Origin`, or `Referer` show a cross-site origin. Requests without browser-origin headers still work for CLI and scripts.

Failed password checks are rate-limited by resolved client IP; additional login attempts are rejected while the limit is active.

### Reverse Proxies

When SynapS3 is behind a reverse proxy, forwarded client, scheme, and host headers are used only when `admin.trusted_proxies` contains the proxy IP or CIDR. Keep it empty unless the proxy removes untrusted `X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, and `X-Forwarded-Host` headers.

### Admin Credentials

Admin credentials are created by `synaps3 init`. Interactive init prints the password once. Non-interactive and Docker init write it to `admin-initial-password` in the runtime data directory with file mode `0600`. Local `synaps3 admin` commands locate config through `--config`, `SYNAPS3_CONFIG`, then the default path; they use `SYNAPS3_ADMIN_PASSWORD` first, then `admin-initial-password` next to the config file, then the prompt.

Resetting the password also rotates `admin.auth.session_secret`, invalidating existing browser sessions. Reset it offline with:

```bash
synaps3 admin-auth reset-password --config /var/lib/synaps3/config.toml
```

## Auth Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/v1/auth/login` | Validate username and password, accept optional `remember`, set the browser cookie, and return the session. |
| `GET` | `/api/v1/auth/session` | Return the current browser session. |
| `POST` | `/api/v1/auth/refresh` | Require session and CSRF, then renew an eligible browser session. Early requests return the current session without changing the cookie. |
| `POST` | `/api/v1/auth/logout` | Require session and CSRF, end the current browser session, and clear the cookie. |

After logout or a `401` API response, the dashboard returns to the login page.

## Security Headers

Admin responses include `Content-Security-Policy`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and `Referrer-Policy: strict-origin-when-cross-origin`. The default content policy keeps dashboard resources on the same origin. `/api/`, `/admin/`, `/metrics`, and `/healthz` also send `Cache-Control: no-store`. Dashboard page assets do not.

## High-Risk Operations

Treat these endpoints as change-window operations. They can change data, credentials, wallet payment state, or background state.

| Area | Endpoints | Risk |
| --- | --- | --- |
| Settings | `PUT /api/v1/settings` | Changes can require restart or move the node to a different Filecoin network. Validate settings and Filecoin readiness before saving. |
| Wallet | `POST /api/v1/wallet/fund`, `POST /api/v1/wallet/withdraw`, `POST /api/v1/wallet/approve` | Creates on-chain payment operations. |
| S3 users | `POST /api/v1/s3-users`, `PUT /api/v1/s3-users/{accessKey}`, `POST /api/v1/s3-users/{accessKey}/secret`, `DELETE /api/v1/s3-users/{accessKey}` | Changes client access or invalidates credentials. |
| Buckets and objects | bucket create, owner/copy-policy updates, object upload/download/delete/restore/permanent-delete | Changes or exposes user-visible S3 data and metadata. |
| Tasks and storage health | task retry, diagnostic refresh, storage provider and data set refresh | Requeues work or refreshes operational status. |
| Provider replacement | `POST /api/v1/buckets/{name}/data-sets/{id}/replacement`, `POST /api/v1/storage-replacements/{id}/retry` | Creates a new paid storage service, moves a replica to it, and ends the old service. |
| Storage confirmation | `POST /api/v1/storage-confirmations/{copy-id}/release` | May permit the provider to store the same piece again. Verify the current attempt before releasing it. |

## Health and Metrics

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Health status for database, cache, and background tasks. |
| `GET` | `/metrics` | Prometheus metrics. Requires Admin auth. |
| `GET` | `/api/v1/system/info` | Version and runtime information. |
| `GET` | `/api/v1/workers` | Background task activity and health. |
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
| `PUT` | `/api/v1/buckets/{name}/copy-policy` | Update target replicas and/or the cache-release threshold. |
| `DELETE` | `/api/v1/buckets/{name}` | Not supported. Returns `501 Not Implemented`. |
| `GET` | `/api/v1/buckets/{name}/objects` | List objects. |
| `DELETE` | `/api/v1/buckets/{name}/objects` | Create an object delete marker. |
| `POST` | `/api/v1/buckets/{name}/objects/upload` | Upload an object through the dashboard. |
| `GET` | `/api/v1/buckets/{name}/objects/download` | Download an object through the dashboard. |
| `GET` | `/api/v1/buckets/{name}/objects/versions` | List object versions and return the current version token. |
| `POST` | `/api/v1/buckets/{name}/objects/versions/restore` | Create a new current version from an existing data version. |
| `GET` | `/api/v1/buckets/{name}/objects/provenance` | Inspect object storage provenance. |
| `GET` | `/api/v1/buckets/{name}/objects/status-detail` | Read detailed object state. |
| `GET` | `/api/v1/buckets/{name}/objects/deleted` | List deleted objects. |
| `GET` | `/api/v1/buckets/{name}/objects/deletions` | List object delete markers. |
| `POST` | `/api/v1/buckets/{name}/objects/restore` | Restore an object from a delete marker. |
| `POST` | `/api/v1/buckets/{name}/objects/permanent-delete` | Permanently delete an object version. |
| `POST` | `/api/v1/buckets/{name}/objects/deleted/permanent-delete` | Permanently delete a deleted object version. |
| `GET` | `/api/v1/buckets/{name}/storage-health/affected-versions` | List versions affected by storage health issues. |
| `GET` | `/api/v1/buckets/{name}/data-sets/{id}/replacement/providers` | List the providers this replica can move to, and why the others cannot take it. |
| `POST` | `/api/v1/buckets/{name}/data-sets/{id}/replacement` | Authorize replacing the storage provider behind a replica. |
| `POST` | `/api/v1/storage-replacements/{id}/retry` | Resume a failed or attention-holding provider replacement. |
| `GET` | `/api/v1/storage-confirmations` | List storage confirmations that need operator attention. |
| `POST` | `/api/v1/storage-confirmations/{copy-id}/release` | Release an ambiguous confirmation after acknowledging possible duplicate storage. |

For object upload, the HTTP `Content-Type` is the uploaded object's content type. It is not a JSON request marker.

### Bucket Copy Policy

`POST /api/v1/buckets` accepts optional `default_copies` and `minimum_durable_copies` fields. Bucket list, detail, create, and policy-update responses include:

- `minimum_durable_copies`: the explicit bucket value, or `null` for strict per-upload behavior;
- `effective_minimum_durable_copies`: the current display value after clamping the bucket minimum to the current target.

`PUT /api/v1/buckets/{name}/copy-policy` accepts `default_copies` and `minimum_durable_copies` independently. An omitted field is unchanged. `default_copies: null` inherits the current runtime target for new uploads. `minimum_durable_copies: null` requires every replica frozen for each upload before releasing its cache. An explicit minimum must be between `1` and `8` and cannot exceed the target produced by the same request. An empty request or an invalid final combination returns `400 Bad Request`.

Target changes affect new uploads. Minimum changes also re-evaluate retained cache for current uploads. Increasing the minimum cannot restore cache that has already been deleted.

### Permanently Delete Object Versions

`POST /api/v1/buckets/{name}/objects/permanent-delete` accepts `key` and `version_id`. `POST /api/v1/buckets/{name}/objects/deleted/permanent-delete` accepts `key` and `delete_marker_version_id`.

Both endpoints fail immediately with `409 Conflict` while storage work for a version is active or a submitted Filecoin transaction is awaiting confirmation. Check the related tasks, then try again. For an otherwise eligible data version, stopped storage work that has not submitted a transaction does not block deletion. A selected version or deleted object that is no longer eligible for permanent deletion also returns `409 Conflict`; invalid requests return `400 Bad Request`, and missing buckets, objects, or versions return `404 Not Found`.

After a successful delete, remote storage used only by the removed versions is queued for background cleanup. Storage shared with remaining versions is kept.

### Restore an Object Version

`GET /api/v1/buckets/{name}/objects/versions` includes `current_version_id` on every page when the object has version history. Pass that value when confirming a restore:

```json
{
  "key": "path/file.txt",
  "version_id": "source-version-id",
  "expected_current_version_id": "current-version-id"
}
```

`POST /api/v1/buckets/{name}/objects/versions/restore` copies a historical data version to a new version of the same bucket and key. It does not modify or remove existing data versions or delete markers. The selected version must differ from the readable current object representation. Selecting the current version, or an equivalent historical version, is rejected without changing the object. A failed or unavailable current version can still be restored from a readable historical version.

Successful response:

```json
{
  "key": "path/file.txt",
  "source_version_id": "source-version-id",
  "version_id": "new-current-version-id"
}
```

The request returns `409 Conflict` when the selected version is a delete marker or `expected_current_version_id` is no longer current. Refresh the version list and confirm again with the new token. When the selected version already matches the current object, the response uses a stable code so clients can treat it as a no-op:

```json
{
  "error": "selected version already matches the current object",
  "code": "object_version_already_current"
}
```

A missing or permanently deleted source returns `404 Not Found`; insufficient cache capacity returns `507 Insufficient Storage`; invalid input returns `400 Bad Request`; source read and internal failures return `500 Internal Server Error`.

The restore streams synchronously for up to one hour and requires enough cache capacity for the new destination version.

### Replace a Storage Provider

`POST /api/v1/buckets/{name}/data-sets/{id}/replacement` is the only way to replace the storage provider behind a replica. One confirmation authorizes all of it: a new paid storage service, moving new uploads to it, copying existing data across, and ending the old service once every retained version is readable on the new provider. Objects copy from another replica or from local cache. An object with neither cannot be copied, and the old provider is not shut down.

Choose the new provider automatically:

```json
{ "mode": "automatic", "client_request_id": "019d2e22-8c36-7d5b-a6be-5f7fa6d6f584" }
```

Automatic selection excludes every provider the bucket has ever used, including retired ones. Or name the provider yourself:

```json
{ "mode": "manual", "provider_id": "202", "client_request_id": "019d2e22-8c36-7d5b-a6be-5f7fa6d6f584" }
```

A named provider must appear in the complete available, active, PDP-capable provider inventory. It may be one the bucket used before, provided that earlier service has already been retired. The provider being replaced, and any provider still holding a live generation of this bucket, are rejected.

`client_request_id` is required after trimming and must contain 1–128 characters. The first successful request returns `201 Created`. Replaying the same bucket, source, mode, and manual provider with the same ID returns the original record and `200 OK`, even after the replica has switched. Reusing the ID with different parameters returns `409 Conflict` with `replacement_idempotency_conflict`. An automatic replay is resolved before reading the provider inventory, so a later inventory change cannot choose a different provider.

Confirming again for the same replica supersedes the earlier request and returns `201 Created`; it is not a conflict. The unused provider from the earlier request is shut down. `replacement_active` means something else: the replica is the target of another unfinished replacement, which has to be resolved first.

Only the replica that currently receives writes can be replaced; a historical generation is reported with `"replaceable": false` in `GET /api/v1/buckets/{name}`.

A successful first confirmation returns `201 Created` with the replacement record; an exact replay returns `200 OK`. `GET /api/v1/buckets/{name}` returns the bucket's recent replacements in `replacements`, newest first, up to the 50 most recent.

Replacement moves through these states:

| Status | Meaning |
| --- | --- |
| `preparing_target` | The new service is being created. Writes still go to the current provider. |
| `migrating` | The new provider receives new uploads while existing data is copied across. |
| `waiting` | Paused. `wait_reason` and `wait_message` distinguish service creation (`target_creating`), writable confirmation (`target_writable`), an unreachable provider (`target`), funding, source availability, and safe retirement waits. Most waits resume without action. |
| `retiring` | Everything is copied and the old service is being ended. |
| `cleanup_attention` | Ending the old service needs an operator decision, such as settling payment debt. |
| `failed` | Work ran out of attempts and needs to be retried. |
| `completed` | The old service is ended and the replica now lives on the new provider. |
| `superseded` | A later confirmation replaced this request. |

`last_error` is set only for `failed` and `cleanup_attention`, and is cleared by a retry. Waiting never sets it, because waiting is not a failure. A failed response may also include `failure_reason`. `target_in_use` is permanent for that approved target: choose a different provider; the retry endpoint returns a conflict.

`items_total` and `items_copied` count unique stored content, not object versions: content shared by many versions is copied once. Content deleted while migration is in progress is no longer needed and is not counted as copied. After copying finishes, the response reports how much content was copied and how much no longer needed to move; `items_copied/items_total` is not a completion percentage. The confirmation dialog instead counts referenced versions and total size.

Each replacement also includes a nested `progress` object. During discovery, `seeding_complete` is `false`, `items_total` is only the number discovered so far, and `percent` is omitted. Once discovery finishes, `items_total` is final and `percent` is `items_processed / items_total`, where `items_processed = items_copied + items_no_longer_needed`. This lets a completed replacement reach 100% even when content was deleted before it needed copying. `items_pending`, `items_active`, `items_retrying`, `items_waiting_source`, `items_failed`, and `items_attention` report mutually exclusive current work counts. Confirmation work is included in `items_active`; work that needs an operator is included only in `items_attention`. Both counts remain outstanding work. `next_retry_at` is present when a retry is scheduled for the future. `phase` is `prepare`, `migrate`, `retire`, or `none`.

`POST /api/v1/storage-replacements/{id}/retry` resumes a `failed` or `cleanup_attention` replacement on the same approved provider.

Choosing a different provider requires a new confirmation, and is only available while the retiring provider still holds the replica. Once the new provider has taken the replica over, each generation holds data the other does not, so confirming again on either one is refused (`replacement_source_not_current` on the old, `replacement_active` on the new) and the approved copy has to be finished with retry.

Conflicts return `409 Conflict` with a stable code:

```json
{
  "error": "that provider already stores a replica of this bucket",
  "code": "replacement_target_in_use"
}
```

The codes are `replacement_active`, `replacement_target_in_use`, `replacement_target_unavailable`, `replacement_no_eligible_provider`, `replacement_source_not_current`, `replacement_superseded`, `replacement_not_retryable`, `replacement_task_running`, and `replacement_idempotency_conflict`. An invalid provider choice returns `400 Bad Request` with `replacement_target_invalid`; a currently unavailable manual target returns `400` with `replacement_target_unavailable`; an unknown bucket, data set, or replacement returns `404 Not Found`; an unavailable storage service returns `503 Service Unavailable`; internal failures return `500 Internal Server Error`.

`GET /api/v1/buckets/{name}/data-sets/{id}/replacement/providers` lists every provider currently reported available with `eligible`, an `ineligible_reason` of `current_source` or `already_serves_bucket`, and `previously_used` for a provider this bucket has used and fully retired. Providers that cannot take the replica are listed rather than omitted, so an operator can see why one they expected is unavailable. It is the same inventory the storage topology reports under the `Available` filter, so a provider listed there is offered here and an unreachable one is offered in neither. Eligibility is the same rule the confirmation enforces. Automatic selection is stricter still: it never returns to a provider this bucket has used, which a manual choice may.

Confirmation only checks what SynapS3 has recorded. A provider that still runs a storage service for this bucket on chain is detected when the replacement prepares its target: the replacement stops at `failed` with the provider and data set named, and the operator confirms again on a different provider. The replica has not moved at that point, so nothing is at risk. A provider whose earlier service for this bucket was retired normally can be chosen again.

### Storage confirmation attention

When SynapS3 cannot determine whether a provider accepted a piece, it does not submit that piece again automatically. `GET /api/v1/storage-confirmations?status=needs_attention&limit=100` lists the affected copy, data set, attempt, known transaction, timestamps, and stable `reason_code`.

`POST /api/v1/storage-confirmations/{copy-id}/release` lets normal recovery continue and may permit a duplicate submission. Inspect the current list entry first, then send both its attempt ID and the explicit risk acknowledgement:

```json
{
  "expected_attempt_id": "current-attempt-id",
  "acknowledge_possible_duplicate": true
}
```

If the attempt changed after it was inspected, the API returns `409 Conflict`. If confirmation succeeds before release, the entry disappears automatically.

## Tasks

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | List background tasks. Supports filters such as `type`, `stage`, `status`, `limit`, and `offset`. |
| `GET` | `/api/v1/tasks/stats` | Count tasks by status. |
| `GET` | `/api/v1/tasks/{id}/ref-detail` | Resolve the object or storage operation related to a task. |
| `GET` | `/api/v1/tasks/{id}/diagnostic` | Read task diagnostics. |
| `POST` | `/api/v1/tasks/{id}/diagnostic/refresh` | Refresh diagnostics. |
| `POST` | `/api/v1/tasks/{id}/retry` | Retry an exhausted task. |
| `GET` | `/admin/exhausted-tasks` | List exhausted tasks. Supports `limit` up to `1000`. |
| `POST` | `/admin/exhausted-tasks/{id}/retry` | Retry an exhausted task (legacy path). |

Replacement and retirement task responses that refer to a bucket include `bucket_name` in list and reference-detail responses. Retrying replacement work from the task queue returns `409 Conflict` with `"code": "replacement_task_retry_unsupported"`. When a replacement task has completed or stopped, use **Open Data Sets**, or open the bucket and go to Details → Storage → Data Sets. A `target_in_use` failure has no Retry action because it requires a different provider.

Task list `progress` is a scope-discriminated object. With `scope: "ingress_store"`, it contains `attempt`, `uploaded_bytes`, `total_bytes`, optional `percent`, `done`, and `updated_at`. With `scope: "provider_replacement"`, it contains the same replacement progress returned by the bucket response. Clients must branch on `scope`.

## Wallet and Filecoin

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/wallet` | Wallet identity, balances, contract state, and business counters. |
| `POST` | `/api/v1/wallet/fund` | Create a wallet funding operation. |
| `POST` | `/api/v1/wallet/withdraw` | Create a wallet withdrawal operation. |
| `POST` | `/api/v1/wallet/approve` | Create an explicit FWSS approval operation. Payload only accepts `client_request_id`. |
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

Cache settings expose `eviction_policy`, `lru_high_watermark_percent`, and `lru_low_watermark_percent` under `cache`. Valid policies are `lru`, `after_upload`, and `none`. Watermarks must satisfy `0 <= low < high <= 100` and only affect `lru`.

When the full runtime is available, `GET /api/v1/settings` also returns `runtime_filecoin_default_copies`. This is the value used by the current process. `config.filecoin.default_copies` remains the saved value that takes effect after the next restart.

After saving settings, restart SynapS3, check `/healthz`, and read settings again to confirm the effective values.

## Write Example

```bash
curl -X POST http://127.0.0.1:9090/api/v1/s3-users \
  -u admin \
  -H 'Content-Type: application/json' \
  -d '{"role":"user"}'
```

Enter the Admin password at curl's no-echo prompt. The response contains an access key, secret key, and role. The secret is shown only once; save it in a credential file protected with `0600` and rotate it immediately if exposed.
