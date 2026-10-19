---
title: Admin API
description: Look up SynapS3 Admin API authentication, endpoints, request fields, and error codes.
---

# Admin API

The dashboard and the `synaps3 admin` CLI use the Admin API. Its base URL is `admin.addr`, by default `http://127.0.0.1:9090`. Requests and responses use JSON unless noted. Errors return `{"error": "<message>"}`, plus `code` where listed below.

For safe exposure, see [Network Exposure](../operations/production-checklist.md#network-exposure).

## Authentication

| Path | Authentication |
| --- | --- |
| `/healthz` | None. |
| `/api/v1/auth/login` | Username and password in the body. |
| `/api/v1/auth/session` | Browser session cookie. Returns `401` without a valid session. |
| `/api/v1/auth/refresh`, `/api/v1/auth/logout` | Browser session cookie and CSRF header. Basic authentication is not accepted. Logout also accepts an expired cookie. |
| Other `/api/v1/*` paths and `/metrics` | Browser session cookie, or HTTP Basic authentication. |

The dashboard pages and static files load without authentication; their data does not.

**Browser sessions.** Login sets the HttpOnly cookie `synaps3_admin_session` and returns a CSRF token. Cookie-authenticated `POST`, `PUT`, `PATCH`, and `DELETE` requests must send it in `X-SynapS3-CSRF`. Login, session, and refresh responses contain `username`, `csrf_token`, `expires_at`, and `refresh_after`. The optional login field `remember` (default `false`) makes the cookie persistent for 30 days or `admin.auth.session_ttl`, whichever is longer. Refresh after `refresh_after` extends the session; an earlier refresh returns the current session unchanged. Logout ends the session and all its refreshed tokens. Sessions survive restarts, except sessions created in setup mode.

**Basic authentication.** Use `admin.auth.username` and the Admin password; no CSRF header is needed. Basic-authenticated requests whose `Sec-Fetch-Site`, `Origin`, or `Referer` header shows a cross-site browser origin are rejected.

After 5 failed password attempts from one client IP, further attempts are rejected for one minute. Behind a reverse proxy, the client IP comes from forwarded headers only when the proxy address is in `admin.trusted_proxies`.

Every response carries `Content-Security-Policy`, `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and `Referrer-Policy: strict-origin-when-cross-origin`. Responses under `/api/`, `/metrics`, and `/healthz` also carry `Cache-Control: no-store`.

### Setup Mode

While `/healthz` returns `{"status":"setup"}`, only these endpoints are available: `/healthz`, the `/api/v1/auth/*` endpoints, `GET` and `PUT /api/v1/settings`, `POST /api/v1/settings/validate`, `POST /api/v1/filecoin/readiness/preflight`, and the dashboard pages. Save valid settings and restart SynapS3 to use the rest of the API.

## Endpoints

### Auth

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/v1/auth/login` | Log in with `username`, `password`, and optional `remember`. |
| `GET` | `/api/v1/auth/session` | Return the current session. |
| `POST` | `/api/v1/auth/refresh` | Extend the session. |
| `POST` | `/api/v1/auth/logout` | End the session. Returns `204`. |

### Health and Monitoring

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Health status; see [Health](../operations/health-metrics.md#health). |
| `GET` | `/metrics` | Prometheus metrics; see [Prometheus Metrics](../operations/health-metrics.md#prometheus-metrics). |
| `GET` | `/api/v1/system/info` | Version, commit, and uptime. |
| `GET` | `/api/v1/workers` | Background task status. |
| `GET` | `/api/v1/cache/stats` | Cache usage and capacity. |
| `GET` | `/api/v1/overview` | Dashboard summary. `tasks.attention.failed` counts unacknowledged failed tasks. |
| `GET` | `/api/v1/monitoring` | CPU, memory, and traffic samples for the last hour. |
| `GET` | `/api/v1/events` | Server-sent event stream for the dashboard. A cookie-authenticated stream closes when its session ends. |

### Buckets and Objects

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/buckets` | List buckets. |
| `POST` | `/api/v1/buckets` | Create a bucket. |
| `GET` | `/api/v1/buckets/{name}` | Bucket detail, including data sets and the 50 most recent storage provider replacements (`replacements`, newest first). |
| `PUT` | `/api/v1/buckets/{name}/owner` | Change the owner (`owner_access_key`). |
| `PUT` | `/api/v1/buckets/{name}/copy-policy` | Change the replica settings. |
| `DELETE` | `/api/v1/buckets/{name}` | Not supported. Returns `501`. |
| `GET` | `/api/v1/buckets/{name}/objects` | List objects. |
| `DELETE` | `/api/v1/buckets/{name}/objects` | Add a delete marker. |
| `POST` | `/api/v1/buckets/{name}/objects/upload` | Upload an object. The request `Content-Type` becomes the object's content type. |
| `GET` | `/api/v1/buckets/{name}/objects/download` | Download an object. |
| `GET` | `/api/v1/buckets/{name}/objects/versions` | List versions. Includes `current_version_id` when the object has versions. |
| `POST` | `/api/v1/buckets/{name}/objects/versions/restore` | Copy an earlier version to a new current version. |
| `GET` | `/api/v1/buckets/{name}/objects/provenance` | Replicas of an object version and their storage providers. |
| `GET` | `/api/v1/buckets/{name}/objects/status-detail` | Detailed storage state of an object version. |
| `GET` | `/api/v1/buckets/{name}/objects/deleted` | List deleted objects. |
| `GET` | `/api/v1/buckets/{name}/objects/deletions` | List delete markers. |
| `POST` | `/api/v1/buckets/{name}/objects/restore` | Restore a deleted object. |
| `POST` | `/api/v1/buckets/{name}/objects/permanent-delete` | Permanently delete a version. |
| `POST` | `/api/v1/buckets/{name}/objects/deleted/permanent-delete` | Permanently delete a deleted object. |
| `GET` | `/api/v1/buckets/{name}/storage-health/affected-versions` | Versions affected by storage health problems. |

#### Replica Settings

`POST /api/v1/buckets` accepts `name`, `owner_access_key`, and optional `default_copies`, `minimum_durable_copies`, and `provider_selection_strategy` (`distribution` or `speed`). Omitted values come from the configuration. A bucket keeps its values when the configuration changes later. Bucket responses include `default_copies`, `minimum_durable_copies`, `provider_selection_strategy`, `status`, and `setup_message` while storage is being prepared. See [Target and Minimum Replicas](../concepts/filecoin-storage-flow.md#target-and-minimum-replicas).

`PUT /api/v1/buckets/{name}/copy-policy` accepts `default_copies` and `minimum_durable_copies`:

- An omitted field is unchanged. `"default_copies": null` resets the target to `filecoin.default_copies`; `"minimum_durable_copies": null` sets the minimum to the target.
- Values must be `1` to `8`, and the minimum must not exceed the resulting target.
- Lowering `default_copies`, including through a `null` reset, returns `400`. Replicas above a lower target would keep running and costing money.
- An empty request or an invalid combination returns `400`.

#### Permanent Deletion

`permanent-delete` takes `key` and `version_id`; `deleted/permanent-delete` takes `key` and `delete_marker_version_id`. Both return `409` while storage work for the version is active or a submitted Filecoin transaction awaits confirmation, and when the version is not eligible for deletion. Failed storage work that never submitted a transaction does not block deletion. Invalid requests return `400`; missing buckets, objects, or versions return `404`. Remote replicas used only by the deleted versions are removed in the background.

#### Version Restore

Request:

```json
{
  "key": "path/file.txt",
  "version_id": "source-version-id",
  "expected_current_version_id": "current-version-id"
}
```

Use `current_version_id` from the version list as `expected_current_version_id`. On success, the response contains `key`, `source_version_id`, and the new `version_id`; earlier versions and delete markers are unchanged. The restore runs synchronously for up to one hour and needs cache space for the new version.

| Status | Cause |
| --- | --- |
| `400` | Invalid request. |
| `404` | The source version does not exist. |
| `409` | The source is a delete marker, or the current version changed; reload the list and confirm again. With `"code": "object_version_already_current"`, the source already matches the current object. |
| `503` | Not enough cache space. |

### Storage Providers and Replacement

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/buckets/{name}/data-sets/{id}/replacement/providers` | Storage providers that can take this replica, and why the others cannot. |
| `POST` | `/api/v1/buckets/{name}/data-sets/{id}/replacement` | Replace the storage provider of a replica. |
| `GET` | `/api/v1/filecoin/warm-storage/price-list` | Current storage prices and a `fingerprint` to confirm a replacement. |
| `GET` | `/api/v1/observability/providers` | Storage providers with health, registry profile, approved and endorsed membership, and latest speed test. |
| `POST` | `/api/v1/observability/providers/refresh` | Recheck storage provider health. |
| `POST` | `/api/v1/observability/providers/{provider_id}/refresh` | Recheck one storage provider's profile and health. |
| `POST` | `/api/v1/observability/provider-tiers/refresh` | Reload the approved and endorsed lists. |
| `POST` | `/api/v1/observability/providers/{provider_id}/upload-speed-test` | Start a speed test. Returns `202` with `task_id`; `404` for an unknown storage provider; `409` if a test is running or the storage provider is not eligible. |
| `POST` | `/api/v1/observability/providers/upload-speed-tests` | Queue speed tests for eligible storage providers without a result. Returns `202` with `queued`. |
| `GET` | `/api/v1/observability/providers/upload-speed-summary` | Speed test progress and whether storage providers can be selected. |
| `GET` | `/api/v1/observability/data-sets` | Health of this node's data sets. |
| `POST` | `/api/v1/observability/data-sets/refresh` | Recheck data set health. |

A speed test uploads up to 32 MiB within 30 seconds and needs at least 8 MiB to produce a result. Results apply only to the storage provider's current service URL. Storage provider entries include `upload_speed_testable`, `upload_speed_unavailable_reason`, and `upload_speed_test` (`bytes_per_second`, `duration_ms`, `sample_bytes`, `tested_at`, and `state`, which is `testing` during a retest of the same URL and `stale` after the URL changes). A failed test includes `retry_task_id`. When a health check fails, `signal.last_error` gives the reason.

The speed test summary returns `auto_scope`, `trusted` and `other` counts (`total`, `succeeded`, `failed`, `testing`, `untested`), `selectable_providers`, `required_copies`, `ready`, `can_provision_default`, and `can_test_remaining`.

#### Replacement Request

```json
{ "mode": "automatic", "client_request_id": "019d2e22-8c36-7d5b-a6be-5f7fa6d6f584", "price_list_fingerprint": "<fingerprint>" }
```

```json
{ "mode": "manual", "provider_id": "202", "client_request_id": "019d2e22-8c36-7d5b-a6be-5f7fa6d6f584", "price_list_fingerprint": "<fingerprint>" }
```

- `client_request_id` (1 to 128 characters) makes the request idempotent. The first request returns `201`; the same request again returns the original record with `200`. The same ID with different parameters returns `409` with `replacement_idempotency_conflict`.
- `price_list_fingerprint` comes from the price list. If prices changed, the request fails with `price_list_changed`; `price_list_unavailable` and `unsupported_price_token` also block it. Reload the price list and use a new request ID.
- Automatic mode never picks a storage provider the bucket has used. It returns `409` with `provider_speed_tests` while eligible storage providers are still being tested.
- Manual mode accepts a storage provider with a fresh healthy check and a successful speed test, which does not hold a current replica of the bucket. A storage provider whose data sets for this bucket are all retired can be chosen again.
- Both modes keep the `filecoin.anchor_provider_tier` requirement.
- If the chosen storage provider already holds a data set for this bucket on chain, the replacement stops with `failed` before the replica moves. Confirm again with another storage provider.

The candidate list (`replacement/providers`) includes every storage provider with a health result, with `manual_selectable` and `manual_block_reason`: `current_source`, `already_serves_bucket`, `provider_unavailable`, `observation_stale`, `profile_missing`, `profile_url_changed`, `upload_speed_untested`, or `upload_speed_failed`.

Data set entries in bucket responses include `replaceable`, `replacement_blocked_reason`, and `setup_error`. A replica can be replaced when its data set is ready or its setup task has failed.

#### Replacement Status

| `status` | Meaning |
| --- | --- |
| `preparing_target` | The new data set is being created. |
| `migrating` | New uploads go to the new storage provider while stored data is copied. |
| `waiting` | Paused until a condition clears; see `wait_reason` and `wait_message`. |
| `retiring` | Copying is done; the old data set is being terminated. |
| `cleanup_attention` | Terminating the old data set needs operator action, such as settling a payment. |
| `failed` | Stopped. Follow `retryable` and the error. `failure_reason` is `target_in_use` (choose another storage provider) or `target_rejected`. |
| `completed` | Every retained version is readable on the new storage provider, and the old data set is terminated. |
| `superseded` | A later confirmation for the same replica replaced this request. |

`wait_reason` values: `target_creating`, `target_writable`, `target` (new storage provider unreachable), `funding`, `readable_source` (no replica or cache copy to copy from), `provider`, `source_writes`, `coverage`, and `termination_epoch`. Most waits clear on their own.

To retry, pass `retry_task_id` to `POST /api/v1/tasks/{id}/retry`. The retry continues with the same storage provider. To choose a different storage provider, confirm a new replacement; this works only while the old storage provider still holds the replica. After the new storage provider takes over, a new confirmation returns `replacement_source_not_current` (old replica) or `replacement_active` (new replica), and the replacement must be finished with retries.

`progress` reports `phase` (`prepare`, `migrate`, `retire`, or `none`), `seeding_complete`, `items_total`, `items_processed` (`items_copied` + `items_no_longer_needed`), `percent` (only after `seeding_complete`), the current counts `items_pending`, `items_active`, `items_retrying`, `items_waiting_source`, `items_failed`, and `items_attention`, and `next_retry_at`. Items count unique stored content, not object versions.

#### Replacement Errors

| Status | Code |
| --- | --- |
| `400` | `replacement_target_invalid`, `replacement_target_unavailable` (manual mode) |
| `404` | Unknown bucket, data set, or replacement. |
| `409` | `replacement_active`, `replacement_target_in_use`, `replacement_target_unavailable`, `replacement_no_eligible_provider`, `replacement_target_creating`, `replacement_source_not_current`, `replacement_source_running`, `replacement_source_outcome_unknown`, `replacement_superseded`, `replacement_not_retryable`, `replacement_task_running`, `replacement_idempotency_conflict`, `provider_speed_tests` |
| `503` | Filecoin storage or the on-chain approval check is unavailable. |

### Batches

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/commit-batches` | List batches. Accepts `status` (`collecting`, `ready`, `submitted`, `confirmed`, `abandoned`), `limit` (1 to 100, default 20), and `cursor`. |
| `GET` | `/api/v1/commit-batches/{id}` | Batch detail, including pieces and transaction. |
| `POST` | `/api/v1/commit-batches/{id}/seal` | Request early submission of the next batch (**Submit next batch**). Empty body. |
| `GET` | `/api/v1/storage-confirmations` | Batches that need attention. Accepts `status=needs_attention` and `limit`. |

Each entry from `storage-confirmations` has `request_id`, `task_id`, `provider_id`, `data_set_id`, `piece_count`, `piece_cids`, `transaction_id`, `submit_error` (the storage provider's reply), `reason_code`, `submitted_at`, and `attention_at`. A batch appears here when the storage provider rejected it or it is still unconfirmed 15 minutes after submission. Retry it with `POST /api/v1/tasks/{task_id}/retry`.

### Tasks

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | List tasks. Accepts `scope`, `type`, `status`, `limit`, and `cursor`. |
| `GET` | `/api/v1/tasks/stats` | Count tasks by status in a `scope`. |
| `GET` | `/api/v1/tasks/{id}` | A task and its retry policy. |
| `GET` | `/api/v1/tasks/{id}/history` | Earlier executions. Accepts `limit` and `cursor`. |
| `GET` | `/api/v1/tasks/{id}/events` | Key events. Accepts `limit` and `cursor`. |
| `GET` | `/api/v1/task-subjects/{subject_type}/{subject_key}` | What a task works on. |
| `POST` | `/api/v1/tasks/{id}/retry` | Retry a failed task. |
| `POST` | `/api/v1/tasks/{id}/acknowledge` | Acknowledge a failed task. |
| `GET` | `/api/v1/tasks/acknowledge/preview` | Count the failures a bulk acknowledgement would cover. |
| `POST` | `/api/v1/tasks/acknowledge` | Acknowledge failed tasks in bulk. |

`scope` is `work` (default: pending, running, and unacknowledged failed tasks) or `history` (completed, cancelled, acknowledged, and superseded executions). `status` accepts `pending`, `running`, or `failed` with `work`, and `completed`, `cancelled`, or `failed` with `history`; other values return `400`. Lists are newest first; pass `next_cursor` as `cursor` for the next page. Task types are listed in the [CLI Reference](./cli.md#tasks).

A task includes `type`, `operation`, `status` (`pending`, `running`, `completed`, `failed`, `cancelled`), `presentation_status` (`queued`, `scheduled`, or `waiting` for pending tasks), `retryable`, `retry_task_id`, `retry_unavailable_reason`, `acknowledgeable`, `last_error`, `started_at`, and `finished_at`. A **Commit batch** task whose batch needs attention also includes `storage_confirmation`; it cannot be acknowledged (`409`) and must be retried.

**Retry.** `POST /api/v1/tasks/{retry_task_id}/retry` takes no body and returns `202` with `{"task_id": <new-id>}`. Repeating it returns the same new task. It returns `404` for an unknown task, `409` with an explanation when retry is not safe, and `503` when the server is busy; retry the request then. Replica, replacement, and speed test entries elsewhere in the API expose `retryable` and `retry_task_id` for this endpoint.

**Acknowledge.** Acknowledging moves a failure to `history` without changing its result or whether it can be retried. The bulk endpoint takes an optional `type` and an optional RFC 3339 `failed_before` (default: the time of the request), and returns `acknowledged`. Unknown fields, an unknown `type`, an invalid time, or a body over 4 KiB return `400`. To acknowledge exactly what you counted, pass the preview's `as_of` as `failed_before`:

```bash
curl -s -u admin "http://127.0.0.1:9090/api/v1/tasks/acknowledge/preview?type=storage_store"
# {"count":12,"as_of":"2026-09-12T08:30:00.123456789Z"}
```

**Subjects.** `subject_type` is `storage_content`, `storage_copy`, `storage_data_set`, `bucket`, `provider`, `storage_replacement`, `wallet_operation`, or `storage_commit_request`. URL-encode each path segment. Wallet amounts are strings in USDFC base units.

### Wallet and Filecoin

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/wallet` | Wallet address, balances, payment account, and FWSS approval. |
| `POST` | `/api/v1/wallet/fund` | Deposit USDFC into the payment account. Body: `client_request_id`, `amount`. |
| `POST` | `/api/v1/wallet/withdraw` | Withdraw USDFC from the payment account. Body: `client_request_id`, `amount`. |
| `POST` | `/api/v1/wallet/approve` | Approve FWSS. Body: `client_request_id` only. |
| `GET` | `/api/v1/wallet/operations` | Recent wallet operations. |
| `GET` | `/api/v1/filecoin/readiness` | Filecoin readiness checks. |
| `POST` | `/api/v1/filecoin/readiness/preflight` | Check Filecoin settings before saving them. |

`amount` is a decimal string in USDFC base units. These endpoints send on-chain transactions that spend wallet funds.

### Settings and S3 Users

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v1/settings` | Saved settings, field metadata, and fields set by environment variables. |
| `PUT` | `/api/v1/settings` | Save settings. Applies after a restart. |
| `POST` | `/api/v1/settings/validate` | Validate settings without saving. |
| `GET` | `/api/v1/s3-users` | List S3 users. |
| `POST` | `/api/v1/s3-users` | Create an S3 user. |
| `PUT` | `/api/v1/s3-users/{accessKey}` | Change an S3 user's name or role. |
| `POST` | `/api/v1/s3-users/{accessKey}/secret` | Rotate the secret key. |
| `DELETE` | `/api/v1/s3-users/{accessKey}` | Delete an S3 user. |

`GET /api/v1/settings` also returns `runtime_filecoin_default_copies`, the default replica target the running process uses; `config.filecoin.default_copies` is the saved value for the next restart.

`POST /api/v1/s3-users` accepts `role` (`user`, `userplus`, `admin`; default `userplus`) and an optional `name`. `PUT` accepts either field; `"name": ""` clears the name. Names are at most 128 characters, contain no control characters, and are unique (case-sensitive); a duplicate returns `409`. The create and rotate responses contain the secret key, which is not shown again.

```bash
curl -X POST http://127.0.0.1:9090/api/v1/s3-users \
  -u admin \
  -H 'Content-Type: application/json' \
  -d '{"role":"userplus"}'
```

curl prompts for the Admin password.
