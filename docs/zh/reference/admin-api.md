---
title: Admin API
description: 查询 SynapS3 Admin API 的认证方式、端点、请求字段和错误码。
---

# Admin API

仪表盘和 `synaps3 admin` CLI 都使用 Admin API。基础 URL 是 `admin.addr`，默认 `http://127.0.0.1:9090`。除非另有说明，请求和响应都使用 JSON。错误返回 `{"error": "<message>"}`，下文列出的情况还会带 `code`。

如何安全暴露，见[网络暴露](../operations/production-checklist.md#网络暴露)。

## 认证

| 路径 | 认证方式 |
| --- | --- |
| `/healthz` | 无。 |
| `/api/v1/auth/login` | 请求体中的用户名和密码。 |
| `/api/v1/auth/session` | 浏览器会话 Cookie。没有有效会话时返回 `401`。 |
| `/api/v1/auth/refresh`、`/api/v1/auth/logout` | 浏览器会话 Cookie 和 CSRF 标头，不接受 Basic 认证。登出也接受已过期的 Cookie。 |
| 其他 `/api/v1/*` 路径和 `/metrics` | 浏览器会话 Cookie，或 HTTP Basic 认证。 |

仪表盘页面和静态文件无需认证即可加载，但其中的数据需要认证。

**浏览器会话。** 登录会设置 HttpOnly Cookie `synaps3_admin_session`，并返回 CSRF token。使用 Cookie 认证的 `POST`、`PUT`、`PATCH`、`DELETE` 请求必须在 `X-SynapS3-CSRF` 中携带它。登录、会话和续期响应包含 `username`、`csrf_token`、`expires_at` 和 `refresh_after`。登录时可选字段 `remember`（默认 `false`）会让 Cookie 持久保存，有效期取 30 天和 `admin.auth.session_ttl` 中较长的一个。在 `refresh_after` 之后续期会延长会话；更早续期则原样返回当前会话。登出会结束该会话及其所有续期 token。会话在重启后仍然有效，设置模式下创建的会话除外。

**Basic 认证。** 使用 `admin.auth.username` 和 Admin 密码，不需要 CSRF 标头。如果 `Sec-Fetch-Site`、`Origin` 或 `Referer` 标头显示请求来自跨站浏览器页面，Basic 认证请求会被拒绝。

同一客户端 IP 连续 5 次密码错误后，之后的尝试会在一分钟内被拒绝。位于反向代理之后时，只有代理地址在 `admin.trusted_proxies` 中，才会从转发标头中获取客户端 IP。

所有响应都带有 `Content-Security-Policy`、`X-Content-Type-Options: nosniff`、`X-Frame-Options: DENY` 和 `Referrer-Policy: strict-origin-when-cross-origin`。`/api/`、`/metrics` 和 `/healthz` 下的响应还带有 `Cache-Control: no-store`。

### 设置模式

`/healthz` 返回 `{"status":"setup"}` 时，只能使用这些端点：`/healthz`、`/api/v1/auth/*`、`GET` 和 `PUT /api/v1/settings`、`POST /api/v1/settings/validate`、`POST /api/v1/filecoin/readiness/preflight`，以及仪表盘页面。保存有效设置并重启 SynapS3 后，才能使用其余 API。

## 端点

### 认证

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/auth/login` | 用 `username`、`password` 和可选的 `remember` 登录。 |
| `GET` | `/api/v1/auth/session` | 返回当前会话。 |
| `POST` | `/api/v1/auth/refresh` | 延长会话。 |
| `POST` | `/api/v1/auth/logout` | 结束会话，返回 `204`。 |

### 健康检查与监控

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/healthz` | 健康状态，见[健康检查](../operations/health-metrics.md#健康检查)。 |
| `GET` | `/metrics` | Prometheus 指标，见 [Prometheus 指标](../operations/health-metrics.md#prometheus-指标)。 |
| `GET` | `/api/v1/system/info` | 版本、提交和运行时长。 |
| `GET` | `/api/v1/workers` | 后台任务状态。 |
| `GET` | `/api/v1/cache/stats` | 缓存用量和容量。 |
| `GET` | `/api/v1/overview` | 仪表盘概要。`tasks.attention.failed` 是未确认的失败任务数。 |
| `GET` | `/api/v1/monitoring` | 最近一小时的 CPU、内存和流量采样。 |
| `GET` | `/api/v1/events` | 仪表盘使用的服务器推送事件流。使用 Cookie 认证的事件流会在会话结束时关闭。 |

### 存储桶与对象

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/buckets` | 列出存储桶。 |
| `POST` | `/api/v1/buckets` | 创建存储桶。 |
| `GET` | `/api/v1/buckets/{name}` | 存储桶详情，包括数据集，以及最近 50 次存储提供方替换（`replacements`，最新的在前）。 |
| `PUT` | `/api/v1/buckets/{name}/owner` | 修改所有者（`owner_access_key`）。 |
| `PUT` | `/api/v1/buckets/{name}/copy-policy` | 修改副本设置。 |
| `DELETE` | `/api/v1/buckets/{name}` | 不支持，返回 `501`。 |
| `GET` | `/api/v1/buckets/{name}/objects` | 列出对象。 |
| `DELETE` | `/api/v1/buckets/{name}/objects` | 添加删除标记。 |
| `POST` | `/api/v1/buckets/{name}/objects/upload` | 上传对象。请求的 `Content-Type` 即对象的内容类型。 |
| `GET` | `/api/v1/buckets/{name}/objects/download` | 下载对象。 |
| `GET` | `/api/v1/buckets/{name}/objects/versions` | 列出版本。对象有版本时包含 `current_version_id`。 |
| `POST` | `/api/v1/buckets/{name}/objects/versions/restore` | 把较早的版本复制为新的当前版本。 |
| `GET` | `/api/v1/buckets/{name}/objects/provenance` | 对象版本的副本及其存储提供方。 |
| `GET` | `/api/v1/buckets/{name}/objects/status-detail` | 对象版本的详细存储状态。 |
| `GET` | `/api/v1/buckets/{name}/objects/deleted` | 列出已删除的对象。 |
| `GET` | `/api/v1/buckets/{name}/objects/deletions` | 列出删除标记。 |
| `POST` | `/api/v1/buckets/{name}/objects/restore` | 恢复已删除的对象。 |
| `POST` | `/api/v1/buckets/{name}/objects/permanent-delete` | 永久删除一个版本。 |
| `POST` | `/api/v1/buckets/{name}/objects/deleted/permanent-delete` | 永久删除一个已删除的对象。 |
| `GET` | `/api/v1/buckets/{name}/storage-health/affected-versions` | 受存储健康问题影响的版本。 |

#### 副本设置

`POST /api/v1/buckets` 接受 `name`、`owner_access_key`，以及可选的 `default_copies`、`minimum_durable_copies` 和 `provider_selection_strategy`（`distribution` 或 `speed`）。省略的值取自配置。之后配置变化时，存储桶保留自己的值。存储桶响应包含 `default_copies`、`minimum_durable_copies`、`provider_selection_strategy`、`status`，存储准备期间还包含 `setup_message`。见[目标副本数与最小持久副本数](../concepts/filecoin-storage-flow.md#目标副本数与最小持久副本数)。

`PUT /api/v1/buckets/{name}/copy-policy` 接受 `default_copies` 和 `minimum_durable_copies`：

- 省略的字段保持不变。`"default_copies": null` 把目标副本数重置为 `filecoin.default_copies`；`"minimum_durable_copies": null` 把最小值设为目标副本数。
- 取值必须在 `1` 到 `8` 之间，最小值不能超过修改后的目标副本数。
- 调低 `default_copies`（包括通过 `null` 重置导致的调低）返回 `400`。高于新目标的副本会继续运行并持续计费。
- 空请求或无效组合返回 `400`。

#### 永久删除

`permanent-delete` 接受 `key` 和 `version_id`；`deleted/permanent-delete` 接受 `key` 和 `delete_marker_version_id`。该版本的存储工作仍在进行、已提交的 Filecoin 交易等待确认，或该版本不符合删除条件时，两者都返回 `409`。从未提交过交易的失败存储工作不会阻止删除。无效请求返回 `400`；存储桶、对象或版本不存在时返回 `404`。只被已删除版本使用的远端副本会在后台移除。

#### 版本恢复

请求：

```json
{
  "key": "path/file.txt",
  "version_id": "source-version-id",
  "expected_current_version_id": "current-version-id"
}
```

`expected_current_version_id` 填版本列表中的 `current_version_id`。成功时响应包含 `key`、`source_version_id` 和新的 `version_id`；原有版本和删除标记保持不变。恢复同步执行，最长一小时，需要能容纳新版本的缓存空间。

| 状态码 | 原因 |
| --- | --- |
| `400` | 请求无效。 |
| `404` | 源版本不存在。 |
| `409` | 源版本是删除标记，或当前版本已变化；重新加载列表后再确认。带 `"code": "object_version_already_current"` 时，源版本与当前对象相同。 |
| `503` | 缓存空间不足。 |

### 存储提供方与替换

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/buckets/{name}/data-sets/{id}/replacement/providers` | 可以接管该副本的存储提供方，以及其他存储提供方不能接管的原因。 |
| `POST` | `/api/v1/buckets/{name}/data-sets/{id}/replacement` | 替换副本的存储提供方。 |
| `GET` | `/api/v1/filecoin/warm-storage/price-list` | 当前存储价格，以及用于确认替换的 `fingerprint`。 |
| `GET` | `/api/v1/observability/providers` | 存储提供方的健康状态、注册信息、approved 和 endorsed 成员资格，以及最新测速。 |
| `POST` | `/api/v1/observability/providers/refresh` | 重新检查存储提供方健康状态。 |
| `POST` | `/api/v1/observability/providers/{provider_id}/refresh` | 重新检查单个存储提供方的注册信息和健康状态。 |
| `POST` | `/api/v1/observability/provider-tiers/refresh` | 重新加载 approved 和 endorsed 列表。 |
| `POST` | `/api/v1/observability/providers/{provider_id}/upload-speed-test` | 开始测速。返回 `202` 和 `task_id`；存储提供方未知时返回 `404`；已有测速在运行或存储提供方不符合条件时返回 `409`。 |
| `POST` | `/api/v1/observability/providers/upload-speed-tests` | 为尚无结果、符合条件的存储提供方排队测速。返回 `202` 和 `queued`。 |
| `GET` | `/api/v1/observability/providers/upload-speed-summary` | 测速进度，以及是否可以选择存储提供方。 |
| `GET` | `/api/v1/observability/data-sets` | 本节点数据集的健康状态。 |
| `POST` | `/api/v1/observability/data-sets/refresh` | 重新检查数据集健康状态。 |

一次测速在 30 秒内最多上传 32 MiB，至少上传 8 MiB 才能得出结果。结果只对存储提供方当前的服务 URL 有效。存储提供方条目包含 `upload_speed_testable`、`upload_speed_unavailable_reason` 和 `upload_speed_test`（`bytes_per_second`、`duration_ms`、`sample_bytes`、`tested_at` 和 `state`；同一 URL 重测期间 `state` 为 `testing`，URL 变化后为 `stale`）。失败的测速包含 `retry_task_id`。健康检查失败时，`signal.last_error` 给出原因。

测速概要返回 `auto_scope`、`trusted` 和 `other` 计数（`total`、`succeeded`、`failed`、`testing`、`untested`）、`selectable_providers`、`required_copies`、`ready`、`can_provision_default` 和 `can_test_remaining`。

#### 替换请求

```json
{ "mode": "automatic", "client_request_id": "019d2e22-8c36-7d5b-a6be-5f7fa6d6f584", "price_list_fingerprint": "<fingerprint>" }
```

```json
{ "mode": "manual", "provider_id": "202", "client_request_id": "019d2e22-8c36-7d5b-a6be-5f7fa6d6f584", "price_list_fingerprint": "<fingerprint>" }
```

- `client_request_id`（1 到 128 个字符）让请求具有幂等性。首次请求返回 `201`；重复同一请求返回原记录和 `200`。同一 ID 搭配不同参数返回 `409` 和 `replacement_idempotency_conflict`。
- `price_list_fingerprint` 取自价格表。价格变化时请求失败，返回 `price_list_changed`；`price_list_unavailable` 和 `unsupported_price_token` 也会阻止请求。重新加载价格表，并使用新的请求 ID。
- 自动模式不会选择该存储桶用过的存储提供方。仍有符合条件的存储提供方在测速时，返回 `409` 和 `provider_speed_tests`。
- 手动模式接受健康检查新鲜、测速成功、且当前未持有该存储桶副本的存储提供方。为该存储桶保存的所有数据集都已退役的存储提供方可以再次选择。
- 两种模式都遵守 `filecoin.anchor_provider_tier` 的要求。
- 所选存储提供方在链上已持有该存储桶的数据集时，替换会在副本迁移前以 `failed` 停止。请换一个存储提供方重新确认。

候选列表（`replacement/providers`）包含所有有健康检查结果的存储提供方，带有 `manual_selectable` 和 `manual_block_reason`：`current_source`、`already_serves_bucket`、`provider_unavailable`、`observation_stale`、`profile_missing`、`profile_url_changed`、`upload_speed_untested` 或 `upload_speed_failed`。

存储桶响应中的数据集条目包含 `replaceable`、`replacement_blocked_reason` 和 `setup_error`。副本的数据集已就绪，或其创建任务已失败时，可以替换该副本。

替换记录的 `source.termination` 和 `target.termination` 包含最新交易哈希、终止 `epoch`，以及记录各次终止尝试的 `attempts` 数组。各次尝试的 `receipt_status` 为 `0`（失败）、`1`（成功）或 `null`（未知）；`confirmed_tx_hash` 表示已上链的交易，不代表执行成功。`unsent_at` 不为空表示该次尝试没有发出交易。

终止交易回执失败后，Retry 会先检查服务；如果服务仍未终止，可能提交一笔新的付费交易。结果未知时，Retry 只检查已记录的尝试；但如果经存储提供方发送的终止一直没有执行，且服务仍未终止，Retry 会改由付款钱包发送终止交易。如果什么都没有发出（例如付款账户欠费），解决原因后 Retry 会重新提交终止。

#### 替换状态

| `status` | 含义 |
| --- | --- |
| `preparing_target` | 正在创建新数据集。 |
| `migrating` | 新上传发往新存储提供方，同时复制已存储的数据。 |
| `waiting` | 暂停，等待条件满足；见 `wait_reason` 和 `wait_message`。 |
| `retiring` | 复制完成，正在终止旧数据集。 |
| `cleanup_attention` | 终止旧数据集需要运维人员处理，例如结清付款。 |
| `failed` | 已停止。按 `retryable` 和错误信息处理。`failure_reason` 为 `target_in_use`（需换一个存储提供方）或 `target_rejected`。 |
| `completed` | 每个保留的版本都能从新存储提供方读取，旧数据集已终止。 |
| `superseded` | 同一副本之后的确认取代了此请求。 |

`wait_reason` 取值：`target_creating`、`target_writable`、`target`（新存储提供方不可达）、`funding`、`readable_source`（没有可复制的副本或缓存）、`provider`、`source_writes`、`coverage` 和 `termination_epoch`。多数等待会自行解除。

重试时，把 `retry_task_id` 传给 `POST /api/v1/tasks/{id}/retry`，重试会继续使用同一个存储提供方。要改选其他存储提供方，需要重新确认一次替换；这只能在旧存储提供方仍持有该副本时进行。新存储提供方接管后，再次确认会返回 `replacement_source_not_current`（旧副本）或 `replacement_active`（新副本），此时只能通过重试完成这次替换。

`progress` 报告 `phase`（`prepare`、`migrate`、`retire` 或 `none`）、`seeding_complete`、`items_total`、`items_processed`（`items_copied` + `items_no_longer_needed`）、`percent`（仅在 `seeding_complete` 后提供）、当前计数 `items_pending`、`items_active`、`items_retrying`、`items_waiting_source`、`items_failed` 和 `items_attention`，以及 `next_retry_at`。这些计数按唯一存储内容统计，不按对象版本统计。

#### 替换错误

| 状态码 | Code |
| --- | --- |
| `400` | `replacement_target_invalid`、`replacement_target_unavailable`（手动模式） |
| `404` | 存储桶、数据集或替换不存在。 |
| `409` | `replacement_active`、`replacement_target_in_use`、`replacement_target_unavailable`、`replacement_no_eligible_provider`、`replacement_target_creating`、`replacement_source_not_current`、`replacement_source_running`、`replacement_source_outcome_unknown`、`replacement_superseded`、`replacement_not_retryable`、`replacement_task_running`、`replacement_idempotency_conflict`、`provider_speed_tests` |
| `503` | Filecoin 存储或链上批准检查不可用。 |

### 批次

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/commit-batches` | 列出批次。接受 `status`（`collecting`、`ready`、`submitted`、`confirmed`、`abandoned`）、`limit`（1 到 100，默认 20）和 `cursor`。 |
| `GET` | `/api/v1/commit-batches/{id}` | 批次详情，包括 piece 和交易。 |
| `POST` | `/api/v1/commit-batches/{id}/seal` | 请求提前提交下一批（**Submit next batch**）。请求体为空。 |
| `GET` | `/api/v1/storage-confirmations` | 需要处理的批次。接受 `status=needs_attention` 和 `limit`。 |

`storage-confirmations` 的每个条目包含 `request_id`、`task_id`、`provider_id`、`data_set_id`、`piece_count`、`piece_cids`、`tx_hash`、`confirmed_tx_hash`、`submit_error`（存储提供方的回复）、`reason_code`、`submitted_at` 和 `attention_at`。`storage-confirmations` 和 `commit-batches` 响应中的 `tx_hash` 表示提交交易的 hash，`confirmed_tx_hash` 表示链上确认交易的 hash；未知值为 `null`。存储提供方拒绝批次，或批次提交 15 分钟后仍未确认时，会出现在 `storage-confirmations` 中。用 `POST /api/v1/tasks/{task_id}/retry` 重试。

### 任务

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | 列出任务。接受 `scope`、`type`、`status`、`limit` 和 `cursor`。 |
| `GET` | `/api/v1/tasks/stats` | 按状态统计某个 `scope` 中的任务数。 |
| `GET` | `/api/v1/tasks/{id}` | 任务及其重试策略。 |
| `GET` | `/api/v1/tasks/{id}/history` | 之前的执行。接受 `limit` 和 `cursor`。 |
| `GET` | `/api/v1/tasks/{id}/events` | 关键事件。接受 `limit` 和 `cursor`。 |
| `GET` | `/api/v1/task-subjects/{subject_type}/{subject_key}` | 任务处理的对象。 |
| `POST` | `/api/v1/tasks/{id}/retry` | 重试失败的任务。 |
| `POST` | `/api/v1/tasks/{id}/acknowledge` | 确认失败的任务。 |
| `GET` | `/api/v1/tasks/acknowledge/preview` | 统计批量确认会覆盖多少失败。 |
| `POST` | `/api/v1/tasks/acknowledge` | 批量确认失败的任务。 |

`scope` 为 `work`（默认：等待中、运行中和未确认的失败任务）或 `history`（已完成、已取消、已确认和已被取代的执行）。`work` 下 `status` 可取 `pending`、`running`、`failed`；`history` 下可取 `completed`、`cancelled`、`failed`；其他值返回 `400`。列表按从新到旧排列；把 `next_cursor` 作为 `cursor` 传入即可获取下一页。任务类型见 [CLI 参考](./cli.md#任务)。

任务包含 `type`、`operation`、`status`（`pending`、`running`、`completed`、`failed`、`cancelled`）、`presentation_status`（等待中的任务为 `queued`、`scheduled` 或 `waiting`）、`retryable`、`retry_task_id`、`retry_unavailable_reason`、`acknowledgeable`、`last_error`、`started_at` 和 `finished_at`。批次需要处理的 **Commit batch** 任务还包含 `storage_confirmation`；这类任务不能确认（返回 `409`），只能重试。

**重试。** `POST /api/v1/tasks/{retry_task_id}/retry` 不带请求体，返回 `202` 和 `{"task_id": <new-id>}`。重复请求返回同一个新任务。任务不存在时返回 `404`；重试不安全时返回 `409` 并说明原因；服务繁忙时返回 `503`，稍后重发请求即可。API 中副本、替换和测速条目提供的 `retryable` 和 `retry_task_id` 都用于此端点。

**确认。** 确认会把失败移到 `history`，不改变其结果，也不影响能否重试。批量端点接受可选的 `type` 和可选的 RFC 3339 时间 `failed_before`（默认：收到请求的时间），返回 `acknowledged`。包含未知字段、未知 `type`、无效时间或请求体超过 4 KiB 时返回 `400`。要确认的范围与统计结果完全一致，把预览返回的 `as_of` 作为 `failed_before` 传入：

```bash
curl -s -u admin "http://127.0.0.1:9090/api/v1/tasks/acknowledge/preview?type=storage_store"
# {"count":12,"as_of":"2026-09-12T08:30:00.123456789Z"}
```

**任务对象。** `subject_type` 为 `storage_content`、`storage_copy`、`storage_data_set`、`bucket`、`provider`、`storage_replacement`、`wallet_operation` 或 `storage_commit_request`。每个路径段分别做 URL 编码。钱包金额是以 USDFC 最小单位表示的字符串。

### 钱包与 Filecoin

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/wallet` | 钱包地址、余额、支付账户和 FWSS 批准状态。 |
| `POST` | `/api/v1/wallet/fund` | 向支付账户存入 USDFC。请求体：`client_request_id`、`amount`。 |
| `POST` | `/api/v1/wallet/withdraw` | 从支付账户取出 USDFC。请求体：`client_request_id`、`amount`。 |
| `POST` | `/api/v1/wallet/approve` | 批准 FWSS。请求体只有 `client_request_id`。 |
| `GET` | `/api/v1/wallet/operations` | 最近的钱包操作。 |
| `GET` | `/api/v1/filecoin/readiness` | Filecoin 就绪检查。 |
| `POST` | `/api/v1/filecoin/readiness/preflight` | 保存前检查 Filecoin 设置。 |

`amount` 是以 USDFC 最小单位表示的十进制字符串。这些端点会发送链上交易，动用钱包资金。

### 设置与 S3 用户

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/settings` | 已保存的设置、字段元数据，以及由环境变量设置的字段。 |
| `PUT` | `/api/v1/settings` | 保存设置，重启后生效。 |
| `POST` | `/api/v1/settings/validate` | 校验设置但不保存。 |
| `GET` | `/api/v1/s3-users` | 列出 S3 用户。 |
| `POST` | `/api/v1/s3-users` | 创建 S3 用户。 |
| `PUT` | `/api/v1/s3-users/{accessKey}` | 修改 S3 用户的名称或角色。 |
| `POST` | `/api/v1/s3-users/{accessKey}/secret` | 轮换 secret key。 |
| `DELETE` | `/api/v1/s3-users/{accessKey}` | 删除 S3 用户。 |

`GET /api/v1/settings` 还返回 `runtime_filecoin_default_copies`，即当前进程使用的默认目标副本数；`config.filecoin.default_copies` 是下次重启后生效的已保存值。

`POST /api/v1/s3-users` 接受 `role`（`user`、`userplus`、`admin`；默认 `userplus`）和可选的 `name`。`PUT` 接受其中任一字段；`"name": ""` 清除名称。名称最多 128 个字符，不能包含控制字符，且不能重复（区分大小写）；重复时返回 `409`。创建和轮换的响应中包含 secret key，之后不再显示。

```bash
curl -X POST http://127.0.0.1:9090/api/v1/s3-users \
  -u admin \
  -H 'Content-Type: application/json' \
  -d '{"role":"userplus"}'
```

curl 会提示输入 Admin 密码。
