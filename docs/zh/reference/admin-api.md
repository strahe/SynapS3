---
title: Admin API
description: SynapS3 健康检查、指标、仪表盘、设置、钱包、任务和 S3 用户端点参考。
---

# Admin API

Admin API 供仪表盘和 CLI 使用。Admin 认证默认开启。默认监听本机回环地址；远程访问应使用 HTTPS 反向代理或 SSH 隧道。

默认 base URL：

```text
http://127.0.0.1:9090
```

## Setup 模式

当 `/healthz` 返回 `{"status":"setup"}` 时，Admin 端点只开放完成配置所需的范围：

- `/healthz`，以及 Admin 登录、会话、续期和退出端点；
- 仪表盘外壳；
- `GET /api/v1/settings`、`PUT /api/v1/settings` 和 `POST /api/v1/settings/validate`；
- `POST /api/v1/filecoin/readiness/preflight`。

Setup 模式不提供运行时指标、存储桶、对象、后台任务、钱包操作、存储健康状态和 S3 用户管理。保存有效设置后重启 SynapS3；确认 `/healthz` 返回 `{"status":"ok"}`，再使用完整 API。

## 认证模型

`/healthz` 不需要认证，便于进程健康检查。仪表盘外壳和静态资源可以在登录前加载，但仪表盘数据和写操作都由 Admin 认证保护。

| 访问范围 | 认证要求 |
| --- | --- |
| `/healthz` | 无。 |
| `/api/v1/auth/login`、`/api/v1/auth/session` | 登录和会话端点。没有有效浏览器会话时，`/session` 返回 `401`。 |
| `/api/v1/auth/refresh`、`/api/v1/auth/logout` | 需要有效浏览器会话和 CSRF header；不接受 HTTP Basic auth。 |
| `/api/v1/*` | 浏览器 session cookie；写请求方法需要 CSRF。也可用 HTTP Basic auth。 |
| `/metrics` | 浏览器 session cookie 或 HTTP Basic auth。 |
| `/admin/exhausted-tasks*` | 浏览器 session cookie；写请求方法需要 CSRF。也可用 HTTP Basic auth。 |

### 浏览器会话

浏览器登录会设置 `synaps3_admin_session` HttpOnly cookie，并返回 CSRF token。使用 cookie 认证的 `POST`、`PUT`、`PATCH`、`DELETE` 必须带 `X-SynapS3-CSRF`。续期和退出只接受浏览器会话。

登录请求中的可选布尔字段 `remember` 默认为 `false`。普通登录使用 browser-session cookie 和配置的 `admin.auth.session_ttl`。`remember = true` 时，cookie 会持久化 30 天或配置的 session TTL，以较长者为准。部分浏览器在恢复上次浏览会话时也会恢复 browser-session cookie。

登录、会话和续期响应包含 `username`、`csrf_token`、`expires_at` 和 `refresh_after`。任何持有有效 session cookie 和对应 CSRF token 的客户端，都可以在 `refresh_after` 之后请求续期。登录没有绝对时长上限。没有客户端请求续期时，token 会在 `expires_at` 到期。

续期会保留会话时长、CSRF token 和登录 family。退出会撤销整个 family，包括最近一次续期之前签发的 token。撤销记录保存在内存中；重启 SynapS3 会清空这些记录，但正常退出时浏览器 cookie 仍会被删除。

### CLI 和 Basic auth

CLI 和脚本可以使用 HTTP Basic auth，不需要 CSRF header。如果浏览器发起的 Basic auth 请求通过 `Sec-Fetch-Site`、`Origin` 或 `Referer` 暴露出跨站来源，请求会被拒绝。没有浏览器来源标头的请求仍按 CLI/脚本处理。

密码失败会按解析后的客户端 IP 限流；达到限制后，后续登录尝试会被拒绝一段时间。

### 反向代理

SynapS3 位于反向代理之后时，只有代理 IP 或 CIDR 配置在 `admin.trusted_proxies` 中，才会使用转发的客户端、协议和主机 header。除非代理会清理不可信的 `X-Forwarded-For`、`X-Real-IP`、`X-Forwarded-Proto` 和 `X-Forwarded-Host`，否则保持为空。

### Admin 凭据

Admin 凭据由 `synaps3 init` 创建。交互式 init 只打印一次密码。非交互和 Docker init 会把密码写到运行数据目录下的 `admin-initial-password`，文件权限为 `0600`。本地 `synaps3 admin` 命令按 `--config`、`SYNAPS3_CONFIG`、默认路径定位配置，再优先使用 `SYNAPS3_ADMIN_PASSWORD`，其次读取配置文件同目录的 `admin-initial-password`，最后提示输入。

重置密码会同时轮换 `admin.auth.session_secret`，使已有浏览器会话失效。离线重置密码：

```bash
synaps3 admin-auth reset-password --config /var/lib/synaps3/config.toml
```

## 认证端点

| Method | Path | 用途 |
| --- | --- | --- |
| `POST` | `/api/v1/auth/login` | 校验用户名和密码，接受可选的 `remember`，设置浏览器 cookie，并返回会话。 |
| `GET` | `/api/v1/auth/session` | 返回当前浏览器会话。 |
| `POST` | `/api/v1/auth/refresh` | 需要会话和 CSRF；会话允许续期时重新签发，过早请求只返回当前会话且不修改 cookie。 |
| `POST` | `/api/v1/auth/logout` | 需要会话和 CSRF，结束当前浏览器会话并清除 cookie。 |

退出登录或收到 `401` API 响应后，仪表盘会返回登录页。

## 安全标头

Admin 响应包含 `Content-Security-Policy`、`X-Content-Type-Options: nosniff`、`X-Frame-Options: DENY` 和 `Referrer-Policy: strict-origin-when-cross-origin`。默认内容策略让仪表盘资源保持同源。`/api/`、`/admin/`、`/metrics` 和 `/healthz` 还会发送 `Cache-Control: no-store`。仪表盘页面资源不受该标头影响。

## 高风险操作

把这些端点当作变更窗口操作处理。它们可能改变数据、凭据、钱包支付状态或后台状态。

| 范围 | 端点 | 风险 |
| --- | --- | --- |
| 设置 | `PUT /api/v1/settings` | 变更可能需要重启，或把节点切换到不同 Filecoin 网络。保存前先验证设置和 Filecoin readiness。 |
| 钱包 | `POST /api/v1/wallet/fund`、`POST /api/v1/wallet/withdraw`、`POST /api/v1/wallet/approve` | 创建链上支付操作。 |
| S3 用户 | `POST /api/v1/s3-users`、`PUT /api/v1/s3-users/{accessKey}`、`POST /api/v1/s3-users/{accessKey}/secret`、`DELETE /api/v1/s3-users/{accessKey}` | 改变客户端访问权限，或让已有凭据失效。 |
| 存储桶和对象 | 创建存储桶、更新 owner/copy-policy，以及上传、下载、删除、恢复或永久删除对象 | 改变或暴露用户可见的 S3 数据和元数据。 |
| 后台任务和存储健康 | 任务重试、诊断刷新、存储提供方和数据集刷新 | 重新入队任务，或刷新运维状态。 |
| 存储提供方替换 | `POST /api/v1/buckets/{name}/data-sets/{id}/replacement`、`POST /api/v1/storage-replacements/{id}/retry` | 创建新的付费存储服务，把副本迁移过去，并终止旧服务。 |
| 存储确认 | `POST /api/v1/storage-confirmations/{copy-id}/release` | 可能允许存储提供方再次存储同一个 piece。释放前必须核对当前 attempt。 |

## 健康检查和指标

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/healthz` | 数据库、缓存和后台任务健康状态。 |
| `GET` | `/metrics` | Prometheus 指标。需要 Admin 认证。 |
| `GET` | `/api/v1/system/info` | 版本和运行时信息。 |
| `GET` | `/api/v1/workers` | 后台任务活动和健康状态。 |
| `GET` | `/api/v1/cache/stats` | 缓存使用量和容量。 |

## 仪表盘数据

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/overview` | 仪表盘摘要。 |
| `GET` | `/api/v1/events` | 仪表盘事件流。 |
| `GET` | `/api/v1/buckets` | 列出存储桶。 |
| `POST` | `/api/v1/buckets` | 创建存储桶。 |
| `GET` | `/api/v1/buckets/{name}` | 读取存储桶详情。 |
| `PUT` | `/api/v1/buckets/{name}/owner` | 更新存储桶 owner。 |
| `PUT` | `/api/v1/buckets/{name}/copy-policy` | 更新目标副本数和/或缓存释放门槛。 |
| `DELETE` | `/api/v1/buckets/{name}` | 不支持，返回 `501 Not Implemented`。 |
| `GET` | `/api/v1/buckets/{name}/objects` | 列出对象。 |
| `DELETE` | `/api/v1/buckets/{name}/objects` | 创建对象 delete marker。 |
| `POST` | `/api/v1/buckets/{name}/objects/upload` | 通过仪表盘上传对象。 |
| `GET` | `/api/v1/buckets/{name}/objects/download` | 通过仪表盘下载对象。 |
| `GET` | `/api/v1/buckets/{name}/objects/versions` | 列出对象版本，并返回当前版本 token。 |
| `POST` | `/api/v1/buckets/{name}/objects/versions/restore` | 基于已有数据版本创建新的当前版本。 |
| `GET` | `/api/v1/buckets/{name}/objects/provenance` | 查看对象存储来源。 |
| `GET` | `/api/v1/buckets/{name}/objects/status-detail` | 读取对象详细状态。 |
| `GET` | `/api/v1/buckets/{name}/objects/deleted` | 列出已删除对象。 |
| `GET` | `/api/v1/buckets/{name}/objects/deletions` | 列出对象 delete markers。 |
| `POST` | `/api/v1/buckets/{name}/objects/restore` | 从 delete marker 恢复对象。 |
| `POST` | `/api/v1/buckets/{name}/objects/permanent-delete` | 永久删除对象版本。 |
| `POST` | `/api/v1/buckets/{name}/objects/deleted/permanent-delete` | 永久删除已删除对象版本。 |
| `GET` | `/api/v1/buckets/{name}/storage-health/affected-versions` | 列出受存储健康问题影响的版本。 |
| `GET` | `/api/v1/buckets/{name}/data-sets/{id}/replacement/providers` | 列出该副本可以迁往的存储提供方，以及其他存储提供方不能接管的原因。 |
| `POST` | `/api/v1/buckets/{name}/data-sets/{id}/replacement` | 授权替换某个副本背后的存储提供方。 |
| `POST` | `/api/v1/storage-replacements/{id}/retry` | 恢复处于 `failed` 或 `cleanup_attention` 的存储提供方替换。 |
| `GET` | `/api/v1/storage-confirmations` | 列出需要运营者处理的存储确认。 |
| `POST` | `/api/v1/storage-confirmations/{copy-id}/release` | 确认可能产生重复存储后，释放一条无法判定的确认。 |

对象上传时，HTTP `Content-Type` 表示上传对象的内容类型，不是 JSON 请求标记。

### 存储桶副本策略

`POST /api/v1/buckets` 接受可选的 `default_copies` 和 `minimum_durable_copies` 字段。存储桶列表、详情、创建和策略更新响应包含：

- `minimum_durable_copies`：存储桶显式设置的值；`null` 表示按每次上传采用严格策略；
- `effective_minimum_durable_copies`：将当前存储桶门槛限制在当前目标副本数以内后，用于展示的值。

`PUT /api/v1/buckets/{name}/copy-policy` 可以独立接收 `default_copies` 和 `minimum_durable_copies`。字段缺省时保持不变。`default_copies: null` 表示新上传继承当前运行时目标副本数。`minimum_durable_copies: null` 表示必须完成单次上传冻结的所有副本后才能释放缓存。显式门槛必须在 `1` 到 `8` 之间，且不能超过同一请求产生的最终目标副本数。空请求或无效的最终组合返回 `400 Bad Request`。

目标副本数变更只影响新上传。最低耐久副本数变更还会重新评估当前上传仍保留的缓存。提高门槛无法恢复已经删除的缓存。

### 永久删除对象版本

`POST /api/v1/buckets/{name}/objects/permanent-delete` 接受 `key` 和 `version_id`。`POST /api/v1/buckets/{name}/objects/deleted/permanent-delete` 接受 `key` 和 `delete_marker_version_id`。

如果将被删除的任一版本仍有存储工作在进行，或已提交的 Filecoin 交易仍在等待确认，两个端点会立即返回 `409 Conflict`。请检查相关任务后重试。对于其他条件均符合永久删除要求的数据版本，已停止且尚未提交交易的存储工作不会阻止删除。所选版本或已删除对象不再符合永久删除条件时，同样返回 `409 Conflict`；无效请求返回 `400 Bad Request`，存储桶、对象或版本不存在时返回 `404 Not Found`。

删除成功后，仅由已删除版本使用的远程存储会排入后台清理；仍被其他版本共享的存储会保留。

### 恢复对象版本

对象存在版本历史时，`GET /api/v1/buckets/{name}/objects/versions` 的每一页都会返回 `current_version_id`。确认恢复时把该值传回服务端：

```json
{
  "key": "path/file.txt",
  "version_id": "source-version-id",
  "expected_current_version_id": "current-version-id"
}
```

`POST /api/v1/buckets/{name}/objects/versions/restore` 会把选中的历史数据版本复制为同一存储桶、同一对象键下的新版本。已有数据版本和 delete marker 不会被修改或删除。选中版本必须与当前可读对象的表示不同；直接选择当前版本，或选择与当前版本等价的历史版本，请求会被拒绝，并且不会改变对象。当前版本失败或不可用时，仍可使用可读的历史版本恢复。

成功响应：

```json
{
  "key": "path/file.txt",
  "source_version_id": "source-version-id",
  "version_id": "new-current-version-id"
}
```

选中 delete marker，或 `expected_current_version_id` 已不再是当前版本时，请求返回 `409 Conflict`。刷新版本列表后，使用新的 token 重新确认。如果选中版本已经与当前对象一致，响应会返回稳定错误码，客户端可将其作为无操作处理：

```json
{
  "error": "selected version already matches the current object",
  "code": "object_version_already_current"
}
```

源版本不存在或已被永久删除时返回 `404 Not Found`；缓存容量不足时返回 `507 Insufficient Storage`；输入无效时返回 `400 Bad Request`；源版本读取失败或内部错误返回 `500 Internal Server Error`。

恢复操作同步流式执行，最长一小时，并且缓存必须能容纳新的目标版本。

### 替换存储提供方

`POST /api/v1/buckets/{name}/data-sets/{id}/replacement` 是替换某个副本背后存储提供方的唯一入口。一次确认即授权全部动作：新建付费存储服务、把新上传切换过去、复制已有数据，并在每个保留版本都能从新存储提供方读取之后关闭旧存储提供方。对象从其他副本或本地缓存复制。两者都没有的对象无法复制，旧存储提供方也不会被关闭。

自动选择新的存储提供方：

```json
{ "mode": "automatic", "client_request_id": "019d2e22-8c36-7d5b-a6be-5f7fa6d6f584" }
```

自动选择会排除该存储桶用过的所有存储提供方，包括已退休的。也可以指定存储提供方：

```json
{ "mode": "manual", "provider_id": "202", "client_request_id": "019d2e22-8c36-7d5b-a6be-5f7fa6d6f584" }
```

指定的存储提供方必须出现在完整的可用、活跃且支持 PDP 的存储提供方清单中。它可以是该存储桶以前用过的，前提是那次服务已经退休。正在被替换的存储提供方，以及任何仍持有该存储桶活跃代的存储提供方，都会被拒绝。

`client_request_id` 为必填项，trim 后长度必须为 1–128 个字符。首次成功返回 `201 Created`。同一存储桶、来源、模式和手动存储提供方使用同一个 ID 精确重放时，会返回原记录和 `200 OK`，即使副本已经切换也一样。同一个 ID 携带不同参数会返回 `409 Conflict` 和 `replacement_idempotency_conflict`。自动模式的重放会在读取存储提供方清单前命中原记录，因此清单后续变化不会改选存储提供方。

对同一副本再次确认会取代先前的请求并返回 `201 Created`，不是冲突。先前请求里尚未使用的存储提供方会被关闭。`replacement_active` 表示的是另一件事：该副本是另一次未完成替换的目标，必须先处理那一次。

只有当前接收写入的副本可以被替换；历史代在 `GET /api/v1/buckets/{name}` 中返回 `"replaceable": false`。

首次确认成功返回 `201 Created` 和替换记录；精确重放返回 `200 OK`。`GET /api/v1/buckets/{name}` 在 `replacements` 中返回该存储桶最近的替换记录，最新的在前，最多 50 条。

替换会经历以下状态：

| 状态 | 含义 |
| --- | --- |
| `preparing_target` | 正在创建新服务。写入仍然发往当前存储提供方。 |
| `migrating` | 新存储提供方开始接收新上传，同时复制已有数据。 |
| `waiting` | 已暂停。`wait_reason` 与 `wait_message` 会区分服务创建（`target_creating`）、可写确认（`target_writable`）、存储提供方不可达（`target`）、资金、来源可用性和安全退休等待。多数等待无需操作即可继续。 |
| `retiring` | 数据已复制完毕，正在终止旧服务。 |
| `cleanup_attention` | 终止旧服务需要运营者决定，例如结清欠费。 |
| `failed` | 重试次数用尽，需要重新发起。 |
| `completed` | 旧服务已终止，该副本已落在新存储提供方上。 |
| `superseded` | 更晚的一次确认取代了本次请求。 |

`last_error` 只在 `failed` 和 `cleanup_attention` 时设置，重试会清空它。等待状态从不设置它，因为等待不是失败。失败响应还可能包含 `failure_reason`。`target_in_use` 对当前已批准目标是永久失败：需要改选存储提供方，重试接口会返回冲突。

`items_total` 与 `items_copied` 统计的是唯一的已存储内容，而不是对象版本：被多个版本共享的内容只复制一次。迁移期间删除的内容已不再需要，不会算作已复制。复制结束后，响应会分别说明已复制的内容，以及已无需迁移的内容；`items_copied/items_total` 不是完成百分比。确认页统计的是引用版本数和数据量。

每条替换记录还包含嵌套的 `progress` 对象。发现内容期间，`seeding_complete` 为 `false`，`items_total` 只是当前已发现数量，并且省略 `percent`。发现完成后，`items_total` 才是最终总数，`percent` 按 `items_processed / items_total` 计算，其中 `items_processed = items_copied + items_no_longer_needed`。因此，即使部分内容在复制前已删除，完成状态仍会达到 100%。`items_pending`、`items_active`、`items_retrying`、`items_waiting_source`、`items_failed` 与 `items_attention` 返回互斥的当前工作数量。等待确认的工作只计入 `items_active`；需要运营者处理的工作只计入 `items_attention`，两者都属于尚未完成的工作。存在未来的重试时返回 `next_retry_at`。`phase` 取 `prepare`、`migrate`、`retire` 或 `none`。

`POST /api/v1/storage-replacements/{id}/retry` 在同一个已批准的存储提供方上恢复 `failed` 或 `cleanup_attention` 的替换。

更换存储提供方需要重新确认，且仅在旧存储提供方仍持有该副本时可用。新存储提供方接管副本之后，两代各自持有对方没有的数据，因此对任意一代再次确认都会被拒绝（旧代返回 `replacement_source_not_current`，新代返回 `replacement_active`），此时只能用重试完成已批准的复制。

冲突返回 `409 Conflict` 并附带稳定的 code：

```json
{
  "error": "that provider already stores a replica of this bucket",
  "code": "replacement_target_in_use"
}
```

这些 code 包括 `replacement_active`、`replacement_target_in_use`、`replacement_target_unavailable`、`replacement_no_eligible_provider`、`replacement_source_not_current`、`replacement_superseded`、`replacement_not_retryable`、`replacement_task_running` 和 `replacement_idempotency_conflict`。无效存储提供方选择返回 `400 Bad Request` 和 `replacement_target_invalid`；当前不可用的手动目标返回 `400` 和 `replacement_target_unavailable`；未知的存储桶、数据集或替换记录返回 `404 Not Found`；存储服务不可用返回 `503 Service Unavailable`；内部失败返回 `500 Internal Server Error`。

`GET /api/v1/buckets/{name}/data-sets/{id}/replacement/providers` 列出当前探测为可用的存储提供方，附带 `eligible`、取值为 `current_source` 或 `already_serves_bucket` 的 `ineligible_reason`，以及标记该存储桶用过并已完全退休的 `previously_used`。不能接管该副本的存储提供方会照常列出而不是省略，便于运营者看清预期中的存储提供方为何不可用。这份清单与存储拓扑页在 `Available` 过滤下读取的是同一份：在那里可用的存储提供方这里会提供，探测不通的两边都不会出现。可选性判定与确认阶段完全一致。自动选择更严格：它绝不会回到该存储桶用过的存储提供方，而手动选择可以。

确认阶段只能检查 SynapS3 已记录的信息。如果某个存储提供方在链上仍为该存储桶运行着存储服务，会在替换准备目标时被发现：替换停在 `failed`，并写明存储提供方与数据集，运营者改选另一个存储提供方重新确认即可。此时副本尚未迁移，没有任何风险。此前已正常退休的存储提供方可以再次选择。

### 存储确认处理

当 SynapS3 无法判定存储提供方是否已接受 piece 时，不会自动再次提交该 piece。`GET /api/v1/storage-confirmations?status=needs_attention&limit=100` 会列出受影响的 copy、data set、attempt、已知 transaction、时间和稳定的 `reason_code`。

`POST /api/v1/storage-confirmations/{copy-id}/release` 会让正常恢复继续，并可能产生重复提交。先核对清单中的当前记录，再同时发送该记录的 attempt ID 和明确的风险确认：

```json
{
  "expected_attempt_id": "current-attempt-id",
  "acknowledge_possible_duplicate": true
}
```

如果核对后 attempt 已发生变化，API 返回 `409 Conflict`。如果确认在人工释放前自行成功，该记录会自动消失。

## 任务

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | 列出后台任务。支持 `type`、`stage`、`status`、`limit`、`offset` 等过滤。 |
| `GET` | `/api/v1/tasks/stats` | 按状态统计任务。 |
| `GET` | `/api/v1/tasks/{id}/ref-detail` | 解析后台任务关联的对象或存储操作。 |
| `GET` | `/api/v1/tasks/{id}/diagnostic` | 读取任务诊断。 |
| `POST` | `/api/v1/tasks/{id}/diagnostic/refresh` | 刷新诊断。 |
| `POST` | `/api/v1/tasks/{id}/retry` | 重试 exhausted 任务。 |
| `GET` | `/admin/exhausted-tasks` | 列出 exhausted 任务。支持最大为 `1000` 的 `limit`。 |
| `POST` | `/admin/exhausted-tasks/{id}/retry` | 重试 exhausted 任务（遗留路径）。 |

引用存储桶的替换和退休任务会在列表与引用详情响应中包含 `bucket_name`。从任务队列重试存储提供方替换工作会返回 `409 Conflict` 和 `"code": "replacement_task_retry_unsupported"`。替换任务完成或停止后，可以使用 **Open Data Sets**，或打开存储桶并前往 Details → Storage → Data Sets。`target_in_use` 失败不会显示 Retry，因为它需要改选存储提供方。

任务列表中的 `progress` 是按 `scope` 区分的联合对象。`scope: "ingress_store"` 返回 `attempt`、`uploaded_bytes`、`total_bytes`、可选 `percent`、`done` 与 `updated_at`。`scope: "provider_replacement"` 返回与存储桶响应相同的替换进度。客户端必须先按 `scope` 分支。

## 钱包和 Filecoin

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/wallet` | 钱包身份、余额、合约状态和业务计数。 |
| `POST` | `/api/v1/wallet/fund` | 创建钱包充值操作。 |
| `POST` | `/api/v1/wallet/withdraw` | 创建钱包提现操作。 |
| `POST` | `/api/v1/wallet/approve` | 创建显式 FWSS approval 操作。payload 只接受 `client_request_id`。 |
| `GET` | `/api/v1/wallet/operations` | 列出钱包操作。 |
| `GET` | `/api/v1/filecoin/readiness` | 检查 Filecoin readiness。 |
| `POST` | `/api/v1/filecoin/readiness/preflight` | 验证待保存的 Filecoin 设置。 |
| `GET` | `/api/v1/observability/providers` | 存储提供方健康数据。 |
| `POST` | `/api/v1/observability/providers/refresh` | 刷新存储提供方健康状态。 |
| `GET` | `/api/v1/observability/data-sets` | 本地数据集健康数据。 |
| `POST` | `/api/v1/observability/data-sets/refresh` | 刷新数据集健康状态。 |

## 设置和 S3 用户

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/settings` | 读取当前设置和元数据。 |
| `PUT` | `/api/v1/settings` | 持久化设置变更。 |
| `POST` | `/api/v1/settings/validate` | 验证设置请求内容，但不保存。 |
| `GET` | `/api/v1/s3-users` | 列出 S3 用户。 |
| `POST` | `/api/v1/s3-users` | 创建 S3 用户。 |
| `PUT` | `/api/v1/s3-users/{accessKey}` | 更新 S3 用户 role。 |
| `POST` | `/api/v1/s3-users/{accessKey}/secret` | 轮换 S3 secret key。 |
| `DELETE` | `/api/v1/s3-users/{accessKey}` | 删除 S3 用户。 |

缓存设置在 `cache` 下提供 `eviction_policy`、`lru_high_watermark_percent` 和 `lru_low_watermark_percent`。有效策略为 `lru`、`after_upload` 和 `none`。水位必须满足 `0 <= low < high <= 100`，且只在 `lru` 策略下生效。

完整运行时可用时，`GET /api/v1/settings` 还会返回 `runtime_filecoin_default_copies`，表示当前进程实际使用的值。`config.filecoin.default_copies` 仍表示已保存、下次重启后生效的值。

保存设置后，重启 SynapS3，检查 `/healthz`，再读取设置以确认实际生效值。

## 写请求示例

```bash
curl -X POST http://127.0.0.1:9090/api/v1/s3-users \
  -u admin \
  -H 'Content-Type: application/json' \
  -d '{"role":"user"}'
```

在 curl 的无回显提示中输入 Admin 密码。响应包含 access key、secret key 和 role。secret key 只显示一次；请保存到权限为 `0600` 的凭据文件，如果泄露则立即轮换。
