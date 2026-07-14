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

## 认证模型

`/healthz` 不需要认证，便于进程健康检查。仪表盘外壳和静态资源可以在登录前加载，但仪表盘数据和写操作都由 Admin 认证保护。

| 访问范围 | 认证要求 |
| --- | --- |
| `/healthz` | 无。 |
| `/api/v1/auth/login`、`/api/v1/auth/session` | 登录和会话端点。没有有效浏览器会话时，`/session` 返回 `401`。 |
| `/api/v1/auth/logout` | 需要有效浏览器会话和 CSRF header；不接受 HTTP Basic auth。 |
| `/api/v1/*` | 浏览器 session cookie；写请求方法需要 CSRF。也可用 HTTP Basic auth。 |
| `/metrics` | 浏览器 session cookie 或 HTTP Basic auth。 |
| `/admin/exhausted-tasks*` | 浏览器 session cookie；写请求方法需要 CSRF。也可用 HTTP Basic auth。 |

### 浏览器会话

浏览器登录会设置 `synaps3_admin_session` HttpOnly cookie，并返回 CSRF token。使用 cookie 认证的 `POST`、`PUT`、`PATCH`、`DELETE` 必须带 `X-SynapS3-CSRF`。Logout 只接受浏览器会话。

### CLI 和 Basic auth

CLI 和脚本可以使用 HTTP Basic auth，不需要 CSRF header。如果浏览器发起的 Basic auth 请求通过 `Sec-Fetch-Site`、`Origin` 或 `Referer` 暴露出跨站来源，请求会被拒绝。没有浏览器来源标头的请求仍按 CLI/脚本处理。

密码失败会按解析后的客户端 IP 限流；限流器满时会拒绝请求。Basic auth 成功后，凭据会按客户端 IP 短暂缓存，避免重复 bcrypt。

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
| `POST` | `/api/v1/auth/login` | 校验用户名和密码，设置浏览器 cookie，并返回 CSRF token。 |
| `GET` | `/api/v1/auth/session` | 返回当前浏览器会话和 CSRF token。 |
| `POST` | `/api/v1/auth/logout` | 需要会话和 CSRF，在内存中撤销当前 token 直到过期，并清除浏览器 cookie。 |

仪表盘在任何 logout 尝试或 `401` API 响应后都会清理本地认证状态。即使服务端 logout 请求失败，UI 也会回到登录页。

## 安全标头

Admin 响应包含 `Content-Security-Policy`、`X-Content-Type-Options: nosniff`、`X-Frame-Options: DENY` 和 `Referrer-Policy: strict-origin-when-cross-origin`。CSP 默认同源，并允许当前内嵌仪表盘需要的 inline script/style。

## 高风险操作

把这些端点当作变更窗口操作处理。它们可能改变数据、凭据、钱包支付状态或后台状态。

| 范围 | 端点 | 风险 |
| --- | --- | --- |
| 设置 | `PUT /api/v1/settings` | 变更可能需要重启，或把节点切换到不同 Filecoin 网络。保存前先验证设置和 Filecoin readiness。 |
| 钱包 | `POST /api/v1/wallet/fund`、`POST /api/v1/wallet/withdraw`、`POST /api/v1/wallet/approve` | 创建链上支付操作。 |
| S3 用户 | `POST /api/v1/s3-users`、`PUT /api/v1/s3-users/{accessKey}`、`POST /api/v1/s3-users/{accessKey}/secret`、`DELETE /api/v1/s3-users/{accessKey}` | 改变客户端访问权限，或让已有凭据失效。 |
| Bucket/object | bucket 创建、owner/copy-policy 更新、object 上传/下载/删除/恢复/永久删除 | 改变或暴露用户可见的 S3 数据和元数据。 |
| 任务和观测数据 | 任务重试、诊断刷新、provider/data-set refresh | 重新入队任务，或刷新运维状态。 |

## 健康检查和指标

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/healthz` | 数据库、缓存和工作进程健康状态。 |
| `GET` | `/metrics` | Prometheus 指标。需要 Admin 认证。 |
| `GET` | `/api/v1/system/info` | 版本和运行时信息。 |
| `GET` | `/api/v1/workers` | 工作进程存活状态。 |
| `GET` | `/api/v1/cache/stats` | 缓存使用量和容量。 |

## 仪表盘数据

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/overview` | 仪表盘摘要。 |
| `GET` | `/api/v1/events` | 仪表盘事件流。 |
| `GET` | `/api/v1/buckets` | 列出 bucket。 |
| `POST` | `/api/v1/buckets` | 创建 bucket。 |
| `GET` | `/api/v1/buckets/{name}` | 读取 bucket 详情。 |
| `PUT` | `/api/v1/buckets/{name}/owner` | 更新 bucket owner。 |
| `PUT` | `/api/v1/buckets/{name}/copy-policy` | 更新默认 copy policy。 |
| `DELETE` | `/api/v1/buckets/{name}` | 暂未实现（返回 `501 Not Implemented`）。 |
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

对象上传时，HTTP `Content-Type` 表示上传对象的内容类型，不是 JSON 请求标记。

### 恢复对象版本

对象存在版本历史时，`GET /api/v1/buckets/{name}/objects/versions` 的每一页都会返回 `current_version_id`。确认恢复时把该值传回服务端：

```json
{
  "key": "path/file.txt",
  "version_id": "source-version-id",
  "expected_current_version_id": "current-version-id"
}
```

`POST /api/v1/buckets/{name}/objects/versions/restore` 会把选中的历史数据版本复制为同一 bucket、同一 key 下的新版本。已有数据版本和 delete marker 不会被修改或删除。选中版本必须与当前可读对象的表示不同；直接选择当前版本，或选择与当前版本等价的历史版本，请求会被拒绝，并且不会创建版本、缓存或任务。当前版本失败或不可用时，仍可使用可读的历史版本进行修复。

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

恢复操作同步流式执行，最长一小时，并且缓存必须能容纳新的目标版本。源数据只存在于 provider 时，本次操作不会把它重新写回原 cache key。

## 任务

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | 列出后台任务。支持 `type`、`stage`、`status`、`limit`、`offset` 等过滤。 |
| `GET` | `/api/v1/tasks/stats` | 按状态统计任务。 |
| `GET` | `/api/v1/tasks/{id}/ref-detail` | 解析任务关联的 object 或 storage upload。 |
| `GET` | `/api/v1/tasks/{id}/diagnostic` | 读取任务诊断。 |
| `POST` | `/api/v1/tasks/{id}/diagnostic/refresh` | 刷新诊断。 |
| `POST` | `/api/v1/tasks/{id}/retry` | 重试 exhausted 任务。 |
| `GET` | `/admin/exhausted-tasks` | 列出 exhausted 任务。支持最大为 `1000` 的 `limit`。 |
| `POST` | `/admin/exhausted-tasks/{id}/retry` | 重试 exhausted 任务（遗留路径）。 |

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
| `GET` | `/api/v1/observability/data-sets` | 本地 data set 健康数据。 |
| `POST` | `/api/v1/observability/data-sets/refresh` | 刷新 data set 健康状态。 |

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

## 写请求示例

```bash
export SYNAPS3_ADMIN_PASSWORD='replace-with-admin-password'

curl -X POST http://127.0.0.1:9090/api/v1/s3-users \
  -u "admin:${SYNAPS3_ADMIN_PASSWORD}" \
  -H 'Content-Type: application/json' \
  -d '{"role":"user"}'
```

响应包含 access key、secret key 和 role。
