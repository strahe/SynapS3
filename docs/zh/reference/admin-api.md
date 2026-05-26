---
title: Admin API
description: SynapS3 health、metrics、dashboard、settings、wallet、task 和 S3 user 端点参考。
---

# Admin API

Admin API 为仪表盘和 CLI 提供能力。除非放在经过认证的私有访问层后面，否则应保持监听 loopback。

默认 base URL：

```text
http://127.0.0.1:9090
```

## 安全模型

Admin API 应保持 loopback。写端点会拒绝不安全暴露。多数 JSON 写请求还要求显式确认头。

| 端点组 | 保护要求 |
| --- | --- |
| Settings、wallet、bucket/object writes、S3 user writes、Filecoin preflight | Admin 绑定 loopback，`Content-Type: application/json`，并带 `X-SynapS3-Settings-Write: 1`。 |
| Observability refresh | Admin 绑定 loopback，并带 `X-SynapS3-Observability-Refresh: 1`。 |
| Task retry 和 diagnostic refresh | 私有 Admin 访问；retry 会改变后台任务状态。 |
| S3 user list 和 object download | 私有 Admin 访问；这些端点会暴露访问元数据或对象数据。 |

如果 Admin server 没有绑定 loopback，受保护操作会返回 `403`。

## 高风险操作

把这些端点当作变更窗口操作处理。部分端点使用 settings 写入头；所有端点都可能改变数据、凭据、钱包余额或后台状态。

| 范围 | 端点 | 风险 |
| --- | --- | --- |
| Settings | `PUT /api/v1/settings` | 变更可能需要重启，或把节点切换到不同 Filecoin 网络。保存前先验证 settings 和 Filecoin readiness。 |
| Wallet | `POST /api/v1/wallet/fund`、`POST /api/v1/wallet/withdraw` | 创建链上支付操作。 |
| S3 users | `POST /api/v1/s3-users`、`PUT /api/v1/s3-users/{accessKey}`、`POST /api/v1/s3-users/{accessKey}/secret`、`DELETE /api/v1/s3-users/{accessKey}` | 改变客户端访问能力，或让已有凭据失效。 |
| Buckets and objects | bucket 创建、owner/copy-policy 更新、object 上传/下载/删除/恢复/永久删除 | 改变或暴露用户可见的 S3 数据和元数据。 |
| Tasks and observability | task retry、diagnostic refresh、provider/data-set refresh | 重新入队任务，或刷新运维状态。 |

## Health 和 Metrics

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/healthz` | 数据库、缓存和 worker 健康状态。 |
| `GET` | `/metrics` | Prometheus metrics。 |
| `GET` | `/api/v1/system/info` | 版本和运行时信息。 |
| `GET` | `/api/v1/workers` | Worker liveness map。 |
| `GET` | `/api/v1/cache/stats` | 缓存使用量和容量。 |

## Dashboard Data

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/overview` | 仪表盘摘要。 |
| `GET` | `/api/v1/events` | 仪表盘事件流。 |
| `GET` | `/api/v1/buckets` | 列出 buckets。 |
| `POST` | `/api/v1/buckets` | 创建 bucket。 |
| `GET` | `/api/v1/buckets/{name}/objects` | 列出对象。 |
| `POST` | `/api/v1/buckets/{name}/objects/upload` | 通过仪表盘上传对象。 |
| `GET` | `/api/v1/buckets/{name}/objects/download` | 通过仪表盘下载对象。 |
| `GET` | `/api/v1/buckets/{name}/objects/versions` | 列出对象版本。 |
| `GET` | `/api/v1/buckets/{name}/objects/provenance` | 查看对象存储来源。 |

## Tasks

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/tasks` | 列出后台任务。支持 `type`、`stage`、`status`、`limit`、`offset` 等过滤。 |
| `GET` | `/api/v1/tasks/stats` | 按状态统计任务。 |
| `GET` | `/api/v1/tasks/{id}/ref-detail` | 解析任务背后的 object 或 storage upload。 |
| `GET` | `/api/v1/tasks/{id}/diagnostic` | 读取任务诊断。 |
| `POST` | `/api/v1/tasks/{id}/diagnostic/refresh` | 刷新诊断。 |
| `POST` | `/api/v1/tasks/{id}/retry` | 重试 exhausted task。 |

## Wallet 和 Filecoin

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/wallet` | 钱包身份、余额、合约状态和业务计数。 |
| `POST` | `/api/v1/wallet/fund` | 创建 wallet funding operation。 |
| `POST` | `/api/v1/wallet/withdraw` | 创建 wallet withdrawal operation。 |
| `GET` | `/api/v1/wallet/operations` | 列出 wallet operations。 |
| `GET` | `/api/v1/filecoin/readiness` | 检查 Filecoin readiness。 |
| `POST` | `/api/v1/filecoin/readiness/preflight` | 验证待保存的 Filecoin 设置。 |
| `GET` | `/api/v1/observability/providers` | Provider health 数据。 |
| `POST` | `/api/v1/observability/providers/refresh` | 刷新 provider health。 |
| `GET` | `/api/v1/observability/data-sets` | 本地 data set health 数据。 |
| `POST` | `/api/v1/observability/data-sets/refresh` | 刷新 data set health。 |

## Settings 和 S3 Users

| Method | Path | 用途 |
| --- | --- | --- |
| `GET` | `/api/v1/settings` | 读取当前设置和元数据。 |
| `PUT` | `/api/v1/settings` | 持久化设置变更。 |
| `POST` | `/api/v1/settings/validate` | 验证设置 payload，但不保存。 |
| `GET` | `/api/v1/s3-users` | 列出 S3 users。 |
| `POST` | `/api/v1/s3-users` | 创建 S3 user。 |
| `PUT` | `/api/v1/s3-users/{accessKey}` | 更新 S3 user role。 |
| `POST` | `/api/v1/s3-users/{accessKey}/secret` | 轮换 S3 secret key。 |
| `DELETE` | `/api/v1/s3-users/{accessKey}` | 删除 S3 user。 |

## 写请求示例

```bash
curl -X POST http://127.0.0.1:9090/api/v1/s3-users \
  -H 'Content-Type: application/json' \
  -H 'X-SynapS3-Settings-Write: 1' \
  -d '{"role":"user"}'
```

预期结果：响应包含 access key、secret key 和 role。
