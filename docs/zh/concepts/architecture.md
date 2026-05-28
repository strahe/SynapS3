---
title: 架构
description: 理解 SynapS3 单机网关架构和数据流边界。
---

# 架构

SynapS3 通过 cache-first gateway、repository-backed metadata 和 background workers，把 S3 客户端连接到 Filecoin 存储。

## 系统形态

```text
S3 client
  -> VersityGW
  -> SynapseBackend
  -> local cache + repositories + state machine
  -> background workers
  -> synapse-go SDK
  -> Filecoin storage providers
```

关键运维边界在 S3 响应和 Filecoin 上传之间。一次已确认写入表示本地持久化完成，Filecoin 存储在响应后继续进行。

## 主要层次

| 层 | 职责 |
| --- | --- |
| `cmd/synaps3` | CLI 入口、配置加载、数据库迁移、运行时组装。 |
| `internal/backend` | S3 行为和 VersityGW backend 实现。 |
| `internal/cache` | 可靠的本地文件系统缓存。 |
| `internal/db/repository` | backend 和 worker 的持久化边界。 |
| `internal/state` | 对象生命周期状态转换校验。 |
| `internal/worker` | 异步上传、缓存淘汰、租约、重试、恢复。 |
| `internal/admin` 和 `ui/` | Dashboard、Admin API、Admin auth、健康检查、指标。 |
| `internal/synapse` | Synapse SDK 行为的窄封装。 |

## 设计原则

- 已确认的 S3 写入必须能承受异步上传失败。
- 原始数据库访问留在 repositories 后面。
- 对象可见性和对象存储状态是两个问题。
- Generation 值保护较新的写入不被 stale worker 覆盖。
- 只有存在足够远端持久化元数据后才执行缓存淘汰。
- 设计优先单机，不依赖分布式协调。

## 对运维的影响

| 行为 | 运维影响 |
| --- | --- |
| S3 写入成功是 local-first | Provider 故障不会让已接受写入消失。 |
| 后台任务处理 Filecoin 存储 | 需要关注 task queues 和 exhausted tasks。 |
| Cache 是持久性的一部分 | Cache 磁盘不是可随意丢弃的临时目录。 |
| Admin API 控制运维操作 | 使用 Admin auth；保持 loopback 或放在 HTTPS 和访问控制之后。 |

## Dashboard 角色

内嵌 React dashboard 是运维界面。它展示 buckets、objects、wallet state、background tasks、storage topology、settings 和 health signals。它共享 admin server，使用 Admin auth session，不应直接暴露给不可信网络。

## Admin Auth 边界

Admin API 请求会先按 canonical path 分类，再进入 Go `ServeMux`。`/healthz` 保持公开；`/api/v1/*`、`/metrics` 和 `/admin/exhausted-tasks*` 需要 Admin auth。浏览器 session 使用 HttpOnly cookie，并在 unsafe methods 上要求 `X-SynapS3-CSRF`。CLI 和脚本可使用 HTTP Basic auth；浏览器来源的 unsafe Basic auth 请求会做来源校验。

密码失败按解析后的客户端 IP 限流。只有直接来源命中 `admin.trusted_proxies` 时，才信任 forwarded client、scheme 和 host headers。Logout 会清除 cookie，并在当前进程内撤销当前 session token 直到过期。
