---
title: Filecoin 存储流程
description: 理解后台 workers 如何把本地持久化对象推进到 Filecoin-backed storage。
---

# Filecoin 存储流程

Filecoin 存储发生在 S3 写入被接受之后。后台 workers 会把对象从本地持久化推进到 provider-backed storage。

## 任务链

```text
PutObject
  -> cached object + upload task
  -> uploading
  -> committing
  -> replicating
  -> stored
  -> evict_cache task
  -> cache_evicted
```

## 对象状态

| 状态 | 含义 |
| --- | --- |
| `cached` | 对象已在本地持久化，并排队等待上传。 |
| `uploading` | Worker 正在准备 provider storage 或上传对象数据。 |
| `committing` | Provider 已有 piece，commit 步骤正在进行。 |
| `replicating` | 至少已有一个可读副本，目标副本数仍在补齐。 |
| `stored` | 目标远端副本策略已满足，并且已有存储元数据。 |
| `failed` | 正在执行的生命周期步骤失败，可重试。 |
| `cache_evicted` | 远端持久化后，本地缓存已清理。 |

## 重试与租约

Workers 使用 lease 语义 claim tasks。如果进程崩溃，启动恢复会释放 expired leases，并重置 stale upload states，让任务可以继续。

重试次数由 worker settings 限制。耗尽重试的任务需要运维处理：

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task retry 42
```

重试前先修复原因，例如 RPC 可用性、provider 可达性、钱包余额或缓存容量。

## Provider 健康

Observability checks 会记录 provider 和本地 data set health。这些信号驱动 dashboard storage-health 视图，帮助识别 unavailable、degraded 或 unknown storage copies。

## 用户能看到什么

- S3 upload 可以在 Filecoin 存储完成前成功。
- Dashboard task 和 topology 视图展示存储进度。
- 读取优先使用本地 cache，有 provider metadata 时可从 provider retrieve。
- 缓存淘汰是运维优化，不是写入接受点。
