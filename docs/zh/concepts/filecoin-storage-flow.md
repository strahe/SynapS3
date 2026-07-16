---
title: Filecoin 存储流程
description: 理解后台任务如何把本地持久化对象存入 Filecoin 存储提供方。
---

# Filecoin 存储流程

S3 写入被接受后，Filecoin 存储才开始推进。后台任务会读取本地持久化对象，将其存入存储提供方，并记录最终形成的远端副本。

## 任务链

```mermaid
flowchart TD
  put["S3 写入已接受"] --> cached["cached"]
  cached --> uploading["uploading"]
  uploading --> committing["committing"]
  committing --> replicating["replicating"]
  replicating --> stored["stored"]
  stored --> evict["evict_cache task"]
  evict --> evicted["cache_evicted"]
```

## 对象状态

| 状态 | 含义 |
| --- | --- |
| `cached` | 对象已在本地持久化，并排队等待上传。 |
| `uploading` | 后台任务正在准备远端存储或上传对象数据。 |
| `committing` | 存储提供方已有 piece，commit 步骤正在进行。 |
| `replicating` | 至少已有一个可读副本，目标副本数仍在补齐。 |
| `stored` | 目标远端副本策略已满足，并且已有存储元数据。 |
| `failed` | 正在执行的生命周期步骤失败，可重试。 |
| `cache_evicted` | 远端持久化后，本地缓存已清理。 |

## 重试与恢复

如果 SynapS3 运行中断，未完成的后台任务会在服务重启后重新进入可继续处理的状态。

重试次数由后台任务设置限制。耗尽重试次数的任务需要运维处理：

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task retry 42
```

重试前先恢复 RPC 连接、存储提供方可达性、钱包余额、FWSS approval 或缓存容量。

## 存储提供方健康状态

健康检查会记录存储提供方和本地数据集的状态。仪表盘会使用这些结果，标出 `unavailable`、`degraded` 或 `unknown` 的存储副本。这些结果用于观测；存储提供方不可用后的恢复由下面的副本修复愿景说明。

## 用户能看到什么

- S3 上传可以在 Filecoin 存储完成前成功。
- 仪表盘的任务和拓扑视图会展示存储进度。
- 读取优先使用本地缓存；已有远端元数据时，可以从存储提供方取回对象。
- 缓存淘汰是运维优化，不是写入接受点。

## 副本修复愿景

即将支持的副本修复，将帮助运维人员在存储提供方变为不可用后恢复配置的目标副本数。它将：

- 识别因存储提供方不可用而受影响的副本；
- 通过安全、可追踪的修复恢复目标副本数；
- 展示修复进度和需要人工处理的情况。

它不同于首次目标副本补齐和失败存储任务重试流程。
