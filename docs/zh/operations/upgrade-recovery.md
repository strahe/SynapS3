---
title: 升级与恢复
description: 安全切换到当前数据库基线，并在不重复外部效果的前提下恢复后台工作。
---

# 升级与恢复

当前 SynapS3 版本使用全新的数据库基线，不迁移也不接管旧数据库中的数据。切换时应按一次全新安装处理，并保留旧运行数据的只读副本。

## 必须执行的全新基线切换

1. 停止新的 S3 流量，并停止所有旧 SynapS3 进程。
2. 备份旧数据库，并在继续之前验证备份。
3. 将旧数据库和旧缓存保留为只读数据；新版本不能使用这两个位置。
4. 配置新的空数据库路径和新的空缓存目录。
5. 启动新版本；验证健康状态、生效设置和任务处理后，再恢复流量。

SQLite 在停止进程后创建一致性备份，并验证能够打开：

```bash
sqlite3 /old/path/synaps3.db ".backup '/backup/path/synaps3-pre-baseline.db'"
sqlite3 -readonly /backup/path/synaps3-pre-baseline.db "PRAGMA integrity_check;"
```

完整性检查必须输出 `ok`。把备份、需要保留的 WAL/SHM 文件、旧缓存和匹配的配置作为同一恢复集保护。PostgreSQL 部署应使用 `pg_dump` 或部署批准的数据库快照，并单独验证该备份产物。

新版本会拒绝包含旧 SynapS3 migration marker 或任意业务表的数据库，且不会删除或修改该数据库。旧的 `worker.upload`、`worker.provider_replacement`、`worker.evictor` 和 `worker.storage_cleanup` 配置段也会被拒绝，必须改为 `worker.tasks`。

## 不会接管的内容

全新安装不会接管旧的存储桶、对象、用户、存储数据集、piece、钱包操作、存储提供方替换记录或任务状态。重建本地数据库不会终止已经创建的远端付费存储服务。请保留经过验证的旧备份，后续人工核对并处理这些服务和记录。

不要让新旧 SynapS3 同时使用同一数据库、缓存、钱包工作流或 S3 流量。新版本绝不能连接保留的旧数据库。

## 验证新安装

运行：

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin settings get
synaps3 admin task stats
```

预期结果：健康状态为 `ok`，数据库与缓存均指向新位置，任务引擎正常报告活动。恢复正常流量前，创建测试存储桶，写入并读取测试对象，再确认其后台存储任务。

## 运行时恢复

切换完成后，所有未完成工作由统一任务引擎处理，持久状态只有 `pending`、`running`、`completed`、`failed` 和 `cancelled`。

- 中断的 running 任务会在 lease 过期后被接管，并强制从恢复模式开始。
- 恢复过程会先检查 checkpoint 和领域证据，再决定能否发起新的外部效果。
- 只有 API 标记为可重试的失败任务才能重试；重试始终从恢复模式开始。
- 存储提供方替换仍在存储桶的 Data Sets 页面恢复。
- 钱包任务不能从 Tasks 重试。广播结果不确定时，钱包记录会保留 unknown 结果，不会盲目重播。
- 无法证明存储提供方结果的确认会出现在 `synaps3 admin storage-confirmation list`，等待显式核对。

常用命令：

```bash
synaps3 admin task list --status failed --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin task acknowledge 42
synaps3 admin storage-confirmation list
synaps3 admin settings get
```

重试前先恢复失败的依赖。不要手工编辑任务行、清空 checkpoint 或缩短 lease。

## 恢复矩阵

| 场景 | 恢复方式 |
| --- | --- |
| 存储提供方或 RPC 暂时不可用 | 恢复连接。等待中的工作会自动继续；只重试标记为可重试的失败任务。 |
| 数据库空间不足 | 停止流量，释放空间或扩容数据库，再检查健康状态。 |
| 缓存磁盘空间不足 | 扩容磁盘、提高 `cache.max_size_gb`，或恢复远端存储与缓存清理进度。 |
| 需要迁离存储提供方 | 打开存储桶并使用 **Details** → **Storage** → **Data Sets**。不要从 Tasks 重试替换。 |
| 进程崩溃 | 重启 SynapS3。过期 claim 会在不改变任务身份的前提下恢复；核对仍然不确定的存储确认或钱包结果。 |
| 全新基线启动报告数据库不兼容 | 停止进程，确认配置 DSN 指向预期的新空数据库，并保持报告的旧数据库不变。 |

## 备份新安装

切换完成后，后续备份仍需把配置、数据库和缓存视为同一恢复时间点。创建文件系统备份前停止 SynapS3；或者使用数据库原生一致性快照，并与缓存恢复点协调。当前布局和验证步骤见[运行数据](../configuration/runtime-data.md)。
