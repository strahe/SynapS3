---
title: 升级与恢复
description: 安全升级 SynapS3 并恢复后台工作。
---

# 升级与恢复

更改版本前，把数据库和缓存作为同一恢复点保护。重试后台工作前，先恢复失效的依赖。

## 升级前

运行：

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status failed --limit 50
```

预期结果：健康状态为 `ok`，替换进程前每个 failed 任务都有明确处理方式。

创建备份前，停止新的 S3 流量和 SynapS3。数据库、缓存、配置和凭据必须位于同一恢复点。备份和验证步骤见[运行数据](../configuration/runtime-data.md)。

## 升级 SynapS3

使用部署环境原有的安装方式替换可执行文件、软件包或容器镜像。Docker 命令见 [Docker 部署](../getting-started/docker.md)。

废弃的 `worker.upload`、`worker.provider_replacement`、`worker.evictor` 和 `worker.storage_cleanup` 配置段会被拒绝；请替换为 `worker.tasks` 设置。启动升级版本前，还必须删除 `worker.tasks.max_retries`、`worker.tasks.retention`、`SYNAPS3_WORKER_TASKS_MAX_RETRIES` 和 `SYNAPS3_WORKER_TASKS_RETENTION`，否则配置校验会拒绝启动。重试预算由任务类型决定，历史永久保留。

使用预期的数据库和缓存启动 SynapS3。如果启动时报告数据库不兼容，请停止进程、保持数据库不变，然后按[数据库不兼容时](#数据库不兼容时)处理。

启动后运行：

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin settings get
synaps3 admin task stats
```

预期结果：健康状态为 `ok`，生效设置与部署一致，后台工作继续且没有意外失败。恢复正常流量前，通过 S3 API 读取一个已知对象。

## 数据库不兼容时

1. 停止新的 S3 流量，并停止使用该部署的所有 SynapS3 进程。
2. 备份报告的数据库，并验证该备份。
3. 将数据库和匹配的缓存保留为只读数据。不要修改这两个位置来绕过兼容性检查。
4. 为替换安装配置空数据库和空缓存目录。
5. 启动 SynapS3；验证健康状态、生效设置和后台任务处理后，再恢复流量。

SQLite 在停止进程后创建一致性备份，并验证能够打开：

```bash
sqlite3 /old/path/synaps3.db ".backup '/backup/path/synaps3-pre-upgrade.db'"
sqlite3 -readonly /backup/path/synaps3-pre-upgrade.db "PRAGMA integrity_check;"
```

完整性检查必须输出 `ok`。把备份、需要保留的 WAL/SHM 文件、匹配的缓存和配置作为同一恢复集保护。PostgreSQL 部署应使用 `pg_dump` 或部署批准的数据库快照，并单独验证该备份产物。

SynapS3 不会修改不兼容的数据库。

使用空数据库启动时，不会导入原有的存储桶、对象、用户、存储数据集、钱包操作、存储提供方替换或任务。已创建的远端付费存储服务仍会运行。请保留经过验证的备份，以便单独核对和处理这些服务与记录。

不要让保留的安装和替换安装共用同一数据库、缓存、钱包工作流或 S3 流量。启动替换安装后，创建 S3 用户和测试存储桶，再写入并读取测试对象，然后恢复正常流量。

## 恢复后台工作

重启后，未完成的工作会自动恢复处理。

- 使用 **Retry** 恢复已停止的任务。任务列表按结果显示重试资格，打开详情后再检查当前条件。如果其他操作已接管，或恢复不安全，Retry 会说明原因并刷新列表。
- 从 **Details** → **Storage** → **Data Sets** 恢复存储提供方替换。
- 存储传输任务因内部错误停止时仍可重试。重试会先检查上一次已经完成的部分，再继续。如果错误来自无法处理该任务的版本，请先安装兼容的版本。
- 钱包操作在尚未发出广播、或因内部错误停止时，可以从 Tasks 重试。重试会先核对这笔操作：从未广播过时才会发出交易，否则只检查已经发出的交易。如果广播可能已经发出、却没有记录到交易，操作会被标为结果未知，不能再重试。
- **Retry** 会先检查存储提供方是否已有分片；确认缺失后才重新上传。重传可能增加带宽用量或开启另一次上传会话。
- 确认将失败移入 History；用 `scope=history&status=failed` 查看。
- 存储登记恢复会保留已签名请求，并在再次提交前检查链上结果。缺失的分片会在安全时重新传输。存储提供方错误和传输失败使用有限预算。登记明确被拒绝且链上核实未落地时，无法恢复的分片不会阻止其余分片继续登记。恢复停止后，确认会保留已有证据。在 **Tasks**（Confirm storage、Failed）中或通过 `synaps3 admin storage-confirmation list` 找到它，恢复依赖后使用 **Retry**。已被接受的请求如果与链上证据冲突，会继续停止并等待核查。
- Pull 队列已满时，会按存储提供方要求的延迟再次尝试，不消耗重试预算。其他失败在新建或手动重试的 Pull 轮次中最多使用 12 次机会；已有轮次保留原策略。

常用命令：

```bash
synaps3 admin task list --status failed --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin task acknowledge 42
synaps3 admin storage-confirmation list
synaps3 admin settings get
```

重试前先恢复失效的依赖。使用仪表盘、Admin API 或 CLI 操作，不要直接修改应用数据库。

## 恢复矩阵

| 场景 | 恢复方式 |
| --- | --- |
| 存储提供方或 RPC 暂时不可用 | 恢复连接。等待中的工作会自动继续；只重试标记为可重试的失败任务。 |
| 数据库空间不足 | 停止流量，释放空间或扩容数据库，再检查健康状态。 |
| 缓存磁盘空间不足 | 扩容磁盘、提高 `cache.max_size_gb`，或恢复远端存储与缓存清理进度。 |
| 需要迁离存储提供方 | 打开存储桶并使用 **Details** → **Storage** → **Data Sets**。不要从 Tasks 重试替换。 |
| 进程崩溃 | 重启 SynapS3，验证健康状态和任务统计，再恢复已停止的存储确认，并核对钱包结果。 |
| 启动时报告数据库不兼容 | 停止进程，确认配置的数据库是预期目标，将其原样保留，然后改用空数据库。 |

## 恢复或回滚

1. 停止 S3 流量和 SynapS3。
2. 验证备份校验和，选择同一恢复点的数据库和缓存产物。
3. SQLite 恢复完整运行数据卷。PostgreSQL 先恢复数据库原生备份，再恢复匹配的配置和缓存数据。
4. 回滚应用时，只使用与所选版本兼容的数据。无法确认兼容性时，恢复升级前的恢复点。
5. 启动 SynapS3，然后检查 `/healthz`、生效设置、任务统计、failed 任务、钱包准备状态和已知 S3 对象。

这些检查全部通过前，不要恢复正常流量。
