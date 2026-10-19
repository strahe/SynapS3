---
title: 备份与恢复
description: 从备份恢复 SynapS3，处理数据库不兼容，并从常见故障中恢复。
---

# 备份与恢复

一个恢复点包括数据库备份、与之匹配的配置和缓存打包文件，以及写出它们的 SynapS3 版本。按[运行数据](../configuration/runtime-data.md)完成备份和恢复；Docker 命令见[备份 Docker 数据](../getting-started/docker.md#备份-docker-数据)。

## 重启后验证

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin settings get
synaps3 admin task stats
```

健康状态为 `ok`，设置与部署一致，任务继续运行且没有新的失败。恢复正常流量前，通过 S3 API 读取一个已知对象。

## 数据库不兼容时

启动报错 `database contains an incompatible SynapS3 schema` 时，SynapS3 不会修改该数据库。

1. 停止 SynapS3，确认 `database.dsn` 指向预期的数据库。
2. 保持该数据库和与之匹配的缓存不变。不要为了绕过检查而修改它们。
3. 用该恢复点保存的版本，把经过验证的备份恢复到新的空数据库和数据目录。按[恢复顺序](../configuration/runtime-data.md#恢复顺序)操作。

使用空数据库启动不会导入旧数据库中的存储桶、对象、用户或任务。旧数据库创建的数据集会留在链上，继续从钱包扣费。不要让两个 SynapS3 数据库同时使用同一个钱包。

## 恢复矩阵

| 情况 | 恢复方式 |
| --- | --- |
| 存储提供方或 RPC 端点暂时不可用 | 恢复连接。等待中的工作会自行继续；只重试失败的任务。见[失败任务](./troubleshooting.md#失败任务)。 |
| 存储提供方永久不可用 | 在存储桶的 **Storage** 标签页替换它。见[替换存储提供方](../concepts/filecoin-storage-flow.md#替换存储提供方)。 |
| 缓存磁盘已满 | 见[缓存已满](./troubleshooting.md#缓存已满)。 |
| 数据库卷已满 | 停止 S3 流量，释放空间或扩容，再检查健康状态。 |
| SynapS3 崩溃 | 重启。检查健康状态和任务统计，重试已停止的批次，并核对崩溃时正在执行的钱包操作结果。 |
| 启动时报告数据库不兼容 | 见[数据库不兼容时](#数据库不兼容时)。 |

通过仪表盘、Admin API 或 CLI 恢复。不要直接修改 SynapS3 数据库。
