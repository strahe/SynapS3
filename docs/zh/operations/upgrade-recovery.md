---
title: 升级与恢复
description: 安全升级 SynapS3，并从常见单机故障场景恢复。
---

# 升级与恢复

SynapS3 是单机网关。升级或恢复时，先把本地持久化对象和元数据作为一个一致的数据集保护起来，再恢复后台任务。依赖失败时，先恢复依赖，再重试任务。

## 升级前

运行：

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 50
```

预期结果：`/healthz` 返回 `ok`，并且所有 exhausted 任务在升级前都有明确处理方式。

创建备份前，停止 S3 流量和 SynapS3：

```bash
docker compose stop synaps3
```

- SQLite 部署：归档完整运行数据卷，并验证归档和校验和。
- PostgreSQL 部署：创建数据库原生备份，并归档同一时间点的配置和缓存数据。

所有备份产物必须处于同一恢复时间点。准确的备份、校验和重启步骤见[运行数据](../configuration/runtime-data.md)。

## 升级 Docker Compose

```bash
docker compose pull
docker compose up -d
docker compose logs --tail=100 synaps3
curl http://127.0.0.1:9090/healthz
docker compose exec synaps3 synaps3 admin settings get
docker compose exec synaps3 synaps3 admin task stats
```

预期结果：服务使用预期运行数据启动，`/healthz` 返回 `ok`，生效设置与部署一致，任务队列恢复推进，并且没有意外出现耗尽重试的任务。恢复正常流量前，通过 S3 API 读取一个已知对象。

## 运行流程

```text
接收写入 -> 保存对象 -> 记录元数据 -> 返回成功 -> 继续后台存储
```

- 写入会先提交到本地缓存和元数据，再上传到存储提供方。
- 存储任务失败后会重试，达到配置的重试上限后进入 `exhausted`。
- `GetObject` 优先从缓存读取；已有远端元数据时，可以从存储提供方取回对象。
- 删除存储桶不受支持并返回 `501`；删除对象会让对象从 S3 视图中消失，后续清理会安全继续。

## 恢复矩阵

| 场景 | 恢复方式 |
| --- | --- |
| 后台存储任务无法连接存储提供方 | 恢复连接，然后重试 exhausted 存储任务。 |
| RPC 节点故障 | 恢复 RPC 连接，然后重试 exhausted 任务。 |
| 私有存储提供方 URL 被阻止 | 默认保持阻止；只在可信私有部署中开启 `filecoin.allow_private_networks`。 |
| 数据库空间不足 | 释放空间或扩容数据库。 |
| 缓存磁盘空间不足 | 扩容磁盘、提高 `cache.max_size_gb`，或恢复上传和淘汰进度。 |
| 进程崩溃 | 重启服务，再检查健康状态和任务统计；未完成任务会重新进入可继续处理的状态。 |

副本完成存储后，如果存储提供方变为不可用，不一定会产生可重试任务。使用存储健康视图识别受影响副本；恢复目标副本数属于[副本修复愿景](../concepts/filecoin-storage-flow.md#副本修复愿景)。

## 恢复或回退

1. 停止 S3 流量和 SynapS3。
2. 验证归档校验和，并选择同一恢复时间点的数据库和缓存产物。
3. SQLite 恢复完整运行数据卷；PostgreSQL 先恢复数据库原生备份，再恢复匹配的配置和缓存数据。
4. 回退应用版本时，只让固定的旧镜像使用与其兼容的数据。如果无法确认兼容性，恢复升级前的完整恢复时间点。
5. 启动 SynapS3，检查 `/healthz`、生效设置、任务统计、耗尽重试的任务、钱包就绪状态，并通过 S3 读取一个已知对象。

这些检查通过前，不要恢复正常流量。

常用命令：

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin s3-user list
synaps3 admin settings get
```

修改恢复相关设置后，重启 SynapS3，并同时检查 `/healthz` 和 `synaps3 admin settings get`。
