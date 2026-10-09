---
title: 运行数据
description: 理解 SynapS3 配置、元数据、缓存数据的存放位置和备份范围。
---

# 运行数据

SynapS3 将配置和缓存对象数据存储在本地磁盘，元数据存储在 PostgreSQL 中。应把它们都放在可靠存储上。可用的备份必须让数据库和缓存处于同一恢复时间点。

## 默认本地布局

```text
~/.synaps3/
  config.toml
  admin-initial-password
  cache/
```

显式配置的 `cache.dir` 优先于默认值。

## Docker 布局

容器使用 `/var/lib/synaps3`：

```text
/var/lib/synaps3/
  config.toml
  admin-initial-password
  cache/
```

Docker 部署通过 `synaps3-data` volume 挂载该路径，托管 PostgreSQL 数据库保存在 `synaps3-postgres-data` volume 中。Docker 专用的生命周期和备份命令见 [Docker 部署](../getting-started/docker.md)。

## 必须持久保存的数据

| 数据 | 原因 |
| --- | --- |
| `config.toml` | 保存未由环境变量管理的稳定运行设置。 |
| `admin-initial-password` | 保存非交互 init 和密码重置生成的 Admin 密码。保持 `0600` 权限；安全保存密码后，只在本地 CLI 仍需自动读取时保留。 |
| PostgreSQL 数据库 | 保存存储桶、对象、版本、后台任务、S3 用户和存储元数据。 |
| `cache/` | 保存本地持久化的对象字节，用于 Filecoin 上传和读取回填。 |
| 环境密钥 | 可能保存 Filecoin 私钥和部署特定覆盖项。 |

让 `config.toml`、`.env`、凭据文件和导出的密钥保持 `0600` 权限。不要把钱包私钥提交到仓库或放进未受保护的归档。

## 备份前检查

1. 检查 `curl http://127.0.0.1:9090/healthz`，并记录任何非 `ok` 结果。
2. 运行 `synaps3 admin task stats` 和 `synaps3 admin task list --status failed`，检查活动任务和失败任务。
3. 使用当前部署方式的服务管理器停止 SynapS3，避免备份过程中对象数据、元数据和任务状态继续变化。

不要在 SynapS3 仍在运行时创建文件系统归档。

## 备份

停止 SynapS3 后：

1. 使用 `pg_dump`、托管数据库快照，或部署批准的 PostgreSQL 备份工具创建数据库原生备份。
2. 备份 SynapS3 配置和缓存目录。
3. 为数据库备份以及配置和缓存归档标记相同的恢复时间点。
4. 验证两个备份产物后再重启服务。

托管数据库的具体命令见 [Docker 部署](../getting-started/docker.md#备份-docker-数据)。

## 重启并验证

备份成功后，使用当前部署方式的服务管理器启动 SynapS3，然后运行：

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
```

`/healthz` 应返回 `{"status":"ok"}`。恢复 S3 流量前，先排查 `setup` 或 `unhealthy`。

## 恢复顺序

1. 停止 SynapS3，并保持 S3 流量关闭。
2. 验证备份校验和，确认数据库与缓存带有相同的恢复时间点标记。
3. 先把数据库原生备份恢复到空数据库，再把匹配的配置和缓存数据恢复到空位置。
4. 确认恢复后的配置和凭据文件权限为 `0600`，并允许 SynapS3 运行账户读取。
5. 启动 SynapS3，检查 `/healthz`、任务统计和失败任务，再通过 S3 API 读取一个已知对象。

不要把一个时间点的数据库备份与另一个时间点的缓存数据混用。

只使用兼容的 SynapS3 版本恢复备份。如果启动时报告数据库不兼容，请保持备份不变，并按[升级与恢复](../operations/upgrade-recovery.md)处理。
