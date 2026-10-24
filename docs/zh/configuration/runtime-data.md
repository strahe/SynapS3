---
title: 运行数据
description: 了解 SynapS3 的数据存放位置，并把它们作为同一个恢复点进行备份和恢复。
---

# 运行数据

SynapS3 把配置和缓存对象放在本地磁盘上，把元数据放在 PostgreSQL 中。缓存里可能有尚未存到 Filecoin 的对象，数据库则记录每个对象存在哪里。两者都要放在持久存储上，并一起备份、一起恢复。缓存目录所在的文件系统必须支持对目录执行 `fsync`（例如本地磁盘），否则 SynapS3 无法启动。

## 存放位置

| 数据 | 默认位置 | 内容 |
| --- | --- | --- |
| `config.toml` | `~/.synaps3/` | 设置和 Admin 凭据；通常还包含数据库 DSN 和钱包私钥。 |
| `admin-initial-password` | `~/.synaps3/` | 非交互式 `synaps3 init` 或 `synaps3 admin-auth reset-password` 写入的 Admin 密码。 |
| `cache/` | `~/.synaps3/cache/`，或 `cache.dir` | 缓存的对象数据，包括尚未存到 Filecoin 的对象。 |
| PostgreSQL 数据库 | `database.dsn` | 存储桶、对象、版本、副本、任务、钱包操作和 S3 用户。S3 secret key 以明文保存。 |
| 环境变量中的密钥 | `.env` 或主机环境 | 私钥、DSN 和其他覆盖项。 |

Docker 部署中，这些文件位于 `synaps3-data` 卷的 `/var/lib/synaps3/`，托管数据库位于 `synaps3-postgres-data` 卷。

`config.toml`、`admin-initial-password`、`.env` 和备份文件只允许运行 SynapS3 的账户读取。密码存入密码管理器后，删除 `admin-initial-password`，除非本机的 `synaps3 admin` 命令还要读取它。

## 备份

Docker 部署可以直接使用[备份 Docker 数据](../getting-started/docker.md#备份-docker-数据)中的命令。

1. 记录当前状态：

   ```bash
   curl http://127.0.0.1:9090/healthz
   synaps3 admin task stats
   synaps3 admin task list --status failed
   ```

2. 用服务管理器停止 SynapS3。SynapS3 运行期间打包的数据不一致。
3. 用 `pg_dump` 或数据库快照备份数据库。
4. 打包配置文件和缓存目录。
5. 给数据库备份和打包文件标记同一个恢复点，记录校验和，并逐一验证。
6. 启动 SynapS3，确认 `/healthz` 返回 `{"status":"ok"}`。

## 恢复顺序

1. 停止 SynapS3，并保持 S3 流量关闭。
2. 验证校验和，确认数据库备份和打包文件带有同一个恢复点标记。
3. 把数据库恢复到空数据库，再把配置和缓存恢复到空目录。
4. 确认恢复后的配置和凭据文件权限为 `0600`，属主是运行 SynapS3 的账户。
5. 启动写出该备份的 SynapS3 版本。
6. 检查 `/healthz`、`synaps3 admin task stats` 和失败任务，再通过 S3 API 读取一个已知对象。

不要把数据库备份和另一个恢复点的缓存数据混用。启动时报告数据库不兼容，见[数据库不兼容时](../operations/backup-recovery.md#数据库不兼容时)。
