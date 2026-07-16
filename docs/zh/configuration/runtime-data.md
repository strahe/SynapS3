---
title: 运行数据
description: 理解 SynapS3 配置、元数据、缓存数据的存放位置和备份范围。
---

# 运行数据

SynapS3 将配置、元数据和缓存对象数据存储在本地磁盘。应把这些数据放在可靠存储上。可用的备份必须让数据库和缓存处于同一恢复时间点。

## 默认本地布局

```text
~/.synaps3/
  config.toml
  admin-initial-password
  db/
    synaps3.db
    synaps3.db-shm
    synaps3.db-wal
  cache/
```

SQLite WAL 和 SHM 文件是正常现象。显式配置的 `database.dsn` 和 `cache.dir` 优先于默认值。

## Docker 布局

容器使用 `/var/lib/synaps3`：

```text
/var/lib/synaps3/
  config.toml
  admin-initial-password
  db/
  cache/
```

Compose 部署通过 `synaps3-data` Docker volume 挂载该路径。

## 必须持久保存的数据

| 数据 | 原因 |
| --- | --- |
| `config.toml` | 保存未由环境变量管理的稳定运行设置。 |
| `admin-initial-password` | 保存非交互 init 和密码重置生成的 Admin 密码。保持 `0600` 权限；安全保存密码后，只在本地 CLI 仍需自动读取时保留。 |
| `db/` | 保存存储桶、对象、版本、后台任务、S3 用户和存储元数据。 |
| `cache/` | 保存本地持久化的对象字节，用于 Filecoin 上传和读取回填。 |
| 环境密钥 | 可能保存 Filecoin 私钥和部署特定覆盖项。 |

让 `config.toml`、`.env`、凭据文件和导出的密钥保持 `0600` 权限。不要把钱包私钥提交到仓库或放进未受保护的归档。

## 备份前检查

1. 检查 `curl http://127.0.0.1:9090/healthz`，并记录任何非 `ok` 结果。
2. 运行 `synaps3 admin task stats` 和 `synaps3 admin task list --status exhausted`，检查活动任务和耗尽重试的任务。
3. 停止 SynapS3，避免备份过程中对象数据、元数据和任务状态继续变化。Compose 部署运行 `docker compose stop synaps3`。

不要在 SynapS3 仍在运行时创建文件系统归档。

## SQLite 备份

SQLite 是默认数据库。停止 SynapS3 后，备份完整运行数据卷，让数据库、WAL/SHM 文件、配置和缓存处于同一恢复时间点：

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
docker run --rm \
  -v "$PWD":/backup \
  alpine:3 \
  sh -c 'cd /backup && tar tzf synaps3-data.tgz >/dev/null && sha256sum synaps3-data.tgz > synaps3-data.tgz.sha256 && sha256sum -c synaps3-data.tgz.sha256'
```

归档列表和校验和验证必须成功退出。将 `synaps3-data.tgz` 与 `synaps3-data.tgz.sha256` 一起保存到受保护的备份存储。

## PostgreSQL 备份

如果部署使用 PostgreSQL，停止 SynapS3 后：

1. 使用 `pg_dump`、托管数据库快照，或部署批准的 PostgreSQL 备份工具创建数据库原生备份。
2. 单独备份 SynapS3 配置和缓存数据卷。
3. 为数据库备份和数据卷归档标记相同的恢复时间点。
4. 验证两个备份产物后再重启服务。

PostgreSQL 原生备份代替复制 SQLite 数据库目录，但不能代替配置和缓存备份。

## 重启并验证

备份成功后：

```bash
docker compose start synaps3
curl http://127.0.0.1:9090/healthz
docker compose exec synaps3 synaps3 admin task stats
```

`/healthz` 应返回 `{"status":"ok"}`。恢复 S3 流量前，先排查 `setup` 或 `unhealthy`。

## 恢复顺序

恢复 SQLite 归档前，先验证保存的副本：

```bash
docker run --rm \
  -v "$PWD":/backup:ro \
  alpine:3 \
  sh -c 'cd /backup && sha256sum -c synaps3-data.tgz.sha256'
```

1. 停止 SynapS3，并保持 S3 流量关闭。
2. 验证归档校验和，确认数据库与缓存带有相同的恢复时间点标记。
3. 把运行数据卷恢复到空的替换位置。PostgreSQL 部署先恢复数据库原生备份，再连接匹配的配置和缓存数据。
4. 确认恢复后的配置和凭据文件权限为 `0600`，并允许 SynapS3 运行账户读取。
5. 启动 SynapS3，检查 `/healthz`、任务统计和耗尽重试的任务，再通过 S3 API 读取一个已知对象。

不要把一个时间点的数据库备份与另一个时间点的缓存数据混用。
