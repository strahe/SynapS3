---
title: 运行数据
description: 理解 SynapS3 配置、元数据、缓存数据的存放位置和备份范围。
---

# 运行数据

SynapS3 将配置、元数据和缓存对象数据存储在本地磁盘。长期运行节点应把这些数据放在可靠存储上，并在升级前备份。

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
| `admin-initial-password` | 保存非交互 init 和密码重置生成的 Admin 密码。文件权限为 `0600`；安全保存密码后应删除或轮换。 |
| `db/` | 保存 buckets、objects、versions、tasks、users 和 storage metadata。 |
| `cache/` | 保存 Filecoin 上传前后本地持久化对象字节。 |
| 环境密钥 | 可能保存 Filecoin private key 和部署特定覆盖项。 |

## 备份示例

Docker Compose：

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
```

预期结果：归档包含 volume 中的配置、数据库和缓存数据。
