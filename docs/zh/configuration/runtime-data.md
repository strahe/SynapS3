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
  db/
  cache/
```

Compose 部署通过 `synaps3-data` Docker volume 挂载该路径。

## 必须持久保存的数据

| 数据 | 原因 |
| --- | --- |
| `config.toml` | 保存未由环境变量管理的稳定运行设置。 |
| `db/` | 保存 buckets、objects、versions、tasks、users 和 storage metadata。 |
| `cache/` | 保存 Filecoin 上传前后本地持久化对象字节。 |
| 环境密钥 | 可能保存 Filecoin private key 和部署特定覆盖项。 |

## 缓存策略

`cache.eviction_policy = "lru"` 会在远端存储成功后排队执行本地缓存淘汰。它不是任意旧文件扫描器。

默认容量设置对单机节点较保守：

```toml
[server]
max_connections = 4096
max_requests = 512

[database]
max_open_conns = 4
max_idle_conns = 2

[cache]
max_size_gb = 100
eviction_policy = "lru"
```

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

## 运维检查

```bash
synaps3 admin status
synaps3 admin settings get cache.max_size_gb
```

预期结果：status 输出显示 cache usage 和 worker health。如果缓存接近容量上限，查看[故障排查](../operations/troubleshooting.md)。
