---
title: 升级与恢复
description: 安全升级 SynapS3，并从常见单机故障场景恢复。
---

# 升级与恢复

SynapS3 是单机网关。恢复时先保护本地持久数据，再处理后台任务；依赖失败时，先恢复依赖，再重试任务。

## 升级前

运行：

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 50
```

预期结果：`/healthz` 返回 `ok`，并且所有 exhausted 任务在升级前都有明确处理方式。

重大变更前备份运行数据：

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
```

## 升级 Docker Compose

```bash
docker compose pull
docker compose up -d
docker compose logs --tail=100 synaps3
```

预期结果：服务使用同一个 `synaps3-data` volume 启动，并且 `/healthz` 返回 `ok`。

## 运行流程

```text
PutObject -> cache + DB -> worker -> storage provider + Filecoin
```

- 写入会先提交到本地缓存和元数据，再上传到存储提供方。
- 上传任务使用 backoff 重试，达到最大重试次数后进入 `exhausted`。
- `GetObject` 优先从缓存读取；已有远端元数据时，可以从存储提供方取回对象。
- 不支持 bucket deletion；object delete 是软删除。

## 恢复矩阵

| 场景 | 恢复方式 |
| --- | --- |
| 存储提供方不可达 | 恢复连接，然后重试 exhausted 任务。 |
| RPC 节点故障 | 恢复 RPC 连接，然后重试 exhausted 任务。 |
| 私有 provider URL 被阻止 | 默认保持阻止；只在可信私有部署中开启 `filecoin.allow_private_networks`。 |
| 数据库空间不足 | 释放空间或扩容数据库。 |
| 缓存磁盘空间不足 | 扩容磁盘、提高 `cache.max_size_gb`，或恢复上传和淘汰进度。 |
| 进程崩溃 | 重启；启动恢复会释放过期租约，并重置停滞的上传状态。 |

常用命令：

```bash
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task stats
synaps3 admin task retry 42
synaps3 admin s3-user list
synaps3 admin settings get
```

高风险设置需要 `--yes`：

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
