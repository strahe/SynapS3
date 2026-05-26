---
title: 写入路径与缓存
description: 理解 SynapS3 如何接受 S3 写入、在本地持久化对象，并在读取时使用缓存。
---

# 写入路径与缓存

SynapS3 使用 cache-first durability model。一次成功的 S3 写入表示对象字节已持久化到本地磁盘，并且元数据已提交到数据库。

## PutObject 流程

```text
Client PUT /bucket/key
  -> SynapseBackend.PutObject
  -> cache.Put(bucket, key, body)
  -> repositories transaction
  -> return 200 OK with ETag
```

缓存写入会：

- 写入临时文件，
- 计算 MD5 ETag 和 SHA-256 checksum，
- fsync 文件，
- 原子 rename 到最终位置，
- fsync 父目录。

数据库事务会 upsert object、递增 generation，并创建 upload task。

## 持久化不变量

::: tip
SynapS3 只有在本地缓存持久化和数据库提交都成功后才返回成功。
:::

这让 S3 响应不依赖 Filecoin provider 延迟。Provider upload 在写入被接受后进行。

## 读取路径

`GetObject` 优先读取本地缓存。如果缓存缺失且已有已提交 provider metadata，SynapS3 可以从 provider storage retrieve 对象，验证 checksum，返回响应，并尽力 rehydrate cache。

## Multipart Uploads

Multipart uploads 会把 parts 暂存在 cache。Complete 会验证请求的 parts、计算 S3 multipart ETag、组装最终对象、提交元数据，然后清理 upload staging directory。

## 运维影响

| 条件 | 含义 |
| --- | --- |
| Cache disk full | 新写入可能在进入 Filecoin 存储前失败。 |
| Upload worker down | 已确认写入仍在本地，但远端存储不会推进。 |
| Cache entry evicted | 如果有 provider metadata 且可 retrieve，读取仍可成功。 |
| Database commit failed | S3 写入不会返回成功。 |

容量和恢复步骤见[运行数据](../configuration/runtime-data.md)和[故障排查](../operations/troubleshooting.md)。
