---
title: 写入路径与缓存
description: 理解 SynapS3 如何接受 S3 写入、在本地持久化对象，并在读取时使用缓存。
---

# 写入路径与缓存

SynapS3 采用缓存优先的写入模型。一次 S3 写入返回成功，表示对象内容已经持久化到本地磁盘，元数据也已经提交到数据库。

## PutObject 流程

```mermaid
flowchart TD
  request["接收写入"] --> save["在本地保存对象"]
  save --> metadata["记录元数据"]
  metadata --> response["返回成功和 ETag"]
  response --> storage["在后台继续 Filecoin 存储"]
```

SynapS3 会校验请求，保存对象及其元数据，再返回 S3 兼容的 ETag。客户端收到成功响应后，Filecoin 存储会继续推进。

## 持久化不变量

> [!IMPORTANT]
> SynapS3 只有在本地缓存持久化和数据库提交都成功后才返回成功。

因此，S3 响应不需要等待 Filecoin 存储提供方。对象被接受后，后台任务再继续上传。

## 读取路径

`GetObject` 会先读本地缓存。缓存缺失时，如果记录了可用的远端副本，SynapS3 可以从存储提供方取回并校验对象、返回响应，并在可能时恢复本地缓存。

## 分段上传

分段上传会把各分段保存在本地，直到完成上传。完成时会校验请求中的分段、组装最终对象、返回 S3 multipart ETag，并安排后台 Filecoin 存储。

## 运维影响

| 条件 | 含义 |
| --- | --- |
| 缓存磁盘已满 | 新写入可能在进入 Filecoin 存储前失败。 |
| 后台存储未运行 | 已确认写入仍在本地，但远端存储不会推进。 |
| 缓存对象已淘汰 | 如果远端元数据存在且对象可取回，读取仍可成功。 |
| 数据库提交失败 | S3 写入不会返回成功。 |

容量和恢复步骤见[运行数据](../configuration/runtime-data.md)和[故障排查](../operations/troubleshooting.md)。
