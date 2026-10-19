---
title: 概览
description: 了解 SynapS3 的功能、节点由哪些部分组成，并选择安装方式。
---

# 概览

SynapS3 是可自托管的 S3 兼容网关，把对象存到 Filecoin。S3 客户端使用 S3 API 读写；SynapS3 负责选择存储提供方、创建数据集，并用自己的钱包支付存储费用。

## SynapS3 做什么

- 支持常用的 S3 存储桶、对象、版本控制和分段上传操作。见 [S3 兼容性](../reference/s3-compatibility.md)。
- 把每个对象作为副本存到 Filecoin 存储提供方，并在前面维护一层本地缓存。
- 提供仪表盘、Admin API 和 CLI，用于管理存储桶、对象、钱包、任务、存储提供方、设置和健康状态。

即将支持：存储提供方不可用时的副本修复。见[计划支持的副本修复](../concepts/filecoin-storage-flow.md#计划支持的副本修复)。

## 架构

<img class="architecture-overview architecture-overview--light" src="/architecture-overview-light.svg" alt="SynapS3 架构图">
<img class="architecture-overview architecture-overview--dark" src="/architecture-overview.svg" alt="SynapS3 架构图">

SynapS3 以单个进程运行。每个数据库只能运行一个 SynapS3 实例；不支持多个实例共用同一个数据库或钱包。

| 组成部分 | 地址或位置 | 作用 |
| --- | --- | --- |
| S3 API | `server.port`，默认 `:8080` | 处理 S3 请求。 |
| 仪表盘和 Admin API | `admin.addr`，默认 `127.0.0.1:9090` | 提供仪表盘、Admin API、`/healthz` 和 `/metrics`。 |
| 本地缓存 | `cache.dir` | 保存对象数据，直到已提交的副本足够、淘汰策略将其删除。 |
| PostgreSQL | `database.dsn` | 保存元数据、任务和存储记录。 |
| 后台任务 | 进程内 | 上传和复制对象、提交批次、释放缓存、清理已删除数据、执行钱包操作、检查存储提供方健康状态。 |
| Filecoin RPC | `filecoin.rpc_url`（出站） | 读取链上状态，发送钱包签名的交易。 |
| 存储提供方 | 存储提供方的服务 URL（出站） | 存储、复制和提供每个副本的 piece。 |

数据在节点中这样流转：

1. S3 写入先存入本地缓存并提交到 PostgreSQL，SynapS3 才返回响应。见[写入路径与缓存](../concepts/write-path-cache.md)。
2. 后台任务把对象上传到存储桶的一个存储提供方，并加入该存储提供方在链上的数据集；存储桶的其他存储提供方再从这个副本复制。见 [Filecoin 存储流程](../concepts/filecoin-storage-flow.md)。
3. 读取时使用本地缓存；缓存中没有该对象时，从已提交的副本读取。
4. 已提交的副本达到存储桶的最小持久副本数后，缓存淘汰策略可以删除本地副本。

本地缓存和数据库中可能有尚未存到 Filecoin 的数据。两者都要放在持久存储上，并一起备份，见[运行数据](../configuration/runtime-data.md)。哪些端口需要暴露，见[网络暴露](../operations/production-checklist.md#网络暴露)。

## 从哪里开始

1. 按 [Docker 部署](./docker.md)或[源码构建](./source.md)安装。
2. 按[快速开始](./quick-start.md)检查节点并上传第一个对象。
3. 按 [S3 客户端](./s3-clients.md)配置客户端。
4. 承接生产流量前，完成[生产环境检查清单](../operations/production-checklist.md)。
