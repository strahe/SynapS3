---
title: 概览
description: 了解 SynapS3 的网关角色、写入边界和入门路径。
---

# 概览

SynapS3 是开源、可自托管的 S3 兼容网关，用来把对象存到 Filecoin。现有 S3 客户端继续按 S3 API 读写；SynapS3 先写本地缓存、提交元数据，再把 Filecoin 上传交给后台任务。

## 为什么需要它

S3 客户端只需要端点、访问凭据，以及 bucket 和 object 操作。Filecoin 侧还要处理存储提供方、数据提交和远端持久化。SynapS3 把这些工作留在网关之后，客户端不需要理解这条存储路径。

## SynapS3 负责什么

- 接受常用的 S3 bucket、object、versioning 和 multipart 请求。
- 写入成功前，把对象持久化到本地缓存，并提交元数据。
- 由后台任务处理 Filecoin 上传、失败重试、可读副本修复和缓存淘汰。
- 在仪表盘和 Admin API 中查看 bucket、任务、钱包操作、存储拓扑、设置和健康状态。

## 架构

<img class="architecture-overview architecture-overview--light" src="/architecture-overview-light.svg" alt="SynapS3 架构图">
<img class="architecture-overview architecture-overview--dark" src="/architecture-overview.svg" alt="SynapS3 架构图">

S3 写入返回成功时，对象已经写入本地缓存，元数据也已经提交。读取时会先查本地缓存；缓存缺失时，如果远端已有已提交副本，SynapS3 可以从远端取回对象。Filecoin 上传、失败重试、副本修复和缓存清理都在 S3 响应之后由后台任务执行。

SynapS3 目前按单机部署设计。缓存磁盘和数据库都是运行时数据，不能当作临时目录清理。

更完整的说明见[架构](../concepts/architecture.md)、[写入路径与缓存](../concepts/write-path-cache.md)和 [Filecoin 存储流程](../concepts/filecoin-storage-flow.md)。

## 从哪里开始

- 评估功能时，使用[快速开始](./quick-start.md)运行临时节点。
- 部署节点时，使用 [Docker 部署](./docker.md)。
- 连接工具时，使用 [S3 客户端](./s3-clients.md) 查看 AWS CLI、rclone 和 MinIO Client 示例。
