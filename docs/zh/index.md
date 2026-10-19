---
layout: home
title: SynapS3
description: 开源、可自托管的 Filecoin S3 兼容网关。
hero:
  name: SynapS3
  tagline: 开源、可自托管的 Filecoin S3 网关。
  image:
    src: /readme-dashboard.png
    alt: SynapS3 仪表盘
  actions:
    - theme: brand
      text: 入门概览
      link: /zh/getting-started/overview
    - theme: alt
      text: 安装方式
      link: /zh/getting-started/overview#从哪里开始
    - theme: alt
      text: S3 客户端
      link: /zh/getting-started/s3-clients
features:
  - title: 自部署网关
    details: 使用 Docker Compose 或源码构建运行单节点 SynapS3。
    link: /zh/getting-started/overview#从哪里开始
    linkText: 安装方式
  - title: S3 客户端兼容
    details: 以 path-style 方式连接 AWS CLI、rclone 或 MinIO Client。
    link: /zh/getting-started/s3-clients
    linkText: 客户端示例
  - title: Filecoin 存储
    details: 通过标准 S3 端点，把对象存到 Filecoin 存储提供方。
    link: /zh/concepts/filecoin-storage-flow
    linkText: 存储流程
  - title: 仪表盘
    details: 管理存储桶、对象、钱包、任务、存储提供方、设置和健康状态。
    link: /zh/reference/admin-api
    linkText: Admin API 参考
  - title: 运维
    details: 监控健康状态和任务，备份运行数据，从故障中恢复。
    link: /zh/operations/troubleshooting
    linkText: 故障排查
  - title: 副本修复
    details: 即将支持：存储提供方不可用时的副本修复。
    link: /zh/concepts/filecoin-storage-flow#计划支持的副本修复
    linkText: 功能说明
---
