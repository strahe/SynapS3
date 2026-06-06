---
layout: home
title: SynapS3
description: 开源、可自托管的 Filecoin S3 兼容网关。
hero:
  name: SynapS3
  tagline: 开源、可自托管的 Filecoin S3 网关。
  image:
    src: /readme-dashboard.png
    alt: SynapS3 dashboard
  actions:
    - theme: brand
      text: 入门概览
      link: /zh/getting-started/overview
    - theme: alt
      text: Docker 部署
      link: /zh/getting-started/docker
    - theme: alt
      text: S3 客户端
      link: /zh/getting-started/s3-clients
features:
  - title: 自部署网关
    details: 使用 Docker 或源码构建，在自己的环境中运行 SynapS3。
    link: /zh/getting-started/docker
    linkText: Docker 部署
  - title: S3 客户端兼容
    details: 创建 S3 凭据，连接 AWS CLI、rclone 或 MinIO Client。
    link: /zh/getting-started/s3-clients
    linkText: 客户端示例
  - title: Filecoin 存储
    details: 把对象写入 Filecoin，同时保留标准 S3 访问方式。
    link: /zh/concepts/filecoin-storage-flow
    linkText: 存储流程
  - title: 仪表盘
    details: 查看 bucket、object、钱包、后台任务、存储拓扑、设置和健康状态。
    link: /zh/reference/admin-api
    linkText: Admin API
  - title: 运维
    details: 检查健康状态和任务队列，处理升级、恢复和故障排查。
    link: /zh/operations/troubleshooting
    linkText: 故障排查
  - title: 副本修复
    details: 即将支持：存储提供方不可用时的副本修复。
    link: /zh/operations/upgrade-recovery
    linkText: 恢复指南
---
