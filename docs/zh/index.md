---
layout: home
title: SynapS3
description: 开源、自部署的 Filecoin S3 兼容网关。
hero:
  name: SynapS3
  tagline: 开源、自部署的 Filecoin S3 网关。
  image:
    src: /readme-dashboard.png
    alt: SynapS3 dashboard
  actions:
    - theme: brand
      text: 快速开始
      link: /zh/getting-started/quick-start
    - theme: alt
      text: Docker 部署
      link: /zh/getting-started/docker
    - theme: alt
      text: S3 客户端
      link: /zh/getting-started/s3-clients
features:
  - title: 自部署网关
    details: 使用 Docker 或源码构建，在自己的环境运行 SynapS3。
    link: /zh/getting-started/docker
    linkText: Docker 部署
  - title: S3 客户端兼容
    details: 创建 S3 凭据，并连接 AWS CLI、rclone 或 MinIO Client。
    link: /zh/getting-started/s3-clients
    linkText: 客户端示例
  - title: Filecoin 存储
    details: 通过 Filecoin 存储提供商保存对象，并保留标准 S3 访问方式。
    link: /zh/concepts/filecoin-storage-flow
    linkText: 存储流程
  - title: 管理后台
    details: 管理 buckets、objects、wallet、tasks、topology、settings 和 health。
    link: /zh/reference/admin-api
    linkText: Admin API
  - title: 运维
    details: 监控健康状态、检查任务，并处理升级、恢复和故障排查。
    link: /zh/operations/troubleshooting
    linkText: 故障排查
  - title: 副本修复
    details: 即将支持。当存储提供商不可用时，SynapS3 会将数据复制到其他可用存储提供商，保持配置的副本数。
    link: /zh/operations/upgrade-recovery
    linkText: 恢复指南
---
