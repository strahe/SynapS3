---
layout: home
title: SynapS3
description: An open-source, self-hosted S3-compatible gateway for Filecoin storage.
hero:
  name: SynapS3
  tagline: Open-source, self-hosted S3 gateway for Filecoin.
  image:
    src: /readme-dashboard.png
    alt: SynapS3 dashboard
  actions:
    - theme: brand
      text: Quick Start
      link: /en/getting-started/quick-start
    - theme: alt
      text: Docker Deployment
      link: /en/getting-started/docker
    - theme: alt
      text: S3 Clients
      link: /en/getting-started/s3-clients
features:
  - title: Self-Hosted Gateway
    details: Run SynapS3 in your own environment with Docker or source builds.
    link: /en/getting-started/docker
    linkText: Docker Deployment
  - title: S3 Client Compatibility
    details: Create S3 credentials and connect AWS CLI, rclone, or MinIO Client.
    link: /en/getting-started/s3-clients
    linkText: Client Examples
  - title: Filecoin Storage
    details: Store objects through Filecoin storage providers while keeping standard S3 access.
    link: /en/concepts/filecoin-storage-flow
    linkText: Storage Flow
  - title: Admin Dashboard
    details: Manage buckets, objects, wallet, tasks, topology, settings, and health.
    link: /en/reference/admin-api
    linkText: Admin API
  - title: Operations
    details: Monitor health, inspect tasks, and handle upgrades, recovery, and troubleshooting.
    link: /en/operations/troubleshooting
    linkText: Troubleshooting
  - title: Replica Repair
    details: Coming soon. If a storage provider becomes unavailable, SynapS3 will copy data to another available provider to maintain the configured copy count.
    link: /en/operations/upgrade-recovery
    linkText: Recovery Guide
---
