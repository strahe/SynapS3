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
      text: Get Started
      link: /en/getting-started/overview
    - theme: alt
      text: Docker Deployment
      link: /en/getting-started/docker
    - theme: alt
      text: S3 Clients
      link: /en/getting-started/s3-clients
features:
  - title: Self-Hosted Gateway
    details: Run SynapS3 in your own environment with Docker or a local build.
    link: /en/getting-started/docker
    linkText: Docker Deployment
  - title: S3 Client Compatibility
    details: Create S3 credentials and connect AWS CLI, rclone, or MinIO Client.
    link: /en/getting-started/s3-clients
    linkText: Client Examples
  - title: Filecoin Storage
    details: Write objects to Filecoin while keeping standard S3 access.
    link: /en/concepts/filecoin-storage-flow
    linkText: Storage Flow
  - title: Admin Dashboard
    details: View buckets, objects, wallet, tasks, topology, settings, and health.
    link: /en/reference/admin-api
    linkText: Admin API Reference
  - title: Operations
    details: Check health and task queues, then handle upgrades, recovery, and troubleshooting.
    link: /en/operations/troubleshooting
    linkText: Troubleshooting
  - title: Replica Repair
    details: "Coming soon: replica repair for provider outages."
    link: /en/concepts/filecoin-storage-flow#replica-repair-vision
    linkText: Storage Vision
---
