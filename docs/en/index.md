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
      text: Installation Options
      link: /en/getting-started/overview#start-here
    - theme: alt
      text: S3 Clients
      link: /en/getting-started/s3-clients
features:
  - title: Self-Hosted Gateway
    details: Run a single SynapS3 node with Docker Compose or a source build.
    link: /en/getting-started/overview#start-here
    linkText: Installation Options
  - title: S3 Client Compatibility
    details: Connect AWS CLI, rclone, or MinIO Client with path-style addressing.
    link: /en/getting-started/s3-clients
    linkText: Client Examples
  - title: Filecoin Storage
    details: Store objects with Filecoin storage providers behind a standard S3 endpoint.
    link: /en/concepts/filecoin-storage-flow
    linkText: Storage Flow
  - title: Admin Dashboard
    details: Manage buckets, objects, wallet, tasks, storage providers, settings, and health.
    link: /en/reference/admin-api
    linkText: Admin API Reference
  - title: Operations
    details: Monitor health and tasks, back up runtime data, and recover from failures.
    link: /en/operations/troubleshooting
    linkText: Troubleshooting
  - title: Replica Repair
    details: "Coming soon: replica repair for storage provider outages."
    link: /en/concepts/filecoin-storage-flow#planned-replica-repair
    linkText: Feature Details
---
