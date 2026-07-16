---
title: Overview
description: Understand the SynapS3 gateway role, write boundary, and first setup path.
---

# Overview

SynapS3 is an open-source, self-hosted S3-compatible gateway for Filecoin storage. Existing S3 clients keep using the S3 API; SynapS3 saves objects locally before returning success, then continues Filecoin storage in the background.

## Why It Exists

S3 clients need an endpoint, credentials, and bucket/object operations. Filecoin storage also needs provider selection, data submission, and remote durability after the S3 response. SynapS3 keeps that Filecoin work behind the gateway so clients do not need to understand the storage path.

## What SynapS3 Does

- Accepts common S3 bucket, object, versioning, and multipart requests.
- Persists object data and metadata locally before returning write success.
- Uses background tasks to complete the initial target copies, retry failed work, and safely evict cache.
- Shows buckets, tasks, wallet operations, topology, settings, and health in the dashboard and Admin API.

## Architecture

<img class="architecture-overview architecture-overview--light" src="/architecture-overview-light.svg" alt="SynapS3 architecture">
<img class="architecture-overview architecture-overview--dark" src="/architecture-overview.svg" alt="SynapS3 architecture">

A successful S3 write means the object is durable in local cache and its metadata has been recorded. Reads use local cache first; on a cache miss, SynapS3 can fetch an available remote copy. After the S3 response, background tasks complete the initial target copies, retry failures, and clean cache when the configured policy allows it.

Repairing copies affected by a storage provider becoming unavailable is a separate product capability that is coming soon. See [Replica Repair Vision](../concepts/filecoin-storage-flow.md#replica-repair-vision).

SynapS3 supports single-node deployments. The cache disk and database are runtime data; do not treat them as scratch storage.

For deeper details, see [Architecture](../concepts/architecture.md), [Write Path and Cache](../concepts/write-path-cache.md), and [Filecoin Storage Flow](../concepts/filecoin-storage-flow.md).

## Start Here

- For evaluation, use [Quick Start](./quick-start.md) to run a temporary node.
- For deployment, use [Docker Deployment](./docker.md).
- To connect tooling, use [S3 Clients](./s3-clients.md) for AWS CLI, rclone, and MinIO Client examples.
