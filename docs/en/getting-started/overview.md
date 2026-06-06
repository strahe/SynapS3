---
title: Overview
description: Understand the SynapS3 gateway role, write boundary, and first setup path.
---

# Overview

SynapS3 is an open-source, self-hosted S3-compatible gateway for Filecoin storage. Existing S3 clients keep using the S3 API; SynapS3 writes object data to local cache, commits metadata, and moves Filecoin upload work to background workers.

## Why It Exists

S3 clients need an endpoint, credentials, and bucket/object operations. Filecoin storage also needs provider selection, data submission, and remote durability after the S3 response. SynapS3 keeps that Filecoin work behind the gateway so clients do not need to understand the storage path.

## What SynapS3 Does

- Accepts common S3 bucket, object, versioning, and multipart requests.
- Persists object bytes to local cache and commits metadata before returning write success.
- Runs Filecoin uploads, retries, readable-copy repair, and cache eviction in background workers.
- Shows buckets, tasks, wallet operations, topology, settings, and health in the dashboard and Admin API.

## Architecture

<img class="architecture-overview architecture-overview--light" src="/architecture-overview-light.svg" alt="SynapS3 architecture">
<img class="architecture-overview architecture-overview--dark" src="/architecture-overview.svg" alt="SynapS3 architecture">

A successful S3 write means the object is durable in local cache and its metadata has been committed. Reads use local cache first; on a cache miss, SynapS3 can fetch a committed remote copy. After the S3 response, background workers upload to Filecoin, retry failures, repair missing readable copies, and clean cache.

SynapS3 is designed for single-node deployments today. The cache disk and database are runtime data; do not treat them as scratch storage.

For deeper details, see [Architecture](../concepts/architecture.md), [Write Path and Cache](../concepts/write-path-cache.md), and [Filecoin Storage Flow](../concepts/filecoin-storage-flow.md).

## Start Here

- For evaluation, use [Quick Start](./quick-start.md) to run a temporary node.
- For a long-running single-host node, use [Docker Deployment](./docker.md).
- To connect tooling, use [S3 Clients](./s3-clients.md) for AWS CLI, rclone, and MinIO Client examples.
