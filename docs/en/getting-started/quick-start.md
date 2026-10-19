---
title: Quick Start
description: Check a new SynapS3 node, create S3 credentials, and upload a first object.
---

# Quick Start

Complete [Docker Deployment](./docker.md) or [Build from Source](./source.md) first, including the wallet setup.

## Check the Node

```bash
curl http://127.0.0.1:9090/healthz
```

Continue when the response is `{"status":"ok"}`. For `setup` or `unhealthy`, see [Troubleshooting](../operations/troubleshooting.md).

To open the dashboard on a remote host without Admin HTTPS, forward the Admin port and browse to `http://127.0.0.1:9090`:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

## Create an S3 User

In the dashboard, open **S3 users** and select **Create S3 user**, or run:

```bash
synaps3 admin s3-user create
```

The secret key is shown only once. Store it in a client configuration file with mode `0600`.

## Upload the First Object

This MinIO Client example reads the credentials at a prompt so the secret key stays out of shell history:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
printf 'S3 access key: '
read -r S3_ACCESS_KEY
printf 'S3 secret key: '
read -rs S3_SECRET_KEY
printf '\n'
mc alias set synaps3 http://localhost:8080 "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}"
unset S3_ACCESS_KEY S3_SECRET_KEY
chmod 600 ~/.mc/config.json
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

`mc cat` prints the uploaded content. Objects must be at least 127 bytes, so the sample file is padded to 128 characters.

Uploads to a new bucket return `503 SlowDown` until SynapS3 finishes preparing its storage. Retry when the bucket is ready; see [S3 Requests Return SlowDown](../operations/troubleshooting.md#s3-requests-return-slowdown).

Next, configure other clients with [S3 Clients](./s3-clients.md), and complete the [Production Checklist](../operations/production-checklist.md) before serving production traffic.
