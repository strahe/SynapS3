---
title: Quick Start
description: Start a SynapS3 node and upload the first object.
---

# Quick Start

Choose an installation method first:

- Run the node in containers: follow [Docker Deployment](./docker.md).
- Build and run the binary directly: follow [Build from Source](./source.md).

Complete initialization, wallet configuration, and startup on the selected page before continuing here.

## Check the Node

The default Admin address is `http://127.0.0.1:9090`:

```bash
curl http://127.0.0.1:9090/healthz
```

The expected response is `{"status":"ok"}`. `setup` means required settings are still missing. `unhealthy` means the database, cache, or workers need attention. See [Troubleshooting](../operations/troubleshooting.md) for recovery steps.

If the node runs on a remote host without Admin HTTPS, use an SSH tunnel for dashboard access:

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

Do not bind port 9090 directly to a public interface.

## Create an S3 User

Sign in to the dashboard and create a user from S3 Users. You can also use the native Admin CLI in the installation environment:

```bash
synaps3 admin s3-user create
```

The secret key is shown only once. Store the credentials in a client configuration protected with `0600`, and rotate them immediately if exposed.

## Upload the First Object

This MinIO Client example reads credentials interactively so the secret key is not placed in shell history:

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

`mc cat` prints the uploaded content. The sample file is padded because the Filecoin upload path requires objects of at least 127 bytes.

See [S3 Clients](./s3-clients.md) for AWS CLI and rclone examples. Complete the [Production Checklist](../operations/production-checklist.md) before serving production traffic.
