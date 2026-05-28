---
title: S3 Clients
description: Create SynapS3 S3 credentials and verify path-style access with common clients.
---

# S3 Clients

After SynapS3 is serving and the admin API is reachable on `127.0.0.1:9090`, create an S3 user and verify bucket and object operations with a path-style client.

## Prerequisites

- SynapS3 is running.
- `curl http://127.0.0.1:9090/healthz` returns `{"status":"ok"}`.
- The S3 API is reachable at `http://localhost:8080`.
- The admin API is reachable from the machine where you run `synaps3 admin`.
- `SYNAPS3_ADMIN_PASSWORD` is set, `admin-initial-password` exists next to the config file, or you can enter the Admin password at the prompt.

## Create Credentials

Create a regular S3 user:

```bash
export SYNAPS3_ADMIN_PASSWORD='replace-with-admin-password'
synaps3 admin s3-user create
```

Expected result: the command prints an access key, secret key, and role. Store the secret once; rotate it if it is exposed.

List users later with:

```bash
synaps3 admin s3-user list
```

## Prepare Test Object

Create one test file before choosing a client:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
```

Expected result: `hello.txt` is at least 128 bytes. The Filecoin upload path requires objects of at least 127 bytes.

## AWS CLI

Configure path-style addressing for the profile that talks to SynapS3:

```bash
aws configure set profile.synaps3.aws_access_key_id replace-with-access-key
aws configure set profile.synaps3.aws_secret_access_key replace-with-secret-key
aws configure set profile.synaps3.region us-east-1
aws configure set profile.synaps3.s3.addressing_style path
```

Create a bucket and upload a small test object:

```bash
aws --profile synaps3 --endpoint-url http://localhost:8080 s3api create-bucket --bucket demo
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp hello.txt s3://demo/hello.txt
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp s3://demo/hello.txt -
```

Expected result: the final command prints the padded `hello synaps3` content.

## rclone

Create a remote in `rclone.conf`:

```ini
[synaps3]
type = s3
provider = Other
access_key_id = replace-with-access-key
secret_access_key = replace-with-secret-key
endpoint = http://localhost:8080
region = us-east-1
force_path_style = true
```

Verify bucket and object access:

```bash
rclone mkdir synaps3:demo
rclone copyto hello.txt synaps3:demo/hello.txt
rclone cat synaps3:demo/hello.txt
```

Expected result: `rclone cat` prints the uploaded object.

## MinIO Client

Create an alias and upload the same file:

```bash
mc alias set synaps3 http://localhost:8080 replace-with-access-key replace-with-secret-key
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

Expected result: `mc cat` prints the uploaded object.

## Common Failures

| Symptom | Check |
| --- | --- |
| `AccessDenied` | Confirm the access key and secret came from `synaps3 admin s3-user create`. |
| Client tries virtual-hosted buckets | Enable path-style addressing or equivalent client setting. |
| Upload succeeds but Filecoin storage is pending | Check the dashboard task view or `synaps3 admin task list --status queued`. |
| Small test object fails later in the pipeline | Use a file of at least 127 bytes. |
| Remote host cannot reach the admin dashboard | Keep admin on loopback and use `ssh -L 9090:127.0.0.1:9090 user@server`. |
