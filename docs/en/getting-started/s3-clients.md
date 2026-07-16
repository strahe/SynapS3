---
title: S3 Clients
description: Create SynapS3 S3 credentials and verify path-style access with common clients.
---

# S3 Clients

After SynapS3 is running, confirm the Admin API is reachable on `127.0.0.1:9090`. Then create an S3 user and verify bucket and object operations with a path-style client.

## Prerequisites

- SynapS3 is running.
- `curl http://127.0.0.1:9090/healthz` returns `{"status":"ok"}`.
- The S3 API is reachable at `http://localhost:8080`.
- The admin API is reachable from the machine where you run `synaps3 admin`.
- `SYNAPS3_ADMIN_PASSWORD` is set, `admin-initial-password` exists next to the config file, or you can enter the Admin password at the prompt.

## Create Credentials

Create a regular S3 user:

```bash
synaps3 admin s3-user create
```

Enter the Admin password at the no-echo prompt when no protected password file is available. The command prints an access key, secret key, and role. The secret is shown only once: save it in a client credential file protected with `0600`, and rotate it immediately if it is exposed.

List users later with:

```bash
synaps3 admin s3-user list
```

## Prepare Test Object

Create one test file before choosing a client:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
```

`hello.txt` should be at least 128 bytes.

> [!IMPORTANT]
> The Filecoin upload path requires objects of at least 127 bytes. Keep test files at 128 bytes or larger.

## Choose a Bucket Name

Bucket names may be recorded publicly on-chain. Do not include sensitive information.

## Stable Object Limits

- Object size must be from `127` through `1,065,353,216` bytes.
- Object keys must be valid UTF-8, must not contain NUL, and must be no more than `1024` bytes.
- Multipart uploads support at most `10,000` parts, and the completed object remains subject to the object size limit.

See [S3 Compatibility](../reference/s3-compatibility.md) for the full support boundary.

## AWS CLI

Enter the access key and secret interactively, then configure path-style addressing:

```bash
aws configure --profile synaps3
aws configure set profile.synaps3.region us-east-1
aws configure set profile.synaps3.s3.addressing_style path
chmod 600 ~/.aws/credentials
```

Create a bucket and upload a small test object:

```bash
aws --profile synaps3 --endpoint-url http://localhost:8080 s3api create-bucket --bucket demo
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp hello.txt s3://demo/hello.txt
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp s3://demo/hello.txt -
```

The final command prints the padded `hello synaps3` content.

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

`rclone cat` prints the uploaded object.

Keep `rclone.conf` at permission mode `0600` because it contains the secret key.

## MinIO Client

Read the credentials interactively, create an alias, and upload the same file:

```bash
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

`mc cat` prints the uploaded object.

The alias stores credentials in `~/.mc/config.json`. The no-echo prompt keeps the secret key out of shell history; keep the resulting configuration protected with `0600` permissions.

The HTTP endpoints in these examples are for local evaluation. For production, use the HTTPS endpoint provided by native TLS or your controlled TLS reverse proxy.

## Common Failures

| Symptom | Check |
| --- | --- |
| `AccessDenied` | Confirm the access key and secret key came from `synaps3 admin s3-user create`. |
| Client tries virtual-hosted buckets | Enable path-style addressing or equivalent client setting. |
| Upload succeeds but Filecoin storage is pending | Check the dashboard task view or `synaps3 admin task list --status queued`. |
| Object size is rejected | Keep the object between `127` and `1,065,353,216` bytes. |
| Remote host cannot reach the admin dashboard | Keep admin on loopback and use `ssh -L 9090:127.0.0.1:9090 user@server`. |
