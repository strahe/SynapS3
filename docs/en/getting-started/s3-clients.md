---
title: S3 Clients
description: Create S3 credentials and configure AWS CLI, rclone, and MinIO Client for SynapS3.
---

# S3 Clients

SynapS3 requires path-style addressing. The examples use `http://localhost:8080`; for production, use the HTTPS endpoint of your S3 TLS setup.

## Create Credentials

Create an S3 user on the dashboard's **S3 users** page, or with the CLI:

```bash
synaps3 admin s3-user create
synaps3 admin s3-user list
```

The CLI needs the Admin password; see [Admin Commands](../reference/cli.md#admin-commands). `create` prints the access key, secret key, and role. The secret key is shown only once; store it in a client configuration file with mode `0600`.

| Role | Dashboard label | Permissions |
| --- | --- | --- |
| `userplus` (default) | **User+** | Creates buckets and accesses the buckets it owns. |
| `user` | **User** | Accesses the buckets it owns; cannot create buckets. |
| `admin` | **Admin** | Accesses all buckets and administers S3 API operations. |

Bucket names are written to Filecoin data set metadata, which is public on-chain. Do not put sensitive information in bucket names.

Objects must be 127 to 1,065,353,216 bytes. See [Stable Limits](../reference/s3-compatibility.md#stable-limits) for key and multipart limits. The examples upload this test file:

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
```

## AWS CLI

Enter the access key and secret key at the prompts, then set the region and path-style addressing:

```bash
aws configure --profile synaps3
aws configure set profile.synaps3.region us-east-1
aws configure set profile.synaps3.s3.addressing_style path
chmod 600 ~/.aws/credentials
```

```bash
aws --profile synaps3 --endpoint-url http://localhost:8080 s3api create-bucket --bucket demo
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp hello.txt s3://demo/hello.txt
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp s3://demo/hello.txt -
```

The last command prints the file content.

## rclone

Add a remote to `rclone.conf` and keep the file at mode `0600`:

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

```bash
rclone mkdir synaps3:demo
rclone copyto hello.txt synaps3:demo/hello.txt
rclone cat synaps3:demo/hello.txt
```

## MinIO Client

The `read -rs` prompt keeps the secret key out of shell history; `mc` stores it in `~/.mc/config.json`:

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

## Common Failures

| Symptom | Fix |
| --- | --- |
| `AccessDenied` | Check the access key and secret key. Creating a bucket needs the `userplus` or `admin` role. |
| The client uses virtual-hosted bucket URLs | Enable path-style addressing. |
| `503 SlowDown` | Read the error message and see [S3 Requests Return SlowDown](../operations/troubleshooting.md#s3-requests-return-slowdown). |
| Object size is rejected | Keep the object between 127 and 1,065,353,216 bytes. |
| The upload succeeds but Filecoin storage is pending | Check the dashboard **Tasks** page or `synaps3 admin task list --status pending`. |
