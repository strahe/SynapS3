---
title: S3 客户端
description: 创建 SynapS3 S3 凭据，并使用常见客户端验证 path-style 访问。
---

# S3 客户端

SynapS3 启动且 Admin API 可通过 `127.0.0.1:9090` 访问后，创建 S3 用户，并用 path-style 客户端验证 bucket 和 object 操作。

## 前置条件

- SynapS3 正在运行。
- `curl http://127.0.0.1:9090/healthz` 返回 `{"status":"ok"}`。
- S3 API 可通过 `http://localhost:8080` 访问。
- 运行 `synaps3 admin` 的机器能访问 Admin API。

## 创建凭据

创建普通 S3 用户：

```bash
synaps3 admin s3-user create
```

预期结果：命令输出 access key、secret key 和 role。Secret 只会显示一次；如果泄露，应立即轮换。

之后可用以下命令列出用户：

```bash
synaps3 admin s3-user list
```

## 准备测试对象

选择客户端前先创建一个测试文件：

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
```

预期结果：`hello.txt` 至少 128 字节。Filecoin 上传路径要求对象不小于 127 字节。

## AWS CLI

为 SynapS3 profile 配置 path-style addressing：

```bash
aws configure set profile.synaps3.aws_access_key_id replace-with-access-key
aws configure set profile.synaps3.aws_secret_access_key replace-with-secret-key
aws configure set profile.synaps3.region us-east-1
aws configure set profile.synaps3.s3.addressing_style path
```

创建 bucket 并上传测试对象：

```bash
aws --profile synaps3 --endpoint-url http://localhost:8080 s3api create-bucket --bucket demo
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp hello.txt s3://demo/hello.txt
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp s3://demo/hello.txt -
```

预期结果：最后一个命令输出填充后的 `hello synaps3` 内容。

## rclone

在 `rclone.conf` 中创建 remote：

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

验证 bucket 和 object 访问：

```bash
rclone mkdir synaps3:demo
rclone copyto hello.txt synaps3:demo/hello.txt
rclone cat synaps3:demo/hello.txt
```

预期结果：`rclone cat` 输出上传的对象内容。

## MinIO Client

创建 alias 并上传同一个文件：

```bash
mc alias set synaps3 http://localhost:8080 replace-with-access-key replace-with-secret-key
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

预期结果：`mc cat` 输出上传的对象内容。

## 常见失败

| 现象 | 检查项 |
| --- | --- |
| `AccessDenied` | 确认 access key 和 secret 来自 `synaps3 admin s3-user create`。 |
| 客户端尝试 virtual-hosted bucket | 开启 path-style addressing 或客户端中的等价设置。 |
| 上传成功但 Filecoin 存储仍在等待 | 查看仪表盘任务页，或运行 `synaps3 admin task list --status queued`。 |
| 很小的测试对象后续上传失败 | 使用至少 127 字节的文件。 |
| 远程主机无法访问 Admin dashboard | 保持 Admin 监听 loopback，并使用 `ssh -L 9090:127.0.0.1:9090 user@server`。 |
