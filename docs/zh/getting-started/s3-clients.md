---
title: S3 客户端
description: 创建 SynapS3 S3 凭据，并使用常见客户端验证 path-style 访问。
---

# S3 客户端

SynapS3 启动后，先确认 Admin API 可以通过 `127.0.0.1:9090` 访问。然后创建 S3 用户，并用 path-style 客户端验证存储桶和对象操作。

## 前置条件

- SynapS3 正在运行。
- `curl http://127.0.0.1:9090/healthz` 返回 `{"status":"ok"}`。
- S3 API 可通过 `http://localhost:8080` 访问。
- 运行 `synaps3 admin` 的机器能访问 Admin API。
- 已设置 `SYNAPS3_ADMIN_PASSWORD`，或配置文件同目录存在 `admin-initial-password`，或可以在命令提示中输入 Admin 密码。

## 创建凭据

创建普通 S3 用户：

```bash
synaps3 admin s3-user create
```

没有受保护的密码文件时，在无回显提示中输入 Admin 密码。命令会输出 access key、secret key 和 role。secret key 只显示一次：请保存到权限为 `0600` 的客户端凭据文件；如果泄露，立即轮换。

之后可以列出用户：

```bash
synaps3 admin s3-user list
```

## 准备测试对象

选择客户端前先创建一个测试文件：

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
```

`hello.txt` 应至少有 128 字节。

> [!IMPORTANT]
> Filecoin 上传路径要求对象不小于 127 字节。测试文件请保持 128 字节或更大。

## 选择存储桶名称

存储桶名称可能公开记录在链上，请勿包含敏感信息。

## 稳定对象限制

- 对象大小必须在 `127` 到 `1,065,353,216` 字节之间。
- 对象键必须是有效 UTF-8，不得包含 NUL，并且最多 `1024` 字节。
- 分段上传最多支持 `10,000` 个 parts，完整对象仍受对象大小上限约束。

完整支持边界见 [S3 兼容性](../reference/s3-compatibility.md)。

## AWS CLI

交互输入 access key 和 secret key，再配置 path-style addressing：

```bash
aws configure --profile synaps3
aws configure set profile.synaps3.region us-east-1
aws configure set profile.synaps3.s3.addressing_style path
chmod 600 ~/.aws/credentials
```

创建存储桶并上传测试对象：

```bash
aws --profile synaps3 --endpoint-url http://localhost:8080 s3api create-bucket --bucket demo
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp hello.txt s3://demo/hello.txt
aws --profile synaps3 --endpoint-url http://localhost:8080 s3 cp s3://demo/hello.txt -
```

最后一个命令会输出填充后的 `hello synaps3` 内容。

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

验证存储桶和对象访问：

```bash
rclone mkdir synaps3:demo
rclone copyto hello.txt synaps3:demo/hello.txt
rclone cat synaps3:demo/hello.txt
```

`rclone cat` 会输出上传的对象内容。

`rclone.conf` 包含 secret key，应保持 `0600` 权限。

## MinIO Client

通过终端交互读取凭据，创建 alias，并上传同一个文件：

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

`mc cat` 会输出上传的对象内容。

alias 会把凭据保存在 `~/.mc/config.json`。无回显提示可以避免 secret key 进入 shell history；生成的配置应保持 `0600` 权限。

这些示例中的 HTTP 端点只用于本地评估。生产环境请使用原生 TLS 或受控 TLS 反向代理提供的 HTTPS 端点。

## 常见失败

| 现象 | 检查项 |
| --- | --- |
| `AccessDenied` | 确认 access key 和 secret key 来自 `synaps3 admin s3-user create`。 |
| 客户端使用 virtual-hosted 存储桶访问 | 开启 path-style addressing 或客户端中的等价设置。 |
| 上传成功但 Filecoin 存储仍在等待 | 查看仪表盘任务页，或运行 `synaps3 admin task list --status queued`。 |
| 对象大小被拒绝 | 确保对象大小在 `127` 到 `1,065,353,216` 字节之间。 |
| 远程主机无法访问 Admin 仪表盘 | 保持 Admin 监听本机回环地址，并使用 `ssh -L 9090:127.0.0.1:9090 user@server`。 |
