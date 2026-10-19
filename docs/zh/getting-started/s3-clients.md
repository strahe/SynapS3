---
title: S3 客户端
description: 创建 S3 凭据，并为 SynapS3 配置 AWS CLI、rclone 和 MinIO Client。
---

# S3 客户端

SynapS3 要求使用 path-style 寻址。示例使用 `http://localhost:8080`；生产环境请改用 S3 TLS 配置提供的 HTTPS 端点。

## 创建凭据

在仪表盘的 **S3 users** 页面创建 S3 用户，或者使用 CLI：

```bash
synaps3 admin s3-user create
synaps3 admin s3-user list
```

CLI 需要 Admin 密码，见 [Admin 命令](../reference/cli.md#admin-命令)。`create` 输出 access key、secret key 和角色。Secret key 只显示一次，请保存到权限为 `0600` 的客户端配置文件中。

| 角色 | 仪表盘标签 | 权限 |
| --- | --- | --- |
| `userplus`（默认） | **User+** | 可以创建存储桶，并访问自己拥有的存储桶。 |
| `user` | **User** | 可以访问自己拥有的存储桶，不能创建存储桶。 |
| `admin` | **Admin** | 可以访问所有存储桶，并管理 S3 API 操作。 |

存储桶名称会写入 Filecoin 数据集元数据，在链上公开可见。不要在存储桶名称中包含敏感信息。

对象大小必须在 127 到 1,065,353,216 字节之间。对象键和分段上传的限制见[稳定限制](../reference/s3-compatibility.md#稳定限制)。示例上传下面这个测试文件：

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
```

## AWS CLI

按提示输入 access key 和 secret key，再设置区域和 path-style 寻址：

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

最后一条命令会输出文件内容。

## rclone

在 `rclone.conf` 中添加 remote，并保持文件权限为 `0600`：

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

`read -rs` 提示符让 secret key 不进入 shell 历史；`mc` 会把它保存在 `~/.mc/config.json`：

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

## 常见失败

| 现象 | 处理 |
| --- | --- |
| `AccessDenied` | 检查 access key 和 secret key。创建存储桶需要 `userplus` 或 `admin` 角色。 |
| 客户端使用 virtual-hosted 风格的存储桶 URL | 启用 path-style 寻址。 |
| `503 SlowDown` | 查看错误消息，并参考 [S3 请求返回 SlowDown](../operations/troubleshooting.md#s3-请求返回-slowdown)。 |
| 对象大小被拒绝 | 对象大小保持在 127 到 1,065,353,216 字节之间。 |
| 上传成功，但 Filecoin 存储仍在等待 | 查看仪表盘 **Tasks** 页面，或运行 `synaps3 admin task list --status pending`。 |
