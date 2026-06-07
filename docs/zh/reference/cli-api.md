---
title: CLI 参考
description: SynapS3 setup、serve、钱包、S3 用户、设置和任务常用 CLI 命令。
---

# CLI 参考

SynapS3 提供 S3 API、Admin API，以及用于本地操作的 CLI 命令。

## 端点

| 界面 | 默认地址 |
| --- | --- |
| S3 API | `http://localhost:8080` |
| 仪表盘和 Admin API | `http://127.0.0.1:9090` |
| 健康检查 | `GET http://127.0.0.1:9090/healthz` |
| 指标 | `GET http://127.0.0.1:9090/metrics` |

## 配置文件来源

需要配置文件的命令优先使用 `--config <path>`；未传时读取非空 `SYNAPS3_CONFIG`；仍未设置时使用 `~/.synaps3/config.toml`。`synaps3 init` 只用 `--dir` 选择运行目录，不读取 `SYNAPS3_CONFIG`。`admin-auth reset-password` 必须通过 `--config` 或 `SYNAPS3_CONFIG` 指定目标配置。

## 运行时命令

| 命令 | 用途 |
| --- | --- |
| `synaps3 init` | 初始化 `~/.synaps3` 运行数据。 |
| `synaps3 init --dir /var/lib/synaps3` | 初始化自定义运行数据目录。 |
| `synaps3 serve` | 启动 S3 网关、仪表盘、Admin API 和工作进程。 |
| `synaps3 migrate` | 执行数据库迁移并退出。 |
| `synaps3 admin-auth reset-password --config <path>` | 离线重置 Admin 密码，轮换 session secret，并写入新的 `admin-initial-password` 文件。 |
| `synaps3 version` | 打印版本信息。 |

## Wallet 命令

```bash
synaps3 wallet generate
synaps3 wallet fund-testnet 0x...
synaps3 wallet deposit 2 # 2 USDFC
```

`generate` 输出钱包材料，`fund-testnet` 领取 Calibration 资产，`deposit` 使用已配置的 private key 存入 `2 USDFC`。

## Admin 命令

Admin 命令使用 HTTP Basic auth。用户名来自 `admin.auth.username`；密码来自 `SYNAPS3_ADMIN_PASSWORD`、config 同目录的 `admin-initial-password` 或终端提示。

```bash
export SYNAPS3_ADMIN_PASSWORD='replace-with-admin-password'

synaps3 admin status
synaps3 admin s3-user create
synaps3 admin s3-user list
synaps3 admin settings get
synaps3 admin settings set cache.max_size_gb=200
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task retry 42
```

脚本需要解析成功响应时，可以在 admin 命令上使用 `--json`。

## 设置安全

Admin API 包含设置、钱包操作、任务重试和 S3 用户管理的写端点。默认需要 Admin 认证。保持监听本机回环地址；远程访问应放在 HTTPS 和明确访问控制之后。

高风险变更需要确认：

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
synaps3 admin s3-user create --role admin --yes
synaps3 admin s3-user delete <access-key> --yes
```

## 远程 Admin 访问

如果 SynapS3 运行在另一台主机，保持 `admin.addr` 为 `127.0.0.1:9090` 并使用 SSH 隧道：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

然后使用默认 admin URL 运行本地 admin 命令，或显式传入 `--admin-url`。命令会优先使用 `SYNAPS3_ADMIN_PASSWORD`，其次读取 config 同目录的 `admin-initial-password`，最后提示输入。浏览器访问时，用 init 或 `admin-auth reset-password` 得到的 Admin 用户名和密码登录。
