---
title: CLI 参考
description: SynapS3 setup、serve、wallet、S3 users、settings 和 tasks 常用 CLI 命令。
---

# CLI 参考

SynapS3 暴露 S3 API、Admin API 和用于本地操作的 CLI 命令。

## 端点

| 界面 | 默认地址 |
| --- | --- |
| S3 API | `http://localhost:8080` |
| Dashboard 和 Admin API | `http://127.0.0.1:9090` |
| 健康检查 | `GET http://127.0.0.1:9090/healthz` |
| 指标 | `GET http://127.0.0.1:9090/metrics` |

## 运行时命令

| 命令 | 用途 |
| --- | --- |
| `synaps3 init` | 初始化 `~/.synaps3` 运行数据。 |
| `synaps3 init --dir /var/lib/synaps3` | 初始化自定义 app data directory。 |
| `synaps3 serve` | 启动 S3 gateway、dashboard、Admin API 和 workers。 |
| `synaps3 migrate` | 执行数据库迁移并退出。 |
| `synaps3 version` | 打印版本信息。 |

## Wallet 命令

```bash
synaps3 wallet generate
synaps3 wallet fund-testnet 0x...
synaps3 wallet deposit 2 # 2 USDFC
```

预期结果：`generate` 输出钱包材料，`fund-testnet` 领取 Calibration 资产，`deposit` 使用已配置 private key 提交 `2 USDFC` deposit。

## Admin 命令

```bash
synaps3 admin status
synaps3 admin s3-user create
synaps3 admin s3-user list
synaps3 admin settings get
synaps3 admin settings set cache.max_size_gb=200
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 100
synaps3 admin task retry 42
```

脚本化成功响应时可在 admin 命令上使用 `--json`。

## Settings 安全

Admin API 包含 settings、wallet operations、task retries 和 S3 user management 的写端点。保持它绑定 loopback，或放在经过认证的私有访问层后面。

高风险变更需要确认：

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
synaps3 admin s3-user create --role admin --yes
synaps3 admin s3-user delete <access-key> --yes
```

## 远程 Admin 访问

如果 SynapS3 运行在另一台主机，保持 `admin.addr` 为 `127.0.0.1:9090` 并使用 tunnel：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

然后使用默认 admin URL 运行本地 admin 命令，或显式传入 `--admin-url`。
