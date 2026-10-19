---
title: CLI 参考
description: 查询 synaps3 用于初始化、钱包、存储提供方和管理的命令、参数与输出。
---

# CLI 参考

## 配置文件

需要读取配置的命令依次使用 `--config <path>`（简写 `-c`）、`SYNAPS3_CONFIG`、`~/.synaps3/config.toml`。

## 运行命令

| 命令 | 用途 |
| --- | --- |
| `synaps3 init [--dir <path>]` | 在 `<path>`（默认 `~/.synaps3`）中创建 `config.toml`、`cache/` 目录和 Admin 凭据。`config.toml` 已存在时失败。不接受 `--config`。 |
| `synaps3 serve` | 执行数据库迁移，然后启动 S3 API、仪表盘和 Admin API，以及后台任务。 |
| `synaps3 migrate` | 执行数据库迁移后退出。 |
| `synaps3 migrate --force-unlock` | 释放被强行终止的迁移留下的锁。只在没有迁移运行时使用。 |
| `synaps3 admin-auth reset-password` | 设置新的 Admin 密码，轮换会话密钥，并把密码写入配置文件旁的 `admin-initial-password`。需要 `--config` 或 `SYNAPS3_CONFIG`。重启 SynapS3 后生效。 |
| `synaps3 version` | 输出版本信息。 |

## Wallet 命令

钱包命令使用配置中的私钥和 RPC 端点，不需要服务在运行。

```bash
synaps3 wallet generate
synaps3 wallet fund-testnet <address>
synaps3 wallet deposit <amount>   # 单位为 USDFC，例如 2
synaps3 wallet approve
```

| 命令 | 输出 |
| --- | --- |
| `generate` | `Address: <address>` 和 `Private key: <key>`。请在私密终端中运行。 |
| `fund-testnet` | 领取 Calibration 测试 FIL 和 USDFC。成功时输出 `CalibnetUSDFC: <hash>` 和 `CalibnetFIL: <hash>`。 |
| `deposit` | 向支付账户存入 USDFC。输出 `Transaction: <hash>` 和 `Status: confirmed`。 |
| `approve` | 批准 FWSS 从支付账户扣款。输出 `Transaction: <hash>` 和 `Status: confirmed`，或 `FWSS approval: already approved`。 |

所有钱包命令都支持 `--json`。`deposit` 和 `approve` 最多等待 `--timeout`（默认 `5m`）获取交易回执；超时则输出 `Status: submitted`。

## 存储提供方命令

列出网络上注册的 PDP 存储提供方：

```bash
synaps3 provider list
synaps3 provider list --active --no-health
synaps3 provider list --network mainnet --json
```

| 参数 | 用途 |
| --- | --- |
| `--json` | 输出 JSON。 |
| `--active` | 只显示活跃的存储提供方。 |
| `--rpc-url <url>` | 覆盖配置中的 RPC 端点。 |
| `--network <calibration\|mainnet>` | 选择网络。 |
| `--timeout <duration>` | 每个存储提供方的健康检查超时，默认 `5s`。 |
| `--no-health` | 跳过健康检查。 |

## Admin 命令

`synaps3 admin` 命令调用配置中 `admin.addr` 上的 Admin API，以 `admin.auth.username` 认证，密码依次取自 `SYNAPS3_ADMIN_PASSWORD`、配置文件旁的 `admin-initial-password`、不回显的密码提示符。

以下参数放在 `admin` 之后、子命令之前：

| 参数 | 用途 |
| --- | --- |
| `--admin-url <url>` | Admin API 基础 URL。 |
| `--json` | 以 JSON 输出成功响应。 |
| `--timeout <duration>` | 请求超时，默认 `10s`。 |

```bash
synaps3 admin status
synaps3 admin s3-user list
synaps3 admin s3-user create --name "Backup client"
synaps3 admin s3-user create --role admin --yes
synaps3 admin s3-user update <access-key> --name "Archive client"
synaps3 admin s3-user update <access-key> --role userplus
synaps3 admin s3-user rotate-secret <access-key>
synaps3 admin s3-user delete <access-key> --yes
synaps3 admin settings get
synaps3 admin settings get cache.max_size_gb
synaps3 admin settings set cache.eviction_policy=lru cache.lru_high_watermark_percent=80
synaps3 admin task stats
synaps3 admin task list --status failed --limit 100
synaps3 admin task retry 42
synaps3 admin task acknowledge 42
synaps3 admin task acknowledge --type storage_store --yes
synaps3 admin storage-confirmation list
```

### S3 用户

- 角色有 `user`、`userplus`（默认）和 `admin`，见[创建凭据](../getting-started/s3-clients.md#创建凭据)。
- `create` 和 `rotate-secret` 只输出一次 secret key。
- 创建 `admin` 用户、授予 `admin` 角色和删除用户都需要 `--yes`。
- 名称可选，且不能重复。`update <access-key> --name ''` 清除名称。

### 设置

- `settings get` 输出全部设置；指定字段时只输出该字段。
- `settings set` 保存 `field=value` 键值对。由环境变量设置的字段会被拒绝。调高 `server.max_connections` 或 `server.max_requests`、修改 `filecoin.network`、启用 `filecoin.allow_private_networks` 时需要 `--yes`。
- 保存的设置在重启后生效。

### 任务

- `task list` 和 `task stats` 接受 `--scope work`（默认）或 `--scope history`。`work` 包含等待中、运行中和未确认的失败任务；`history` 包含已完成、已取消、已确认和已被取代的执行。
- `task list` 可用 `--type`、`--status`、`--limit`（默认 `20`）和 `--cursor <task-id>` 过滤。`work` 下 `--status` 可取 `pending`、`running`、`failed`；`history` 下可取 `completed`、`cancelled`、`failed`。
- `task retry <id>` 为可重试的失败任务启动新的执行，并输出 `Retry task <new-id>`。重复执行返回同一个新执行。
- `task acknowledge <id>` 把已确认的失败移到 `history`，不改变其结果，也不影响能否重试。`task acknowledge --yes` 确认所有失败，可用 `--type` 和 `--before <RFC 3339 时间>` 过滤。
- `storage-confirmation list` 列出需要处理的批次，包括所属任务 ID、存储提供方、数据集、piece 数和存储提供方的响应。用 `task retry <task-id>` 重试。`--limit` 取 1 到 1,000（默认 `100`）。

| 任务类型 | 仪表盘中的操作 |
| --- | --- |
| `bucket_provision` | **Prepare bucket** |
| `upload_plan` | **Prepare upload** |
| `storage_dataset_ensure` | **Create data set** |
| `storage_transfer_plan` | **Prepare replica** |
| `storage_store` | **Upload data** |
| `storage_pull` | **Replicate data** |
| `storage_commit` | **Commit batch** |
| `provider_replacement_coordinate` | **Replace provider** |
| `cache_evict` | **Release cache** |
| `cache_reconcile_durability` | **Recheck cache release** |
| `cache_capacity_reconcile` | **Manage cache capacity** |
| `storage_cleanup` | **Clean up deleted data** |
| `storage_dataset_retire` | **Terminate data set** |
| `wallet_operation` | **Wallet operation** |
| `provider_upload_speed_test` | **Test upload speed** |
| `observability_refresh` | **Refresh storage health** |
| `approved_provider_refresh` | **Refresh approved providers** |
| `endorsed_provider_refresh` | **Refresh endorsed providers** |

## 远程 Admin 访问

要从自己的机器管理远程主机，先转发 Admin 端口：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

之后，本机的 `synaps3 admin` 命令会访问默认地址 `http://127.0.0.1:9090`，并提示输入 Admin 密码。也可以直接在该主机上运行这些命令。
