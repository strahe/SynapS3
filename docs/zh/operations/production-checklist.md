---
title: 生产环境检查清单
description: 承接生产流量前，检查网络暴露、数据、密钥、钱包、配置和监控。
---

# 生产环境检查清单

承接生产流量前，完成以下检查。

## 网络暴露

| 入口 | 暴露方式 |
| --- | --- |
| S3 API（`server.port`，默认 `:8080`） | 通过 TLS 提供服务，使用原生 TLS 或你自己的代理。只对需要的客户端开放。 |
| 仪表盘和 Admin API（`admin.addr`，默认 `127.0.0.1:9090`） | 保持在回环地址。远程访问时使用 SSH 隧道、Docker 部署的 Admin HTTPS 选项，或带访问控制的 HTTPS 反向代理。不要绑定到公网接口。 |
| `/metrics` | 需要 Admin 认证。从同一主机或私有网络抓取。 |
| `/healthz` | 无需认证。`unhealthy` 响应会列出失败的检查项。 |

Admin API 可以修改数据、S3 凭据和钱包资金。保持 `admin.auth.enabled = true`。位于反向代理之后时，只有代理会清除客户端发送的转发标头，才把代理地址填入 `admin.trusted_proxies`。

SynapS3 需要能出站访问 `filecoin.rpc_url` 和存储提供方的服务 URL。

## 数据与备份

- 把 SynapS3 数据目录（`~/.synaps3` 或 `/var/lib/synaps3`）和 PostgreSQL 放在持久存储上，并监控两者的剩余空间。
- 数据库及其备份中有明文的 S3 secret key。按照 S3 凭据的标准限制访问。
- 按[运行数据](../configuration/runtime-data.md)完成备份，并演练一次恢复。
- `config.toml`、`.env`、`.postgres-admin-password`、数据库导出文件、缓存数据和钱包密钥不要进入 git；凭据文件权限设为 `0600`。

## 密钥与钱包

- `SYNAPS3_FILECOIN_PRIVATE_KEY` 放在主机环境、`.env` 或密钥管理系统中。
- Admin 密码存入密码管理器。丢失或泄露时，运行 `synaps3 admin-auth reset-password --config <path>` 并重启 SynapS3；所有浏览器会话也会随之退出登录。
- 上传前为钱包准备资金：保留用于 gas 的 FIL，在 **Wallet** 页面或用 CLI 存入 USDFC 并批准 FWSS：

  ```bash
  synaps3 wallet deposit 2 # 2 USDFC
  synaps3 wallet approve
  ```

## 配置检查

```bash
synaps3 admin settings get
```

| 字段 | 检查 |
| --- | --- |
| `admin.addr` | `127.0.0.1:9090`，除非有 HTTPS 和访问控制保护。 |
| `admin.trusted_proxies` | 为空，除非可信代理会清除客户端的转发标头。 |
| `admin.auth.enabled` | `true`。 |
| `filecoin.network`、`filecoin.rpc_url` | 目标网络；每个网络使用单独的数据库。 |
| `filecoin.allow_private_networks` | `false`，除非信任私有网络中的存储提供方。 |
| `cache.max_size_gb` | 足够容纳尚未存到 Filecoin 的上传积压。 |
| `logging.format` | 接入日志采集时用 `json`。内置默认值是 `text`；Docker 部署设为 `json`。 |

## 监控

至少监控：

- `/healthz` 的 `status` 字段，而不只是 HTTP 状态码；
- `synaps3_cache_used_bytes` 相对 `cache.max_size_gb` 的比例；
- `synaps3_task_queue_depth`，以及 `synaps3 admin task stats` 中的失败任务数；
- 仪表盘中存储提供方和数据集的健康状态。

见[告警建议](./health-metrics.md#告警建议)。

## 上线前

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status failed --limit 50
```

健康状态为 `ok`、每个失败任务的原因都已查明、且有经过验证的备份后，再上线。
