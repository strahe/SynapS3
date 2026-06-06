---
title: 配置模型
description: 理解 SynapS3 配置来源、默认值、可编辑设置和高风险字段。
---

# 配置模型

SynapS3 先读取 TOML 配置，再应用 `SYNAPS3_` 环境变量覆盖。稳定设置建议写在配置文件中；密钥和部署专用设置更适合放在环境变量里。

## 来源规则

- 不传 `--config` 时，SynapS3 读取 `~/.synaps3/config.toml`。
- 使用 `--config <path>` 指定其他文件。
- 当前目录中的 `config.toml` 不会自动读取，除非显式传入。
- `synaps3 init --dir <path>` 会创建文件，但不会改变默认配置来源。
- Admin settings 写入会重写 `config.toml`；注释和顺序不会保留。

查看当前生效设置：

```bash
synaps3 admin settings get
```

输出会显示配置路径、是否允许写入，以及是否需要重启。

## 必需密钥

正常启动服务前，需要设置 Filecoin 钱包 private key：

```toml
[filecoin]
private_key = "0x..."
```

也可以用 `SYNAPS3_FILECOIN_PRIVATE_KEY` 管理这个值；支持的覆盖项见[环境变量](./environment.md)。

不要把 private key 放进代码仓库、容器镜像或 shell history。

当 `admin.auth.enabled = true` 时，Admin 认证还需要密码 hash 和 `admin.auth.session_secret`。新配置会由 `synaps3 init` 创建；如果缺失或需要轮换密码，运行 `synaps3 admin-auth reset-password --config <path>` 重新生成。重置密码也会轮换 session secret。

## 主要配置段

| 配置段 | 用途 |
| --- | --- |
| `server` | S3 API 监听、并发限制和 TLS 字段。 |
| `s3` | 返回给 S3 客户端的 region。 |
| `filecoin` | 网络、RPC、钱包、上传来源、存储提供方 URL 策略、CDN hints 和副本策略。 |
| `filecoin.observability` | 存储提供方和本地 data set 健康检查。 |
| `database` | SQLite 或 Postgres 元数据数据库。 |
| `cache` | 本地对象缓存目录、容量和淘汰策略。 |
| `worker.upload` | 后台 Filecoin 上传并发、轮询和重试。 |
| `worker.evictor` | 本地缓存淘汰工作进程。 |
| `worker.storage_cleanup` | 远端副本清理工作进程。 |
| `logging` | 运行时日志等级、格式和 S3 access log。 |
| `admin` | 仪表盘、Admin API 监听地址和 Admin 认证设置。 |

## 重要默认值

| 字段 | 默认值 |
| --- | --- |
| `server.port` | `:8080` |
| `server.max_connections` | `4096` |
| `server.max_requests` | `512` |
| `s3.region` | `us-east-1` |
| `filecoin.network` | `calibration` |
| `filecoin.source` | `synaps3` |
| `filecoin.default_copies` | `3` |
| `database.driver` | `sqlite` |
| `database.max_open_conns` | `4` |
| `database.max_idle_conns` | `2` |
| `cache.max_size_gb` | `100` |
| `cache.eviction_policy` | `lru` |
| `worker.upload.concurrency` | `4` |
| `worker.upload.max_retries` | `5` |
| `admin.addr` | `127.0.0.1:9090` |
| `admin.trusted_proxies` | `[]` |
| `admin.auth.enabled` | `true` |
| `admin.auth.username` | `admin` |
| `admin.auth.session_ttl` | `12h` |

## 允许值

- `filecoin.network`: `calibration`, `mainnet`。
- `filecoin.default_copies`: `1` 到 `8`。
- `database.driver`: `sqlite`, `postgres`。
- `cache.eviction_policy`: `lru`, `manual`, `none`。
- `logging.level`: `debug`, `info`, `warn`, `error`。
- `logging.format`: `json`, `text`。
- `admin.trusted_proxies`: IP 或 CIDR。除非可信反向代理会清理不可信 forwarded headers，否则保持空。

## 高风险字段

| 字段 | 风险 |
| --- | --- |
| `admin.addr` | 暴露 Admin API 会允许运维写操作。除非有 HTTPS 和访问控制保护，否则保持本机回环地址。 |
| `admin.trusted_proxies` | 对匹配代理信任 `X-Forwarded-For`、`X-Real-IP`、`X-Forwarded-Proto` 和 `X-Forwarded-Host`。只配置你控制的代理。 |
| Admin password hash | 控制 Admin 登录。不要手动配置；用 `synaps3 init` 或 `synaps3 admin-auth reset-password` 生成。 |
| `admin.auth.session_secret` | 用于签名 Admin 浏览器会话。按密钥处理。 |
| `filecoin.private_key` | 控制钱包支付和存储操作。必须作为密钥处理。 |
| `filecoin.network` | 切换到 `mainnet` 会改变支付和存储环境。 |
| `filecoin.allow_private_networks` | 允许私有网络 provider URL。只在可信私有部署中开启。 |
| `cache.max_size_gb` | 太小会阻塞写入；太大会占满主机磁盘。 |

高风险设置可能需要显式确认：

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
