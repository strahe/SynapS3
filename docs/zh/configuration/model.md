---
title: 配置模型
description: 理解 SynapS3 配置来源、默认值、可编辑设置和高风险字段。
---

# 配置模型

SynapS3 读取 TOML 配置，并应用 `SYNAPS3_` 环境变量覆盖。稳定设置建议放在配置文件中，密钥或部署特定设置建议放在环境变量中。

## 来源规则

- 不传 `--config` 时，SynapS3 读取 `~/.synaps3/config.toml`。
- 使用 `--config <path>` 指定其他文件。
- 当前目录中的 `config.toml` 不会自动读取，除非显式传入。
- `synaps3 init --dir <path>` 会创建文件，但不会改变默认配置来源。
- Admin settings 写入会重写 `config.toml`；注释和顺序不会保留。

运维检查：

```bash
synaps3 admin settings get
```

输出会显示配置路径、是否允许写入，以及是否需要重启。

## 必需密钥

正常 serve 前需要设置 Filecoin wallet private key：

```toml
[filecoin]
private_key = "0x..."
```

也可以通过 `SYNAPS3_FILECOIN_PRIVATE_KEY` 管理这个值；支持的覆盖项见[环境变量](./environment.md)。

不要把 private key 放进代码仓库、容器镜像或 shell history。

## 主要配置段

| 配置段 | 用途 |
| --- | --- |
| `server` | S3 API 监听、并发限制和 TLS 字段。 |
| `s3` | 返回给 S3 客户端的 region。 |
| `filecoin` | 网络、RPC、钱包、上传来源、provider URL 策略、CDN hints 和副本策略。 |
| `filecoin.observability` | Provider 和本地 data set 健康检查。 |
| `database` | SQLite 或 Postgres 元数据数据库。 |
| `cache` | 本地对象缓存目录、容量和淘汰策略。 |
| `worker.upload` | 后台 Filecoin 上传并发、轮询和重试。 |
| `worker.evictor` | 本地缓存淘汰 worker 行为。 |
| `worker.storage_cleanup` | 远端 replica cleanup worker 行为。 |
| `logging` | 运行时日志等级、格式和 S3 access log。 |
| `admin` | Dashboard 和 Admin API 监听地址。 |

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

## 允许值

- `filecoin.network`: `calibration`, `mainnet`。
- `filecoin.default_copies`: `1` 到 `8`。
- `database.driver`: `sqlite`, `postgres`。
- `cache.eviction_policy`: `lru`, `manual`, `none`。
- `logging.level`: `debug`, `info`, `warn`, `error`。
- `logging.format`: `json`, `text`。

## 高风险字段

| 字段 | 风险 |
| --- | --- |
| `admin.addr` | 暴露 Admin API 会允许运维写操作。除非有保护，否则保持 loopback。 |
| `filecoin.private_key` | 控制钱包支付和存储操作。必须作为密钥处理。 |
| `filecoin.network` | 切换到 `mainnet` 会改变支付和存储环境。 |
| `filecoin.allow_private_networks` | 允许私有网络 provider URL。只在可信私有部署中开启。 |
| `cache.max_size_gb` | 太小会阻塞写入；太大会占满主机磁盘。 |

高风险设置可能需要显式确认：

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
