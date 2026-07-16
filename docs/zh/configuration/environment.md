---
title: 环境变量
description: 使用 SYNAPS3 环境变量覆盖配置，并理解适用场景。
---

# 环境变量

配置环境变量使用 `SYNAPS3_` 前缀，下划线会映射到配置路径。环境变量会覆盖文件值，适合放部署密钥和主机专用设置。

## 常用覆盖项

| 环境变量 | 配置路径 |
| --- | --- |
| `SYNAPS3_SERVER_PORT` | `server.port` |
| `SYNAPS3_SERVER_MAX_CONNECTIONS` | `server.max_connections` |
| `SYNAPS3_SERVER_MAX_REQUESTS` | `server.max_requests` |
| `SYNAPS3_SERVER_TLS_ENABLED` | `server.tls.enabled` |
| `SYNAPS3_SERVER_TLS_CERT_FILE` | `server.tls.cert_file` |
| `SYNAPS3_SERVER_TLS_KEY_FILE` | `server.tls.key_file` |
| `SYNAPS3_S3_REGION` | `s3.region` |
| `SYNAPS3_FILECOIN_NETWORK` | `filecoin.network` |
| `SYNAPS3_FILECOIN_RPC_URL` | `filecoin.rpc_url` |
| `SYNAPS3_FILECOIN_PRIVATE_KEY` | `filecoin.private_key` |
| `SYNAPS3_FILECOIN_WITH_CDN` | `filecoin.with_cdn` |
| `SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS` | `filecoin.allow_private_networks` |
| `SYNAPS3_FILECOIN_DEFAULT_COPIES` | `filecoin.default_copies` |
| `SYNAPS3_FILECOIN_OBSERVABILITY_INTERVAL` | `filecoin.observability.interval` |
| `SYNAPS3_FILECOIN_OBSERVABILITY_TIMEOUT` | `filecoin.observability.timeout` |
| `SYNAPS3_FILECOIN_OBSERVABILITY_CONCURRENCY` | `filecoin.observability.concurrency` |
| `SYNAPS3_DATABASE_DRIVER` | `database.driver` |
| `SYNAPS3_DATABASE_DSN` | `database.dsn` |
| `SYNAPS3_DATABASE_MAX_OPEN_CONNS` | `database.max_open_conns` |
| `SYNAPS3_DATABASE_MAX_IDLE_CONNS` | `database.max_idle_conns` |
| `SYNAPS3_CACHE_DIR` | `cache.dir` |
| `SYNAPS3_CACHE_MAX_SIZE_GB` | `cache.max_size_gb` |
| `SYNAPS3_CACHE_EVICTION_POLICY` | `cache.eviction_policy` |
| `SYNAPS3_WORKER_UPLOAD_CONCURRENCY` | `worker.upload.concurrency` |
| `SYNAPS3_WORKER_UPLOAD_POLL_INTERVAL` | `worker.upload.poll_interval` |
| `SYNAPS3_WORKER_UPLOAD_MAX_RETRIES` | `worker.upload.max_retries` |
| `SYNAPS3_WORKER_EVICTOR_CONCURRENCY` | `worker.evictor.concurrency` |
| `SYNAPS3_WORKER_EVICTOR_POLL_INTERVAL` | `worker.evictor.poll_interval` |
| `SYNAPS3_WORKER_EVICTOR_MAX_RETRIES` | `worker.evictor.max_retries` |
| `SYNAPS3_WORKER_STORAGE_CLEANUP_CONCURRENCY` | `worker.storage_cleanup.concurrency` |
| `SYNAPS3_WORKER_STORAGE_CLEANUP_POLL_INTERVAL` | `worker.storage_cleanup.poll_interval` |
| `SYNAPS3_WORKER_STORAGE_CLEANUP_MAX_RETRIES` | `worker.storage_cleanup.max_retries` |
| `SYNAPS3_LOGGING_LEVEL` | `logging.level` |
| `SYNAPS3_LOGGING_FORMAT` | `logging.format` |
| `SYNAPS3_LOGGING_S3_ACCESS_ENABLED` | `logging.s3_access.enabled` |
| `SYNAPS3_LOGGING_S3_ACCESS_LEVEL` | `logging.s3_access.level` |
| `SYNAPS3_ADMIN_ADDR` | `admin.addr` |
| `SYNAPS3_ADMIN_TRUSTED_PROXIES` | `admin.trusted_proxies` |
| `SYNAPS3_ADMIN_AUTH_ENABLED` | `admin.auth.enabled` |
| `SYNAPS3_ADMIN_AUTH_USERNAME` | `admin.auth.username` |
| `SYNAPS3_ADMIN_AUTH_SESSION_SECRET` | `admin.auth.session_secret` |
| `SYNAPS3_ADMIN_AUTH_SESSION_TTL` | `admin.auth.session_ttl` |

`SYNAPS3_ADMIN_TRUSTED_PROXIES` 是逗号分隔的 IP 或 CIDR 列表。

## 高级生成字段

| 环境变量 | 配置路径 |
| --- | --- |
| `SYNAPS3_ADMIN_AUTH_PASSWORD_HASH` | `admin.auth.password_hash` |

`admin.auth.password_hash` 通常由 `synaps3 init` 或 `synaps3 admin-auth reset-password` 生成。除非外部流程已经接管 Admin 密码 hash，否则不要手写。

## CLI 和容器路径变量

下面这些变量不映射到 TOML 配置字段。

| 环境变量 | 用途 |
| --- | --- |
| `SYNAPS3_DATA_DIR` | 自动执行 `synaps3 init` 时使用的运行数据目录；默认是 `/var/lib/synaps3`。 |
| `SYNAPS3_CONFIG` | CLI 命令在未传 `--config` 时使用的配置文件；Compose 示例默认使用 `/var/lib/synaps3/config.toml` 且允许覆盖，entrypoint 启动 `serve` 时默认使用 `$SYNAPS3_DATA_DIR/config.toml`。 |

## 何时使用环境变量

适合放在环境变量中的内容：

- 钱包 private key，
- 外部管理的 Admin session secret，
- 容器专用路径，
- 网络特定 RPC URL，
- 部署特定日志格式，
- 排障期间的临时覆盖。

稳定设置如果希望在 `synaps3 admin settings get` 中清楚展示，建议放在 TOML 配置文件中。`SYNAPS3_DATA_DIR` 只用于 Docker entrypoint 路径控制；`SYNAPS3_CONFIG` 适合让容器内命令复用同一个配置文件。

## 安全建议

> [!WARNING]
> 不要把钱包 private key 和 Admin session secret 放进 git、容器镜像或 shell history。

- 将 `SYNAPS3_FILECOIN_PRIVATE_KEY` 放在密钥管理系统、`.env` 或主机环境中。`synaps3 init` 和 `synaps3 admin-auth reset-password` 会生成 `admin.auth.session_secret`；只有部署策略要求在 TOML 外管理时，才使用 `SYNAPS3_ADMIN_AUTH_SESSION_SECRET`。
- 除非可信代理会在请求到达 SynapS3 前清理不可信 forwarded headers，否则保持 `SYNAPS3_ADMIN_TRUSTED_PROXIES` 为空。
- 不要提交 `.env`、`config.toml`、本地数据库、缓存数据或钱包材料。
- 除非明确信任私有 provider URL，否则保持 `filecoin.allow_private_networks = false`。
- 环境变量管理的字段会覆盖文件值；只改文件不会改变这些字段的生效值。

## 验证生效设置

```bash
synaps3 admin settings get
```

输出会显示当前值，以及每个设置是否可写。
