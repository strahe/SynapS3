---
title: 环境变量
description: 使用 SYNAPS3 环境变量覆盖配置，并理解适用场景。
---

# 环境变量

环境变量使用 `SYNAPS3_` 前缀，并将下划线映射到配置路径。它们会覆盖文件值，适合保存部署密钥和主机特定设置。

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
| `SYNAPS3_FILECOIN_SOURCE` | `filecoin.source` |
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

## 何时使用环境变量

适合使用环境变量的内容：

- wallet private key，
- 容器专用路径，
- 网络特定 RPC URL，
- 部署特定日志格式，
- 排障期间的临时覆盖。

稳定且希望被 `synaps3 admin settings get` 清楚展示的设置，建议放在 TOML 配置文件中。

## 安全建议

- 将 `SYNAPS3_FILECOIN_PRIVATE_KEY` 放在 secret manager、`.env` 或主机环境中。
- 不要提交 `.env`、`config.toml`、本地数据库、缓存数据或钱包材料。
- 除非明确信任私有 provider URL，否则保持 `filecoin.allow_private_networks = false`。
- 环境变量管理的字段会覆盖文件值；只改文件不会改变这些字段的生效值。

## 验证生效设置

```bash
synaps3 admin settings get
```

预期结果：输出显示当前值和 settings 是否可写。
