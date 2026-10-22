---
title: 环境变量
description: 查询用于覆盖配置字段、控制 CLI 和容器的 SYNAPS3_ 环境变量。
---

# 环境变量

环境变量会覆盖配置文件中的值。以这种方式设置的字段，不能在 **Settings** 页面或通过 `synaps3 admin settings set` 修改。私钥、数据库 DSN 等密钥，以及与主机相关的值，适合用环境变量设置。

## 配置覆盖

| 环境变量 | 配置字段 |
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
| `SYNAPS3_FILECOIN_ANCHOR_PROVIDER_TIER` | `filecoin.anchor_provider_tier` |
| `SYNAPS3_FILECOIN_UPLOAD_SPEED_TEST_AUTO_SCOPE` | `filecoin.upload_speed_test.auto_scope` |
| `SYNAPS3_FILECOIN_UPLOAD_SPEED_TEST_RETEST_INTERVAL` | `filecoin.upload_speed_test.retest_interval` |
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
| `SYNAPS3_CACHE_LRU_HIGH_WATERMARK_PERCENT` | `cache.lru_high_watermark_percent` |
| `SYNAPS3_CACHE_LRU_LOW_WATERMARK_PERCENT` | `cache.lru_low_watermark_percent` |
| `SYNAPS3_WORKER_TASKS_CONCURRENCY` | `worker.tasks.concurrency` |
| `SYNAPS3_WORKER_TASKS_POLL_INTERVAL` | `worker.tasks.poll_interval` |
| `SYNAPS3_WORKER_TASKS_LEASE_DURATION` | `worker.tasks.lease_duration` |
| `SYNAPS3_WORKER_TASKS_UPLOAD_CONCURRENCY` | `worker.tasks.upload_concurrency` |
| `SYNAPS3_WORKER_TASKS_COMMIT_MAX_PIECES` | `worker.tasks.commit_max_pieces` |
| `SYNAPS3_WORKER_TASKS_COMMIT_MAX_WAIT` | `worker.tasks.commit_max_wait` |
| `SYNAPS3_WORKER_TASKS_REMOVAL_MAX_PIECES` | `worker.tasks.removal_max_pieces` |
| `SYNAPS3_WORKER_TASKS_REMOVAL_MAX_WAIT` | `worker.tasks.removal_max_wait` |
| `SYNAPS3_WORKER_TASKS_COMMIT_SEAL_ON_CACHE_PRESSURE` | `worker.tasks.commit_seal_on_cache_pressure` |
| `SYNAPS3_WORKER_TASKS_COMMIT_MAX_BACKLOG` | `worker.tasks.commit_max_backlog` |
| `SYNAPS3_LOGGING_LEVEL` | `logging.level` |
| `SYNAPS3_LOGGING_FORMAT` | `logging.format` |
| `SYNAPS3_LOGGING_S3_ACCESS_ENABLED` | `logging.s3_access.enabled` |
| `SYNAPS3_LOGGING_S3_ACCESS_LEVEL` | `logging.s3_access.level` |
| `SYNAPS3_ADMIN_ADDR` | `admin.addr` |
| `SYNAPS3_ADMIN_TRUSTED_PROXIES` | `admin.trusted_proxies`（逗号分隔的 IP 或 CIDR） |
| `SYNAPS3_ADMIN_AUTH_ENABLED` | `admin.auth.enabled` |
| `SYNAPS3_ADMIN_AUTH_USERNAME` | `admin.auth.username` |
| `SYNAPS3_ADMIN_AUTH_PASSWORD_HASH` | `admin.auth.password_hash` |
| `SYNAPS3_ADMIN_AUTH_SESSION_SECRET` | `admin.auth.session_secret` |
| `SYNAPS3_ADMIN_AUTH_SESSION_TTL` | `admin.auth.session_ttl` |

`synaps3 init` 和 `synaps3 admin-auth reset-password` 会把 `admin.auth.password_hash` 和 `admin.auth.session_secret` 写入配置文件。只有在外部密钥存储管理 Admin 凭据时，才通过环境变量设置这两项。

## CLI 和容器变量

这些变量不对应配置字段。

| 环境变量 | 用途 |
| --- | --- |
| `SYNAPS3_CONFIG` | 命令未传 `--config` 时使用的配置文件。Docker 部署将其设为 `/var/lib/synaps3/config.toml`。 |
| `SYNAPS3_DATA_DIR` | 仅用于容器。配置文件不存在时，容器用 `synaps3 init` 初始化该数据目录。默认值：`/var/lib/synaps3`。 |
| `SYNAPS3_ADMIN_PASSWORD` | `synaps3 admin` 命令使用的 Admin 密码。优先使用密码提示符，避免密码进入 shell 历史。 |

## 密钥处理

- `SYNAPS3_FILECOIN_PRIVATE_KEY`、`SYNAPS3_DATABASE_DSN`、`SYNAPS3_ADMIN_PASSWORD` 和 `SYNAPS3_ADMIN_AUTH_SESSION_SECRET` 都按密钥处理。放在 `.env`、主机环境或密钥管理系统中，不要进入 git 或容器镜像。
- `.env` 权限保持 `0600`。
- 除非你的代理会清除客户端发送的转发标头，否则 `SYNAPS3_ADMIN_TRUSTED_PROXIES` 保持为空。
- 除非信任私有网络中的存储提供方，否则不设置 `SYNAPS3_FILECOIN_ALLOW_PRIVATE_NETWORKS`，或设为 `false`。
