---
title: 配置模型
description: 了解 SynapS3 从哪里读取设置、修改如何生效，以及各组设置控制什么。
---

# 配置模型

SynapS3 先读取 TOML 配置文件，再用 `SYNAPS3_` [环境变量](./environment.md)覆盖。稳定的设置放在配置文件中，密钥和与主机相关的值放在环境变量中。

## 来源与修改

- 命令按顺序读取 `--config` 指定的文件、`SYNAPS3_CONFIG`、`~/.synaps3/config.toml`。当前目录下的 `config.toml` 不会被读取，除非显式传入。
- 运行 `synaps3 init --dir <path>` 后，后续命令要传 `--config <path>/config.toml`。
- **Settings** 页面和 `synaps3 admin settings set` 会重写 `config.toml`，注释和顺序不会保留。
- 由环境变量设置的字段，不能在 **Settings** 页面或 CLI 中修改，要改环境变量。
- 保存的设置在重启后生效。`synaps3 admin settings get` 显示已保存的值和 **Restart required**。

```bash
synaps3 admin settings get
```

## 必需设置

| 字段 | 说明 |
| --- | --- |
| `database.dsn` | PostgreSQL 连接 URL，例如 `postgres://synaps3:PASSWORD@db.example.com:5432/synaps3?sslmode=require`。没有默认值。该角色必须能建表；SynapS3 在启动时执行数据库迁移。 |
| `filecoin.private_key` | 钱包私钥。未设置时 SynapS3 以设置模式启动。在配置文件或 `SYNAPS3_FILECOIN_PRIVATE_KEY` 中设置；仪表盘不能设置。 |
| `admin.auth.password_hash`、`admin.auth.session_secret` | `admin.auth.enabled = true` 时必需。`synaps3 init` 会生成这两项。`synaps3 admin-auth reset-password --config <path>` 设置新密码并轮换会话密钥，所有浏览器会话随之退出登录。 |

数据库与第一次使用它的钱包地址和 Filecoin 网络绑定。换用其他钱包或网络启动会失败，报错 `database belongs to another Filecoin wallet or network`。`filecoin.rpc_url` 可以换成同一网络的其他端点。

配置文件、`.env` 和其他凭据文件的权限保持 `0600`；私钥不要进入提交、镜像或 shell 历史。

## 配置段

| 配置段 | 控制内容 |
| --- | --- |
| `server` | S3 API 监听地址、连接和请求上限、TLS。 |
| `s3` | 向 S3 客户端报告的区域。 |
| `filecoin` | 网络、RPC 端点、钱包、私有网络中的存储提供方 URL、CDN 检索、默认目标副本数和存储提供方选择。 |
| `filecoin.observability` | 存储提供方和数据集健康检查。 |
| `filecoin.upload_speed_test` | 自动上传测速。 |
| `database` | PostgreSQL 连接和连接池大小。 |
| `cache` | 本地缓存目录、容量和淘汰策略。 |
| `worker.tasks` | 后台任务并发和批次提交。 |
| `logging` | 日志级别、格式和 S3 访问日志。 |
| `admin` | 仪表盘和 Admin API 监听地址、可信代理和认证。 |

## 默认值

| 字段 | 默认值 |
| --- | --- |
| `server.port` | `:8080` |
| `server.max_connections` | `4096` |
| `server.max_requests` | `512` |
| `s3.region` | `us-east-1` |
| `filecoin.network` | `calibration` |
| `filecoin.rpc_url` | `https://api.calibration.node.glif.io/rpc/v1` |
| `filecoin.default_copies` | `3` |
| `filecoin.anchor_provider_tier` | `approved` |
| `filecoin.upload_speed_test.auto_scope` | `trusted` |
| `filecoin.upload_speed_test.retest_interval` | `0s` |
| `filecoin.observability.interval` | `5m` |
| `filecoin.observability.timeout` | `5s` |
| `filecoin.observability.concurrency` | `8` |
| `database.max_open_conns` | `32` |
| `database.max_idle_conns` | `2` |
| `cache.dir` | `~/.synaps3/cache` |
| `cache.max_size_gb` | `100` |
| `cache.eviction_policy` | `lru` |
| `cache.lru_high_watermark_percent` | `80` |
| `cache.lru_low_watermark_percent` | `50` |
| `worker.tasks.concurrency` | `12` |
| `worker.tasks.poll_interval` | `5s` |
| `worker.tasks.lease_duration` | `5m` |
| `worker.tasks.upload_concurrency` | `4` |
| `worker.tasks.commit_max_pieces` | `32` |
| `worker.tasks.commit_max_wait` | `30m` |
| `worker.tasks.commit_max_backlog` | `256` |
| `worker.tasks.commit_seal_on_cache_pressure` | `false` |
| `logging.level` | `info` |
| `logging.format` | `text`（Docker 部署设为 `json`） |
| `admin.addr` | `127.0.0.1:9090` |
| `admin.trusted_proxies` | `[]` |
| `admin.auth.enabled` | `true` |
| `admin.auth.username` | `admin` |
| `admin.auth.session_ttl` | `12h` |

## 允许值

- `filecoin.network`：`calibration`、`mainnet`。`filecoin.rpc_url` 必须属于同一网络；Mainnet 默认端点是 `https://api.node.glif.io/rpc/v1`。在 **Settings** 页面切换网络时，默认 RPC URL 会随之切换。
- `filecoin.default_copies`：`1` 到 `8`。
- `filecoin.anchor_provider_tier`：`approved`、`endorsed`、`none`。
- `filecoin.upload_speed_test.auto_scope`：`trusted`、`all`。`retest_interval` 不小于 `0s`。
- `cache.eviction_policy`：`lru`、`after_upload`、`none`。
- `cache.lru_low_watermark_percent` 和 `cache.lru_high_watermark_percent`：`0 <= low < high <= 100`。
- `worker.tasks.commit_max_pieces`：`1` 到 `200`。`commit_max_wait`：`0s` 到 `30m`。`commit_max_backlog`：不小于 `commit_max_pieces`。
- `server.max_requests` 不大于 `server.max_connections`；`database.max_idle_conns` 不大于 `database.max_open_conns`。
- `logging.level`：`debug`、`info`、`warn`、`error`。`logging.format`：`json`、`text`。
- `admin.trusted_proxies`：IP 地址或 CIDR 网段。

## S3 监听

为 S3 API 启用原生 TLS：

```toml
[server.tls]
enabled = true
cert_file = "/path/to/tls.crt"
key_file = "/path/to/tls.key"
```

SynapS3 进程必须能读取这两个文件。不启用原生 TLS 时，生产 S3 流量要经过你自己的 TLS 代理。

同时处理的 S3 请求超过 `server.max_requests` 时，多出的请求收到 `503 SlowDown`。

## 缓存

`cache.max_size_gb` 是本地缓存用量上限，单位 GiB。写入在读取请求体之前先预留完整大小；放不下时，写入失败并返回 `503 SlowDown`。见[缓存已满](../operations/troubleshooting.md#缓存已满)。

| `cache.eviction_policy` | 行为 |
| --- | --- |
| `lru` | 用量达到高水位，或有写入被拒绝时，SynapS3 按最近读取时间删除可安全删除的版本，直到用量降到低水位。清理目标始终至少留出一个最大对象（1,065,353,216 字节）的空间。 |
| `after_upload` | 版本达到所在存储桶的最小持久副本数并完成提交后，SynapS3 删除其缓存。 |
| `none` | SynapS3 从不自动删除缓存数据。 |

水位只对 `lru` 生效。删除在后台进行，写入不会等待。哪些版本可以安全删除，见[写入路径与缓存](../concepts/write-path-cache.md#缓存淘汰)。

## 后台任务

`worker.tasks.concurrency`（**Task concurrency**）限制所有后台任务的并发数。`worker.tasks.upload_concurrency`（**Upload concurrency**）另外限制上传到存储提供方的并发数，包括读取缓存数据和计算哈希。达到上限时，任务在队列中等待。失败的任务按固定次数重试；任务历史永久保留。

## 批次

SynapS3 把上传的 piece 按批次加入链上数据集，每个批次一笔交易。**Batches** 页面列出这些批次。

| 字段 | Settings 标签 | 作用 |
| --- | --- | --- |
| `worker.tasks.commit_max_pieces` | **Batch size** | 每笔交易最多包含的 piece 数。 |
| `worker.tasks.commit_max_wait` | **Batch wait** | 最早的 piece 等待批次凑满的最长时间。`0s` 表示不等待，直接提交。 |
| `worker.tasks.commit_max_backlog` | **Batch backlog** | 某个数据集等待提交的已传输 piece 达到该数量时，发往该数据集的新上传和新副本会等待。 |
| `worker.tasks.commit_seal_on_cache_pressure` | **Submit batches early to free cache space** | 自动缓存清理释放不出足够空间时，提前提交批次。`cache.eviction_policy = none` 时无效。 |

满足以下任一条件时提交批次：达到批次大小；最早的 piece 等到批次等待时间；数据集不再接收数据；在 **Batches** 页面点击 **Submit next batch**。每个数据集最多有四个已提交、待确认的批次，之后的批次排队等待。

等待提交的 piece 保留在本地缓存中。批次等待时间越长，缓存释放和从首个副本复制的其他副本就越晚。批次越小，交易越多，费用越高。

打开批次可以查看其中的 piece、交易和错误。已停止的批次会显示 **Retry**。

## 存储提供方选择

SynapS3 在准备存储桶、补充副本或自动替换存储提供方时选择存储提供方。候选者必须有近期的健康检查结果，并且在当前服务 URL 下上传测速成功。已分配的存储提供方不会因层级成员资格或测速结果变化而被撤换，发往它们的上传也不等待测速。

`filecoin.anchor_provider_tier`（**Provider requirement**）要求选出的存储提供方中至少有一个来自 FWSS approved 列表（`approved`）或 endorsed 列表（`endorsed`）；`none` 不作要求。所需列表为空时，存储桶准备会停止；按存储桶或任务上显示的原因处理后，点击 **Retry**。

上传测速：

- approved、endorsed 和已分配的存储提供方在首次符合条件时以及服务 URL 变化后自动测速。
- `auto_scope = "trusted"`（**Trusted providers**）只测这些存储提供方。其他存储提供方要在 **Storage** 页面通过 **Test remaining providers** 或其详情中的测速入口完成测速后，才能成为候选者。`all`（**All providers**）测试所有符合条件的存储提供方。
- `retest_interval` 大于 `0s` 时定期重测。为 `0s` 时，失败或超时的测速保持失败，直到点击 **Retry**。

存储桶会立即创建。其存储准备会等到符合条件的 approved 和 endorsed 存储提供方完成测速，再检查目标副本数和存储提供方要求。等待不消耗重试次数。

**Provider preference** 在创建存储桶时选择，之后不能修改：

| 选项 | API 值 | 排序 |
| --- | --- | --- |
| **Distribution first**（默认） | `distribution` | 先选活跃数据集较少的存储提供方，再看上传速度。 |
| **Speed first** | `speed` | 先看上传速度，再看活跃数据集数量。 |

排序相同时随机选择。通过 S3 API 创建的存储桶使用 `distribution`。该偏好也用于补充副本和自动替换。存储提供方为某个存储桶保存的所有数据集都退役后，才能再次服务该存储桶；自动替换不会选择该存储桶用过的存储提供方。

## Admin 会话

`admin.auth.session_ttl` 设置仪表盘会话的有效期。你在仪表盘上操作（指针、点击、键盘或滚轮输入）时，会话会续期；后台轮询不会续期。未续期的会话在 `session_ttl` 后过期。登录没有最长有效期上限。

默认情况下，登录 Cookie 只在浏览器会话内有效。勾选 **Keep me signed in** 会设置持久 Cookie，有效期取 30 天和 `session_ttl` 中较长的一个。

## 高风险字段

| 字段 | 风险 |
| --- | --- |
| `admin.addr` | Admin API 可以修改数据、凭据和钱包资金。除非有 HTTPS 和访问控制保护，否则保持在回环地址。 |
| `admin.trusted_proxies` | SynapS3 信任来自这些地址的 `X-Forwarded-For`、`X-Real-IP`、`X-Forwarded-Proto` 和 `X-Forwarded-Host`。只填写由你控制、并会清除客户端同名标头的代理。 |
| `admin.auth.password_hash` | 控制 Admin 登录。用 `synaps3 init` 或 `synaps3 admin-auth reset-password` 生成。 |
| `admin.auth.session_secret` | 签名 Admin 会话，按密钥保管。 |
| `filecoin.private_key` | 可以动用钱包资金，按密钥保管。 |
| `database.dsn` | 通常包含数据库密码，按密钥保管。 |
| `filecoin.network` | 必须与数据库和 `filecoin.rpc_url` 一致。每个网络使用单独的数据库。 |
| `filecoin.allow_private_networks` | 允许使用私有网络中的存储提供方 URL。只在信任这些存储提供方时启用。 |
| `cache.max_size_gb` | 太小会拒绝写入，太大可能占满磁盘。 |
| `cache.lru_high_watermark_percent` | 值越高，清理期间留给写入的空间越少。 |
| `cache.lru_low_watermark_percent` | 值越低，每次清理删除的缓存越多。 |
