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

保存设置后，重启 SynapS3，检查 `/healthz`，再运行 `synaps3 admin settings get`，确认实际生效值。

## 必需密钥

正常启动服务前，需要设置 Filecoin 钱包私钥：

```toml
[filecoin]
private_key = "0x..."
```

也可以用 `SYNAPS3_FILECOIN_PRIVATE_KEY` 管理这个值；支持的覆盖项见[环境变量](./environment.md)。

不要把私钥放进代码仓库、容器镜像或 shell history。

当 `admin.auth.enabled = true` 时，Admin 认证还需要密码 hash 和 `admin.auth.session_secret`。新配置会由 `synaps3 init` 创建；如果缺失或需要轮换密码，运行 `synaps3 admin-auth reset-password --config <path>` 重新生成。重置密码也会轮换 session secret。

配置、`.env` 和凭据文件都应保持 `0600` 权限。

## S3 服务

S3 API 通过以下字段支持原生 TLS：

```toml
[server.tls]
enabled = true
cert_file = "/path/to/tls.crt"
key_file = "/path/to/tls.key"
```

证书和私钥必须允许 SynapS3 进程读取。在容器部署中，配置的路径必须存在于容器内，通常通过只读挂载提供。生产 S3 流量必须使用原生 TLS 或受控的 TLS 反向代理。

Admin 端点有独立的暴露范围控制。让 `admin.addr` 保持回环地址、使用 SSH 隧道，或放在带访问控制的 HTTPS 反向代理之后。

## 数据库选择

SynapS3 把元数据存储在 PostgreSQL 中。把 `database.dsn` 设为连接 URL，例如 `postgres://synaps3:PASSWORD@db.example.com:5432/synaps3?sslmode=require`；它没有默认值。SynapS3 启动时会执行迁移，因此该角色必须能在数据库中建表。DSN 必须保存在受保护的配置或密钥存储中。

`database.max_open_conns` 决定连接池大小。

## 主要配置段

| 配置段 | 用途 |
| --- | --- |
| `server` | S3 API 监听、并发限制和 TLS 字段。 |
| `s3` | 返回给 S3 客户端的 region。 |
| `filecoin` | 网络、RPC、钱包、存储提供方 URL 策略、CDN hints 和副本策略。 |
| `filecoin.observability` | 存储提供方和本地数据集健康检查。 |
| `database` | PostgreSQL 元数据数据库。 |
| `cache` | 本地对象缓存目录、容量和淘汰策略。 |
| `worker.tasks` | 统一后台任务执行和存储变更并发限制。 |
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
| `filecoin.default_copies` | `3` |
| `filecoin.anchor_provider_tier` | `approved` |
| `database.driver` | `postgres` |
| `database.max_open_conns` | `32` |
| `database.max_idle_conns` | `2` |
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
| `worker.tasks.commit_seal_on_cache_pressure` | `false` |
| `worker.tasks.commit_max_backlog` | `256` |
| `admin.addr` | `127.0.0.1:9090` |
| `admin.trusted_proxies` | `[]` |
| `admin.auth.enabled` | `true` |
| `admin.auth.username` | `admin` |
| `admin.auth.session_ttl` | `12h` |

`worker.tasks.concurrency` 限制全部正在运行的后台任务，包括恢复任务。`upload_concurrency` 额外限制同时上传的数量，读取和哈希本地缓存也计入其中。两项都必须大于零；上传上限可以高于总上限，实际仍受总上限约束。没有空闲名额的任务保持排队，保留原有到期顺序，其他类型任务可以继续执行。钱包操作和上传测速各自一次执行一个。任务设置修改后必须重启 SynapS3。每类任务声明有限重试策略，每轮执行保留创建时记录的策略。任务历史永久保存。

写入同一存储服务的上传和复制副本会一起收集，直到可以提交。`commit_max_pieces`（1–200）只限制一笔交易的 piece 数，不限制收集数量；Mainnet 上 data set ID 小于 1,559、Calibration 上小于 32,331 的存储服务每次最多 80 个。收集数量达到该服务适用的单笔上限、存储服务不再接受新数据、手动提交或最早的 piece 等待达到 `commit_max_wait`（0–30m）后，下一批达到提交条件；`0s` 表示无需等待收集窗口。所有批次都要等该服务的 4 个在途名额空出，并让更早的可提交已签名批次先发送。轮到下一批时，取最早准备好的 piece，最多取单笔上限，签名后立即尝试发送；剩余数据继续收集，保留原来的等待起点。等待提交的上传数据保留在本地缓存。更长的窗口会推迟提交、需要已登记来源的副本复制及本地缓存淘汰。

某个存储服务上等待未提交批次的已传输 piece（包括上传和复制的副本）达到 `commit_max_backlog`（不小于 `commit_max_pieces`）后，写入该存储服务的新上传和新复制都会等待。一个批次超出单条 add-pieces 消息大小时，会拆分后用更少的 piece 重新签名。

在 Settings 启用 **Submit batches early to free cache space**（`worker.tasks.commit_seal_on_cache_pressure`），可在安全自动清理无法腾出足够空间时提前提交未满批次。提前提交仍需等待在途名额和更早的可提交批次。默认关闭，修改后需要重启。`lru` 按实际清理目标判断；`after_upload` 按被拒绝写入所需的容量判断；`none` 不自动提前提交。更小的批次可能增加交易成本。缓存删除仍需等待确认并满足存储桶的耐久要求。

打开 **Batches** 查看已收集的数据，点击 **Submit next batch** 请求提交下一批，最多提交单笔上限的 piece，其余数据继续等待。手动操作独立于自动开关和缓存策略，也需要等待提交名额。点击 **Details** 查看成员、提交时间、交易 ID 和错误；停止的任务在允许恢复时显示 **Retry**。每个成员展示一个关联对象，以及共享该数据的其他版本数量。历史对象信息或大小缺失时会标明不可用。

## 节点选择

`filecoin.anchor_provider_tier` 要求新选择后的节点集合至少保留一个 Approved 或 Endorsed 节点。Endorsed 不要求同时属于 Approved；`none` 取消名单要求。所有候选仍须有当前服务地址的有效健康观测。修改后需重启，已接受的目标不会重新选择。

创建存储桶时，**Provider preference** 可选分布优先（`distribution`，默认）或速度优先（`speed`）。分布优先先比较未退役数据集数量，再比较上传测速；速度优先先选择当前服务地址下测速成功的节点，未测速节点按负载补足。条件相同则随机选择。保存的偏好用于补副本和自动替换，手动替换跳过排序。S3 创建和现有存储桶默认分布优先，创建 API 接受 `provider_selection_strategy`，查询返回保存值；创建后不能修改偏好。节点在该桶的旧数据集全部退役后可再次使用。

## Admin 会话时长

`admin.auth.session_ttl` 控制每个普通 Admin UI 会话 token 的有效时长，既不是服务端强制的空闲超时，也不是一次登录的绝对上限。经过 5 分钟或 token 有效时长的一半（取较短者）后，服务端允许续期。官方仪表盘只在可信的指针、点击、键盘或滚轮操作后请求续期；后台轮询和仅切回可见标签页不会触发续期。任何持有有效 session cookie 和对应 CSRF token 的客户端，都可以在 `refresh_after` 之后调用续期接口。没有客户端请求续期时，token 会在 `expires_at` 到期。

登录页默认使用 browser-session cookie。选择 **Keep me signed in** 后，会改用持久 cookie，并使用 30 天或 `admin.auth.session_ttl` 中较长的时长。只要仪表盘持续收到用户操作，就可以继续请求续期；服务端不设置登录的绝对时长上限。

## 允许值

- `filecoin.network`: `calibration`, `mainnet`。
- `filecoin.default_copies`: `1` 到 `8`。
- `filecoin.anchor_provider_tier`: `approved`, `endorsed`, `none`.
- `database.driver`: `postgres`。
- `cache.eviction_policy`: `lru`, `after_upload`, `none`。
- `logging.level`: `debug`, `info`, `warn`, `error`。
- `logging.format`: `json`, `text`。
- `admin.trusted_proxies`: IP 或 CIDR。除非可信反向代理会清理不可信 forwarded headers，否则保持空。

缓存淘汰策略会产生以下用户可见结果：

- `lru`：缓存使用量达到高水位后，或有写入因缓存已满被拒绝时，SynapS3 按最近访问时间淘汰最久未使用且远端安全的条目，直到降至低水位。实际低水位不会让可用空间小于一个最大对象（`1,065,353,216` 字节），这只在缓存较小时起作用。
- `after_upload`：版本达到其存储桶要求的最低耐久副本数后，会加入异步清理。之后从远端读取并回填的缓存不会再次被立即删除。
- `none`：SynapS3 不会自动清理本地缓存。

LRU 水位始终必须满足 `0 <= low < high <= 100`。在 `after_upload` 或 `none` 下仍会保存这些值，但不会生效。

```toml
[cache]
eviction_policy = "lru"
lru_high_watermark_percent = 80
lru_low_watermark_percent = 50
```

淘汰设置会在重启后生效。缓存清理是异步流程：写入不会等待 LRU 检查，被拒绝的写入只会请求下一次检查。

## 高风险字段

| 字段 | 风险 |
| --- | --- |
| `admin.addr` | 暴露 Admin API 会允许运维写操作。除非有 HTTPS 和访问控制保护，否则保持本机回环地址。 |
| `admin.trusted_proxies` | 对匹配代理信任 `X-Forwarded-For`、`X-Real-IP`、`X-Forwarded-Proto` 和 `X-Forwarded-Host`。只配置你控制的代理。 |
| Admin password hash | 控制 Admin 登录。不要手动配置；用 `synaps3 init` 或 `synaps3 admin-auth reset-password` 生成。 |
| `admin.auth.session_secret` | 用于签名 Admin 浏览器会话。按密钥处理。 |
| `filecoin.private_key` | 控制钱包支付和存储操作。必须作为私钥处理。 |
| `database.dsn` | 可能包含数据库凭据，必须作为敏感值处理。 |
| `filecoin.network` | 切换到 `mainnet` 会改变支付和存储环境。 |
| `filecoin.allow_private_networks` | 允许私有网络存储提供方 URL。只在可信私有部署中开启。 |
| `cache.max_size_gb` | 太小会阻塞写入；太大会占满主机磁盘。 |
| `cache.lru_high_watermark_percent` | 高水位过高会减少淘汰追赶期间可供新写入使用的余量。 |
| `cache.lru_low_watermark_percent` | 低水位过低会让每轮 LRU 清理更多缓存数据。 |

高风险设置可能需要显式确认：

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```
