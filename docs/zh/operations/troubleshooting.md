---
title: 故障排查
description: 诊断并处理 SynapS3 的启动、健康状态、S3、缓存、任务、钱包和存储提供方问题。
---

# 故障排查

## 首先检查

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin status
synaps3 admin task stats
```

状态不是 `ok` 时，转到下面对应的小节。各状态的含义见[健康检查](./health-metrics.md#健康检查)。

## 设置模式

`{"status":"setup"}` 表示缺少必需设置或设置无效。`synaps3 admin status` 和 **Settings** 页面会列出这些设置。

最常见的原因是缺少钱包私钥。设置 `SYNAPS3_FILECOIN_PRIVATE_KEY` 或 `filecoin.private_key`（需要时用 `synaps3 wallet generate` 生成），重启 SynapS3，确认 `/healthz` 返回 `ok`。

## 启动失败

| 报错 | 处理 |
| --- | --- |
| `database.dsn is not set` | 设置 `database.dsn` 或 `SYNAPS3_DATABASE_DSN`。 |
| `database belongs to another Filecoin wallet or network` | 私钥或 `filecoin.network` 与创建该数据库时不同。改回创建数据库时的私钥和网络，或改用单独的数据库。 |
| `database contains an incompatible SynapS3 schema` | 见[数据库不兼容时](./backup-recovery.md#数据库不兼容时)。 |
| `locking migrator` | 上一次迁移被强行终止。确认没有其他 SynapS3 进程在运行后，执行 `synaps3 migrate --force-unlock`，再重新启动。 |
| `admin.auth.*` 校验失败 | 运行 `synaps3 admin-auth reset-password --config <path>`。 |

## 后台任务不健康

`{"status":"unhealthy","errors":["worker/tasks: not responding"]}` 表示后台任务处理已停止。

1. 记录当前状态：

   ```bash
   synaps3 admin task stats
   synaps3 admin task list --status running --limit 20
   ```

2. 查看 SynapS3 日志中的错误。
3. 重启 SynapS3。未完成的任务会在重启后继续。

## S3 请求返回 SlowDown

多种情况下 SynapS3 都会返回 `503 SlowDown`，错误消息会说明原因：

| 消息 | 原因 | 处理 |
| --- | --- | --- |
| `The bucket is still being prepared. Please retry shortly.` | 存储桶的数据集尚未就绪。 | 在仪表盘查看存储桶的状态和准备信息，以及 **Storage** 页面的测速提示。存储桶变为 **Ready** 后重试。 |
| `The local cache is full. Please retry later.` | 缓存放不下这次写入。 | 见[缓存已满](#缓存已满)。 |
| `The same data is still being removed after an earlier delete. Please retry shortly.` | 你删除了持有这些字节的最后一个版本，SynapS3 仍在删除远端副本。 | 几分钟后重试。届时这次上传会作为新数据保存。 |
| 其他消息 | 同时处理的请求超过 `server.max_requests`。 | 退避后重试，或调高 `server.max_requests`。 |

S3 客户端默认只对 `SlowDown` 重试几次，可能在问题消除前就放弃；请稍后重新执行该操作。

## 缓存已满

缓存放不下的写入会失败并返回 `503 SlowDown`。进行中的写入会预留完整大小，因此报告的用量低于上限时，写入也可能被拒绝。

```bash
synaps3 admin status
synaps3 admin settings get cache.max_size_gb
synaps3 admin settings get cache.eviction_policy
```

只有版本达到所在存储桶的最小持久副本数后，缓存才能释放空间，见[缓存淘汰](../concepts/write-path-cache.md#缓存淘汰)。处理方法：

- 恢复存储提供方连接和后台任务进度，让等待中的上传完成，缓存得以释放。
- 如果批次在等待凑满，在 **Batches** 页面点击 **Submit next batch**，或在 **Settings** 中启用 **Submit batches early to free cache space** 并重启。
- 主机磁盘有余量时，调高 `cache.max_size_gb` 并重启。
- 使用 `lru` 时，调低 `cache.lru_high_watermark_percent`，让清理更早开始。
- 调低存储桶的 **Release cache after**，让缓存更早释放。

如果 **Manage cache capacity** 任务处于等待状态，并显示 `Waiting for reliable cache access records`，说明 LRU 清理已暂停，直到重启。先根据日志修复数据库错误，再重启 SynapS3。

失败的 **Release cache** 任务会留在失败列表中。修复报告的文件系统或数据库问题后，重试该任务。

## 失败任务

```bash
synaps3 admin task list --status failed --limit 100
synaps3 admin task retry 42
```

先排除原因：RPC 连接、存储提供方可达性、钱包资金、FWSS 批准或缓存空间。然后在 **Tasks** 页面点击 **Retry**，或运行 `synaps3 admin task retry <id>`。只有在安全时才会提供 **Retry**；否则任务会显示原因。

| 操作 | 恢复方式 |
| --- | --- |
| **Create data set** | **Retry** 会继续使用同一个存储提供方。要改用其他存储提供方，打开存储桶的 **Storage** 标签页替换存储提供方。 |
| **Upload data** | **Retry** 先检查存储提供方是否已有该数据，缺失时才重新上传。 |
| **Replace provider** | 在存储桶的 **Storage** 标签页恢复。 |
| **Commit batch** | 恢复依赖后点击 **Retry**。SynapS3 会先查询链上状态再重新提交批次。`synaps3 admin storage-confirmation list` 列出需要处理的批次。 |
| **Wallet operation** | **Retry** 继续处理已记录的同一笔交易，或启动从未广播的操作。如果交易可能已广播但没有记录，请到链上核对钱包；该操作不能重试。 |
| **Remove batch** | **Retry** 会查询链上状态，并可能再提交一次付费删除请求。之前的请求仍可能成功。 |
| **Clean up deleted data** | **Retry** 会重试失败的副本并继续清理。 |

副本失败时，在 **Tasks** 页面或对象的 **Provenance** 对话框中点击 **Retry**。副本显示 **Stored** 即表示完成。

确认过的失败，点击 **Acknowledge** 或运行 `synaps3 admin task acknowledge <id>`，即可移到 **Closed**。确认不会改变任务结果，也不影响能否重试。失败较多时，在 **Open** 中筛选 **Failed**，按需再筛选 **Operation**，然后点击 **Acknowledge all**；或者运行 `synaps3 admin task acknowledge --type <type> --yes`。确认之后才记录的失败仍保留在 **Open** 中。

## 钱包操作

**Awaiting confirmation** 表示交易已记录、结果尚未确定，包括 RPC 拒绝广播的情况。SynapS3 每 30 秒查询一次，五分钟内没有回执才消耗一次重试。持有完整签名交易时，可以重发同一笔交易，不产生另一笔付款。自动确认停止后，在 **Tasks** 页面点击 **Retry** 继续处理同一笔交易。任务停止不会取消链上交易。

在 **Wallet** 页面查看余额和 FWSS 批准状态，或者运行：

```bash
curl -u admin http://127.0.0.1:9090/api/v1/wallet
```

在 Calibration 上，重新领取测试资金，然后存入并批准：

```bash
synaps3 wallet fund-testnet 0x...
synaps3 wallet deposit 2 # 2 USDFC
synaps3 wallet approve
```

水龙头领取失败时，到 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动领取。命令失败时，检查 RPC 连接和钱包中用于支付 gas 的 FIL 余额。预期输出见 [Wallet 命令](../reference/cli.md#wallet-命令)。

## 存储提供方或 RPC 问题

**Overview** 页面显示存储健康状态，**Settings** 页面显示 Filecoin 就绪状态。也可以通过 Admin API 获取：

```bash
curl -u admin http://127.0.0.1:9090/api/v1/filecoin/readiness
curl -u admin http://127.0.0.1:9090/api/v1/observability/providers
```

处理方法：

- 修正 `filecoin.rpc_url`，或修复到 RPC 的网络连接。
- 确认 SynapS3 主机能访问存储提供方 URL。**Storage** 页面的存储提供方详情会显示上次健康检查失败的原因。
- 除非信任私有网络中的存储提供方，否则保持 `filecoin.allow_private_networks = false`。

存储桶会等测速完成后再选择存储提供方。用 **Test remaining providers** 测试尚无结果的存储提供方，用 **Retry** 重试失败的测速。见[存储提供方选择](../configuration/model.md#存储提供方选择)。

## S3 客户端无法上传

依次检查：

1. 客户端使用 path-style 寻址。
2. Access key 和 secret key 正确，用户角色允许该操作，见[创建凭据](../getting-started/s3-clients.md#创建凭据)。
3. 端点正确：本地评估用 `http://localhost:8080`，生产环境用你的 HTTPS 端点。
4. 对象大小在 127 到 1,065,353,216 字节之间，对象键符合[稳定限制](../reference/s3-compatibility.md#稳定限制)。
5. 遇到 `503 SlowDown` 时，见 [S3 请求返回 SlowDown](#s3-请求返回-slowdown)。
