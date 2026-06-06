---
title: 故障排查
description: 排查 SynapS3 setup 状态、健康检查、钱包、缓存、任务和存储提供方常见问题。
---

# 故障排查

当用户可见流程失败时，从这里开始。先检查健康状态，再按钱包、缓存、任务或存储提供方信号缩小范围。

## 首先检查

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin status
synaps3 admin task stats
```

健康基线：

```json
{"status":"ok"}
```

如果健康状态不是 `ok`，优先按返回的错误文本继续排查。

## setup 状态

`/healthz` 可能返回：

```json
{"status":"setup"}
```

这表示 SynapS3 缺少必要配置，常见原因是没有设置 `filecoin.private_key`。

修复：

```bash
synaps3 wallet generate
```

将生成的 private key 写入 `SYNAPS3_FILECOIN_PRIVATE_KEY`，或写入配置文件中的 `filecoin.private_key`，然后重启 SynapS3。

预期结果：重启后健康状态从 `setup` 变为 `ok`。

## 工作进程不健康

示例：

```json
{"status":"unhealthy","errors":["worker/uploader: not responding"]}
```

检查任务状态：

```bash
synaps3 admin task stats
synaps3 admin task list --status running --limit 20
```

如果进程刚重启，启动恢复会释放过期租约。若工作进程持续不健康，先记录当前任务状态并查看日志，然后重启服务。

## 钱包充值或 deposit 失败

检查钱包状态：

```bash
synaps3 admin status
```

在 Calibration 上重新为地址领取测试资产：

```bash
synaps3 wallet fund-testnet 0x...
```

然后重试 deposit：

```bash
synaps3 wallet deposit 2 # 2 USDFC
```

如果 faucet 失败，使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动领取，然后重新运行 `synaps3 admin status`。

## 缓存已满

缓存容量耗尽时上传端点可能失败。检查使用量：

```bash
synaps3 admin status
synaps3 admin settings get cache.max_size_gb
```

恢复方式：

- 如果磁盘容量允许，增大 `cache.max_size_gb`。
- 恢复存储提供方或工作进程连接，让排队上传完成并触发缓存淘汰。
- 如果希望远端存储成功后自动排队清理本地缓存，保持 `cache.eviction_policy = "lru"`。

## Exhausted 任务

列出 exhausted 任务：

```bash
synaps3 admin task list --status exhausted --limit 100
```

只有在底层依赖修复后再重试。常见依赖包括 RPC 连接、存储提供方可用性、钱包余额或缓存磁盘容量。

```bash
synaps3 admin task retry 42
```

## 存储提供方或 RPC 问题

在仪表盘查看存储提供方健康状态和 Filecoin readiness，或检查 Admin API：

```bash
curl -u "admin:${SYNAPS3_ADMIN_PASSWORD}" http://127.0.0.1:9090/api/v1/filecoin/readiness
curl -u "admin:${SYNAPS3_ADMIN_PASSWORD}" http://127.0.0.1:9090/api/v1/observability/providers
```

恢复方式：

- 恢复配置中的 `filecoin.rpc_url`。
- 确认 SynapS3 主机能访问 provider URL。
- 除非明确需要并信任私有 provider URL，否则保持 `filecoin.allow_private_networks = false`。

## S3 客户端无法上传

按顺序检查：

1. S3 客户端使用 path-style addressing。
2. Access key 和 secret 来自 `synaps3 admin s3-user create`。
3. endpoint 是 `http://localhost:8080` 或正确的远程 S3 地址。
4. 测试对象至少 127 字节。
5. 仪表盘任务页显示 Filecoin 存储处于 `queued`、`running` 还是 `exhausted`。
