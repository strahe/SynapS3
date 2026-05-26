---
title: 生产环境检查清单
description: 准备长期运行的单机 SynapS3 部署。
---

# 生产环境检查清单

把 SynapS3 作为长期运行的单机服务前，检查本地磁盘、数据库健康、后台 worker 和恢复路径。

## 网络暴露

| 界面 | 建议暴露方式 |
| --- | --- |
| S3 API | 只暴露给可信客户端或认证后的入口。 |
| Dashboard 和 Admin API | 保持在 `127.0.0.1:9090`；远程访问使用 SSH tunnel。 |
| Metrics | 只允许私有网络或本机采集 agent 访问。 |

不要把 Dashboard 或 Admin API 直接发布到互联网。Settings、wallet、task retry 和 S3 user 端点都是运维控制面。

## 运行数据

- 将 `/var/lib/synaps3` 或 `~/.synaps3` 放在可靠磁盘上。
- 升级前备份 `config.toml`、`db/` 和缓存元数据。
- 监控数据库卷和缓存卷的剩余空间。
- 不要把 `config.toml`、`.env`、数据库、缓存数据和钱包材料提交到 git。

## 密钥和钱包

- 将 `SYNAPS3_FILECOIN_PRIVATE_KEY` 放在主机环境、`.env` 或 secret manager 中。
- 启动后确认 `synaps3 admin status` 显示钱包健康。
- 在预期上传前 deposit USDFC。以下示例 deposit `2 USDFC`：

```bash
synaps3 wallet deposit 2 # 2 USDFC
```

预期结果：wallet operation 被接受，随后可在仪表盘或 `GET /api/v1/wallet/operations` 中看到。

## 配置检查

查看当前生效设置：

```bash
synaps3 admin settings get
```

优先检查这些字段：

| 字段 | 检查点 |
| --- | --- |
| `admin.addr` | 除非有私有访问保护，否则保持 `127.0.0.1:9090`。 |
| `filecoin.network` | 明确迁移到 `mainnet` 前保持 `calibration` |
| `filecoin.allow_private_networks` | 除非 provider URL 是可信私有端点，否则保持 `false` |
| `cache.max_size_gb` | 按预期上传积压量规划 |
| `logging.format` | Compose 设置为 `json`；内置默认值是 `text`。 |

高风险设置需要显式确认：

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```

## 监控

至少监控：

- `GET /healthz`
- `GET /metrics`
- cache usage
- task queue depth
- exhausted task count
- worker liveness
- provider 和 data set health

`{"status":"unhealthy"}` 应视为需要处理的信号。它表示数据库、缓存或 worker 检查失败。

## 升级准备

升级前运行：

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 50
```

预期结果：health 为 `ok`，任务队列状态已确认，任何 exhausted task 都已经有明确处理决策。

## 恢复入口

- 健康问题：先看[健康检查与指标](./health-metrics.md)。
- 后台任务失败：使用[故障排查](./troubleshooting.md)。
- 版本变更：按[升级与恢复](./upgrade-recovery.md)处理。
