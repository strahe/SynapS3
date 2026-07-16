---
title: 生产环境检查清单
description: 准备 SynapS3 部署。
---

# 生产环境检查清单

承载流量前，检查本地磁盘、数据库健康、后台任务、传输安全和恢复路径。

## 网络暴露

| 界面 | 建议暴露方式 |
| --- | --- |
| S3 API | 使用原生 TLS 或受控的 TLS 反向代理；只暴露给可信客户端或认证后的入口。 |
| 仪表盘和 Admin API | 保持在 `127.0.0.1:9090`；远程访问使用 SSH 隧道或 HTTPS 反向代理。 |
| 指标 | 使用 Admin 认证，只允许私有网络或本机采集 agent 访问。 |

不要把仪表盘或 Admin API 直接发布到互联网。设置、钱包、任务重试和 S3 用户端点都属于运维控制面。

## 运行数据

- 将 `/var/lib/synaps3` 或 `~/.synaps3` 放在可靠磁盘上。
- 除非部署需要外置 PostgreSQL 服务，否则使用默认的 SQLite 数据库。
- 备份前停止 SynapS3。SQLite 备份完整运行数据卷；PostgreSQL 使用数据库原生备份，并保存同一时间点的配置和缓存。
- 让数据库与缓存处于同一恢复时间点，验证备份归档，并演练文档中的恢复顺序。
- 监控数据库卷和缓存卷的剩余空间。
- 不要把 `config.toml`、`.env`、数据库、缓存数据和钱包材料提交到 git。配置、密钥和凭据文件使用 `0600` 权限。

## 密钥和钱包

- 将 `SYNAPS3_FILECOIN_PRIVATE_KEY` 放在主机环境、`.env` 或密钥管理系统中。
- 安全保存 Admin 密码。密码丢失或泄露时，用 `synaps3 admin-auth reset-password --config <path>` 离线轮换；这也会让已有浏览器 session 失效。
- 启动后确认 `synaps3 admin status` 显示钱包状态正常。
- 在预期上传前存入 USDFC 并批准 FWSS。以下示例存入 `2 USDFC`：

```bash
synaps3 wallet deposit 2 # 2 USDFC
synaps3 wallet approve
```

新的 deposit 或 approval 应输出 `Transaction: <hash>` 和 `Status: confirmed`；已经完成 approval 时会输出 `FWSS approval: already approved`。

## 配置检查

查看当前生效设置：

```bash
synaps3 admin settings get
```

优先检查这些字段：

| 字段 | 检查点 |
| --- | --- |
| `admin.addr` | 除非有 HTTPS 和访问控制保护，否则保持 `127.0.0.1:9090`。 |
| `admin.trusted_proxies` | 除非可信代理会清理不可信 forwarded headers，否则保持空。 |
| `admin.auth.enabled` | 生产环境保持 `true`。 |
| Admin password hash 和 `admin.auth.session_secret` | 必须存在；password hash 由 init/reset 生成，session secret 按密钥管理。 |
| `filecoin.network` | 明确迁移到 `mainnet` 前保持 `calibration` |
| `filecoin.allow_private_networks` | 除非存储提供方 URL 是可信私有端点，否则保持 `false` |
| `cache.max_size_gb` | 按预计上传积压量规划 |
| `logging.format` | Compose 设置为 `json`；内置默认值是 `text`。 |

保存设置后，重启 SynapS3，检查 `/healthz`，再运行 `synaps3 admin settings get` 验证实际生效值。

高风险设置需要显式确认：

```bash
synaps3 admin settings set filecoin.network=mainnet --yes
```

## 监控

至少监控：

- `GET /healthz`
- `GET /metrics`
- 缓存使用量
- 任务队列深度
- exhausted 任务数量
- 后台任务活动
- 存储提供方和数据集健康状态

`{"status":"unhealthy"}` 表示数据库、缓存或后台任务检查失败，需要处理。

## 升级准备

升级前运行：

```bash
curl http://127.0.0.1:9090/healthz
synaps3 admin task stats
synaps3 admin task list --status exhausted --limit 50
```

预期结果：`/healthz` 返回 `ok`，任务队列状态已确认，所有 exhausted 任务都有明确处理方式。

## 恢复入口

- 健康问题：先看[健康检查与指标](./health-metrics.md)。
- 后台任务失败：使用[故障排查](./troubleshooting.md)。
- 版本变更：按[升级与恢复](./upgrade-recovery.md)处理。
