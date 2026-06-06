---
title: 健康检查与指标
description: 监控 SynapS3 健康检查、工作进程存活状态、缓存使用量、任务队列和 Prometheus 指标。
---

# 健康检查与指标

Admin 服务提供健康检查、Prometheus 指标，以及仪表盘使用的运维视图。`/healthz` 不需要认证；`/metrics` 和仪表盘 API 端点需要 Admin 认证。

## 健康检查

正常运行时，`GET /healthz` 会检查数据库连接、缓存目录可用性和工作进程存活状态。

```bash
synaps3 admin status
curl http://127.0.0.1:9090/healthz
```

健康状态：

```json
{"status":"ok"}
```

缺少必要配置时：

```json
{"status":"setup"}
```

检查失败时：

```json
{"status":"unhealthy","errors":["worker/uploader: not responding"]}
```

`setup` 表示需要补齐缺失配置。`unhealthy` 表示数据库、缓存或工作进程检查失败，优先查看返回的错误列表。

## 工作进程存活状态

如果工作进程没有活跃任务，并且超过 `3 * poll_interval` 没有 tick，会被判定为不健康。即使当时没有上传任务，这个检查也能发现已经停止的工作进程。

检查工作进程状态：

```bash
synaps3 admin status
synaps3 admin task stats
```

status 应显示工作进程健康。task stats 会显示 `queued`、`running`、`failed` 或 `exhausted` 的任务数量。

## Prometheus Metrics

指标暴露在 Admin 端口：

```yaml
scrape_configs:
  - job_name: synaps3
    static_configs:
      - targets: ["synaps3:9090"]
    metrics_path: /metrics
    basic_auth:
      username: admin
      password_file: /run/secrets/synaps3-admin-password
```

关键指标：

| Metric | 含义 |
| --- | --- |
| `synaps3_backend_object_operations_total` | 按类型和状态统计的 S3 操作。 |
| `synaps3_cache_used_bytes` | 当前缓存磁盘使用量。 |
| `synaps3_cache_hits_total` / `synaps3_cache_misses_total` | 缓存读取行为。 |
| `synaps3_worker_tasks_processed_total` | 按结果统计的工作进程吞吐。 |
| `synaps3_worker_tasks_exhausted_total` | 已耗尽重试次数的任务。 |
| `synaps3_worker_task_duration_seconds` | 后台任务处理耗时。 |
| `synaps3_task_queue_depth` | 按类型和状态统计的活跃任务。 |
| `synaps3_object_state_distribution` | 按生命周期状态统计的对象数量。 |

## 运维信号

| 信号 | 处理方式 |
| --- | --- |
| `/healthz` 返回 `setup` | 补齐缺失配置，通常是 `filecoin.private_key`。 |
| `/healthz` 返回 `unhealthy` | 检查数据库、缓存目录和工作进程错误信息。 |
| 缓存使用量接近容量 | 增大容量，或恢复上传和淘汰进度。 |
| exhausted 任务增加 | 修复依赖后重试任务。 |
| 存储提供方健康状态下降 | 检查 RPC、provider URL 和网络可达性。 |

恢复步骤见[故障排查](./troubleshooting.md)。
