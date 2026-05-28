---
title: 健康检查与指标
description: 监控 SynapS3 health、worker liveness、cache usage、task queues 和 Prometheus metrics。
---

# 健康检查与指标

Admin server 暴露健康检查、Prometheus metrics，以及 dashboard 使用的运维视图。`/healthz` 不需要认证；`/metrics` 和 dashboard API 端点需要 Admin auth。

## Health

正常 serve mode 下，`GET /healthz` 检查数据库连接、缓存目录可用性和 worker liveness。

```bash
synaps3 admin status
curl http://localhost:9090/healthz
```

健康响应：

```json
{"status":"ok"}
```

Setup 响应：

```json
{"status":"setup"}
```

不健康响应：

```json
{"status":"unhealthy","errors":["worker/uploader: not responding"]}
```

`setup` 表示需要补齐缺失配置。`unhealthy` 应视为运维事件，优先检查 error list。

## Worker Liveness

当 worker 没有活跃工作，并且超过 `3 * poll_interval` 没有最近 tick 时会被判定为不健康。这可以在没有活跃上传时发现停止的 worker。

检查 worker 状态：

```bash
synaps3 admin status
synaps3 admin task stats
```

预期结果：status 显示 worker 健康，task stats 显示 queued、running、failed 或 exhausted 的任务状态。

## Prometheus Metrics

Metrics 暴露在 admin 端口：

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
| `synaps3_worker_tasks_processed_total` | 按结果统计的 worker 吞吐。 |
| `synaps3_worker_tasks_exhausted_total` | 已耗尽重试次数的任务。 |
| `synaps3_task_queue_depth` | 按类型和状态统计的活跃任务。 |
| `synaps3_object_state_distribution` | 按生命周期状态统计的对象数量。 |

## 运维信号

| 信号 | 处理方式 |
| --- | --- |
| Health 是 `setup` | 补齐缺失配置，通常是 `filecoin.private_key`。 |
| Health 是 `unhealthy` | 检查数据库、缓存目录和 worker 错误信息。 |
| Cache usage 接近容量 | 增大容量，或恢复上传和淘汰进度。 |
| Exhausted task count 增加 | 修复依赖后重试任务。 |
| Provider health degraded | 检查 RPC、provider URL 和网络可达性。 |

恢复步骤见[故障排查](./troubleshooting.md)。
