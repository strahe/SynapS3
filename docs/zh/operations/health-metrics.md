---
title: 健康检查与指标
description: 读取 SynapS3 的健康检查端点、仪表盘监控和 Prometheus 指标。
---

# 健康检查与指标

Admin 监听地址提供无需认证的 `/healthz`，以及需要 Admin 认证的 `/metrics`。

## 健康检查

```bash
curl http://127.0.0.1:9090/healthz
```

| 响应 | HTTP 状态码 | 含义 |
| --- | --- | --- |
| `{"status":"ok"}` | `200` | 数据库可达，缓存目录存在，后台任务正常运行。 |
| `{"status":"setup"}` | `200` | 缺少必需设置或设置无效。只能使用 Admin 设置，不提供 S3 API。 |
| `{"status":"unhealthy","errors":[...]}` | `503` | 有检查项失败。`errors` 中包含 `db: unreachable`、`cache: directory not found` 或 `worker/tasks: not responding`。 |

`setup` 同样返回 `200`，只检查 HTTP 状态码的探针会把它当作健康。请检查 `status` 字段。

`worker/tasks: not responding` 表示后台任务处理已停止上报活动，即使当前没有上传也会出现。

`synaps3 admin status` 显示健康状态、版本、运行时长、缓存用量和后台任务状态。在设置模式下，它会列出需要修正的设置。

## 仪表盘监控

**Overview** 页面显示：

- **Process resources**：SynapS3 的 CPU 使用量（核数）和常驻内存。
- **Application traffic**：上传和下载速率。**Client** 统计 S3 和仪表盘的对象传输；**Storage** 统计 SynapS3 与存储提供方之间的传输。测速和存储提供方之间的复制不计入。信息按钮显示采样时间和启动以来的字节数。

图表每 15 秒采样一次，保留一小时。重启会清空历史；需要更长保留时间时使用 Prometheus。

## Prometheus 指标

同一主机上的 Prometheus 可以用 Basic 认证抓取 Admin 监听地址：

```yaml
scrape_configs:
  - job_name: synaps3
    static_configs:
      - targets: ["127.0.0.1:9090"]
    metrics_path: /metrics
    basic_auth:
      username: admin
      password_file: /run/secrets/synaps3-admin-password
```

容器中的 Prometheus 无法访问宿主机的回环地址。把 `admin.addr` 绑定到只有 Prometheus 能访问的私有网络接口，保持 Admin 认证开启，并且不要公开该端口。

| 指标 | 含义 |
| --- | --- |
| `synaps3_backend_object_operations_total` | 按类型和状态统计的 S3 对象操作数。 |
| `synaps3_transfer_bytes_total` | 按 `scope`（`client`、`storage`）和 `direction`（`upload`、`download`）统计的对象字节数。重启后归零。 |
| `synaps3_cache_used_bytes` | 缓存磁盘用量。 |
| `synaps3_cache_hits_total`、`synaps3_cache_misses_total` | 命中缓存的读取数和未命中的读取数。 |
| `synaps3_task_queue_depth` | 按类型和状态统计的等待中和运行中任务数，不含失败任务。 |
| `synaps3_object_state_distribution` | 按状态（`cached`、`uploading`、`committing`、`replicating`、`stored`、`failed`）统计的对象数。 |
| `synaps3_database_metrics_collection_success` | 上一次采集数据库指标成功为 `1`，否则为 `0`。 |
| `synaps3_database_metrics_last_success_timestamp_seconds` | 上一次采集成功的时间，Unix 秒。 |

基于数据库的指标（`synaps3_task_queue_depth` 和 `synaps3_object_state_distribution`）每 30 秒采集一次。采集失败时保留上一次的值；使用这些值前先检查两个采集状态指标。

## 告警建议

| 信号 | 处理 |
| --- | --- |
| `/healthz` 状态不是 `ok` | 见[故障排查](./troubleshooting.md#首先检查)。 |
| 缓存用量接近 `cache.max_size_gb` | 见[缓存已满](./troubleshooting.md#缓存已满)。 |
| 失败任务增加（`synaps3 admin task stats` 或 `GET /api/v1/tasks/stats`） | 见[失败任务](./troubleshooting.md#失败任务)。 |
| 存储提供方或数据集未通过健康检查 | 见[存储提供方或 RPC 问题](./troubleshooting.md#存储提供方或-rpc-问题)。 |
