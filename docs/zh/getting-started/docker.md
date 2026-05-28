---
title: Docker 部署
description: 使用 Docker Compose 将 SynapS3 部署为长期运行的单机服务。
---

# Docker 部署

使用 Docker Compose 运行长期单机部署。容器将运行数据保存在 `/var/lib/synaps3`，在 `8080` 暴露 S3 API，并默认把 Dashboard 和 Admin API 保持在 loopback。

## 目标

结果：

- SynapS3 作为 detached Compose service 运行。
- 运行数据保存在 `synaps3-data` volume。
- Health 返回 `ok`。
- Dashboard 可在本机或通过 SSH tunnel 访问。

## 前置条件

- Docker Engine 和 Docker Compose v2.24 或更高版本。
- 用于 `synaps3-data` volume 的可靠本地磁盘。
- 如果从另一台机器访问 Dashboard，需要 SSH 权限。
- 评估时需要已充值的 Calibration 钱包。

## 准备配置

创建部署目录，并下载 Compose 文件：

```bash
mkdir synaps3-deploy
cd synaps3-deploy
curl -fsSLO https://raw.githubusercontent.com/strahe/SynapS3/main/compose.yaml
```

生成钱包：

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

预期结果：命令输出 wallet address 和 private key。编辑 `.env`，写入生成的 private key：

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

不要用 shell 命令直接写入真实 private key，避免进入 shell history。其他可用覆盖项见[环境变量](../configuration/environment.md)。

为生成地址在 Calibration 上充值：

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

预期结果：钱包收到测试资产。如果 faucet 不稳定，先使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动充值再启动服务。

## 启动 SynapS3

```bash
docker compose up -d
docker compose logs --tail=50 synaps3
```

预期结果：日志显示服务启动，没有配置校验错误。

从运行数据 volume 读取生成的 Admin 密码：

```bash
ADMIN_PASSWORD=$(docker compose exec synaps3 cat /var/lib/synaps3/admin-initial-password)
```

默认端点：

| 端点 | 地址 |
| --- | --- |
| S3 API | `http://<host>:8080` |
| Dashboard 和 Admin API | `http://127.0.0.1:9090` |
| 运行数据 | Docker volume `synaps3-data` |

Compose 文件使用 host networking，让 S3 API 可以公开监听，同时让 Admin server 保持在 loopback。

## 验证节点

```bash
curl http://127.0.0.1:9090/healthz
docker compose exec -e SYNAPS3_ADMIN_PASSWORD="$ADMIN_PASSWORD" synaps3 \
  synaps3 --config /var/lib/synaps3/config.toml admin status
docker compose exec synaps3 synaps3 --config /var/lib/synaps3/config.toml wallet deposit 2 # 2 USDFC
```

预期结果：health 返回 `{"status":"ok"}`，`admin status` 显示 runtime、worker 和 cache 状态。Deposit 命令提交 wallet operation。

远程访问 Dashboard 使用 SSH：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

::: danger Admin 暴露风险
不要把 Dashboard 或 Admin API 直接发布到不可信网络。远程访问请使用 SSH tunnel，或放在 HTTPS 反向代理和明确访问控制之后。
:::

## 运维部署

正式承载流量前，完成[生产环境检查清单](../operations/production-checklist.md)。

常用命令：

```bash
docker compose ps
docker compose logs --tail=100 synaps3
docker compose exec -e SYNAPS3_ADMIN_PASSWORD="$ADMIN_PASSWORD" synaps3 \
  synaps3 --config /var/lib/synaps3/config.toml admin task stats
```

## 升级

```bash
docker compose pull
docker compose up -d
```

预期结果：Compose 替换容器，并继续挂载 `synaps3-data`。

## 备份运行数据

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
```

预期结果：`synaps3-data.tgz` 包含 volume 中的 `config.toml`、`db/` 和 cache 数据。
