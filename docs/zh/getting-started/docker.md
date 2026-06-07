---
title: Docker 部署
description: 使用 Docker Compose 部署 SynapS3。
---

# Docker 部署

容器会把运行数据保存在 `/var/lib/synaps3`，在 `8080` 暴露 S3 API，并默认让仪表盘和 Admin API 只监听本机回环地址。

## 前置条件

- Docker Engine 和 Docker Compose v2.24 或更高版本。
- 为 `synaps3-data` volume 准备可靠的本地磁盘。
- 如果从另一台机器访问仪表盘，需要 SSH 权限。
- 可充值的 Calibration 钱包，或按下面步骤生成并充值。

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

命令会输出钱包地址和 private key。编辑 `.env`，写入这个 private key：

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

不要用 shell 命令直接写入真实 private key，避免进入 shell history。其他可用覆盖项见[环境变量](../configuration/environment.md)。

给生成的钱包地址充值 Calibration 测试资产：

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

如果 faucet 不稳定，先使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动充值再启动服务。

## 启动 SynapS3

```bash
docker compose up -d
docker compose logs --tail=50 synaps3
```

日志应显示服务启动，且没有配置校验错误。

默认端点：

| 端点 | 地址 |
| --- | --- |
| S3 API | `http://<host>:8080` |
| 仪表盘和 Admin API | `http://127.0.0.1:9090` |
| 运行数据 | Docker volume `synaps3-data` |

Compose 文件使用 host networking。这样 S3 API 可以对外监听，Admin 服务仍保持在本机回环地址。

浏览器登录仪表盘时，读取生成的 Admin 密码：

```bash
docker compose exec synaps3 cat /var/lib/synaps3/admin-initial-password
```

用户名是 `admin`。容器内 `synaps3 admin` 命令会自动读取该密码文件。

## 验证节点

```bash
curl http://127.0.0.1:9090/healthz
docker compose exec synaps3 synaps3 admin status
docker compose exec synaps3 synaps3 wallet deposit 2 # 2 USDFC
```

预期结果：`/healthz` 返回 `{"status":"ok"}`，`admin status` 显示运行时、工作进程和缓存状态。deposit 命令提交钱包操作。

远程访问仪表盘使用 SSH：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

> [!CAUTION]
> 不要把仪表盘或 Admin API 直接发布到不可信网络。远程访问请使用 SSH 隧道，或放在 HTTPS 反向代理和明确访问控制之后。

## 运维部署

正式承载流量前，完成[生产环境检查清单](../operations/production-checklist.md)。

常用命令：

```bash
docker compose ps
docker compose logs --tail=100 synaps3
docker compose exec synaps3 synaps3 admin task stats
```

## 升级

```bash
docker compose pull
docker compose up -d
```

Compose 会替换容器，并继续挂载 `synaps3-data`。

## 备份运行数据

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
```

归档应包含 volume 中的 `config.toml`、`db/` 和缓存数据。
