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

命令会输出钱包地址和私钥。先创建仅当前用户可读写的 `.env`：

```bash
touch .env
chmod 600 .env
```

然后编辑 `.env`，写入生成的私钥：

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

让 `.env` 始终保持 `0600` 权限。不要用 shell 命令直接写入真实私钥，避免进入 shell history。其他可用覆盖项见[环境变量](../configuration/environment.md)。

给生成的钱包地址充值 Calibration 测试资产：

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

如果 faucet 不稳定，先使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动充值再启动服务。

Faucet 领取成功后会输出 `CalibnetUSDFC: <hash>` 和 `CalibnetFIL: <hash>`。

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

上面的 HTTP 地址适合本地评估。生产 S3 流量应配置原生 TLS：设置 `SYNAPS3_SERVER_TLS_ENABLED=true`、`SYNAPS3_SERVER_TLS_CERT_FILE` 和 `SYNAPS3_SERVER_TLS_KEY_FILE`；也可以把 S3 API 放在受控的 TLS 反向代理之后。证书和私钥路径必须在容器内可见，通常应使用只读挂载。Admin 端点继续使用回环地址、SSH 隧道，或带访问控制的 HTTPS 反向代理。

浏览器登录仪表盘时，读取生成的 Admin 密码：

```bash
docker compose exec synaps3 cat /var/lib/synaps3/admin-initial-password
```

用户名是 `admin`。容器内 `synaps3 admin` 命令会自动读取该密码文件。

只在私密终端中读取密码，将其保存到密码管理器，并让密码文件保持 `0600` 权限。不要把密码直接写入 shell history。

## 验证节点

```bash
curl http://127.0.0.1:9090/healthz
docker compose exec synaps3 synaps3 admin status
docker compose exec synaps3 synaps3 wallet deposit 2 # 2 USDFC
docker compose exec synaps3 synaps3 wallet approve
```

预期结果：`/healthz` 返回 `{"status":"ok"}`，`admin status` 显示运行时、后台任务和缓存状态。新的 deposit 或 approval 会输出 `Transaction: <hash>` 和 `Status: confirmed`；已经完成 approval 时会输出 `FWSS approval: already approved`。

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

按照[升级与恢复](../operations/upgrade-recovery.md)操作，其中包含升级前检查、一致性备份、升级验证和回退顺序。

## 备份运行数据

不要直接归档仍在使用的数据卷。先检查健康状态和任务队列、停止 SynapS3，再按照[运行数据](../configuration/runtime-data.md)中的数据库驱动专用步骤备份和恢复。数据库与缓存必须处于同一恢复时间点。
