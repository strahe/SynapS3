---
title: 快速开始
description: 启动临时 SynapS3 节点，在 Calibration 上充值，并上传第一个对象。
---

# 快速开始

快速评估时，启动一个临时 Docker 容器，创建 Calibration 钱包，然后上传一个 S3 对象。

部署节点请使用 [Docker 部署](./docker.md)。

## 前置条件

- Docker Engine 或 Docker Desktop。
- 启用 host networking。Docker Desktop 用户也需要开启 host networking，因为 Admin API 默认监听本机回环地址。
- 能在运行节点的机器上执行 shell 命令。

## 创建配置和钱包

生成钱包：

```bash
docker run --rm ghcr.io/strahe/synaps3:edge synaps3 wallet generate
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
docker run --rm --env-file .env ghcr.io/strahe/synaps3:edge synaps3 wallet fund-testnet 0x...
```

如果 faucet 命令失败，使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动领取，钱包有余额后继续。

Faucet 领取成功后会输出 `CalibnetUSDFC: <hash>` 和 `CalibnetFIL: <hash>`。

## 启动临时节点

```bash
docker volume create synaps3-test-data
docker run -d --name synaps3-test \
  --network host \
  --env-file .env \
  -e SYNAPS3_CONFIG=/var/lib/synaps3/config.toml \
  -v synaps3-test-data:/var/lib/synaps3 \
  ghcr.io/strahe/synaps3:edge
```

Docker 启动后会输出 container ID。

检查健康状态、存入 USDFC，并批准 FWSS：

```bash
curl http://127.0.0.1:9090/healthz
docker exec synaps3-test synaps3 wallet deposit 2 # 2 USDFC
docker exec synaps3-test synaps3 wallet approve
```

预期结果：`/healthz` 返回 `{"status":"ok"}`。新的 deposit 或 approval 会输出 `Transaction: <hash>` 和 `Status: confirmed`；已经完成 approval 时会输出 `FWSS approval: already approved`。如果 `/healthz` 返回 `setup` 或 `unhealthy`，查看[故障排查](../operations/troubleshooting.md)。

本快速开始中的 HTTP 端点只用于本地评估。生产 S3 流量应启用[原生 TLS](../configuration/model.md#s3-服务)，或使用受控的 TLS 反向代理。

## 打开仪表盘

浏览器登录需要生成的 Admin 密码：

```bash
docker exec synaps3-test cat /var/lib/synaps3/admin-initial-password
```

打开：

```text
http://127.0.0.1:9090
```

如果节点运行在远程主机，保持 Admin API 监听本机回环地址，并使用 SSH 隧道：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

仪表盘会要求输入 Admin 用户名 `admin` 和生成的密码。只在私密终端中读取密码，将其保存到密码管理器，并让密码文件保持 `0600` 权限。不要把密码直接写入会保存到 shell history 的命令。登录后可以看到存储桶、钱包状态、后台任务、存储拓扑、设置和健康状态。

## 上传第一个对象

创建 S3 用户：

```bash
docker exec synaps3-test synaps3 admin s3-user create
```

将 access key 和 secret key 用在 path-style S3 客户端中。secret key 只显示一次：请保存到权限为 `0600` 的客户端凭据文件；如果泄露，立即轮换。以下 MinIO Client 示例通过终端交互读取凭据，不会把 secret key 写入 shell history：

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
printf 'S3 access key: '
read -r S3_ACCESS_KEY
printf 'S3 secret key: '
read -rs S3_SECRET_KEY
printf '\n'
mc alias set synaps3 http://localhost:8080 "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}"
unset S3_ACCESS_KEY S3_SECRET_KEY
chmod 600 ~/.mc/config.json
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

`mc cat` 会输出上传内容。示例文件经过填充，因为 Filecoin 上传路径要求对象不小于 127 字节。

AWS CLI 和 rclone 示例见 [S3 客户端](./s3-clients.md)。

## 清理

```bash
docker rm -f synaps3-test
docker volume rm synaps3-test-data
```

不要对正式部署执行这组清理命令。`.env` 包含钱包私钥：如果不再使用这个临时钱包，请安全删除该文件；如果仍需保留钱包，请先把钱包材料移到受保护的存储，再清理评估目录。
