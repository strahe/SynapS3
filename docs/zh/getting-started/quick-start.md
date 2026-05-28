---
title: 快速开始
description: 启动临时 SynapS3 节点，在 Calibration 上充值，并上传第一个对象。
---

# 快速开始

本页用于评估 SynapS3：启动临时 Docker 容器，创建 Calibration 钱包，并上传一个 S3 对象。

长期运行节点请在理解流程后使用 [Docker 部署](./docker.md)。

## 目标

结果：

- `GET /healthz` 返回 `{"status":"ok"}`。
- Dashboard 可通过 `http://127.0.0.1:9090` 打开。
- 可以通过 S3 客户端上传并读回测试对象。

## 前置条件

- Docker Engine 或 Docker Desktop。
- 启用 host networking。Docker Desktop 用户必须启用 host networking 才能完成完整流程，因为 Admin API 默认保持 loopback 监听。
- 能在运行节点的机器上执行 shell 命令。

## 创建配置和钱包

生成钱包：

```bash
docker run --rm ghcr.io/strahe/synaps3:edge synaps3 wallet generate
```

预期结果：命令输出 wallet address 和 private key。编辑 `.env`，写入生成的 private key：

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

不要用 shell 命令直接写入真实 private key，避免进入 shell history。其他可用覆盖项见[环境变量](../configuration/environment.md)。

为生成的地址在 Calibration 上充值：

```bash
docker run --rm --env-file .env ghcr.io/strahe/synaps3:edge synaps3 wallet fund-testnet 0x...
```

预期结果：命令领取测试资产。如果失败，使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动领取，钱包有余额后继续。

## 启动临时节点

```bash
docker volume create synaps3-test-data
docker run -d --name synaps3-test \
  --network host \
  --env-file .env \
  -v synaps3-test-data:/var/lib/synaps3 \
  ghcr.io/strahe/synaps3:edge
```

预期结果：Docker 输出 container ID。

检查健康状态并 deposit USDFC：

```bash
curl http://127.0.0.1:9090/healthz
docker exec synaps3-test synaps3 --config /var/lib/synaps3/config.toml wallet deposit 2 # 2 USDFC
```

预期结果：health 返回 `{"status":"ok"}`，deposit 命令接受 wallet operation。如果 health 返回 `setup` 或 `unhealthy`，查看[故障排查](../operations/troubleshooting.md)。

## 打开 Dashboard

读取生成的 Admin 密码：

```bash
ADMIN_PASSWORD=$(docker exec synaps3-test cat /var/lib/synaps3/admin-initial-password)
```

打开：

```text
http://127.0.0.1:9090
```

如果节点运行在远程主机，保持 Admin API 监听 loopback，并使用 SSH tunnel：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

预期结果：Dashboard 要求输入 Admin 用户名 `admin` 和生成的密码，然后显示 buckets、wallet status、tasks、topology、settings 和 health signals。

## 上传第一个对象

创建 S3 用户：

```bash
docker exec -e SYNAPS3_ADMIN_PASSWORD="$ADMIN_PASSWORD" synaps3-test \
  synaps3 --config /var/lib/synaps3/config.toml admin s3-user create
```

将 access key 和 secret 用在 path-style S3 客户端中。以下示例使用 MinIO Client：

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
mc alias set synaps3 http://localhost:8080 replace-with-access-key replace-with-secret-key
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

预期结果：`mc cat` 输出上传内容。示例文件经过填充，因为 Filecoin 上传路径要求对象不小于 127 字节。

AWS CLI 和 rclone 示例见 [S3 客户端](./s3-clients.md)。

## 清理

```bash
docker rm -f synaps3-test
docker volume rm synaps3-test-data
```

预期结果：临时 container 和 volume 被删除。不要对长期部署执行这组清理命令。
