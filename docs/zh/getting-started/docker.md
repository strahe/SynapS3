---
title: Docker 部署
description: 使用 Docker 部署 SynapS3，并按需为 Admin 启用自动 HTTPS。
---

# Docker 部署

Docker 默认让仪表盘和 Admin API 监听 `127.0.0.1:9090`。需要公网 Admin 域名时，可以在初始化时选择 Caddy HTTPS；Caddy 不代理 S3 API。

## 前置条件

- 安装了 Git、Make、curl、Docker Engine 和 Docker Compose v2.24 或更高版本的 Linux 主机。
- 为 `synaps3-data` volume 准备可靠的本地磁盘。
- 从其他机器访问本机 Admin 时，需要 SSH 权限或可用的公网 HTTPS 域名。
- 可充值的 Calibration 钱包；也可以先以 `setup` 状态启动，再补全钱包设置。

## 初始化部署

克隆仓库：

```bash
git clone --depth 1 https://github.com/strahe/SynapS3.git
cd SynapS3
```

### 默认：本机 Admin

不配置域名时，Admin 只监听回环地址：

```bash
make docker-init
```

### 可选：Admin 自动 HTTPS

公网 HTTPS 还需要：

- Admin 域名的 A 记录指向主机公网 IPv4 地址。
- 只有在 IPv6 同样可达时才配置 AAAA 记录。
- 主机和云防火墙开放 `80/TCP` 与 `443/TCP`。
- 如果域名使用 CAA，允许 Caddy 使用的公共 CA。

初始化时传入域名：

```bash
make docker-init ADMIN_DOMAIN=admin.example.com
```

`ADMIN_DOMAIN` 只填写域名，不要包含 scheme、端口、路径或通配符。此模式会加载 Caddy，自动签发和续签证书，并把 HTTP 重定向到 HTTPS。SynapS3 Admin 仍只监听 `127.0.0.1:9090`。

### 从当前源码构建 Docker 镜像

需要使用当前 checkout 构建容器镜像时：

```bash
make docker-init IMAGE_SOURCE=local
```

如果还需要 Admin HTTPS，同时传入 `ADMIN_DOMAIN`。`make docker-up` 会按需构建本地镜像。

`docker-init` 会创建权限为 `0600` 的 `.env`，已有文件时拒绝覆盖。选择部署模式后，检查 `.env`，并让它始终保持 `0600` 权限。

## 准备钱包

没有钱包时，在私密终端中生成：

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

将输出的私钥保存到受保护的位置，然后编辑 `.env`：

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

不要把真实私钥放进 shell 命令。其他覆盖项见[环境变量](../configuration/environment.md)。

为钱包地址申请 Calibration 测试资产：

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

如果 faucet 暂时不可用，可以使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动领取。也可以暂不配置钱包，让 SynapS3 以 `setup` 状态启动后在仪表盘中完成设置。

## 启动并验证

```bash
make docker-up
make docker-verify
```

`docker-verify` 始终检查本机 `/healthz`。启用 Admin HTTPS 时，它还会检查可信证书和 HTTP 到 HTTPS 的跳转。

| 状态 | 含义 |
| --- | --- |
| `setup` | Admin 可访问，但仍缺少必要设置。 |
| `ok` | Admin 可访问，运行时健康检查通过。 |
| `unhealthy` | 数据库、缓存或 worker 需要处理。 |

只有 `ok` 状态可以承载正常 S3 流量。

| 界面或数据 | 地址或位置 |
| --- | --- |
| S3 API | 单独配置 S3 TLS 前为 `http://<host>:8080` |
| 本机 Admin | `http://127.0.0.1:9090` |
| 公网 Admin | 仅 HTTPS 模式：`https://admin.example.com` |
| SynapS3 运行数据 | Docker volume `synaps3-data` |
| Caddy 状态 | 仅 HTTPS 模式：`synaps3-caddy-data`、`synaps3-caddy-config` |

读取初始 Admin 密码：

```bash
make docker-password
```

用户名是 `admin`。只在私密终端中读取密码，并保存到密码管理器。

本机 Admin 的远程访问使用 SSH 隧道：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

启用 HTTPS 时直接访问配置的域名。不要把 9090 端口绑定到公网接口。

容器内 Admin CLI 仍使用原生命令：

```bash
docker compose exec -T synaps3 synaps3 admin status
```

## S3 传输安全

Caddy 只保护仪表盘和 Admin API，不处理 S3 API。

生产 S3 流量应配置原生 TLS：设置 `SYNAPS3_SERVER_TLS_ENABLED=true`、`SYNAPS3_SERVER_TLS_CERT_FILE` 和 `SYNAPS3_SERVER_TLS_KEY_FILE`；也可以把 S3 API 放在单独的受控 TLS 代理之后。证书和私钥路径必须在容器内可见，通常使用只读挂载。

## 日常操作

```bash
make docker-status
make docker-logs
make docker-logs DOCKER_SERVICE=synaps3
make docker-logs DOCKER_SERVICE=caddy DOCKER_LOG_FOLLOW=1 # 仅 HTTPS 模式
make docker-down
```

日志默认显示最近 100 行。`docker-down` 会移除容器，但保留 `.env`、`synaps3-data` 和已有的 Caddy 证书 volume。

## HTTPS 故障排查

只在启用 Admin HTTPS 时需要本节。如果 `make docker-verify` 提示 HTTPS 尚未就绪：

1. 检查 A 记录和可选的 AAAA 记录。
2. 确认没有其他服务占用 80 或 443 端口。
3. 检查主机防火墙、云防火墙和 CAA 记录。
4. 查看 Caddy 日志：

```bash
make docker-logs DOCKER_SERVICE=caddy
```

修复后 Caddy 会继续重试签发。不要删除 `synaps3-caddy-data`，其中保存证书私钥和 ACME 账户状态。

## 备份 Docker 数据

先检查健康状态并停止容器：

```bash
make docker-verify
make docker-down
```

默认 SQLite 部署可以归档完整运行数据 volume：

```bash
docker run --rm \
  -v synaps3-data:/data:ro \
  -v "$PWD":/backup \
  alpine:3 \
  tar czf /backup/synaps3-data.tgz -C /data .
docker run --rm \
  -v "$PWD":/backup \
  alpine:3 \
  sh -c 'cd /backup && tar tzf synaps3-data.tgz >/dev/null && sha256sum synaps3-data.tgz > synaps3-data.tgz.sha256 && sha256sum -c synaps3-data.tgz.sha256'
```

PostgreSQL 部署需要数据库原生备份，并保存同一时间点的配置和缓存。详细一致性与恢复顺序见[运行数据](../configuration/runtime-data.md)。Caddy volume 不属于数据库与缓存的一致性恢复点，但应保留以避免替换证书和 ACME 账户。

备份完成后运行 `make docker-up` 和 `make docker-verify`。

## 升级

先创建并验证一致性备份。发布镜像部署使用：

```bash
git pull --ff-only
docker compose pull
make docker-up
make docker-verify
```

本地镜像部署省略 `docker compose pull`；更新 checkout 后，`make docker-up` 会重新构建镜像。当前发布通道是可移动的 `edge`，没有自动回退。恢复正常流量前，完成[生产环境检查清单](../operations/production-checklist.md)。
