---
title: Docker 部署
description: 使用 Docker 部署 SynapS3，并按需为 Admin 启用自动 HTTPS。
---

# Docker 部署

Docker 默认让仪表盘和 Admin API 监听 `127.0.0.1:9090`。需要公网 Admin 域名时，可以选择 Caddy HTTPS；Caddy 不代理 S3 API。

## 前置条件

- 安装了 Git、Make、curl、Docker Engine 和 Docker Compose v2.24 或更高版本的 Linux 主机。
- 为 `synaps3-data` volume 准备可靠的本地磁盘。
- 从其他机器访问本机 Admin 时，需要 SSH 权限或可用的公网 HTTPS 域名。
- 可充值的 Calibration 钱包；没有钱包时可以按本文步骤生成。

## 初始化部署

克隆仓库：

```bash
git clone --depth 1 https://github.com/strahe/SynapS3.git
cd SynapS3
```

`docker-init` 创建权限为 `0600` 的 `.env`。首次初始化前，从下面四种组合中选择一条：

| 镜像来源 | Admin 访问方式 | 初始化命令 |
| --- | --- | --- |
| 发布镜像（默认） | 本机 `127.0.0.1:9090` | `make docker-init` |
| 发布镜像（默认） | 公网 HTTPS | `make docker-init ADMIN_DOMAIN=admin.example.com` |
| 当前源码构建 | 本机 `127.0.0.1:9090` | `make docker-init IMAGE_SOURCE=local` |
| 当前源码构建 | 公网 HTTPS | `make docker-init IMAGE_SOURCE=local ADMIN_DOMAIN=admin.example.com` |

默认使用发布镜像，Admin 只监听 `127.0.0.1:9090`。`IMAGE_SOURCE=local` 让 `make docker-up` 从当前 checkout 构建镜像；`ADMIN_DOMAIN` 会加载 Caddy，自动签发和续签证书，并把 HTTP 重定向到 HTTPS。两项可以同时使用。

`docker-init` 拒绝覆盖已有 `.env`。创建后检查文件内容，并始终保持 `0600` 权限。

### Admin HTTPS 要求

公网 HTTPS 还需要：

- Admin 域名的 A 记录指向主机公网 IPv4 地址。
- 只有在 IPv6 同样可达时才配置 AAAA 记录。
- 主机和云防火墙开放 `80/TCP` 与 `443/TCP`。
- 如果域名使用 CAA，允许 Caddy 使用的公共 CA。

`ADMIN_DOMAIN` 只填写域名，不要包含 scheme、端口、路径或通配符。SynapS3 Admin 仍只监听 `127.0.0.1:9090`。

`docker-init` 只检查域名语法。证书签发仍要求该域名可以从公网解析，并且公共 CA 接受签发；内部域名和保留域名会导致 `docker-verify` 失败。HSTS 只作用于配置的 Admin 域名，不继承到子域，也不申请 preload。

## 配置钱包私钥

已有钱包时，把私钥写入 `.env`。没有钱包时，先在私密终端中生成：

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

将输出的私钥保存到受保护的位置，然后在 `.env` 中取消注释或添加：

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

不要把真实私钥放进 shell 命令。仪表盘只显示私钥是否已配置，不接收或保存私钥；Docker 部署应通过 `.env` 设置。其他覆盖项见[环境变量](../configuration/environment.md)。

为钱包地址申请 Calibration 测试资产：

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

如果 faucet 暂时不可用，可以使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动领取。

缺少私钥时容器仍会启动，但 SynapS3 只提供 Admin setup 模式，不能承载正常 S3 流量。写入私钥后启动或重启服务，再使用 `make docker-verify` 确认状态为 `ok`。

## 启动并验证

```bash
make docker-up
make docker-verify
```

`docker-verify` 始终检查本机 `/healthz`。启用 Admin HTTPS 时，它还会检查可信证书和 HTTP 到 HTTPS 的跳转。

启用 Admin HTTPS 后，`https://admin.example.com/healthz` 仍无需 Admin 凭据，便于外部健康检查。`unhealthy` 响应可能指出异常的数据库、缓存或 worker。如果不希望公开这些状态，请改用本机 Admin 和 SSH 隧道。

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

本地镜像部署省略 `docker compose pull`；更新 checkout 后，`make docker-up` 会重新构建镜像。当前发布通道是可移动的 `edge` 标签，没有自动回退。恢复正常流量前，完成[生产环境检查清单](../operations/production-checklist.md)。
