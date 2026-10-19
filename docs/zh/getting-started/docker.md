---
title: Docker 部署
description: 使用 Docker Compose 部署 SynapS3，可选启用公网 Admin HTTPS，并完成备份和升级。
---

# Docker 部署

Docker 部署通过 Docker Compose 运行 SynapS3，默认同时运行 PostgreSQL 18。仪表盘和 Admin API 监听 `127.0.0.1:9090`。可选的 Caddy 代理通过公网 HTTPS 域名提供仪表盘和 Admin API；Caddy 不代理 S3 API。

## 前置条件

- 一台 Linux 主机，已安装 Git、Make、curl、Docker Engine 和 Docker Compose v2.24 或更高版本。
- 用于 `synaps3-data` 和 `synaps3-postgres-data` 卷的持久本地磁盘。
- 如需公网 Admin HTTPS：一个公网域名，并放行入站 `80/TCP` 和 `443/TCP`。
- 一个持有 FIL 和 USDFC 的 Filecoin 钱包。未设置 `SYNAPS3_FILECOIN_NETWORK` 时使用 Calibration。

## 初始化部署

```bash
git clone --depth 1 https://github.com/strahe/SynapS3.git
cd SynapS3
```

选择一条初始化命令：

| 镜像来源 | Admin 访问方式 | 命令 |
| --- | --- | --- |
| 已发布镜像 | 本机 `127.0.0.1:9090` | `make docker-init` |
| 已发布镜像 | 公网 HTTPS | `make docker-init ADMIN_DOMAIN=admin.example.com` |
| 从当前代码构建 | 本机 `127.0.0.1:9090` | `make docker-init IMAGE_SOURCE=local` |
| 从当前代码构建 | 公网 HTTPS | `make docker-init IMAGE_SOURCE=local ADMIN_DOMAIN=admin.example.com` |

`docker-init` 创建权限为 `0600` 的 `.env` 和 `.postgres-admin-password`，任一文件已存在时拒绝覆盖。它会生成两个 PostgreSQL 密码：`synaps3` 应用角色的密码写入 `.env`，管理员密码写入 `.postgres-admin-password`。`synaps3` 角色拥有数据库，但不是超级用户。

托管的 PostgreSQL 监听 `127.0.0.1:15432`。要换端口，在命令中加上 `POSTGRES_PORT=<port>`。

要使用外部 PostgreSQL，在命令中加上 `DATABASE_SOURCE=external`，然后在 `.env` 中设置 `SYNAPS3_DATABASE_DSN`。

### Admin HTTPS 要求

设置 `ADMIN_DOMAIN` 后，Caddy 负责申请和续期证书，把 HTTP 重定向到 HTTPS，并只对该域名发送 HSTS。前提条件：

- 该域名的 A 记录指向主机公网 IPv4 地址；只有主机可通过 IPv6 访问时才添加 AAAA 记录。
- 主机防火墙和云防火墙放行入站 `80/TCP` 和 `443/TCP`。
- 没有阻止 Caddy 所用公共 CA 的 CAA 记录。

`ADMIN_DOMAIN` 只填域名，不带协议、端口、路径或通配符。`docker-init` 只检查语法；内部域名或保留域名会导致证书申请失败，`make docker-verify` 也会失败。

## 配置钱包

需要新钱包时，在私密终端中运行：

```bash
docker compose run --rm synaps3 synaps3 wallet generate
```

妥善保存输出的私钥，并写入 `.env`，不要放在命令行参数中：

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

在 Calibration 上，从水龙头领取测试 FIL 和 USDFC：

```bash
docker compose run --rm synaps3 synaps3 wallet fund-testnet 0x...
```

领取失败时，可以到 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动领取。

未设置私钥时，SynapS3 以设置模式启动：只能使用 Admin 设置，不提供 S3 API。仪表盘会显示私钥是否已设置，但不能在仪表盘中设置私钥。

## 启动与验证

```bash
make docker-up
make docker-verify
```

`docker-verify` 检查本机 `/healthz`。启用 Admin HTTPS 时，还会检查证书、公网 `/healthz` 和 HTTP 到 HTTPS 的重定向。状态为 `ok` 后再承接 S3 流量；其他状态见[健康检查与指标](../operations/health-metrics.md#健康检查)。

启用 Admin HTTPS 后，`https://admin.example.com/healthz` 无需凭据即可访问，`unhealthy` 响应会列出失败的检查项。如果这些信息不能公开，改用 SSH 隧道访问本机 Admin。

在私密终端中输出初始 Admin 密码，并保存到密码管理器。用户名是 `admin`。

```bash
make docker-password
```

从其他机器访问本机 Admin 时，先转发端口，再在浏览器打开 `http://127.0.0.1:9090`：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

在仪表盘打开 **Wallet**，用 **Fund** 存入 USDFC，再点击 **Approve FWSS**。两步都完成后才能上传。

在容器内运行 Admin CLI 命令：

```bash
docker compose exec -T synaps3 synaps3 admin status
```

| 入口或数据 | 地址或位置 |
| --- | --- |
| S3 API | `http://<host>:8080`；配置 S3 TLS 后使用 HTTPS |
| 本机 Admin | `http://127.0.0.1:9090` |
| 公网 Admin | `https://admin.example.com`（仅 HTTPS 模式） |
| SynapS3 运行数据 | Docker 卷 `synaps3-data` |
| PostgreSQL 数据 | Docker 卷 `synaps3-postgres-data`（仅托管数据库） |
| 数据库密码 | `.env` 和 `.postgres-admin-password` |
| Caddy 状态 | Docker 卷 `synaps3-caddy-data` 和 `synaps3-caddy-config`（仅 HTTPS 模式） |

## S3 传输安全

Caddy 只服务仪表盘和 Admin API。生产 S3 流量要么用 `SYNAPS3_SERVER_TLS_ENABLED=true`、`SYNAPS3_SERVER_TLS_CERT_FILE` 和 `SYNAPS3_SERVER_TLS_KEY_FILE` 启用原生 TLS，要么放在你自己的 TLS 代理之后。证书和私钥路径必须在容器内存在，通常以只读方式挂载。

## 日常运维

```bash
make docker-status
make docker-logs
make docker-logs DOCKER_SERVICE=synaps3
make docker-logs DOCKER_SERVICE=postgres
make docker-logs DOCKER_SERVICE=caddy DOCKER_LOG_FOLLOW=1 # 仅 HTTPS 模式
make docker-down
```

`docker-logs` 输出最近 100 行。`docker-down` 删除容器，保留 `.env`、`.postgres-admin-password` 和所有卷。

密码不会自动轮换。要修改应用密码，先在 PostgreSQL 中修改 `synaps3` 角色的密码，再更新 `.env` 中的 `POSTGRES_APP_PASSWORD`，然后运行 `make docker-up`。要修改 PostgreSQL 端口，编辑 `.env` 中的 `POSTGRES_PORT` 并运行 `make docker-up`。

PostgreSQL 只在数据卷为空时创建数据库和 `synaps3` 角色。如果首次启动失败，查看 `make docker-logs DOCKER_SERVICE=postgres`，修复原因后删除 `synaps3-postgres-data` 卷，再运行 `make docker-up`。

## HTTPS 故障排查

`make docker-verify` 报告 Admin HTTPS 未就绪时：

1. 检查 A 记录和可选的 AAAA 记录。
2. 确认 80 和 443 端口没有被其他服务占用。
3. 检查主机防火墙、云防火墙和 CAA 记录。
4. 查看 Caddy 日志：

```bash
make docker-logs DOCKER_SERVICE=caddy
```

修复原因后 Caddy 会自动重试。不要删除 `synaps3-caddy-data`，其中保存证书私钥和 ACME 账户。

## 备份 Docker 数据

1. 检查健康状态，然后停止 SynapS3，保持 PostgreSQL 运行：

   ```bash
   make docker-verify
   docker compose stop synaps3
   ```

2. 导出托管数据库，打包 `synaps3-data` 卷（配置和缓存），并记录校验和：

   ```bash
   docker compose exec -T postgres pg_dump --username=synaps3 --dbname=synaps3 --format=custom > synaps3-db.dump
   docker run --rm \
     -v synaps3-data:/data:ro \
     -v "$PWD":/backup \
     alpine:3 \
     tar czf /backup/synaps3-data.tgz -C /data .
   docker run --rm \
     -v "$PWD":/backup \
     alpine:3 \
     sh -c 'cd /backup && tar tzf synaps3-data.tgz >/dev/null && sha256sum synaps3-db.dump synaps3-data.tgz > synaps3-backup.sha256 && sha256sum -c synaps3-backup.sha256'
   ```

3. 把 `.env` 和 `.postgres-admin-password` 与备份放在一起。
4. 重启并验证：

   ```bash
   make docker-up
   make docker-verify
   ```

使用 `DATABASE_SOURCE=external` 时，用该数据库服务器自己的工具备份；上面的 `pg_dump` 命令只适用于托管数据库。Caddy 卷不属于恢复点，但应保留，以免重新签发证书。

恢复时，从备份的 `.env`、`.postgres-admin-password` 和空卷开始，先恢复数据库，再恢复运行数据：

```bash
docker compose up -d --wait postgres
docker compose exec -T postgres pg_restore --username=synaps3 --dbname=synaps3 --no-owner --exit-on-error < synaps3-db.dump
docker run --rm \
  -v synaps3-data:/data \
  -v "$PWD":/backup \
  alpine:3 \
  tar xzf /backup/synaps3-data.tgz -C /data
make docker-up
make docker-verify
```

一致性要求见[运行数据](../configuration/runtime-data.md)。

## 升级

先完成备份。使用已发布镜像的部署运行：

```bash
git pull --ff-only
docker compose pull
make docker-up
make docker-verify
```

从当前代码构建镜像的部署跳过 `docker compose pull`，`make docker-up` 会重新构建镜像。已发布镜像使用可移动的 `edge` 标签，升级没有自动回滚。恢复正常流量前，完成[生产环境检查清单](../operations/production-checklist.md)。
