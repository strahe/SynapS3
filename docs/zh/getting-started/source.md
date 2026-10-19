---
title: 源码构建
description: 构建 SynapS3 二进制，准备 PostgreSQL 和钱包，并启动节点。
---

# 源码构建

本页构建 `synaps3` 二进制并直接运行。容器部署见 [Docker 部署](./docker.md)。

## 前置条件

- Go 1.27.2 或更高版本。
- `make`。
- 用于 cgo 的 C 工具链，例如 `gcc` 或 `clang`。
- Node.js 22.12 或更高版本，以及 pnpm 11。
- PostgreSQL 18，或者用于本地运行 PostgreSQL 的 Docker。

## 构建

```bash
git clone https://github.com/strahe/SynapS3.git
cd SynapS3
make build
```

`make build` 构建仪表盘并嵌入二进制，输出 `bin/synaps3`。

## 准备 PostgreSQL

数据库应由非超级用户角色拥有。下面用 Docker 在本地运行 PostgreSQL，数据库和角色都叫 `synaps3`：

```bash
printf 'PostgreSQL administrator password: '
read -rs POSTGRES_PASSWORD
printf '\n'
printf 'SynapS3 database password: '
read -rs POSTGRES_APP_PASSWORD
printf '\n'
export POSTGRES_PASSWORD POSTGRES_APP_PASSWORD
docker run -d --name synaps3-postgres --restart unless-stopped \
  -e POSTGRES_DB=synaps3 -e POSTGRES_PASSWORD -e POSTGRES_APP_PASSWORD \
  -p 127.0.0.1:5432:5432 -v synaps3-postgres-local:/var/lib/postgresql \
  -v "$PWD/docker/postgres-init.sql:/docker-entrypoint-initdb.d/synaps3.sql:ro" \
  postgres:18
unset POSTGRES_PASSWORD POSTGRES_APP_PASSWORD
```

PostgreSQL 只在数据卷为空时创建角色和密码。

## 初始化运行数据

```bash
./bin/synaps3 init
./bin/synaps3 wallet generate
```

`synaps3 init` 创建 `~/.synaps3/config.toml`、`cache/` 目录和 Admin 凭据。在终端中运行时，它只输出一次 Admin 密码；否则把密码写入 `~/.synaps3/admin-initial-password`。把密码保存到密码管理器。

把数据库连接 URL 和钱包私钥写入 `~/.synaps3/config.toml`：

```toml
[database]
dsn = "postgres://synaps3:PASSWORD@127.0.0.1:5432/synaps3?sslmode=disable"

[filecoin]
private_key = "0x..."
```

该文件包含凭据和钱包密钥。权限保持 `0600`，只允许运行 SynapS3 的账户读取。

在 Calibration 上领取测试 FIL 和 USDFC：

```bash
./bin/synaps3 wallet fund-testnet 0x...
```

领取成功时输出 `CalibnetUSDFC: <hash>` 和 `CalibnetFIL: <hash>`。

## 启动服务

```bash
./bin/synaps3 serve
```

| 入口 | 默认值 |
| --- | --- |
| S3 API | `http://localhost:8080` |
| 仪表盘和 Admin API | `http://127.0.0.1:9090` |
| 运行数据 | `~/.synaps3/` |

在另一个终端中检查健康状态，存入 USDFC，并批准 FWSS：

```bash
curl http://127.0.0.1:9090/healthz
./bin/synaps3 wallet deposit 2 # 2 USDFC
./bin/synaps3 wallet approve
```

健康检查返回 `{"status":"ok"}`。每笔交易输出 `Transaction: <hash>` 和 `Status: confirmed`；FWSS 已批准时，`approve` 输出 `FWSS approval: already approved`。

明文 HTTP 只用于本地评估。生产环境见[网络暴露](../operations/production-checklist.md#网络暴露)。

然后按[快速开始](./quick-start.md)创建 S3 凭据并上传第一个对象。

## 常见构建和启动问题

| 现象 | 处理 |
| --- | --- |
| 仪表盘构建失败 | 安装 Node.js 22.12 或更高版本和 pnpm 11。 |
| Go 构建在 cgo 处失败 | 安装 C 工具链，并确保它在 `PATH` 中。 |
| `serve` 报告 Admin 认证校验失败 | `config.toml` 存在时运行 `./bin/synaps3 admin-auth reset-password --config ~/.synaps3/config.toml`；否则运行 `./bin/synaps3 init`。 |
| `serve` 报告 `database.dsn is not set` | 在配置文件中设置 `database.dsn`，或设置 `SYNAPS3_DATABASE_DSN`。 |
| `/healthz` 返回 `setup` | 在配置文件中设置 `filecoin.private_key`，或设置 `SYNAPS3_FILECOIN_PRIVATE_KEY`，然后重启。 |
