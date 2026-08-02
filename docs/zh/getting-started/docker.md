---
title: Docker 部署
description: 使用自动 Admin HTTPS 部署 SynapS3。
---

# Docker 部署

这个部署会在同一台 Linux 主机上运行 SynapS3 和 Caddy。SynapS3 的 Admin 服务保持监听 `127.0.0.1:9090`；Caddy 使用你的 HTTPS 域名发布仪表盘，并自动管理证书。

## 前置条件

- 安装了 Git、Make、curl、Docker Engine 和 Docker Compose v2.24 或更高版本的 Linux 主机。
- 为 `synaps3-data` volume 准备可靠的本地磁盘。
- 一个公网域名，例如 `admin.example.com`。
- 配置公网 DNS 和防火墙：
  - 将域名的 A 记录指向主机公网 IPv4 地址。
  - 只有在 IPv6 也能访问这台主机时才添加 AAAA 记录。启动前删除错误的 AAAA 记录。
  - 开放入站 `80/TCP` 和 `443/TCP`。需要 HTTP/3 时再开放 `443/UDP`。
  - 如果域名配置了 CAA 记录，允许 Caddy 可使用的公共 CA。
- 可充值的 Calibration 钱包，或按下面的命令生成并充值。

Caddy 默认的证书验证要求公网可以访问 80 或 443 端口。这个部署不包含 DNS 服务商插件，也不支持泛域名证书。

## 创建部署

克隆仓库并初始化受保护的环境文件：

```bash
git clone --depth 1 https://github.com/strahe/SynapS3.git
cd SynapS3
make docker-init ADMIN_DOMAIN=admin.example.com
```

`ADMIN_DOMAIN` 只填写域名，不要包含 `https://`、端口、路径或通配符。命令会创建权限为 `0600` 的 `.env`，并选择发布镜像和 Admin HTTPS 配置。

启动前检查 `.env`。始终保持 `0600` 权限，不要把真实私钥直接写在 shell 命令中，避免进入 shell history。其他 SynapS3 覆盖项见[环境变量](../configuration/environment.md)。

### 从本地源码构建镜像

需要从当前 checkout 构建镜像时，在初始化阶段选择本地镜像：

```bash
make docker-init ADMIN_DOMAIN=admin.example.com IMAGE_SOURCE=local
make docker-build
```

在 `.env` 创建前选择镜像来源。`docker-init` 不会覆盖已有文件。

## 准备 Calibration 钱包

没有现成钱包时，生成一个钱包：

```bash
make docker-wallet
```

命令会输出钱包地址和私钥。只在私密终端中运行，将私钥保存到受保护的位置，然后加入 `.env`：

```text
SYNAPS3_FILECOIN_PRIVATE_KEY=0x...
```

为生成的地址申请 Calibration 测试资产：

```bash
make docker-fund WALLET_ADDRESS=0x...
```

如果 faucet 暂时不可用，可以使用 [ChainSafe](https://forest-explorer.chainsafe.dev/faucet) 或 [Plumbline](https://faucet.reiers.io/) 手动领取。成功后会输出 `CalibnetUSDFC: <hash>` 和 `CalibnetFIL: <hash>`。

没有钱包私钥时，SynapS3 也可以用 `setup` 状态启动。发送 S3 流量前，在仪表盘中补全钱包设置。

## 启动并验证

启动部署，然后验证本机健康状态和公网 HTTPS：

```bash
make docker-up
make docker-verify
```

`docker-up` 会等待容器运行和 SynapS3 容器健康检查通过，但不代表公网证书已经签发完成。`docker-verify` 会检查本机 Admin 端点、可信 HTTPS 证书，以及 HTTP 到 HTTPS 的跳转。

健康检查可能返回三种状态：

| 状态 | 含义 |
| --- | --- |
| `setup` | HTTPS 已就绪，但仍缺少必要的 SynapS3 设置。 |
| `ok` | Admin HTTPS 已就绪，运行时健康检查通过。 |
| `unhealthy` | HTTPS 可能正常，但数据库、缓存或 worker 需要处理。 |

只有 `ok` 状态可以承载正常 S3 流量。

部署端点和数据位置：

| 界面 | 地址或位置 |
| --- | --- |
| S3 API | 单独配置 S3 TLS 前为 `http://<host>:8080` |
| 仪表盘和 Admin API | `https://admin.example.com` |
| Admin 后端 | `http://127.0.0.1:9090`，公网无法直接访问 |
| SynapS3 运行数据 | Docker volume `synaps3-data` |
| Caddy 证书和状态 | Docker volumes `synaps3-caddy-data`、`synaps3-caddy-config` |

在私密终端中读取生成的 Admin 密码：

```bash
make docker-password
```

用户名是 `admin`。将密码保存到密码管理器。容器内 Admin 命令会自动读取同一个受保护的密码文件：

```bash
make docker-admin ADMIN_ARGS='status'
```

预期上传前，在仪表盘中存入 USDFC 并批准 FWSS。

## S3 传输安全

这个部署中的 Caddy 只保护仪表盘和 Admin API，不代理 S3 API。

生产 S3 流量应配置原生 TLS：设置 `SYNAPS3_SERVER_TLS_ENABLED=true`、`SYNAPS3_SERVER_TLS_CERT_FILE` 和 `SYNAPS3_SERVER_TLS_KEY_FILE`；也可以把 S3 API 放在单独的受控 TLS 代理之后。证书和私钥路径必须在容器内可见，通常使用只读挂载。

## 运维部署

运行 `make docker-help` 查看支持的命令。常用操作：

```bash
make docker-status
make docker-logs DOCKER_SERVICE=synaps3
make docker-logs DOCKER_SERVICE=caddy DOCKER_LOG_FOLLOW=1
make docker-admin ADMIN_ARGS='task stats'
```

`make docker-stop` 会停止两个服务，但保留容器和数据。`make docker-down` 会移除容器，但保留 `.env`、SynapS3 运行数据和 Caddy 证书卷。Makefile 不提供删除 volume 的目标。

承载流量前，完成[生产环境检查清单](../operations/production-checklist.md)。

## 证书故障排查

如果 `make docker-verify` 提示 HTTPS 尚未就绪：

1. 确认 A 记录和可选的 AAAA 记录指向当前主机。
2. 确认没有其他服务占用 80 或 443 端口。
3. 确认主机防火墙和云防火墙允许入站 80/TCP 和 443/TCP。
4. 检查 CAA 记录是否限制了证书签发。
5. 查看 Caddy 证书日志：

```bash
make docker-logs DOCKER_SERVICE=caddy
```

修复 DNS 或防火墙后，Caddy 会继续重试证书签发。不要删除 `synaps3-caddy-data`，其中保存证书私钥和 ACME 账户状态。

## 升级和备份

不要归档仍在使用的数据卷。先检查健康状态和任务队列、停止部署，再按照[运行数据](../configuration/runtime-data.md)中的数据库驱动专用步骤操作。数据库与缓存必须处于同一恢复时间点。

创建并验证备份后，按照[升级与恢复](../operations/upgrade-recovery.md)升级。当前发布通道是可移动的 `edge` 镜像，因此升级不提供自动回退。
