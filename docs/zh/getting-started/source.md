---
title: 源码构建
description: 在本地构建 SynapS3，初始化运行数据，并验证内嵌仪表盘和 S3 API。
---

# 源码构建

开发 SynapS3、定制二进制或检查内嵌仪表盘构建时，从源码构建。

部署节点建议使用 [Docker 部署](./docker.md)。

## 前置条件

- Go 1.26.3 或更高版本。
- `make`。
- cgo 所需的 C toolchain，例如 `gcc` 或 `clang`。
- Node.js 22.12 或更高版本。
- pnpm 11。

## 构建

```bash
git clone https://github.com/strahe/SynapS3.git
cd SynapS3
make build
```

命令会构建 React 仪表盘，将其嵌入二进制，并生成 `bin/synaps3`。

## 初始化运行数据

```bash
./bin/synaps3 init
./bin/synaps3 wallet generate
```

`synaps3 init` 会创建 `~/.synaps3/config.toml`、`db/`、`cache/` 和 Admin 认证。请把命令打印出的 Admin 密码保存到密码管理器。非交互 init 可以在私密终端中从 `~/.synaps3/admin-initial-password` 读取密码。配置文件和密码文件都应保持 `0600` 权限。

把生成的钱包私钥写入 `~/.synaps3/config.toml`：

```toml
[filecoin]
private_key = "0x..."
```

不要让私钥进入 shell history。配置文件包含钱包材料，只应允许 SynapS3 运行账户读取。

在 Calibration 测试时，为钱包充值：

```bash
./bin/synaps3 wallet fund-testnet 0x...
```

Faucet 领取成功后会输出 `CalibnetUSDFC: <hash>` 和 `CalibnetFIL: <hash>`。

## 启动服务

```bash
./bin/synaps3 serve
```

默认端点：

| 端点 | 地址 |
| --- | --- |
| S3 API | `http://localhost:8080` |
| 仪表盘和 Admin API | `http://127.0.0.1:9090` |
| 运行数据 | `~/.synaps3/` |

在另一个终端检查健康状态、存入 USDFC，并批准 FWSS：

```bash
curl http://127.0.0.1:9090/healthz
./bin/synaps3 wallet deposit 2 # 2 USDFC
./bin/synaps3 wallet approve
```

预期结果：`/healthz` 返回 `{"status":"ok"}`。新的 deposit 或 approval 会输出 `Transaction: <hash>` 和 `Status: confirmed`；已经完成 approval 时会输出 `FWSS approval: already approved`。

这些 HTTP 端点用于本地评估。生产 S3 流量必须使用原生 TLS 或受控的 TLS 反向代理；Admin 端点继续使用回环地址、SSH 隧道，或带访问控制的 HTTPS 反向代理。

## 使用 S3 客户端验证

创建 S3 用户：

```bash
./bin/synaps3 admin s3-user create
```

在无回显提示中输入 Admin 密码。命令只显示一次 S3 secret key；请保存到权限为 `0600` 的客户端凭据文件，如果泄露则立即轮换。

然后使用 path-style S3 客户端：

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

`mc cat` 会输出上传对象。alias 会把凭据保存到 `~/.mc/config.json`，该文件应保持 `0600` 权限。更多客户端示例见 [S3 客户端](./s3-clients.md)。

## 常见构建问题

| 现象 | 检查项 |
| --- | --- |
| UI 构建失败 | 确认 Node.js 22.12 或更高版本，以及 pnpm 11 已安装。 |
| Go 构建因 cgo 失败 | 确认 C toolchain 已安装并在 `PATH` 中。 |
| `serve` 因 Admin 认证校验失败 | 新配置运行 `./bin/synaps3 init`；已有配置运行 `./bin/synaps3 admin-auth reset-password --config ~/.synaps3/config.toml`。 |
| `serve` 进入 setup 模式 | 设置 `filecoin.private_key` 或 `SYNAPS3_FILECOIN_PRIVATE_KEY`。 |
