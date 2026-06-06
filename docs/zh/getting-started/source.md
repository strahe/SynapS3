---
title: 源码构建
description: 在本地构建 SynapS3，初始化运行数据，并验证内嵌仪表盘和 S3 API。
---

# 源码构建

开发 SynapS3、定制二进制或检查内嵌仪表盘构建时，从源码构建。

普通长期运行建议使用 [Docker 部署](./docker.md)。

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

`synaps3 init` 会创建 `~/.synaps3/config.toml`、`db/`、`cache/` 和 Admin 认证。请保存命令打印出的 Admin 密码。非交互 init 可以从 `~/.synaps3/admin-initial-password` 读取密码。

把生成的钱包 private key 写入 `~/.synaps3/config.toml`：

```toml
[filecoin]
private_key = "0x..."
```

在 Calibration 测试时，为钱包充值：

```bash
./bin/synaps3 wallet fund-testnet 0x...
```

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

在另一个终端检查健康状态，并存入 USDFC：

```bash
curl http://127.0.0.1:9090/healthz
./bin/synaps3 wallet deposit 2 # 2 USDFC
```

预期结果：`/healthz` 返回 `{"status":"ok"}`，deposit 操作被接受。

## 使用 S3 客户端验证

创建 S3 用户：

```bash
export SYNAPS3_ADMIN_PASSWORD='replace-with-admin-password'
./bin/synaps3 admin s3-user create
```

然后使用 path-style S3 客户端：

```bash
printf '%*s\n' 128 'hello synaps3' > hello.txt
mc alias set synaps3 http://localhost:8080 replace-with-access-key replace-with-secret-key
mc mb synaps3/demo
mc cp hello.txt synaps3/demo/hello.txt
mc cat synaps3/demo/hello.txt
```

`mc cat` 会输出上传对象。更多客户端示例见 [S3 客户端](./s3-clients.md)。

## 常见构建问题

| 现象 | 检查项 |
| --- | --- |
| UI 构建失败 | 确认 Node.js 22.12 或更高版本，以及 pnpm 11 已安装。 |
| Go 构建因 cgo 失败 | 确认 C toolchain 已安装并在 `PATH` 中。 |
| `serve` 因 Admin 认证校验失败 | 新配置运行 `./bin/synaps3 init`；已有配置运行 `./bin/synaps3 admin-auth reset-password --config ~/.synaps3/config.toml`。 |
| `serve` 进入 setup 模式 | 设置 `filecoin.private_key` 或 `SYNAPS3_FILECOIN_PRIVATE_KEY`。 |
