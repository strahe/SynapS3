---
title: 源码构建
description: 在本地构建 SynapS3，初始化运行数据，并验证内嵌 Dashboard 和 S3 API。
---

# 源码构建

当你要开发 SynapS3、需要自定义二进制，或想检查内嵌 Dashboard 构建时，从源码构建。

普通长期运行建议使用 [Docker 部署](./docker.md)。

## 前置条件

- Go 1.26.3 或更高版本。
- `make`。
- 用于 cgo 的 C toolchain，例如 `gcc` 或 `clang`。
- Node.js 22.12 或更高版本。
- pnpm 11。

## 构建

```bash
git clone https://github.com/strahe/SynapS3.git
cd SynapS3
make build
```

预期结果：命令先构建 React dashboard，然后将其嵌入并写出 `bin/synaps3`。

## 初始化运行数据

```bash
./bin/synaps3 init
./bin/synaps3 wallet generate
```

预期结果：创建 `~/.synaps3/config.toml`、`db/` 和 `cache/`。把生成的 private key 写入 `~/.synaps3/config.toml`：

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
| Dashboard 和 Admin API | `http://127.0.0.1:9090` |
| 运行数据 | `~/.synaps3/` |

在另一个终端检查 health 并 deposit USDFC：

```bash
curl http://127.0.0.1:9090/healthz
./bin/synaps3 wallet deposit 2 # 2 USDFC
```

预期结果：health 返回 `{"status":"ok"}`，deposit operation 被接受。

## 使用 S3 客户端验证

创建 S3 用户：

```bash
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

预期结果：`mc cat` 输出上传对象。更多客户端示例见 [S3 客户端](./s3-clients.md)。

## 常见构建问题

| 现象 | 检查项 |
| --- | --- |
| UI 构建失败 | 确认 Node.js 22.12 或更高版本，以及 pnpm 11 已安装。 |
| Go 构建因 cgo 失败 | 确认 C toolchain 已安装并在 `PATH` 中。 |
| Serve 进入 setup mode | 设置 `filecoin.private_key` 或 `SYNAPS3_FILECOIN_PRIVATE_KEY`。 |
