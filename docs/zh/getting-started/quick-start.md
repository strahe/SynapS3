---
title: 快速开始
description: 启动 SynapS3 节点，并上传第一个对象。
---

# 快速开始

先选择安装方式：

- 使用容器运行节点：参见 [Docker 部署](./docker.md)。
- 构建并直接运行二进制：参见[源码构建](./source.md)。

完成对应页面的初始化、钱包配置和启动步骤后，再继续本页。

## 检查节点

默认 Admin 地址是 `http://127.0.0.1:9090`：

```bash
curl http://127.0.0.1:9090/healthz
```

预期返回 `{"status":"ok"}`。`setup` 表示仍缺少必要设置；`unhealthy` 表示数据库、缓存或 worker 需要处理。排查方法见[故障排查](../operations/troubleshooting.md)。

如果节点运行在远程主机，并且没有配置 Admin HTTPS，使用 SSH 隧道访问仪表盘：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

不要把 9090 端口直接绑定到公网接口。

## 创建 S3 用户

登录仪表盘，在 S3 Users 页面创建用户；也可以在安装环境中使用原生 Admin CLI：

```bash
synaps3 admin s3-user create
```

命令只显示一次 secret key。请把凭据保存到权限为 `0600` 的客户端配置中；如果泄露，立即轮换。

## 上传第一个对象

以下 MinIO Client 示例通过终端交互读取凭据，不会把 secret key 写入 shell history：

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

AWS CLI 和 rclone 示例见 [S3 客户端](./s3-clients.md)。承载正式流量前，完成[生产环境检查清单](../operations/production-checklist.md)。
