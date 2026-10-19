---
title: 快速开始
description: 检查新部署的 SynapS3 节点，创建 S3 凭据，并上传第一个对象。
---

# 快速开始

先完成 [Docker 部署](./docker.md)或[源码构建](./source.md)，包括钱包配置。

## 检查节点

```bash
curl http://127.0.0.1:9090/healthz
```

返回 `{"status":"ok"}` 后再继续。返回 `setup` 或 `unhealthy` 时，见[故障排查](../operations/troubleshooting.md)。

如果节点在远程主机上且未启用 Admin HTTPS，先转发 Admin 端口，再在浏览器打开 `http://127.0.0.1:9090`：

```bash
ssh -L 9090:127.0.0.1:9090 user@server
```

## 创建 S3 用户

在仪表盘打开 **S3 users**，点击 **Create S3 user**；或者运行：

```bash
synaps3 admin s3-user create
```

Secret key 只显示一次。把它保存到权限为 `0600` 的客户端配置文件中。

## 上传第一个对象

下面的 MinIO Client 示例通过提示符读取凭据，secret key 不会进入 shell 历史：

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

`mc cat` 会输出上传的内容。对象至少 127 字节，所以示例文件补齐到 128 个字符。

SynapS3 为新存储桶准备好存储之前，上传会返回 `503 SlowDown`。存储桶就绪后重试；见 [S3 请求返回 SlowDown](../operations/troubleshooting.md#s3-请求返回-slowdown)。

按 [S3 客户端](./s3-clients.md)配置其他客户端；承接生产流量前，完成[生产环境检查清单](../operations/production-checklist.md)。
