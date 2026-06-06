---
title: S3 兼容性
description: SynapS3 支持的 bucket、object、versioning 和 multipart S3 操作及限制。
---

# S3 兼容性

SynapS3 主要支持 path-style S3 访问，负责把 bucket 和 object 数据写入 Filecoin。

## 客户端要求

- 使用 path-style addressing。
- 将客户端端点设置为 SynapS3 S3 API，通常是 `http://localhost:8080`。
- 使用 `synaps3 admin s3-user create` 创建的凭据。
- 测试 Filecoin 存储路径时，使用至少 127 字节的对象。

## 操作矩阵

| 领域 | 操作 | 状态 | 说明 |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | 支持 | 创建 bucket。 |
| Bucket | `HeadBucket` | 支持 | 检查 bucket 元数据。 |
| Bucket | `ListBuckets` | 支持 | 列出有效 bucket。 |
| Bucket | `DeleteBucket` | 不支持 | SynapS3 生命周期不包含 bucket 删除。 |
| Bucket | `GetBucketVersioning` | 支持 | Bucket 始终按 versioning-enabled 处理。 |
| Bucket | `PutBucketVersioning` | 部分支持 | 接受 `Enabled`，拒绝 `Suspended`。 |
| Object | `PutObject` | 支持 | 按缓存优先的写入模型存储对象。 |
| Object | `GetObject` | 支持 | 从缓存或已提交的远端存储读取。 |
| Object | `HeadObject` | 支持 | 读取对象元数据。 |
| Object | `DeleteObject` | 支持 | 创建 delete marker，或删除指定 `versionId`。 |
| Object | `DeleteObjects` | 支持 | 创建 delete markers，或删除指定 `versionId` 条目。 |
| Object | `CopyObject` | 支持 | 源对象必须可从缓存或已提交的远端存储读取。 |
| Object | `ListObjects` | 支持 | Marker 分页。 |
| Object | `ListObjectsV2` | 支持 | Continuation-token 分页。 |
| Object | `ListObjectVersions` | 支持 | 列出对象版本和 delete markers。 |
| Object | `GetObjectAttributes` | 支持 | 返回元数据和 multipart `ObjectParts`；不返回 `TotalPartsCount`。 |
| Multipart | `CreateMultipartUpload` | 支持 | 开始上传。 |
| Multipart | `UploadPart` | 支持 | 上传单个 part。 |
| Multipart | `UploadPartCopy` | 部分支持 | 仅支持整对象复制，不支持 range copy。 |
| Multipart | `CompleteMultipartUpload` | 支持 | 组装 parts。 |
| Multipart | `AbortMultipartUpload` | 支持 | 取消上传。 |
| Multipart | `ListMultipartUploads` | 支持 | 列出未完成上传。 |
| Multipart | `ListParts` | 支持 | 列出已上传 parts。 |

## 版本控制行为

Bucket 按 versioning-enabled 处理。普通 object delete 会创建 delete marker。带 `versionId` 的 delete 会删除指定版本。Version listing 会返回对象版本和 delete markers。

## 有意不支持

- 删除 bucket。
- 暂停 bucket versioning。
- `GetObjectAttributes.ObjectParts` 中的 `TotalPartsCount`；当前 VersityGW 响应类型不包含该字段。
- `UploadPartCopy` 的 multipart range copy。
- 多个 SynapS3 节点之间的分布式协调。

## 验证

AWS CLI、rclone 和 MinIO Client 示例见 [S3 客户端](../getting-started/s3-clients.md)。
