---
title: S3 兼容性
description: SynapS3 支持的 bucket、object、versioning 和 multipart S3 操作及限制。
---

# S3 兼容性

SynapS3 聚焦在把对象存储到 Filecoin 所需的 path-style S3 bucket 和 object 工作流。

## 客户端要求

- 使用 path-style addressing。
- 将客户端 endpoint 指向 SynapS3 S3 端点，通常是 `http://localhost:8080`。
- 使用 `synaps3 admin s3-user create` 创建的凭据。
- 测试 Filecoin 存储路径时，使用至少 127 字节的对象。

## 操作矩阵

| 领域 | 操作 | 状态 | 说明 |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | 支持 | 创建 bucket。 |
| Bucket | `HeadBucket` | 支持 | 检查 bucket 元数据。 |
| Bucket | `ListBuckets` | 支持 | 列出 active buckets。 |
| Bucket | `DeleteBucket` | 不支持 | SynapS3 生命周期不包含 bucket 删除。 |
| Bucket | `GetBucketVersioning` | 支持 | Bucket 始终启用 versioning。 |
| Bucket | `PutBucketVersioning` | 部分支持 | 接受 `Enabled`，拒绝 `Suspended`。 |
| Object | `PutObject` | 支持 | 通过 cache-first durability 存储对象。 |
| Object | `GetObject` | 支持 | 从 cache 或已提交 provider storage 读取。 |
| Object | `HeadObject` | 支持 | 读取对象元数据。 |
| Object | `DeleteObject` | 支持 | 软删除单个对象。 |
| Object | `DeleteObjects` | 支持 | 软删除多个对象。 |
| Object | `CopyObject` | 支持 | 源对象必须可从 cache 或已提交 provider storage 读取。 |
| Object | `ListObjects` | 支持 | Marker 分页。 |
| Object | `ListObjectsV2` | 支持 | Continuation-token 分页。 |
| Object | `ListObjectVersions` | 支持 | 列出对象版本和 delete markers。 |
| Object | `GetObjectAttributes` | 支持 | 返回 ETag、checksum、size 和 storage class。 |
| Multipart | `CreateMultipartUpload` | 支持 | 开始上传。 |
| Multipart | `UploadPart` | 支持 | 上传单个 part。 |
| Multipart | `UploadPartCopy` | 部分支持 | 仅支持整对象复制，不支持 range copy。 |
| Multipart | `CompleteMultipartUpload` | 支持 | 组装 parts。 |
| Multipart | `AbortMultipartUpload` | 支持 | 取消上传。 |
| Multipart | `ListMultipartUploads` | 支持 | 列出未完成上传。 |
| Multipart | `ListParts` | 支持 | 列出已上传 parts。 |

## Versioning 行为

Bucket 表现为 versioning-enabled。Object delete 会创建 delete marker，version listing 会暴露对象版本和 delete markers。

## 有意不支持

- Bucket deletion。
- 暂停 bucket versioning。
- `UploadPartCopy` 的 multipart range copy。
- 多个 SynapS3 节点之间的分布式协调。

## 验证

AWS CLI、rclone 和 MinIO Client 示例见 [S3 客户端](../getting-started/s3-clients.md)。
