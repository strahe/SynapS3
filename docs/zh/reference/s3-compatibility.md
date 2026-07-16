---
title: S3 兼容性
description: SynapS3 支持的存储桶、对象、版本控制和分段上传 S3 操作及限制。
---

# S3 兼容性

SynapS3 主要支持 path-style S3 访问，负责把存储桶和对象数据写入 Filecoin。

## 客户端要求

- 使用 path-style addressing。
- 本地评估时，将客户端端点设置为 `http://localhost:8080`。生产 S3 流量必须使用原生 TLS 或受控的 TLS 反向代理。
- 使用 `synaps3 admin s3-user create` 创建的凭据。
- 对象和对象键必须符合下面的稳定限制。

## 稳定限制

- 对象大小：`127` 到 `1,065,353,216` 字节。
- 对象键：必须是有效 UTF-8、不得包含 NUL，并且最多 `1024` 字节。
- 分段上传：最多 `10,000` 个 parts；完整对象仍受对象大小上限约束。

## 操作矩阵

| 领域 | 操作 | 状态 | 说明 |
| --- | --- | --- | --- |
| 存储桶 | `CreateBucket` | 支持 | 创建存储桶。 |
| 存储桶 | `HeadBucket` | 支持 | 检查存储桶元数据。 |
| 存储桶 | `ListBuckets` | 支持 | 列出有效存储桶。 |
| 存储桶 | `DeleteBucket` | 不支持 | 返回 `501`；SynapS3 生命周期不包含删除存储桶。 |
| 存储桶 | `GetBucketVersioning` | 支持 | 存储桶始终按 versioning-enabled 处理。 |
| 存储桶 | `PutBucketVersioning` | 部分支持 | 接受 `Enabled`，拒绝 `Suspended`。 |
| 存储桶 ACL | `GetBucketAcl` | 支持 | 返回已保存的存储桶 ACL。 |
| 存储桶 ACL | `PutBucketAcl` | 支持 | 保存用于访问控制的存储桶 owner 和 ACL 变更。 |
| 对象 ACL | `GetObjectAcl`, `PutObjectAcl` | 不支持 | 不要依赖对象级 ACL 状态或授权变更。 |
| 存储桶 Policy | `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy` | 不支持 | 不保存或执行存储桶 Policy。 |
| 存储桶 Tagging | `GetBucketTagging`, `PutBucketTagging`, `DeleteBucketTagging` | 不支持 | 不保存存储桶 tags。 |
| 对象 Tagging | `GetObjectTagging`, `PutObjectTagging`, `DeleteObjectTagging` | 不支持 | 不保存对象 tags。 |
| Ownership Controls | `GetBucketOwnershipControls` | 部分支持 | 返回 `BucketOwnerPreferred`。 |
| Ownership Controls | `PutBucketOwnershipControls` | 部分支持 | 只接受 `BucketOwnerPreferred`，拒绝其他 ownership modes。 |
| Ownership Controls | `DeleteBucketOwnershipControls` | 部分支持 | 保持 ACL 兼容的 `BucketOwnerPreferred` 行为。 |
| 对象 | `PutObject` | 支持 | 按缓存优先的写入模型存储对象。 |
| 对象 | `GetObject` | 支持 | 从缓存或已提交的远端存储读取。 |
| 对象 | `HeadObject` | 支持 | 读取对象元数据。 |
| 对象 | `DeleteObject` | 支持 | 创建 delete marker，或删除指定 `versionId`。 |
| 对象 | `DeleteObjects` | 支持 | 创建 delete markers，或删除指定 `versionId` 条目。 |
| 对象 | `CopyObject` | 支持 | 源对象必须可从缓存或已提交的远端存储读取。 |
| 对象 | `ListObjects` | 支持 | Marker 分页。 |
| 对象 | `ListObjectsV2` | 支持 | Continuation-token 分页。 |
| 对象 | `ListObjectVersions` | 支持 | 列出对象版本和 delete markers。 |
| 对象 | `GetObjectAttributes` | 部分支持 | 返回元数据和 multipart `ObjectParts`，但不包含 `TotalPartsCount`。 |
| 分段上传 | `CreateMultipartUpload` | 支持 | 开始上传。 |
| 分段上传 | `UploadPart` | 支持 | 上传单个 part。 |
| 分段上传 | `UploadPartCopy` | 部分支持 | 仅支持整对象复制，不支持 range copy。 |
| 分段上传 | `CompleteMultipartUpload` | 支持 | 组装 parts。 |
| 分段上传 | `AbortMultipartUpload` | 支持 | 取消上传。 |
| 分段上传 | `ListMultipartUploads` | 支持 | 列出未完成上传。 |
| 分段上传 | `ListParts` | 支持 | 列出已上传 parts。 |

## 版本控制行为

存储桶按 versioning-enabled 处理。普通对象删除会创建 delete marker。带 `versionId` 的删除会删除指定版本。Version listing 会返回对象版本和 delete markers。

## 有意不支持

- 删除存储桶。
- 暂停存储桶 versioning。
- 对象 ACL 授权、存储桶 Policy，以及存储桶或对象 Tagging。
- `BucketOwnerPreferred` 之外的 Ownership Controls 模式。
- `GetObjectAttributes.ObjectParts` 中的 `TotalPartsCount`。
- `UploadPartCopy` 的 multipart range copy。
- 多个 SynapS3 节点之间的分布式协调。

## 验证

AWS CLI、rclone 和 MinIO Client 示例见 [S3 客户端](../getting-started/s3-clients.md)。
