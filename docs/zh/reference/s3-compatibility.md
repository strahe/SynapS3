---
title: S3 兼容性
description: 查询 SynapS3 支持哪些 S3 操作、对象限制，以及版本控制和删除行为。
---

# S3 兼容性

## 客户端要求

- 使用 path-style 寻址。
- 使用在 **S3 users** 页面或通过 `synaps3 admin s3-user create` 创建的凭据。
- 生产流量使用 TLS，原生 TLS 或你自己的代理均可。

## 稳定限制

- 对象大小：127 到 1,065,353,216 字节。过小或过大的对象会被拒绝，返回 `EntityTooSmall` 或 `EntityTooLarge`。
- 对象键：合法 UTF-8，不含 NUL 字符，最长 1,024 字节。
- 分段上传：最多 10,000 个分段。完成后的对象同样受对象大小限制。

## 操作矩阵

| 类别 | 操作 | 状态 | 说明 |
| --- | --- | --- | --- |
| Bucket | `CreateBucket` | 支持 | |
| Bucket | `HeadBucket` | 支持 | |
| Bucket | `ListBuckets` | 支持 | |
| Bucket | `DeleteBucket` | 不支持 | 返回 `501 NotImplemented`。 |
| Bucket | `GetBucketVersioning` | 支持 | 存储桶始终启用版本控制。 |
| Bucket | `PutBucketVersioning` | 部分支持 | 接受 `Enabled`，拒绝 `Suspended`。 |
| Bucket ACL | `GetBucketAcl` | 支持 | |
| Bucket ACL | `PutBucketAcl` | 支持 | 授权会生效，见[访问控制](#访问控制)。 |
| Object ACL | `GetObjectAcl`, `PutObjectAcl` | 不支持 | |
| Bucket policy | `GetBucketPolicy`, `PutBucketPolicy`, `DeleteBucketPolicy` | 不支持 | 不保存也不执行策略。 |
| Bucket tagging | `GetBucketTagging`, `PutBucketTagging`, `DeleteBucketTagging` | 不支持 | |
| Object tagging | `GetObjectTagging`, `PutObjectTagging`, `DeleteObjectTagging` | 不支持 | |
| Ownership controls | `GetBucketOwnershipControls` | 部分支持 | 返回 `BucketOwnerPreferred`。 |
| Ownership controls | `PutBucketOwnershipControls` | 部分支持 | 只接受 `BucketOwnerPreferred`。 |
| Ownership controls | `DeleteBucketOwnershipControls` | 部分支持 | 存储桶保持 `BucketOwnerPreferred`。 |
| Object | `PutObject` | 支持 | |
| Object | `GetObject` | 支持 | 支持单个字节范围。对未缓存的对象做 Range 读取时，会从存储提供方下载整个对象。 |
| Object | `HeadObject` | 支持 | 返回所请求版本的自定义元数据。 |
| Object | `DeleteObject` | 支持 | 见[版本控制与删除](#版本控制与删除)。 |
| Object | `DeleteObjects` | 支持 | 1 到 1,000 个键。按键报告失败；`Quiet=true` 时不列出成功的键。 |
| Object | `CopyObject` | 支持 | 源对象必须在缓存中，或有已提交的副本。 |
| Object | `ListObjects` | 支持 | Marker 分页。 |
| Object | `ListObjectsV2` | 支持 | Continuation token 分页。 |
| Object | `ListObjectVersions` | 支持 | 列出版本和删除标记。 |
| Object | `GetObjectAttributes` | 部分支持 | 对分段上传的对象，`ObjectParts` 不含 `TotalPartsCount`，也不返回整体 `Checksum`。 |
| Multipart | `CreateMultipartUpload` | 支持 | |
| Multipart | `UploadPart` | 支持 | |
| Multipart | `UploadPartCopy` | 部分支持 | 只支持复制整个对象；指定源范围时返回 `NotImplemented`。 |
| Multipart | `CompleteMultipartUpload` | 支持 | |
| Multipart | `AbortMultipartUpload` | 支持 | |
| Multipart | `ListMultipartUploads` | 支持 | |
| Multipart | `ListParts` | 支持 | |

不支持服务端加密；使用客户提供密钥（SSE-C）的请求返回 `501 NotImplemented`。

## 访问控制

Bucket ACL 授权允许其他 S3 用户按所授权限读取、写入和列出数据。即使是 `FULL_CONTROL`，也不允许 `GetObjectAttributes`、删除指定版本、`ListParts`、`AbortMultipartUpload` 或修改存储桶配置；这些操作需要存储桶所有者，或 `admin` 角色的 S3 用户。拥有写权限的用户可以创建并完成分段上传，但列出分段或中止上传需要所有者或 admin。

## 版本控制与删除

存储桶始终保留版本。

- 不带 `versionId` 的 `DeleteObject` 会添加删除标记。
- 带 `versionId` 的 `DeleteObject` 立即删除该版本或删除标记，否则直接失败；不会排队稍后执行。
- 数据版本的存储工作仍在进行，或已提交的 Filecoin 交易等待确认时，删除该版本会失败并返回 `400 InvalidRequest`。从未提交过交易的失败存储工作不会阻止删除；存储工作也从不阻止删除删除标记。在 `DeleteObjects` 中，该错误只影响对应的键。
- 在 `DeleteObjects` 中，如果某个键解析到存储桶之外（例如 `../file.txt`），整个请求会在删除任何对象前被拒绝。

数据版本删除后，SynapS3 会移除不再被其他版本使用的远端副本，并请求各存储提供方删除其副本。不支持删除的存储提供方会保留其副本。移除完成前（通常几分钟内），向该存储桶上传相同字节会返回 `503 SlowDown`；之后再上传，会作为新数据保存。

客户端配置示例见 [S3 客户端](../getting-started/s3-clients.md)。
