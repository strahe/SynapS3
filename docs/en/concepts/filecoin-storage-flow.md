---
title: Filecoin Storage Flow
description: Learn how SynapS3 stores objects as replicas with storage providers, how replica settings affect the cache, and how to replace a storage provider.
---

# Filecoin Storage Flow

Filecoin storage starts after an S3 write succeeds. Background tasks store each object as replicas with the bucket's storage providers, one replica per storage provider.

## Bucket Preparation

When you create a bucket, SynapS3 assigns a storage provider to each replica and creates a paid data set with each one. The bucket shows **Provisioning** until every data set is ready, then **Ready**. Uploads to a bucket that is not ready fail with `503 SlowDown`. If preparation stops, the bucket shows the reason and links to the task to retry. How storage providers are chosen is described in [Provider Selection](../configuration/model.md#provider-selection).

## Object States

| State | Dashboard | Meaning |
| --- | --- | --- |
| `cached` | **Stored in cache** | Stored locally and waiting for upload. |
| `uploading` | **Uploading** | Being uploaded to a storage provider. |
| `committing` | **Submitting to Filecoin** | The storage provider has the data; its batch is waiting to be submitted or confirmed on chain. |
| `replicating` | **Syncing replicas** | At least one replica is committed, but fewer than the bucket's minimum durable replicas. |
| `stored` | **Stored on Filecoin** | The minimum durable replicas are committed and readable. Remaining replicas may still be syncing. |
| `failed` | **Needs attention** | A storage step failed. See [Failed Tasks](../operations/troubleshooting.md#failed-tasks). |

The state does not say whether the object is still in the local cache; a `stored` object may be cached or only on Filecoin.

## Upload and Replication

1. SynapS3 uploads the object from the local cache to one of the bucket's storage providers: the one with the fastest valid speed test, or, without valid tests, the first in replica order. If that upload fails before any replica is committed, the next assigned storage provider takes over the upload in the same order.
2. The replica is added to the storage provider's data set on chain in a batch. See [Batches](../configuration/model.md#batches).
3. The bucket's other storage providers copy the object from a committed replica and commit their own replicas the same way.

If an assigned storage provider becomes unreachable before its replica is stored, the other replicas continue. The unfinished replica waits without using up retries and resumes when the storage provider is reachable again. SynapS3 does not automatically replace a replica's assigned storage provider; see [Replace a Storage Provider](#replace-a-storage-provider).

Unfinished tasks resume after a restart. A task that exhausts its retries becomes `failed`; fix the cause, then retry it. See [Failed Tasks](../operations/troubleshooting.md#failed-tasks).

## Target and Minimum Replicas

Each bucket has two replica settings. Set them when you create the bucket, or later on the bucket's **Settings** tab under **Replica policy**.

| Setting | API field | Meaning |
| --- | --- | --- |
| **Replicas** | `default_copies` | Replica target, 1 to 8. A new bucket takes `filecoin.default_copies`. |
| **Release cache after** | `minimum_durable_copies` | Minimum durable replicas: how many committed, readable replicas a version needs before it is `stored` and its cache can be released. A new bucket uses the full target. |

- Each upload keeps the replica target that applied when it started. Changing **Replicas** affects new uploads only.
- **Replicas** can only be raised. Raising it creates data sets for the new replicas.
- An explicit **Release cache after** count stays unchanged when **Replicas** is raised. It never exceeds an upload's target.
- Lowering **Release cache after** lets cached versions be released sooner, while remaining replicas keep syncing. Raising it does not move `stored` versions back and cannot restore cache that was already removed.

The dashboard shows replica sync progress until every target replica of a version is stored.

## Storage Provider Health

SynapS3 checks storage providers and its data sets every `filecoin.observability.interval`. The dashboard flags replicas whose storage provider or data set fails these checks. A storage provider's details on the **Storage** page show why its last check failed.

## Replace a Storage Provider

Replace a storage provider when it is permanently unavailable or when you want to move a replica away from it. Replacement creates a new paid data set and changes where the data is stored, so SynapS3 does it only on your request.

1. Open the bucket, go to the **Storage** tab, and in **Data sets** open the replica's menu.
2. Select **Replace provider**.
3. Under **New provider**, select **Choose automatically** or **Choose a provider**, review the price list, and confirm.

One confirmation covers the whole move: SynapS3 creates the new data set, sends new uploads to it once it is ready, copies the bucket's stored data from another replica or the local cache, and then terminates the old data set. Data stays readable from the old storage provider until every retained version is readable on the new one.

A replica can be replaced when its data set is ready, or when its data set setup task has failed. While the setup task is still pending or running, for example because its storage provider is unreachable, the replica cannot be replaced.

The **Storage** tab shows the progress of each replacement and what it is waiting for: the new data set, the new storage provider, wallet funds, or a readable source. Most waits resume on their own. Content that cannot be copied after its retries, such as content with no readable replica and no local copy, is set aside while the rest continues. The replacement then stops before terminating the old data set; fix the cause and select **Retry** on the **Storage** tab. If terminating the old data set requires settling a payment, settle it, then select **Retry**. If the old storage provider never carries out the termination sent to it, the replacement stops once its retries run out. **Retry** then terminates the data set from your wallet: you pay the transaction fee, and payments for the old data set continue until its service end epoch instead of ending immediately. If the storage provider also carries out its termination later, one of the two transactions fails on chain and its fee is spent.

## Planned Replica Repair

Replica repair is planned for a future release. After a storage provider becomes unavailable, it will:

- identify stored replicas affected by the outage,
- create replacement replicas until the bucket's replica target is met again,
- show repair progress and failures that need operator action.

Replica repair is separate from completing an upload's initial replicas and from retrying failed tasks.
