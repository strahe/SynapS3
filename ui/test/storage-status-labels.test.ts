import assert from 'node:assert/strict'
import test from 'node:test'

import {
  objectStateLabel,
  replicaLabel,
  storageCleanupCopyStatusLabel,
  storageCleanupCopyStatusTone,
  storageCleanupStatusLabel,
  taskHasByteTransfer,
  taskOperationLabel,
  taskOperationOptionLabel,
  taskReplicaLabel,
  taskTypeLabel,
  transferMethodLabel,
  uploadStatusLabel,
} from '../src/lib/storage-status-labels.ts'

test('object upload labels describe Filecoin lifecycle in user-facing terms', () => {
  assert.equal(uploadStatusLabel('running'), 'Preparing Filecoin storage')
  assert.equal(uploadStatusLabel('running', 56), 'Uploading to Filecoin 56%')
  assert.equal(uploadStatusLabel('ingress_ready'), 'Registering storage record on-chain')
  assert.equal(uploadStatusLabel('readable'), 'Available, syncing replicas')
  assert.equal(uploadStatusLabel('complete'), 'Stored on Filecoin · On-chain verified')
  assert.equal(uploadStatusLabel('failed'), 'Needs attention')
  assert.equal(uploadStatusLabel('rejected'), 'Upload rejected')
  assert.equal(uploadStatusLabel('superseded'), 'Replaced by newer version')
})

test('object state labels prefer active upload lifecycle when present', () => {
  assert.equal(objectStateLabel('uploading', 'uploading', 'running', 56), 'Uploading to Filecoin 56%')
  assert.equal(objectStateLabel('stored', 'success'), 'Stored')
})

test('task labels use product-facing task and operation names', () => {
  assert.equal(taskTypeLabel('upload'), 'Upload')
  assert.equal(taskTypeLabel('evict_cache'), 'Evict Cache')
  assert.equal(taskTypeLabel('storage_cleanup'), 'Replica Cleanup')
  assert.equal(taskOperationOptionLabel('ensure_dataset'), 'Prepare replica target')
  assert.equal(taskOperationOptionLabel('peer_pull'), 'Sync peer replica')
  assert.equal(taskOperationLabel({ type: 'evict_cache' }), 'Evict local cache')
  assert.equal(taskOperationLabel({ type: 'storage_cleanup' }), 'Delete remote replicas')
  assert.equal(taskOperationLabel({ type: 'upload', stage: '' }), 'Upload object')
  assert.equal(
    taskOperationLabel({ type: 'upload', stage: 'peer_commit', copy_index: 1 }),
    'Register peer replica on-chain'
  )
})

test('replica and transfer labels hide zero-based storage internals', () => {
  assert.equal(replicaLabel(0), 'Replica 1')
  assert.equal(replicaLabel(1), 'Replica 2')
  assert.equal(taskReplicaLabel({ copy_index: 1 }), 'Replica 2')
  assert.equal(taskReplicaLabel({}), '—')
  assert.equal(transferMethodLabel('ingress'), 'Ingress upload')
  assert.equal(transferMethodLabel('peer_pull'), 'Peer sync')
  assert.equal(taskHasByteTransfer({ type: 'upload', stage: 'ingress_store' }), true)
  assert.equal(taskHasByteTransfer({ type: 'upload', stage: '' }), true)
  assert.equal(taskHasByteTransfer({ type: 'upload', stage: 'peer_pull' }), false)
  assert.equal(taskHasByteTransfer({ type: 'evict_cache' }), false)
  assert.equal(taskHasByteTransfer({ type: 'storage_cleanup' }), false)
})

test('replica cleanup status labels describe user-visible cleanup state', () => {
  assert.equal(storageCleanupStatusLabel([{ status: 'pending' }]), 'Waiting to delete replicas')
  assert.equal(storageCleanupStatusLabel([{ status: 'delete_scheduled' }]), 'Replica deletion scheduled')
  assert.equal(storageCleanupStatusLabel([{ status: 'removed' }]), 'Remote replicas deleted')
  assert.equal(storageCleanupStatusLabel([{ status: 'unsupported' }, { status: 'failed' }]), 'Needs attention')
  assert.equal(storageCleanupStatusLabel([]), 'No remote replicas to delete')
})

test('replica cleanup copy status labels expose each cleanup state', () => {
  assert.equal(storageCleanupCopyStatusLabel('pending'), 'Waiting')
  assert.equal(storageCleanupCopyStatusLabel('delete_scheduled'), 'Scheduled')
  assert.equal(storageCleanupCopyStatusLabel('removed'), 'Removed')
  assert.equal(storageCleanupCopyStatusLabel('failed'), 'Failed')
  assert.equal(storageCleanupCopyStatusLabel('unsupported'), 'Unsupported')
})

test('replica cleanup copy status tones expose each cleanup state', () => {
  assert.equal(storageCleanupCopyStatusTone('pending'), 'neutral')
  assert.equal(storageCleanupCopyStatusTone('delete_scheduled'), 'info')
  assert.equal(storageCleanupCopyStatusTone('removed'), 'success')
  assert.equal(storageCleanupCopyStatusTone('failed'), 'danger')
  assert.equal(storageCleanupCopyStatusTone('unsupported'), 'danger')
})
