import assert from 'node:assert/strict'
import test from 'node:test'

import {
  objectStateLabel,
  replicaLabel,
  storageCleanupCopyStatusLabel,
  storageCleanupCopyStatusTone,
  storageCleanupStatusLabel,
  taskReplicaLabel,
  transferMethodLabel,
} from '../src/lib/storage-status-labels.ts'

test('object state labels describe the derived storage position in user-facing terms', () => {
  assert.equal(objectStateLabel('cached', 'uploading'), 'Stored in cache')
  assert.equal(objectStateLabel('uploading', 'uploading'), 'Uploading')
  assert.equal(objectStateLabel('uploading', 'uploading', 56), 'Uploading to Filecoin 56%')
  assert.equal(objectStateLabel('committing', 'syncing'), 'Registering storage record on-chain')
  assert.equal(objectStateLabel('replicating', 'syncing'), 'Syncing replicas')
  assert.equal(objectStateLabel('stored', 'success'), 'Stored on Filecoin')
  assert.equal(objectStateLabel('failed', 'warning'), 'Needs attention')
})

test('replica and transfer labels hide zero-based storage internals', () => {
  assert.equal(replicaLabel(0), 'Replica 1')
  assert.equal(replicaLabel(1), 'Replica 2')
  assert.equal(taskReplicaLabel({ copy_index: 1 }), 'Replica 2')
  assert.equal(taskReplicaLabel({}), '—')
  assert.equal(transferMethodLabel('ingress'), 'Ingress upload')
  assert.equal(transferMethodLabel('peer_pull'), 'Peer sync')
  assert.equal(transferMethodLabel('cache_restore'), 'From local cache')
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
