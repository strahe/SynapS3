import assert from 'node:assert/strict'
import test from 'node:test'

import { attentionDisplayRows, overviewPipelineRows, workerHealthRows } from '../src/lib/overview.ts'

test('worker health rows use stable product labels and ordering', () => {
  assert.deepEqual(workerHealthRows({ wallet_operations: true, unknown_worker: false, uploader: true }), [
    { key: 'uploader', label: 'Upload', healthy: true },
    { key: 'wallet_operations', label: 'Wallet Operations', healthy: true },
    { key: 'unknown_worker', label: 'Unknown Worker', healthy: false },
  ])
})

test('attention rows stay quiet when nothing needs attention', () => {
  const rows = attentionDisplayRows({
    objects: { needs_attention: 0, unavailable: 0 },
    tasks: { failed: 0, exhausted: 0 },
  })

  assert.deepEqual(rows, [])
})

test('attention rows show only nonzero attention items', () => {
  const rows = attentionDisplayRows({
    objects: { needs_attention: 2, unavailable: 1 },
    tasks: { failed: 3, exhausted: 4 },
  })

  assert.deepEqual(rows, [
    { key: 'object_failures', label: 'Object failures', value: 2, tone: 'warning', target: 'buckets' },
    { key: 'unavailable', label: 'Unavailable objects', value: 1, tone: 'danger', target: 'buckets' },
    { key: 'failed_tasks', label: 'Failed tasks', value: 3, tone: 'danger', target: 'tasks', taskStatus: 'failed' },
    {
      key: 'exhausted_tasks',
      label: 'Retry limit reached',
      value: 4,
      tone: 'danger',
      target: 'tasks',
      taskStatus: 'exhausted',
    },
  ])
})

test('pipeline rows keep fixed order and active status breakdown', () => {
  const rows = overviewPipelineRows([
    { pipeline: 'sync', total: 5, by_status: { queued: 2, waiting: 3 } },
    { pipeline: 'upload', total: 1, by_status: { running: 1 } },
  ])

  assert.deepEqual(rows, [
    { key: 'prepare', label: 'Prepare', total: 0, queued: 0, scheduled: 0, waiting: 0, running: 0 },
    { key: 'upload', label: 'Upload', total: 1, queued: 0, scheduled: 0, waiting: 0, running: 1 },
    { key: 'commit', label: 'Commit', total: 0, queued: 0, scheduled: 0, waiting: 0, running: 0 },
    { key: 'sync', label: 'Sync', total: 5, queued: 2, scheduled: 0, waiting: 3, running: 0 },
    { key: 'evict', label: 'Evict', total: 0, queued: 0, scheduled: 0, waiting: 0, running: 0 },
    { key: 'cleanup', label: 'Cleanup', total: 0, queued: 0, scheduled: 0, waiting: 0, running: 0 },
  ])
})
