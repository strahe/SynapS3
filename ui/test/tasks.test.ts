import assert from 'node:assert/strict'
import test from 'node:test'
import type { TaskItem, TaskPolicy } from '../src/api/client.ts'
import { APIError } from '../src/api/client.ts'
import {
  taskDetailsView,
  taskRetryErrorMessage,
  taskRetryPolicyView,
  taskStatusesForScope,
  taskSubjectFields,
  taskSubjectLabel,
  taskTook,
} from '../src/lib/tasks.ts'

test('Retry explains a rejected recovery without exposing server diagnostics', () => {
  assert.equal(taskRetryErrorMessage(new APIError('Object has been deleted.', 409)), 'Object has been deleted.')
  for (const error of [new APIError('private database diagnostics', 500), new Error('fetch failed'), null]) {
    assert.equal(taskRetryErrorMessage(error), 'Could not retry. Refresh and try again.')
  }
})

test('retry policy distinguishes stopped tasks from available automatic retries and counts scheduled retries', () => {
  const policy: TaskPolicy = {
    max_attempts: 6,
    initial_delay: '1s',
    maximum_delay: '1m',
    multiplier: 2,
    jitter: 0,
    invocation_timeout: '1m',
    observation_window: '0s',
  }
  const task = {
    status: 'running' as TaskItem['status'],
    retry_count: 0,
    available_at: '2026-10-11T01:00:00Z',
  }
  for (const [status, retry_count, failure_reason, expected] of [
    ['pending', 0, undefined, '5 remaining'],
    ['running', 2, undefined, '3 remaining'],
    ['failed', 0, 'provider_unavailable', 'Stopped'],
    ['failed', 5, 'attempts_exhausted', 'Limit reached'],
    ['completed', 2, undefined, 'Finished'],
    ['cancelled', 1, undefined, 'Cancelled'],
  ] as const) {
    const view = taskRetryPolicyView({ ...task, status, retry_count, failure_reason }, policy)
    assert.equal(view.attemptLimit, '6')
    assert.equal(view.retriesScheduled, `${retry_count} / 5`)
    assert.equal(view.status, expected)
    assert.equal(view.nextRetryAt, undefined)
  }
  const pending = { ...task, status: 'pending' as const, retry_count: 1, failure_reason: 'provider_unavailable' }
  assert.equal(taskRetryPolicyView(pending, policy).nextRetryAt, task.available_at)
  assert.equal(taskRetryPolicyView({ ...pending, wait_reason: 'storage_pending' }, policy).nextRetryAt, undefined)
  assert.equal(taskRetryPolicyView({ ...pending, retry_count: 0 }, policy).nextRetryAt, undefined)
  assert.equal(taskRetryPolicyView(task, { ...policy, max_attempts: 1 }).status, 'Disabled')
  assert.deepEqual(taskRetryPolicyView(task, { ...policy, max_attempts: null }), {
    attemptLimit: 'Unknown',
    retriesScheduled: 'Unknown',
    status: 'Unknown',
    nextRetryAt: undefined,
  })
  assert.equal(taskRetryPolicyView(task, policy).retriesScheduled, '0 / 5')
})

test('Details shows current progress and keeps earlier errors separate', () => {
  const last_error = 'Previous request failed'
  for (const status of ['pending', 'running', 'completed', 'cancelled'] as const) {
    assert.deepEqual(taskDetailsView({ status, last_error, status_message: 'Checking storage transfer' }), {
      value: 'Checking storage transfer',
      label: 'Task details',
      lastError: last_error,
    })
    assert.equal(taskDetailsView({ status, last_error }).value, undefined)
  }
  assert.deepEqual(taskDetailsView({ status: 'failed', last_error, status_message: 'Earlier progress' }), {
    value: last_error,
    label: 'Task error',
    lastError: undefined,
  })
  assert.equal(taskDetailsView({ status: 'pending', last_error, status_message: last_error }).lastError, undefined)
})

test('Took measures completed work from first start and rejects incomplete or invalid times', () => {
  const started_at = '2026-10-03T00:00:00.900Z'
  for (const [seconds, expected] of [
    [0, '<1s'],
    [0.2, '<1s'],
    [8, '8s'],
    [60, '1m'],
    [133, '2m 13s'],
    [3600, '1h'],
    [3840, '1h 4m'],
    [90000, '1d 1h'],
  ] as const) {
    const finished_at = new Date(Date.parse(started_at) + seconds * 1000).toISOString()
    for (const status of ['completed', 'failed', 'cancelled'] as const) {
      assert.equal(taskTook({ status, started_at, finished_at }), expected)
    }
  }
  for (const status of ['pending', 'running'] as const) {
    assert.equal(taskTook({ status, started_at, finished_at: '2026-10-04T00:00:00Z' }), '—')
  }
  for (const finished_at of [undefined, 'invalid', '2026-10-02T00:00:00Z']) {
    assert.equal(taskTook({ status: 'failed', started_at, finished_at }), '—')
  }
  assert.equal(taskTook({ status: 'completed', finished_at: started_at }), '—')
})

test('Subject preserves identity, one-based replicas, provenance, and exact wallet amounts', () => {
  assert.equal(taskSubjectLabel({}), '—')
  assert.equal(taskSubjectLabel({ subject_type: 'system' }), 'System')
  assert.equal(taskSubjectLabel({ subject_type: 'storage_data_set', subject_key: '23' }), 'Data set #23')
  assert.equal(taskSubjectLabel({ subject_type: 'storage_copy', subject_key: '447' }), 'Replica #447')
  assert.equal(
    taskSubjectLabel({ subject_type: 'provider', subject_key: '184467440737095516160' }),
    'Provider #184467440737095516160'
  )
  const fields = taskSubjectFields({
    subject_type: 'storage_copy',
    subject_key: '447',
    bucket: 'files',
    copy_index: 0,
    size: 1024,
    file: { key: 'a/long/object/path.txt', source: 'deleted', other_versions: 3 },
    provider: { id: '701', name: 'North' },
  })
  assert.equal(fields.length, 4)
  assert.equal(fields[0]?.value, 'a/long/object/path.txt')
  assert.equal(fields[0]?.note, 'Deleted · 3 other linked versions')
  assert.equal(fields[3]?.label, 'Replica / Provider')
  assert.equal(fields[3]?.value, 'Replica 1 · North (#701)')
  const dataset = taskSubjectFields({
    subject_type: 'storage_data_set',
    subject_key: '23',
    local_data_set_id: 23,
    data_set_id: '801',
  })
  assert.equal(dataset[3]?.label, 'On-chain data set ID')
  assert.equal(dataset[3]?.value, '801')
  const registration = taskSubjectFields({
    subject_type: 'storage_commit_request',
    subject_key: 'request:a',
    local_data_set_id: 23,
    data_set_id: '801',
    content_count: 0,
  })
  assert.equal(registration[1]?.value, 'Local #23')
  assert.equal(registration[1]?.note, 'On-chain #801')
  assert.equal(registration[3]?.value, '0')
  const wallet = taskSubjectFields({
    subject_type: 'wallet_operation',
    subject_key: '5',
    wallet: { operation: 'fund', amount: '123456789012345678901' },
  })
  assert.equal(wallet[0]?.value, 'Fund account')
  assert.equal(wallet[1]?.value, '123.45678901 USDFC')
  assert.deepEqual(
    taskSubjectFields({ subject_type: 'wallet_operation', subject_key: '6', wallet: { operation: 'approve' } }),
    [
      { label: 'Operation', value: 'Approve FWSS' },
      { label: 'Authorized service', value: 'FWSS' },
    ]
  )
})

test('work and history offer only their available outcomes', () => {
  assert.deepEqual(taskStatusesForScope('work'), ['pending', 'running', 'failed'])
  assert.deepEqual(taskStatusesForScope('history'), ['completed', 'failed', 'cancelled'])
})
