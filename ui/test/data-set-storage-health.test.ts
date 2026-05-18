import assert from 'node:assert/strict'
import test from 'node:test'

import type { StorageDataSetSummary } from '../src/api/client.ts'
import {
  dataSetStorageHealthDetailParts,
  dataSetStorageHealthRefreshErrorMessage,
} from '../src/lib/data-set-storage-health.ts'

function dataSet(overrides: Partial<StorageDataSetSummary>): StorageDataSetSummary {
  return {
    id: 1,
    bucket_id: 1,
    copy_index: 0,
    provider_id: '101',
    status: 'ready',
    committed_copies: 1,
    readable_copies: 1,
    physical_bytes: 0,
    referenced_version_count: 0,
    current_version_count: 0,
    created_at: '2026-05-17T00:00:00Z',
    updated_at: '2026-05-17T00:00:00Z',
    ...overrides,
  }
}

test('data set storage health details hide ready local state', () => {
  assert.deepEqual(
    dataSetStorageHealthDetailParts(
      dataSet({
        storage_health: {
          status: 'available',
          reason_codes: [],
          active_piece_count: 18,
          last_checked_at: '9999-01-01T00:00:00Z',
          stale: false,
        },
      })
    ),
    ['18 pieces', 'just now']
  )
})

test('data set storage health details include non-ready local state', () => {
  assert.deepEqual(
    dataSetStorageHealthDetailParts(
      dataSet({
        status: 'unavailable',
        storage_health: {
          status: 'unavailable',
          reason_codes: ['chain_data_set_missing'],
          last_checked_at: '9999-01-01T00:00:00Z',
          stale: false,
        },
      })
    ),
    ['chain data set missing', 'local state: unavailable', 'just now']
  )
})

test('data set storage health details include local state without recorded state', () => {
  assert.deepEqual(dataSetStorageHealthDetailParts(dataSet({ status: 'pending' })), [
    'No state recorded',
    'local state: pending',
  ])
})

test('data set storage health details include last error only for attention statuses', () => {
  assert.deepEqual(
    dataSetStorageHealthDetailParts(
      dataSet({
        storage_health: {
          status: 'unknown',
          reason_codes: ['chain_lookup_failed'],
          last_error: 'request timed out',
          last_checked_at: '9999-01-01T00:00:00Z',
          stale: false,
        },
      })
    ),
    ['chain lookup failed', 'last error: request timed out', 'just now']
  )

  assert.deepEqual(
    dataSetStorageHealthDetailParts(
      dataSet({
        storage_health: {
          status: 'degraded',
          reason_codes: ['chain_data_set_unmanaged'],
          last_error: 'request timed out',
          last_checked_at: '9999-01-01T00:00:00Z',
          stale: false,
        },
      })
    ),
    ['chain data set unmanaged', 'just now']
  )
})

test('data set storage health refresh error message is visible', () => {
  assert.equal(dataSetStorageHealthRefreshErrorMessage(new Error('refresh failed')), 'refresh failed')
  assert.equal(dataSetStorageHealthRefreshErrorMessage('bad'), 'Failed to refresh data set storage health')
})
