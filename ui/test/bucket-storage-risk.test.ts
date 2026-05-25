import assert from 'node:assert/strict'
import test from 'node:test'

import { dataSetNeedsStorageRiskReview } from '../src/lib/bucket-storage-risk.ts'

test('data set risk action is shown only for storage attention states', () => {
  assert.equal(
    dataSetNeedsStorageRiskReview({ status: 'ready', storage_health: { status: 'available', reason_codes: [] } }),
    false
  )
  assert.equal(dataSetNeedsStorageRiskReview({ status: 'ready', storage_health: undefined }), true)
  assert.equal(
    dataSetNeedsStorageRiskReview({
      status: 'ready',
      storage_health: { status: 'available', stale: true, reason_codes: [] },
    }),
    true
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({ status: 'ready', storage_health: { status: 'degraded', reason_codes: [] } }),
    true
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({ status: 'draining', storage_health: { status: 'available', reason_codes: [] } }),
    false
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({ status: 'unavailable', storage_health: { status: 'available', reason_codes: [] } }),
    true
  )
})
