import assert from 'node:assert/strict'
import test from 'node:test'

import type { BucketStorageHealthSummary } from '../src/api/client.ts'
import {
  bucketStorageHealthAffectedVersionsLabel,
  bucketStorageHealthLabel,
  bucketStorageHealthObservationLabel,
  bucketStorageHealthStatusTone,
} from '../src/lib/bucket-storage-health.ts'

function health(overrides: Partial<BucketStorageHealthSummary>): BucketStorageHealthSummary {
  return {
    status: 'available',
    reason_codes: [],
    stale: false,
    abnormal_data_sets: 0,
    affected_versions_capped: 0,
    affected_versions_cap: 200,
    affected_versions_exceeds_cap: false,
    ...overrides,
  }
}

test('bucket storage health affected versions shows unknown on query failure', () => {
  assert.equal(
    bucketStorageHealthAffectedVersionsLabel(
      health({
        status: 'unknown',
        last_error: 'storage health query failed',
      })
    ),
    'Unknown'
  )
})

test('bucket storage health labels query failure as not verified', () => {
  const failed = health({
    status: 'unknown',
    last_error: 'storage health query failed',
  })

  assert.equal(bucketStorageHealthLabel(failed), 'Not verified')
  assert.equal(bucketStorageHealthObservationLabel(failed), 'Not verified')
})

test('bucket storage health labels stale available observations as needing refresh', () => {
  assert.equal(bucketStorageHealthLabel(health({ stale: true })), 'Needs refresh')
})

test('bucket storage health observation keeps stale priority over unknown', () => {
  assert.equal(bucketStorageHealthObservationLabel(health({ status: 'unknown', stale: true })), 'Stale')
})

test('bucket storage health stale freshness uses warning tone', () => {
  assert.equal(bucketStorageHealthStatusTone(health({ status: 'unknown', stale: true })), 'warning')
})
