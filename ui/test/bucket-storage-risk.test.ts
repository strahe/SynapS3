import assert from 'node:assert/strict'
import test from 'node:test'

import {
  dataSetNeedsStorageRiskReview,
  dataSetStorageImpactLabel,
  dataSetStorageImpactTone,
} from '../src/lib/bucket-storage-risk.ts'

test('data set risk action is shown only for storage attention states', () => {
  assert.equal(
    dataSetNeedsStorageRiskReview({
      status: 'ready',
      referenced_version_count: 2,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    false
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({
      status: 'ready',
      referenced_version_count: 2,
      storage_health: undefined,
    }),
    true
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({
      status: 'ready',
      referenced_version_count: 2,
      storage_health: { status: 'available', stale: true, reason_codes: [] },
    }),
    true
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({
      status: 'ready',
      referenced_version_count: 2,
      storage_health: { status: 'degraded', reason_codes: [] },
    }),
    true
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({
      status: 'draining',
      referenced_version_count: 2,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    false
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({
      status: 'unavailable',
      referenced_version_count: 2,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    true
  )
  assert.equal(
    dataSetNeedsStorageRiskReview({
      status: 'unavailable',
      referenced_version_count: 0,
      storage_health: { status: 'unavailable', reason_codes: [] },
    }),
    false
  )
})

test('data set storage impact labels distinguish healthy, attention, and cleanup states', () => {
  assert.equal(
    dataSetStorageImpactLabel({
      status: 'ready',
      current_version_count: 1,
      referenced_version_count: 2,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    'Used by current versions'
  )
  assert.equal(
    dataSetStorageImpactLabel({
      status: 'ready',
      current_version_count: 0,
      referenced_version_count: 2,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    'Used by retained versions'
  )
  assert.equal(
    dataSetStorageImpactLabel({
      status: 'unavailable',
      current_version_count: 1,
      referenced_version_count: 2,
      storage_health: { status: 'unavailable', reason_codes: [] },
    }),
    'Affects current versions'
  )
  assert.equal(
    dataSetStorageImpactLabel({
      status: 'unavailable',
      current_version_count: 0,
      referenced_version_count: 2,
      storage_health: { status: 'unavailable', reason_codes: [] },
    }),
    'Affects retained versions'
  )
  assert.equal(
    dataSetStorageImpactLabel({
      status: 'unavailable',
      current_version_count: 0,
      referenced_version_count: 0,
      storage_health: { status: 'unavailable', reason_codes: [] },
    }),
    'No versions used'
  )
  assert.equal(
    dataSetStorageImpactLabel({
      status: 'draining',
      current_version_count: 0,
      referenced_version_count: 0,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    'Cleanup/retired'
  )
  assert.equal(
    dataSetStorageImpactLabel({
      status: 'retired',
      current_version_count: 0,
      referenced_version_count: 0,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    'Cleanup/retired'
  )
})

test('data set storage impact tone highlights only retained versions needing review', () => {
  assert.equal(
    dataSetStorageImpactTone({
      status: 'ready',
      current_version_count: 1,
      referenced_version_count: 2,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    'neutral'
  )
  assert.equal(
    dataSetStorageImpactTone({
      status: 'ready',
      current_version_count: 0,
      referenced_version_count: 2,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    'neutral'
  )
  assert.equal(
    dataSetStorageImpactTone({
      status: 'unavailable',
      current_version_count: 1,
      referenced_version_count: 2,
      storage_health: { status: 'unavailable', reason_codes: [] },
    }),
    'danger'
  )
  assert.equal(
    dataSetStorageImpactTone({
      status: 'unavailable',
      current_version_count: 0,
      referenced_version_count: 2,
      storage_health: { status: 'unavailable', reason_codes: [] },
    }),
    'warning'
  )
  assert.equal(
    dataSetStorageImpactTone({
      status: 'unavailable',
      current_version_count: 0,
      referenced_version_count: 0,
      storage_health: { status: 'unavailable', reason_codes: [] },
    }),
    'neutral'
  )
  assert.equal(
    dataSetStorageImpactTone({
      status: 'retired',
      current_version_count: 0,
      referenced_version_count: 0,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    'info'
  )
  assert.equal(
    dataSetStorageImpactTone({
      status: 'draining',
      current_version_count: 0,
      referenced_version_count: 0,
      storage_health: { status: 'available', reason_codes: [] },
    }),
    'info'
  )
})
