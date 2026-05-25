import assert from 'node:assert/strict'
import test from 'node:test'

import { normalizeBucketRouteSearch } from '../src/lib/bucket-route-search.ts'

test('storage risk prefix keeps raw S3 prefix semantics', () => {
  const search = normalizeBucketRouteSearch({
    view: 'storage-risk',
    prefix: 'objects',
    marker: 'objects/page',
    version_marker: 'object-version-marker',
    risk_prefix: ' invoice ',
    risk_key: ' exact key ',
    risk_key_marker: ' risk key marker ',
    risk_version_marker: 'risk-version-marker',
    risk_created_at_marker: '2026-05-24T12:00:00.123456789Z',
    risk_stale_before: '2026-05-24T11:59:00.123456789Z',
  })

  assert.equal(search.prefix, 'objects/')
  assert.equal(search.marker, 'objects/page')
  assert.equal(search.version_marker, 'object-version-marker')
  assert.equal(search.risk_prefix, ' invoice ')
  assert.equal(normalizeBucketRouteSearch({ risk_prefix: 'foo' }).risk_prefix, 'foo')
  assert.equal(search.risk_key, ' exact key ')
  assert.equal(search.risk_key_marker, ' risk key marker ')
  assert.equal(search.risk_version_marker, 'risk-version-marker')
  assert.equal(search.risk_created_at_marker, '2026-05-24T12:00:00.123456789Z')
  assert.equal(search.risk_stale_before, '2026-05-24T11:59:00.123456789Z')
})

test('storage risk dataset only accepts positive integer strings', () => {
  assert.equal(normalizeBucketRouteSearch({ risk_dataset: '42' }).risk_dataset, '42')
  assert.equal(normalizeBucketRouteSearch({ risk_dataset: ' 42 ' }).risk_dataset, '42')
  assert.equal(normalizeBucketRouteSearch({ risk_dataset: '0' }).risk_dataset, undefined)
  assert.equal(normalizeBucketRouteSearch({ risk_dataset: '42.5' }).risk_dataset, undefined)
  assert.equal(normalizeBucketRouteSearch({ risk_dataset: 'abc' }).risk_dataset, undefined)
  assert.equal(normalizeBucketRouteSearch({ risk_dataset: ' ' }).risk_dataset, undefined)
})
