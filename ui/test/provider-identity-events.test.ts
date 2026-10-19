import assert from 'node:assert/strict'
import test from 'node:test'
import { QueryClient } from '@tanstack/react-query'

import type { BucketDetail, ObjectProvenance } from '../src/api/client.ts'
import { applyProviderIdentityEventData } from '../src/lib/provider-identity-events.ts'

test('provider identity events invalidate matching bucket detail', () => {
  const qc = new QueryClient()
  qc.setQueryData<BucketDetail>(['bucket', 'photos'], {
    id: 1,
    name: 'photos',
    owner_access_key: null,
    status: 'ready',
    object_count: 1,
    total_size_bytes: 1,
    created_at: '2026-05-06T00:00:00Z',
    updated_at: '2026-05-06T00:00:00Z',
    versioning_status: 'disabled',
    versioning_enforced: false,
    data_sets: [dataSet('101'), dataSet('202')],
  })

  applyProviderIdentityEventData(
    qc,
    JSON.stringify({
      seq: 1,
      topic: 'provider_identity_updated',
      provider_id: '101',
    })
  )

  assert.equal(qc.getQueryState(['bucket', 'photos'])?.isInvalidated, true)
  assert.equal(qc.getQueryData<BucketDetail>(['bucket', 'photos'])?.data_sets[0]?.provider_identity, undefined)
})

test('provider identity events invalidate matching provenance', () => {
  const qc = new QueryClient()
  qc.setQueryData<ObjectProvenance>(['objectProvenance', 'photos', 'v1'], {
    version_id: 'v1',
    state: 'stored',
    status: 'success',
    requested_copies: 2,
    success_copies: 1,
    copies: [
      {
        copy_index: 0,
        status: 'committed',
        provider_id: '101',
        data_set_id: '1001',
        piece_id: '2001',
        transfer_method: 'ingress',
        is_new_data_set: true,
      },
      {
        copy_index: 1,
        status: 'failed',
        provider_id: '202',
        transfer_method: 'peer_pull',
        is_new_data_set: false,
      },
    ],
    updated_at: '2026-05-06T00:00:00Z',
  })

  applyProviderIdentityEventData(
    qc,
    JSON.stringify({
      seq: 1,
      topic: 'provider_identity_updated',
      provider_id: '101',
    })
  )

  assert.equal(qc.getQueryState(['objectProvenance', 'photos', 'v1'])?.isInvalidated, true)
  assert.equal(
    qc.getQueryData<ObjectProvenance>(['objectProvenance', 'photos', 'v1'])?.copies[0]?.provider_identity,
    undefined
  )
})

test('provider identity events ignore unrelated providers', () => {
  const qc = new QueryClient()
  qc.setQueryData<BucketDetail>(['bucket', 'photos'], {
    id: 1,
    name: 'photos',
    owner_access_key: null,
    status: 'ready',
    object_count: 1,
    total_size_bytes: 1,
    created_at: '2026-05-06T00:00:00Z',
    updated_at: '2026-05-06T00:00:00Z',
    versioning_status: 'disabled',
    versioning_enforced: false,
    data_sets: [dataSet('202')],
  })

  const before = qc.getQueryData<BucketDetail>(['bucket', 'photos'])
  applyProviderIdentityEventData(
    qc,
    JSON.stringify({
      seq: 1,
      topic: 'provider_identity_updated',
      provider_id: '101',
    })
  )

  assert.equal(qc.getQueryData<BucketDetail>(['bucket', 'photos']), before)
  assert.equal(qc.getQueryState(['bucket', 'photos'])?.isInvalidated, false)
})

test('provider identity events refresh providers referenced only by replacement history', () => {
  for (const side of ['source', 'target'] as const) {
    const qc = new QueryClient()
    qc.setQueryData(['bucket', 'photos'], {
      data_sets: [dataSet('202')],
      replacements: [
        {
          source: { provider_id: side === 'source' ? '101' : '303' },
          target: { provider_id: side === 'target' ? '101' : '303' },
        },
      ],
    })

    applyProviderIdentityEventData(qc, JSON.stringify({ topic: 'provider_identity_updated', provider_id: '101' }))

    assert.equal(qc.getQueryState(['bucket', 'photos'])?.isInvalidated, true, side)
  }
})

test('provider identity events refresh task and batch references while profiles are missing', () => {
  const providerID = '184467440737095516160'
  const qc = new QueryClient()
  const cases: [readonly unknown[], unknown][] = [
    [['tasks', 'work'], { tasks: [{ subject_type: 'provider', subject_key: providerID }] }],
    [['tasks', 'history'], { tasks: [{ storage_confirmation: { provider_id: providerID } }] }],
    [['task', 1], { task: { subject_type: 'provider', subject_key: providerID } }],
    [['task', 2], { task: { storage_confirmation: { provider_id: providerID } } }],
    [['taskEvents', 3], { events: [{ provider_selection: { rejections: [{ provider_id: providerID }] } }] }],
    [['taskSubjectInfo', 'provider', providerID], { provider: { id: providerID } }],
    [['taskSubjectInfo', 'storage_copy', '4'], { provider: { id: providerID } }],
    [['taskSubjectInfo', 'storage_replacement', '5'], { source_provider: { id: providerID } }],
    [['taskSubjectInfo', 'storage_replacement', '6'], { target_provider: { id: providerID } }],
    [['commitBatch', 'request-1'], { provider_id: providerID }],
    [['commitBatch', 'request-2'], { provider_id: '202', task: { storage_confirmation: { provider_id: providerID } } }],
  ]
  for (const [key, data] of cases) qc.setQueryData(key, data)
  const unrelated: [readonly unknown[], unknown][] = [
    [['tasks', 'other'], { tasks: [{ subject_type: 'provider', subject_key: '202' }] }],
    [['task', 7], { task: { subject_type: 'storage_data_set', subject_key: providerID } }],
    [['taskEvents', 8], { events: [{ provider_selection: { rejections: [{ provider_id: '202' }] } }] }],
    [['taskSubjectInfo', 'provider', '202'], { provider: { id: '202' } }],
    [['commitBatch', 'request-3'], { provider_id: '202' }],
  ]
  for (const [key, data] of unrelated) qc.setQueryData(key, data)

  applyProviderIdentityEventData(qc, JSON.stringify({ topic: 'provider_identity_updated', provider_id: providerID }))

  for (const [key] of cases) assert.equal(qc.getQueryState(key)?.isInvalidated, true, JSON.stringify(key))
  for (const [key] of unrelated) assert.equal(qc.getQueryState(key)?.isInvalidated, false, JSON.stringify(key))
})

test('provider identity events invalidate only matching replacement choices', () => {
  const qc = new QueryClient()
  qc.setQueryData(['replacement-providers', 'photos', 1], { providers: [{ provider_id: '101' }] })
  qc.setQueryData(['replacement-providers', 'other', 2], { providers: [{ provider_id: '202' }] })

  applyProviderIdentityEventData(qc, JSON.stringify({ topic: 'provider_identity_updated', provider_id: '101' }))

  assert.equal(qc.getQueryState(['replacement-providers', 'photos', 1])?.isInvalidated, true)
  assert.equal(qc.getQueryState(['replacement-providers', 'other', 2])?.isInvalidated, false)
})

function dataSet(providerID: string) {
  return {
    id: Number(providerID),
    bucket_id: 1,
    copy_index: 0,
    provider_id: providerID,
    status: 'ready',
    created_at: '2026-05-06T00:00:00Z',
    updated_at: '2026-05-06T00:00:00Z',
  }
}
