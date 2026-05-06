import assert from 'node:assert/strict'
import test from 'node:test'
import { QueryClient } from '@tanstack/react-query'

import type { BucketDetail, ObjectProvenance, ProviderIdentity } from '../src/api/client.ts'
import { applyProviderIdentityEventData } from '../src/lib/provider-identity-events.ts'

const identity: ProviderIdentity = {
  registry_provider_id: '101',
  name: 'alpha-pdp',
  filecoin_actor_id: 'f01234',
}

test('provider identity events patch bucket detail cache', () => {
  const qc = new QueryClient()
  qc.setQueryData<BucketDetail>(['bucket', 'photos'], {
    id: 1,
    name: 'photos',
    owner_access_key: null,
    status: 'active',
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
      identity,
    })
  )

  const bucket = qc.getQueryData<BucketDetail>(['bucket', 'photos'])
  assert.equal(bucket?.data_sets[0]?.provider_identity?.name, 'alpha-pdp')
  assert.equal(bucket?.data_sets[1]?.provider_identity, undefined)
})

test('provider identity events patch provenance cache', () => {
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
        role: 'primary',
        is_new_data_set: true,
      },
      {
        copy_index: 1,
        status: 'failed',
        provider_id: '202',
        role: 'secondary',
        is_new_data_set: false,
      },
    ],
    failures: [
      {
        attempt_index: 0,
        provider_id: '101',
        role: 'secondary',
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
      identity,
    })
  )

  const provenance = qc.getQueryData<ObjectProvenance>(['objectProvenance', 'photos', 'v1'])
  assert.equal(provenance?.copies[0]?.provider_identity?.name, 'alpha-pdp')
  assert.equal(provenance?.copies[1]?.provider_identity, undefined)
  assert.equal(provenance?.failures[0]?.provider_identity?.filecoin_actor_id, 'f01234')
})

test('provider identity events ignore unrelated providers', () => {
  const qc = new QueryClient()
  qc.setQueryData<BucketDetail>(['bucket', 'photos'], {
    id: 1,
    name: 'photos',
    owner_access_key: null,
    status: 'active',
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
      identity,
    })
  )

  assert.equal(qc.getQueryData<BucketDetail>(['bucket', 'photos']), before)
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
