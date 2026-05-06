import type { QueryClient } from '@tanstack/react-query'
import type { BucketDetail, ObjectProvenance, ProviderIdentity } from '@/api/client'

interface ProviderIdentityEventPayload {
  topic?: string
  provider_id?: string
  identity?: ProviderIdentity
}

export function applyProviderIdentityEventData(queryClient: QueryClient, raw: string) {
  let payload: ProviderIdentityEventPayload
  try {
    payload = JSON.parse(raw) as ProviderIdentityEventPayload
  } catch {
    return
  }
  if (payload.topic !== 'provider_identity_updated' || !payload.provider_id || !payload.identity) {
    return
  }

  applyProviderIdentityUpdate(queryClient, payload.provider_id, payload.identity)
}

export function applyProviderIdentityUpdate(queryClient: QueryClient, providerID: string, identity: ProviderIdentity) {
  queryClient.setQueriesData<BucketDetail>({ queryKey: ['bucket'] }, (data) =>
    patchBucketDetail(data, providerID, identity)
  )
  queryClient.setQueriesData<ObjectProvenance>({ queryKey: ['objectProvenance'] }, (data) =>
    patchObjectProvenance(data, providerID, identity)
  )
}

function patchBucketDetail(data: BucketDetail | undefined, providerID: string, identity: ProviderIdentity) {
  if (!data?.data_sets?.length) return data

  let changed = false
  const dataSets = data.data_sets.map((dataSet) => {
    if (dataSet.provider_id !== providerID) return dataSet
    changed = true
    return { ...dataSet, provider_identity: identity }
  })
  return changed ? { ...data, data_sets: dataSets } : data
}

function patchObjectProvenance(data: ObjectProvenance | undefined, providerID: string, identity: ProviderIdentity) {
  if (!data) return data

  let changed = false
  const copies = data.copies.map((copy) => {
    if (copy.provider_id !== providerID) return copy
    changed = true
    return { ...copy, provider_identity: identity }
  })
  const failures = data.failures.map((failure) => {
    if (failure.provider_id !== providerID) return failure
    changed = true
    return { ...failure, provider_identity: identity }
  })
  return changed ? { ...data, copies, failures } : data
}
