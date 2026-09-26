import type { QueryClient } from '@tanstack/react-query'
import type { BucketDetail, ObjectProvenance, ReplacementProviderCandidate } from '@/api/client'

interface ProviderIdentityEventPayload {
  topic?: string
  provider_id?: string
}

export function applyProviderIdentityEventData(queryClient: QueryClient, raw: string) {
  let payload: ProviderIdentityEventPayload
  try {
    payload = JSON.parse(raw) as ProviderIdentityEventPayload
  } catch {
    return
  }
  if (payload.topic !== 'provider_identity_updated' || !payload.provider_id) {
    return
  }

  const providerID = payload.provider_id
  queryClient.invalidateQueries({
    predicate: (query) => {
      const family = query.queryKey[0]
      if (family === 'bucket') {
        const bucket = query.state.data as BucketDetail | undefined
        return bucket?.data_sets?.some((dataSet) => dataSet.provider_id === providerID) ?? false
      }
      if (family === 'objectProvenance') {
        const provenance = query.state.data as ObjectProvenance | undefined
        return provenance?.copies?.some((copy) => copy.provider_id === providerID) ?? false
      }
      if (family === 'replacement-providers') {
        const result = query.state.data as { providers?: ReplacementProviderCandidate[] } | undefined
        return result?.providers?.some((provider) => provider.provider_id === providerID) ?? false
      }
      return false
    },
  })
}
