import type { StorageDataSetSummary } from '@/api/client'

export function dataSetNeedsStorageRiskReview(dataSet: Pick<StorageDataSetSummary, 'status' | 'storage_health'>) {
  if (dataSet.status !== 'ready' && dataSet.status !== 'draining') return true
  const health = dataSet.storage_health
  if (!health) return true
  if (health.stale) return true
  return health.status === 'degraded' || health.status === 'unavailable' || health.status === 'unknown'
}
