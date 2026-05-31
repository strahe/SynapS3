import type { StorageDataSetSummary } from '@/api/client'
import type { StatusTone } from '@/components/app/StatusBadge'

type DataSetStorageAttention = Pick<StorageDataSetSummary, 'status' | 'storage_health'>
type DataSetReferenceImpact = Pick<
  StorageDataSetSummary,
  'status' | 'current_version_count' | 'referenced_version_count'
>
type DataSetStorageImpact = DataSetStorageAttention & DataSetReferenceImpact

export function dataSetNeedsStorageRiskReview(
  dataSet: DataSetStorageAttention & Pick<StorageDataSetSummary, 'referenced_version_count'>
) {
  return dataSetHasStorageAttention(dataSet) && dataSet.referenced_version_count > 0
}

export function dataSetStorageImpactLabel(dataSet: DataSetStorageImpact) {
  const needsReview = dataSetNeedsStorageRiskReview(dataSet)
  if (dataSet.current_version_count > 0) {
    return needsReview ? 'Affects current versions' : 'Used by current versions'
  }
  if (dataSet.referenced_version_count > 0) {
    return needsReview ? 'Affects retained versions' : 'Used by retained versions'
  }
  if (dataSet.status === 'draining' || dataSet.status === 'retired') return 'Cleanup/retired'
  return 'No versions used'
}

export function dataSetStorageImpactTone(dataSet: DataSetStorageImpact): StatusTone {
  if (!dataSetNeedsStorageRiskReview(dataSet)) {
    if (
      dataSet.current_version_count === 0 &&
      dataSet.referenced_version_count === 0 &&
      (dataSet.status === 'draining' || dataSet.status === 'retired')
    ) {
      return 'info'
    }
    return 'neutral'
  }
  return dataSet.current_version_count > 0 ? 'danger' : 'warning'
}

function dataSetHasStorageAttention(dataSet: DataSetStorageAttention) {
  if (dataSet.status !== 'ready' && dataSet.status !== 'draining') return true
  const health = dataSet.storage_health
  if (!health) return true
  if (health.stale) return true
  return health.status === 'degraded' || health.status === 'unavailable' || health.status === 'unknown'
}
