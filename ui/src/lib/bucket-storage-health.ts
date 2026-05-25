import type { BucketStorageHealthSummary } from '@/api/client'
import type { StatusTone } from '@/components/app/StatusBadge'
import { storageHealthReasonLabel } from './data-set-storage-health.ts'
import { formatNumber, timeAgo } from './utils.ts'

export function bucketStorageHealthLabel(health: BucketStorageHealthSummary) {
  if (health.status === 'unknown') return 'Not verified'
  if (health.status === 'available' && health.stale) return 'Needs refresh'
  if (health.status === 'available' && !health.stale) return 'Healthy'
  return 'At risk'
}

export function bucketStorageHealthTitle(health: BucketStorageHealthSummary) {
  const parts: string[] = []
  if (health.affected_versions_capped > 0) {
    parts.push(`${bucketStorageHealthAffectedVersionsLabel(health)} retained versions affected`)
  } else if (health.abnormal_data_sets > 0) {
    parts.push(`${formatNumber(health.abnormal_data_sets)} abnormal data sets, no retained versions affected`)
  }
  if (health.abnormal_data_sets > 0 && health.affected_versions_capped > 0) {
    parts.push(`${formatNumber(health.abnormal_data_sets)} abnormal data sets`)
  }
  parts.push(...Array.from(new Set(health.reason_codes.map(storageHealthReasonLabel).filter(Boolean))))
  if (health.stale) parts.push('Storage observation is stale')
  if (health.last_checked_at) parts.push(`Checked ${timeAgo(health.last_checked_at)}`)
  if (health.last_error) parts.push(`Last error: ${health.last_error}`)

  return parts.join(' · ') || 'No retained version storage risk'
}

export function bucketStorageHealthAffectedVersionsLabel(health: BucketStorageHealthSummary) {
  if (health.last_error) return 'Unknown'

  const count = formatNumber(health.affected_versions_capped)
  return health.affected_versions_exceeds_cap ? `${count}+` : count
}

export function bucketStorageHealthObservationLabel(
  health: Pick<BucketStorageHealthSummary, 'last_checked_at' | 'stale' | 'status'>
) {
  if (health.stale) return 'Stale'
  if (health.status === 'unknown') return 'Not verified'
  if (!health.last_checked_at) return 'Not checked'
  return 'Fresh'
}

export function bucketStorageHealthStatusTone(
  health: Pick<BucketStorageHealthSummary, 'status' | 'stale'>
): StatusTone {
  if (health.status === 'unavailable') return 'danger'
  if (health.stale) return 'warning'

  switch (health.status) {
    case 'available':
      return 'success'
    case 'degraded':
      return 'warning'
    case 'unknown':
      return 'neutral'
  }
}
