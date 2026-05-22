import type { CopyHealthInfo, CopyHealthStatus, CopyHealthSummary } from '@/api/client'
import type { StatusTone } from '@/components/app/StatusBadge'
import { formatNumber, timeAgo } from './utils.ts'

export function copyHealthSummaryLabel(health: CopyHealthSummary) {
  if (health.status === 'unknown') return 'Not verified'
  if (health.status === 'available' && !health.stale) return 'Healthy'
  return 'Needs attention'
}

export function copyHealthStatusLabel(status: CopyHealthStatus) {
  switch (status) {
    case 'available':
      return 'Healthy'
    case 'degraded':
    case 'unavailable':
      return 'Needs attention'
    case 'unknown':
      return 'Not verified'
  }
}

export function copyHealthSummaryTitle(health: CopyHealthSummary) {
  if (health.status === 'available' && !health.stale && health.unhealthy_objects === 0) {
    return 'All requested object copies are readable'
  }

  const parts: string[] = []
  if (health.unhealthy_objects > 0) {
    parts.push(`${formatNumber(health.unhealthy_objects)} objects need attention`)
  }
  if (health.requested_copies > health.readable_copies) {
    parts.push(`${formatNumber(health.requested_copies - health.readable_copies)} object copies are not readable`)
  }
  if (health.pending_copies > 0) parts.push(`${formatNumber(health.pending_copies)} pending`)
  if (health.failed_copies > 0) parts.push(`${formatNumber(health.failed_copies)} failed`)
  if (health.unknown_copies > 0) parts.push(`${formatNumber(health.unknown_copies)} unverified`)
  parts.push(...copyHealthReasonLabels(health.reason_codes))
  if (health.stale) parts.push('Storage observation is stale')
  if (health.last_checked_at) parts.push(`Checked ${timeAgo(health.last_checked_at)}`)
  if (health.last_error) parts.push(`Last error: ${health.last_error}`)

  return parts.join(' · ') || copyHealthStatusLabel(health.status)
}

export function copyHealthInfoTitle(health: CopyHealthInfo) {
  if (health.status === 'available' && !health.stale) {
    return 'Replica is readable'
  }

  const parts = copyHealthReasonLabels(health.reason_codes)
  if (health.stale) parts.push('Storage observation is stale')
  if (health.last_checked_at) parts.push(`Checked ${timeAgo(health.last_checked_at)}`)
  if (health.last_error) parts.push(`Last error: ${health.last_error}`)

  return parts.join(' · ') || copyHealthStatusLabel(health.status)
}

export function copyHealthStatusTone(health: Pick<CopyHealthInfo, 'status' | 'stale'>): StatusTone {
  if (health.status === 'available' && health.stale) return 'warning'

  switch (health.status) {
    case 'available':
      return 'success'
    case 'degraded':
      return 'warning'
    case 'unavailable':
      return 'danger'
    case 'unknown':
      return 'neutral'
  }
}

function copyHealthReasonLabels(reasons: string[]) {
  return Array.from(new Set(reasons.map(copyHealthReasonLabel).filter(Boolean)))
}

function copyHealthReasonLabel(reason: string) {
  switch (reason) {
    case 'copy_under_replicated':
      return 'Fewer readable replicas than requested'
    case 'copy_pending':
      return 'Replica is waiting to start'
    case 'copy_committing':
      return 'Replica is registering storage'
    case 'copy_failed':
      return 'Replica failed'
    case 'copy_missing_provider':
      return 'Provider evidence is missing'
    case 'copy_missing_data_set':
      return 'Data set evidence is missing'
    case 'copy_missing_piece':
      return 'Piece evidence is missing'
    case 'copy_missing_retrieval_url':
      return 'Retrieval URL is missing'
    case 'copy_observation_missing':
      return 'Storage observation is missing'
    default:
      return reason.replace(/_/g, ' ')
  }
}
