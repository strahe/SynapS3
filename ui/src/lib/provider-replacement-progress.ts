import type { ProviderReplacementProgress } from '@/api/client'

export interface ProviderReplacementProgressView {
  indeterminate: boolean
  value: number | null
  summary: string
  activity: string
}

export function providerReplacementProgressView(
  progress: ProviderReplacementProgress,
  statusMessage?: string
): ProviderReplacementProgressView {
  const preparing = progress.phase === 'prepare'
  const discovering = !preparing && !progress.seeding_complete
  const indeterminate = preparing || discovering
  const total = progress.items_total ?? 0
  const processed = progress.items_processed ?? 0
  const percent = progress.percent ?? 0
  const summary = preparing
    ? 'Preparing the new storage service'
    : discovering
      ? `Discovering stored content · ${total} found · ${processed} processed`
      : `${processed} of ${total} processed${progress.percent === undefined ? '' : ` · ${percent}%`}`
  const parts: string[] = []
  if (progress.items_active > 0) parts.push(`${progress.items_active} copying`)
  if (progress.items_retrying > 0) parts.push(`${progress.items_retrying} retrying`)
  if (progress.items_waiting_source > 0) parts.push(`${progress.items_waiting_source} waiting for source`)
  if (progress.items_failed > 0) parts.push(`${progress.items_failed} need attention`)
  if (parts.length === 0 && progress.items_pending > 0) parts.push(`${progress.items_pending} queued`)
  if (parts.length === 0 && progress.items_processed === progress.items_total) {
    parts.push(`${progress.items_copied} copied`)
    if (progress.items_no_longer_needed > 0) parts.push(`${progress.items_no_longer_needed} no longer needed`)
  }
  return {
    indeterminate,
    value: indeterminate ? null : percent,
    summary,
    activity: statusMessage?.trim() || parts.join(' · '),
  }
}
