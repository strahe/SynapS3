import { Link } from '@tanstack/react-router'
import type { ObservabilitySignal, ObservabilityStatus, ProviderUploadSpeedTest } from '@/api/client'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import {
  providerUploadSampleSize,
  providerUploadSpeedLabel,
  providerUploadSpeedShortLabel,
} from '@/lib/provider-upload-speed'
import { storageConfirmationTasksSearch } from '@/lib/storage-confirmation-attention'
import { observabilitySignalDetails, observabilityStatusLabel, observabilityStatusTone } from '@/lib/storage-topology'
import { timeAgo } from '@/lib/utils'

/** A health badge that explains itself on hover when the signal carries reasons. */
export function TopologySignalBadge({ status, signal }: { status: ObservabilityStatus; signal?: ObservabilitySignal }) {
  const badge = <StatusBadge tone={observabilityStatusTone(status)}>{observabilityStatusLabel(status)}</StatusBadge>
  const details = observabilitySignalDetails(signal)
  if (details.length === 0) return badge
  return (
    <Tooltip>
      <TooltipTrigger asChild>{badge}</TooltipTrigger>
      <TooltipContent className="max-w-72">{details.join(' · ')}</TooltipContent>
    </Tooltip>
  )
}

/** The latest upload speed result in a few words, with the full outcome and test time on hover. */
export function UploadSpeedText({
  test,
  testing = false,
  hideUntested = false,
}: {
  test?: ProviderUploadSpeedTest
  testing?: boolean
  hideUntested?: boolean
}) {
  if (!test && !testing && hideUntested) return null
  const label = testing ? 'Testing…' : providerUploadSpeedShortLabel(test)
  const details = testing ? [] : uploadSpeedDetails(test)
  const text = <span className="min-w-0 truncate tabular-nums">{label}</span>
  if (details.length === 0) return text
  return (
    <Tooltip>
      <TooltipTrigger asChild>{text}</TooltipTrigger>
      <TooltipContent>{details.join(' · ')}</TooltipContent>
    </Tooltip>
  )
}

function uploadSpeedDetails(test?: ProviderUploadSpeedTest) {
  if (!test) return []
  const details: string[] = []
  const fullLabel = providerUploadSpeedLabel(test)
  if (fullLabel !== providerUploadSpeedShortLabel(test)) details.push(fullLabel)
  if (test.tested_at) details.push(`Tested ${timeAgo(test.tested_at)} with ${providerUploadSampleSize(test)}`)
  return details
}

/** Stopped storage confirmations holding a data set, linked to where they are retried. */
export function StorageConfirmationsBadge({ count }: { count: number }) {
  if (count <= 0) return null
  const label = count === 1 ? '1 stopped confirmation' : `${count} stopped confirmations`
  return (
    <Link to="/tasks" search={storageConfirmationTasksSearch} aria-label={`Open ${label} in Tasks`}>
      <StatusBadge tone="danger">{label}</StatusBadge>
    </Link>
  )
}
