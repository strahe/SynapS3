import type { StatusTone } from '@/components/app/StatusBadge'
import type {
  ProviderReplacement,
  ProviderReplacementStatus,
  ReplacementProviderCandidate,
  StorageDataSetSummary,
} from '../api/client'
import { APIError } from '../api/client.ts'
import { providerDisplayName, providerRegistryLabel } from './provider-display.ts'
import { formatBytes, formatNumber } from './utils.ts'

const replacementStatusLabels: Record<ProviderReplacementStatus, string> = {
  preparing_target: 'Preparing new provider',
  migrating: 'Copying data',
  waiting: 'Waiting',
  retiring: 'Retiring old provider',
  cleanup_attention: 'Could not end the old provider',
  failed: 'Could not finish the replacement',
  completed: 'Completed',
  superseded: 'Replaced by a newer request',
}

export function replacementStatusLabel(status: ProviderReplacementStatus) {
  return replacementStatusLabels[status] ?? 'Unknown'
}

export function replacementStatusTone(status: ProviderReplacementStatus): StatusTone {
  switch (status) {
    case 'completed':
      return 'success'
    case 'failed':
    case 'cleanup_attention':
      return 'danger'
    case 'waiting':
      return 'warning'
    case 'superseded':
      return 'neutral'
    default:
      return 'info'
  }
}

/** A replacement is still doing something on its own. */
export function replacementInProgress(replacement: ProviderReplacement) {
  return ['preparing_target', 'migrating', 'waiting', 'retiring'].includes(replacement.status)
}

/** Only these states are resumed by the operator; the rest resume themselves. */
export function replacementRetryable(replacement: ProviderReplacement) {
  if (replacement.retryable !== undefined) return replacement.retryable
  if (replacement.failure_reason === 'target_in_use' || replacement.failure_reason === 'target_rejected') return false
  return replacement.status === 'failed' || replacement.status === 'cleanup_attention'
}

/** A replacement worth showing above the table: still running, or waiting for a decision. */
export function activeReplacements(replacements: ProviderReplacement[] | undefined) {
  if (!replacements?.length) return []
  return replacements.filter(
    (row) =>
      replacementInProgress(row) ||
      row.status === 'failed' ||
      row.status === 'cleanup_attention' ||
      row.retirement_attention
  )
}

/**
 * A confirmation counts referenced versions and total size, which is what the
 * operator is authorizing storage for.
 */
export function replacementConfirmationSummary(dataSet: StorageDataSetSummary) {
  return `${formatNumber(dataSet.referenced_version_count)} versions · ${formatBytes(dataSet.physical_bytes)}`
}

export function replacementGenerationLabel(dataSet: StorageDataSetSummary) {
  return `Generation ${dataSet.generation}`
}

/** A generation that has no service yet is the incoming replacement target. */
function dataSetIsBeingPrepared(dataSet: StorageDataSetSummary) {
  return !dataSet.is_current && (dataSet.status === 'pending' || dataSet.status === 'creating')
}

export function dataSetGenerationTone(dataSet: StorageDataSetSummary): StatusTone {
  if (dataSet.is_current) return 'success'
  if (dataSet.status === 'retired') return 'neutral'
  // Setting up the replacement is ordinary progress, not something to act on.
  if (dataSetIsBeingPrepared(dataSet)) return 'info'
  return 'warning'
}

/**
 * The replica column is narrow, so these stay about as short as "Current" and
 * "Retired". A label that does not fit is truncated to something unreadable,
 * which is worse than a terser word.
 */
export function dataSetGenerationLabel(dataSet: StorageDataSetSummary) {
  if (dataSet.is_current) return 'Current'
  if (dataSet.status === 'retired') return 'Retired'
  // The pair reads as a direction: one generation is on its way in, the other
  // on its way out.
  if (dataSet.status === 'draining') return 'Outgoing'
  // A generation still being set up is the replacement's destination. Calling it
  // historical points at the past when it is the one arriving.
  if (dataSetIsBeingPrepared(dataSet)) return 'Incoming'
  if (dataSet.status === 'failed') return 'Failed'
  return 'Historical'
}

const replacementErrorMessages: Record<string, string> = {
  replacement_active: 'This replica is already being replaced. Wait for it to finish or retry it below.',
  replacement_target_creating:
    'The earlier replacement of this replica is still setting up its new data set. Wait for it to finish, or retry that setup from Tasks if it stopped.',
  replacement_target_in_use: 'That provider already stores a replica of this bucket. Choose a different one.',
  replacement_target_invalid: 'Choose a provider other than the one being replaced.',
  replacement_no_eligible_provider:
    'No unused storage provider is available right now. Try again later or choose an available provider.',
  replacement_target_unavailable: 'That provider is not available for replacement right now. Choose another provider.',
  approval_check_unavailable: 'Could not check provider requirements. Try again.',
  replacement_idempotency_conflict: 'This confirmation changed after it was submitted. Close it and try again.',
  replacement_source_not_current: 'This replica no longer receives writes, so replacing it would change nothing.',
  replacement_source_running: 'Storage setup is running. Try again shortly.',
  replacement_source_outcome_unknown:
    'Could not confirm whether storage setup succeeded. Check its task before replacing the provider.',
  replacement_superseded: 'A newer request has taken over this replica.',
  replacement_not_retryable: 'This replacement cannot be retried in its current state.',
  replacement_task_running: 'Replacement work is still running. Try again shortly.',
}

/**
 * What the operator should do next, in their terms. The recorded error is
 * developer diagnostics and belongs behind a detail view, not on the card.
 */
export function replacementNextStep(replacement: ProviderReplacement) {
  if (replacement.status === 'superseded' && replacement.retirement_attention) {
    return 'The unused data set could not be shut down. Check its retirement task.'
  }
  switch (replacement.status) {
    case 'failed':
      if (replacement.failure_reason === 'target_rejected') {
        return 'This provider rejected setup. Choose a different provider.'
      }
      if (replacement.failure_reason === 'target_in_use') {
        return 'This provider is already in use. Choose a different provider.'
      }
      if (replacement.retryable === false) {
        if (!replacement.source.is_current && !replacement.target.is_current) {
          return 'Storage setup needs attention. Check its task.'
        }
        return replacement.target.status === 'retired' || replacement.target.status === 'failed'
          ? 'Storage setup has ended. Choose a different provider for the original replica.'
          : 'Storage setup stopped. Check its task before continuing.'
      }
      // Choosing a different provider is only open while the old provider still
      // holds the replica. Once the new one has taken it over, the only way
      // forward is to finish the copy that was started.
      return replacement.source.is_current
        ? 'The replacement has not finished. Retry, or choose a different provider.'
        : 'The replacement has not finished. Retry to continue where it stopped.'
    case 'cleanup_attention':
      return 'The data is on the new provider. The old one could not be shut down and is still being paid for.'
    case 'waiting':
      return replacement.wait_message ?? 'Waiting.'
    default:
      return null
  }
}

export function dataSetSetupMessage(dataSet: StorageDataSetSummary) {
  if (!dataSet.is_current || dataSet.status === 'ready') return null
  return dataSet.setup_error ?? replacementErrorMessages[dataSet.replacement_blocked_reason ?? ''] ?? null
}

/** Turn a failed request into something the operator can act on. */
export function replacementErrorMessage(error: unknown) {
  if (error instanceof APIError) {
    if (
      (error.code === 'replacement_source_outcome_unknown' || error.code === 'replacement_not_retryable') &&
      error.message
    )
      return error.message
    const known = error.code ? replacementErrorMessages[error.code] : undefined
    if (known) return known
    if (error.status === 503) {
      return 'Filecoin storage is unavailable right now. Try again once it recovers.'
    }
    // A 5xx body carries internal diagnostics, which say nothing an operator can
    // act on. Anything the operator can fix arrives as a typed 4xx above.
    if (error.status >= 500) {
      return 'The request could not be completed. Check the server logs for details.'
    }
    return error.message
  }
  if (error instanceof Error) return error.message
  return 'Could not complete the request.'
}

const providerIneligibleReasons: Record<string, string> = {
  current_source: 'This is the provider being replaced',
  already_serves_bucket: 'Already stores a replica of this bucket',
  provider_unavailable: 'Provider is not currently available',
  observation_stale: 'Health information is out of date. Refresh this provider.',
  profile_missing: 'Provider details are unavailable. Refresh this provider.',
  profile_url_changed: 'The provider service URL changed. Refresh this provider to check its health.',
  required_provider_unavailable: 'This choice would leave the bucket without a required provider.',
}

/**
 * Why a provider cannot take this replica. Ineligible providers stay in the
 * list: an operator looking for one they expected needs the reason, not a
 * silent absence.
 */
export function providerCandidateDisabledReason(candidate: ReplacementProviderCandidate) {
  if (candidate.manual_selectable) return null
  return providerIneligibleReasons[candidate.manual_block_reason ?? ''] ?? 'Cannot take this replica'
}

/** A provider reads as its name; the registry ID identifies it. */
export function providerCandidateLabel(candidate: ReplacementProviderCandidate) {
  return providerDisplayName(candidate.provider_id, candidate.provider_profile?.name)
}

/**
 * The registry ID line under a provider's name. A bare number there reads as an
 * index or a rank; the prefix names what it is, matching how a provider ID is
 * written everywhere else in the dashboard. Nothing is shown when the name is
 * already the ID, which would only repeat it.
 */
export function providerCandidateRegistryLine(candidate: ReplacementProviderCandidate) {
  const registryLine = providerRegistryLabel(candidate.provider_id)
  return providerCandidateLabel(candidate) === registryLine ? null : registryLine
}

/** Extra context for a choosable provider, or null when there is nothing to add. */
export function providerCandidateNote(candidate: ReplacementProviderCandidate) {
  if (!candidate.manual_selectable || !candidate.previously_used) return null
  return 'Used by this bucket before'
}

/**
 * Operators search by whichever they have to hand: the name they know the
 * provider by, or the registry ID from a task or log line.
 */
export function providerCandidateMatches(candidate: ReplacementProviderCandidate, query: string) {
  const needle = query.trim().toLowerCase()
  if (!needle) return true
  if (candidate.provider_id.toLowerCase().includes(needle)) return true
  const profile = candidate.provider_profile
  return Boolean(
    profile?.name?.toLowerCase().includes(needle) ||
      profile?.registry_snapshot.pdp_offering?.location?.toLowerCase().includes(needle)
  )
}

export function replacementConfirmationDescription(dataSet: StorageDataSetSummary) {
  const description = dataSet.data_set_id
    ? 'This starts paying a new provider. Once it is ready, new uploads go there. The old provider is ended after existing objects are readable on the new one.'
    : 'This creates a replica on the new provider. Storage charges start when its setup succeeds. New uploads that need this replica wait until the new provider is ready.'
  return dataSet.replacement_has_late_service_risk || (!dataSet.data_set_id && dataSet.client_data_set_id)
    ? `${description} An earlier request may still create a paid data set; this replacement will not close it.`
    : description
}
