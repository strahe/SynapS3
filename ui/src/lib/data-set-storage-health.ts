import type { StorageDataSetSummary } from '../api/client'
import { formatNumber, timeAgo } from './utils.ts'

interface ActivePieceFacts {
  active_piece_count?: number
  has_active_pieces?: boolean
}

// Chain scans report whether a data set holds pieces, not how many, so a list
// column shows presence rather than mixing counts with yes/no answers.
export function activePiecesValue(facts: ActivePieceFacts) {
  const hasPieces =
    facts.has_active_pieces ?? (facts.active_piece_count === undefined ? undefined : facts.active_piece_count > 0)
  if (hasPieces === undefined) return '—'
  return hasPieces ? 'Yes' : 'None'
}

function activePiecesDetail(facts: ActivePieceFacts) {
  if (facts.active_piece_count !== undefined) {
    return `${formatNumber(facts.active_piece_count)} pieces`
  }
  if (facts.has_active_pieces !== undefined) {
    return facts.has_active_pieces ? 'Contains active pieces' : 'No active pieces'
  }
  return null
}

export function dataSetStorageHealthDetailParts(dataSet: Pick<StorageDataSetSummary, 'status' | 'storage_health'>) {
  const localState = dataSet.status === 'ready' ? null : `local state: ${dataSet.status}`
  const storageHealth = dataSet.storage_health
  if (!storageHealth) {
    return ['No state recorded', localState].filter(Boolean) as string[]
  }

  const reasons = (storageHealth.reason_codes ?? []).map(storageHealthReasonLabel).join(', ')
  const activePieces = activePiecesDetail(storageHealth)
  const lastError =
    (storageHealth.status === 'unknown' || storageHealth.status === 'unavailable') && storageHealth.last_error
      ? `last error: ${storageHealth.last_error}`
      : null
  const checked = storageHealth.last_checked_at ? timeAgo(storageHealth.last_checked_at) : 'No state recorded'
  return [reasons, activePieces, localState, lastError, checked].filter(Boolean) as string[]
}

export function storageHealthReasonLabel(reason: string) {
  switch (reason) {
    case 'registry_lookup_failed':
      return 'Provider registry could not be read'
    case 'provider_inactive':
      return 'Provider is inactive in the registry'
    case 'provider_missing_pdp':
      return 'Provider does not offer PDP storage'
    case 'provider_http_unreachable':
      return 'Provider health check failed'
    case 'chain_lookup_failed':
      return 'Could not check data sets on chain'
    case 'chain_data_set_missing':
      return 'Data set not found on chain'
    case 'chain_data_set_inactive':
      return 'Data set is no longer live on chain'
    case 'chain_data_set_unmanaged':
      return 'Data set is not managed by Warm Storage'
    case 'local_status_not_ready':
      return 'Storage setup is not ready'
    case 'provider_mismatch':
      return 'Chain lists a different provider'
    case 'metadata_mismatch':
      return 'Chain metadata does not match'
    default:
      return reason.replace(/_/g, ' ')
  }
}

export function dataSetStorageHealthRefreshErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Failed to refresh data set storage health'
}
