import type { StorageDataSetSummary } from '../api/client'
import { formatNumber, timeAgo } from './utils.ts'

interface ActivePieceFacts {
  active_piece_count?: number
  has_active_pieces?: boolean
}

export function activePiecesValue(facts: ActivePieceFacts) {
  if (facts.active_piece_count !== undefined) {
    return formatNumber(facts.active_piece_count)
  }
  if (facts.has_active_pieces !== undefined) {
    return facts.has_active_pieces ? 'Yes' : 'No'
  }
  return '—'
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
  return reason.replace(/_/g, ' ')
}

export function dataSetStorageHealthRefreshErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Failed to refresh data set storage health'
}
