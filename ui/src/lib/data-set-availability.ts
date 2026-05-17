import type { StorageDataSetSummary } from '../api/client'
import { formatNumber, timeAgo } from './utils.ts'

export function dataSetAvailabilityDetailParts(dataSet: Pick<StorageDataSetSummary, 'status' | 'availability'>) {
  const localState = dataSet.status === 'ready' ? null : `local state: ${dataSet.status}`
  const availability = dataSet.availability
  if (!availability) {
    return ['No snapshot', localState].filter(Boolean) as string[]
  }

  const reasons = (availability.reason_codes ?? []).map(availabilityReasonLabel).join(', ')
  const activePieces =
    availability.active_piece_count === undefined ? null : `${formatNumber(availability.active_piece_count)} pieces`
  const lastError =
    (availability.status === 'unknown' || availability.status === 'unavailable') && availability.last_error
      ? `last error: ${availability.last_error}`
      : null
  const checked = availability.last_checked_at ? timeAgo(availability.last_checked_at) : 'No snapshot'
  return [reasons, activePieces, localState, lastError, checked].filter(Boolean) as string[]
}

export function availabilityReasonLabel(reason: string) {
  return reason.replace(/_/g, ' ')
}

export function dataSetAvailabilityRefreshErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Failed to refresh data set availability'
}
