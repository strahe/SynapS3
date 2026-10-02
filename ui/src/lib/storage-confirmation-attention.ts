import type { StorageCommitAttentionCode } from '@/api/client'

const knownAttentionLabels = {
  attempt_only_ambiguous: 'Submission result unknown',
  unattributed_piece: 'Piece ownership unknown',
  submission_mismatch: 'Confirmation does not match',
  data_set_unavailable: 'Data set is unavailable',
  confirmation_timeout: 'Confirmation timed out',
} satisfies Record<StorageCommitAttentionCode, string>

const attentionLabels = new Map<string, string>(Object.entries(knownAttentionLabels))

/** What recovering a stopped storage confirmation does. */
export const storageConfirmationRetryNote =
  'Recovery checks whether the piece is already registered before continuing the original request.'

/** Tasks page filter that lists stopped storage confirmations. */
export const storageConfirmationTasksSearch = { type: 'storage_commit', status: 'failed' } as const

export interface StorageConfirmationAttentionView {
  known: boolean
  label: string
  reasonCode: string
}

export function storageConfirmationAttentionView(reasonCode: string): StorageConfirmationAttentionView {
  const label = attentionLabels.get(reasonCode)
  return {
    known: label !== undefined,
    label: label ?? 'Unknown confirmation issue',
    reasonCode,
  }
}
