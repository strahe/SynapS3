import type { StorageCommitAttentionCode } from '@/api/client'

const knownAttentionLabels = {
  submission_mismatch: 'Confirmation does not match',
  data_set_unavailable: 'Data set is unavailable',
  confirmation_timeout: 'Confirmation timed out',
} satisfies Record<StorageCommitAttentionCode, string>

const attentionLabels = new Map<string, string>(Object.entries(knownAttentionLabels))

/** What retrying a stopped batch does. */
export const storageConfirmationRetryNote =
  'Retry checks whether the pieces were already added before resubmitting the original batch.'

/** Tasks page filter that lists stopped batches. */
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
