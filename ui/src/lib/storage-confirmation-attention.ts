import type { StorageCommitAttentionCode } from '@/api/client'

const knownAttentionLabels = {
  attempt_only_ambiguous: 'Submission result unknown',
  unattributed_piece: 'Piece ownership unknown',
  submission_mismatch: 'Confirmation does not match',
  data_set_unavailable: 'Data set is unavailable',
  confirmation_timeout: 'Confirmation timed out',
} satisfies Record<StorageCommitAttentionCode, string>

const attentionLabels = new Map<string, string>(Object.entries(knownAttentionLabels))

export const storageConfirmationListCommand = 'synaps3 admin storage-confirmation list'

export const storageConfirmationReleaseWarning =
  'Release the current attempt from the CLI only if you accept that the provider may already have stored the piece. Releasing it can submit the piece again and create duplicate paid storage.'

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
