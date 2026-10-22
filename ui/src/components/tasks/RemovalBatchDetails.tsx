import type { TaskRemovalBatch } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { ProviderIdentityCell } from '@/components/app/ProviderIdentityCell'

const attentionLabels: Record<string, string> = {
  identity_mismatch: 'Storage account changed',
  payer_underfunded: 'Add funds or increase the spending allowance',
  provider_unreachable: 'Provider could not accept removal',
  confirmation_timeout: 'Removal confirmation delayed',
  transaction_reverted: 'Removal transaction failed',
  removal_stalled: 'Provider has not removed the pieces',
  cleanup_stalled: 'Data set cleanup delayed',
}

const statusLabels: Record<string, string> = {
  collecting: 'Collecting removals',
  ready: 'Ready to submit',
  submitted: 'Waiting for confirmation',
  queued: 'Waiting for removal',
  settled: 'Removed',
  abandoned: 'Batch released',
}

export function removalAttentionLabel(code: string) {
  return attentionLabels[code] ?? 'Removal needs review'
}

export function RemovalBatchDetails({ batch }: { batch: TaskRemovalBatch }) {
  return (
    <dl className="flex flex-col gap-2 text-sm">
      <DetailRow label="Status">{statusLabels[batch.status] ?? 'Unknown'}</DetailRow>
      {batch.attention_code && <DetailRow label="Attention">{removalAttentionLabel(batch.attention_code)}</DetailRow>}
      <DetailRow label="Provider">
        <ProviderIdentityCell providerID={batch.provider_id} identity={batch.provider_identity} />
      </DetailRow>
      {batch.data_set_id && (
        <DetailRow label="Data set">
          <CopyableValue label="Data set" value={batch.data_set_id} monospace maxLength={48} />
        </DetailRow>
      )}
      <DetailRow label="Pieces">{batch.piece_count}</DetailRow>
      {batch.tx_hash && (
        <DetailRow label="Transaction">
          <CopyableValue label="Removal transaction" value={batch.tx_hash} monospace maxLength={48} />
        </DetailRow>
      )}
      {batch.last_error && (
        <DetailRow label="Details">
          <CopyableValue label="Removal error" value={batch.last_error} maxLength={80} />
        </DetailRow>
      )}
    </dl>
  )
}

function DetailRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="grid grid-cols-[6rem_minmax(0,1fr)] gap-2">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0">{children}</dd>
    </div>
  )
}
