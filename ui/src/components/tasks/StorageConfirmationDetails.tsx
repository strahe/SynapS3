import type { TaskStorageConfirmation } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { ProviderIdentityCell } from '@/components/app/ProviderIdentityCell'

export function StorageConfirmationDetails({ confirmation }: { confirmation: TaskStorageConfirmation }) {
  const pieceCIDs = confirmation.piece_cids ?? []

  return (
    <dl className="flex flex-col gap-2 text-sm">
      <div className="grid grid-cols-[6rem_minmax(0,1fr)] gap-2">
        <dt className="text-muted-foreground">Provider</dt>
        <dd className="min-w-0">
          <ProviderIdentityCell providerID={confirmation.provider_id} identity={confirmation.provider_identity} />
        </dd>
      </div>
      <DetailRow label="Data set" value={confirmation.data_set_id} />
      <div className="grid grid-cols-[6rem_minmax(0,1fr)] gap-2">
        <dt className="text-muted-foreground">{confirmation.piece_count === 1 ? 'Piece' : 'Pieces'}</dt>
        <dd className="flex min-w-0 flex-col gap-1">
          {confirmation.piece_count !== 1 && <span>{confirmation.piece_count} pieces</span>}
          {pieceCIDs.length > 0 && (
            <ul className="flex max-h-40 flex-col gap-1 overflow-y-auto">
              {pieceCIDs.map((pieceCID) => (
                <li key={pieceCID} className="min-w-0">
                  <CopyableValue label="Piece CID" value={pieceCID} monospace maxLength={48} />
                </li>
              ))}
            </ul>
          )}
        </dd>
      </div>
      {confirmation.tx_hash && <DetailRow label="Submitted transaction" value={confirmation.tx_hash} />}
      {confirmation.confirmed_tx_hash && (
        <DetailRow label="Confirmed transaction" value={confirmation.confirmed_tx_hash} />
      )}
    </dl>
  )
}

function DetailRow({ label, value }: { label: string; value: string | null | undefined }) {
  return (
    <div className="grid grid-cols-[6rem_minmax(0,1fr)] gap-2">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="min-w-0">
        {value ? (
          <CopyableValue label={label} value={value} monospace maxLength={48} />
        ) : (
          <span className="text-muted-foreground">Not recorded</span>
        )}
      </dd>
    </div>
  )
}
