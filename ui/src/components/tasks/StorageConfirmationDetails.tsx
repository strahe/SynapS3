import type { TaskStorageConfirmation } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'

export function StorageConfirmationDetails({ confirmation }: { confirmation: TaskStorageConfirmation }) {
  const fields: Array<[string, string | undefined]> = [
    ['Provider', confirmation.provider_id],
    ['Data set', confirmation.data_set_id],
    ['Piece CID', confirmation.piece_cid],
    ['Transaction', confirmation.transaction_id],
  ]

  return (
    <dl className="flex flex-col gap-2 text-sm">
      {fields.map(([label, value]) => (
        <div key={label} className="grid grid-cols-[6rem_minmax(0,1fr)] gap-2">
          <dt className="text-muted-foreground">{label}</dt>
          <dd className="min-w-0">
            {value ? (
              <CopyableValue label={label} value={value} monospace maxLength={48} />
            ) : (
              <span className="text-muted-foreground">Not recorded</span>
            )}
          </dd>
        </div>
      ))}
    </dl>
  )
}
