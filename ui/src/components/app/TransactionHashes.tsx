import { CopyableValue } from '@/components/app/CopyableValue'

export function TransactionHashes({
  txHash,
  confirmedTxHash,
}: {
  txHash: string | null
  confirmedTxHash: string | null
}) {
  if (!txHash && !confirmedTxHash) return null

  return (
    <dl className="flex min-w-0 flex-col gap-2 text-xs">
      {[
        { label: 'Submitted transaction', value: txHash },
        { label: 'Confirmed transaction', value: confirmedTxHash },
      ].map(
        ({ label, value }) =>
          value && (
            <div key={label} className="flex min-w-0 flex-col gap-1">
              <dt className="text-muted-foreground">{label}</dt>
              <dd className="min-w-0">
                <CopyableValue label={label} value={value} monospace maxLength={32} />
              </dd>
            </div>
          )
      )}
    </dl>
  )
}
