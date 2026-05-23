import { CopyableValue } from '@/components/app/CopyableValue'
import { cn } from '@/lib/utils'

export interface ReviewDetailRow {
  id: string
  label: string
  value: string
  copyable?: boolean
  monospace?: boolean
  displayValue?: string
  maxLength?: number
}

export function ReviewDetails({ rows }: { rows: readonly ReviewDetailRow[] }) {
  return (
    <dl className="grid gap-3 rounded-md border border-border bg-muted/30 p-3 text-sm">
      {rows.map((row) => (
        <div key={row.id} className="grid gap-1">
          <dt className="text-xs text-muted-foreground">{row.label}</dt>
          <dd className="min-w-0">
            {row.copyable && isCopyableReviewValue(row.value) ? (
              <CopyableValue
                label={row.label}
                value={row.value}
                displayValue={row.displayValue}
                monospace={row.monospace ?? true}
                maxLength={row.maxLength}
              />
            ) : (
              <span className={cn('break-all text-xs', (row.monospace ?? true) ? 'font-mono' : undefined)}>
                {row.displayValue ?? row.value}
              </span>
            )}
          </dd>
        </div>
      ))}
    </dl>
  )
}

function isCopyableReviewValue(value: string) {
  return value.trim() !== '' && value !== '—'
}
