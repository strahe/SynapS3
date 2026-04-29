export interface ReviewDetailRow {
  id: string
  label: string
  value: string
}

export function ReviewDetails({ rows }: { rows: readonly ReviewDetailRow[] }) {
  return (
    <dl className="grid gap-3 rounded-md border border-border bg-muted/30 p-3 text-sm">
      {rows.map((row) => (
        <div key={row.id} className="grid gap-1">
          <dt className="text-xs text-muted-foreground">{row.label}</dt>
          <dd className="break-all font-mono text-xs">{row.value}</dd>
        </div>
      ))}
    </dl>
  )
}
