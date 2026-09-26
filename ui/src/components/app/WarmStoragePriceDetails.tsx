import type { WarmStoragePriceList } from '@/api/client'
import { formatTokenAmount, timeAgo } from '@/lib/utils'

const rates = [
  ['storage_per_tib_per_month', 'Storage / TiB / month'],
  ['dataset_fee_per_month', 'Data set / month'],
  ['cdn_egress_per_tib', 'CDN egress / TiB'],
  ['cache_miss_egress_per_tib', 'Cache miss egress / TiB'],
] as const
const fees = [
  ['create_data_set', 'Create data set'],
  ['add_pieces_base', 'Add pieces base'],
  ['add_pieces_per_piece', 'Add pieces / piece'],
  ['schedule_piece_removals', 'Schedule piece removals'],
  ['terminate', 'Terminate service'],
] as const
const lockups = [
  ['lifecycle_reserve_target', 'Lifecycle reserve target'],
  ['replenish_threshold', 'Replenish threshold'],
  ['default_lockup_period', 'Default lockup period'],
  ['cdn_lockup_amount', 'CDN lockup amount'],
  ['cache_miss_lockup_amount', 'Cache miss lockup amount'],
  ['cdn_lockup_period', 'CDN lockup period'],
] as const

export function WarmStoragePriceDetails({ price }: { price: WarmStoragePriceList }) {
  const amount = (key: string, value?: string) =>
    key.includes('period')
      ? value
        ? `${value} epochs`
        : '—'
      : price.supported_token
        ? formatTokenAmount(value, 18, 'USDFC')
        : `${value ?? '—'} token base units`
  return (
    <div className="space-y-2 text-sm">
      <p className="text-xs text-muted-foreground">Warm Storage price list · checked {timeAgo(price.observed_at)}</p>
      <p className="break-all text-xs text-muted-foreground">
        Payment token: {price.supported_token ? 'USDFC' : 'Unsupported token'} ({price.token})
      </p>
      {!price.supported_token && (
        <p className="text-destructive">Replacement is unavailable with this payment token.</p>
      )}
      <PriceGroup
        title="Recurring rates"
        entries={rates.map(([key, label]) => [label, amount(key, price.rates[key])] as const)}
      />
      <PriceGroup
        title="Operation fees"
        collapsible
        entries={fees.map(([key, label]) => [label, amount(key, price.fees[key])] as const)}
      />
      <PriceGroup
        title="Lockups and periods"
        collapsible
        entries={lockups.map(([key, label]) => [label, amount(key, price.lockups[key])] as const)}
      />
    </div>
  )
}

function PriceGroup({
  title,
  entries,
  collapsible = false,
}: {
  title: string
  entries: readonly (readonly [string, string])[]
  collapsible?: boolean
}) {
  const rows = (
    <dl className="grid gap-x-3 gap-y-1 sm:grid-cols-2">
      {entries.map(([label, value]) => (
        <div key={label} className="flex justify-between gap-2">
          <dt className="text-muted-foreground">{label}</dt>
          <dd className="text-right tabular-nums">{value}</dd>
        </div>
      ))}
    </dl>
  )
  if (collapsible) {
    return (
      <details className="rounded-md border p-2">
        <summary className="cursor-pointer font-medium">
          {title} ({entries.length})
        </summary>
        <div className="pt-2">{rows}</div>
      </details>
    )
  }
  return (
    <div>
      <p className="font-medium">{title}</p>
      {rows}
    </div>
  )
}
