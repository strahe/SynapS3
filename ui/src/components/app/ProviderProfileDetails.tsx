import type { ProviderProfile } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { formatTokenAmount, timeAgo } from '@/lib/utils'

export function ProviderProfileDetails({
  profile,
  supportedTokenAddress,
}: {
  profile?: ProviderProfile
  supportedTokenAddress?: string
}) {
  if (!profile)
    return (
      <div className="space-y-2 text-sm text-muted-foreground">
        <p>Provider details have not been collected. Refresh this provider to check again.</p>
        <p>FWSS approved: Unknown · FWSS endorsed: Unknown</p>
      </div>
    )
  const offering = profile.registry_snapshot?.pdp_offering
  const extraCapabilities = offering?.extra_capabilities_hex ?? {}
  const declaredPrice = offering?.storage_price_per_tib_per_day
  const priceIsUSDFC = Boolean(
    supportedTokenAddress && offering?.payment_token_address?.toLowerCase() === supportedTokenAddress.toLowerCase()
  )
  return (
    <div className="@container space-y-4 text-sm">
      <p className="text-xs text-muted-foreground">Registry declaration · checked {timeAgo(profile.last_success_at)}</p>
      {profile.description && <p>{profile.description}</p>}
      <div className="grid gap-x-8 gap-y-5 @xl:grid-cols-2">
        <ProfileValue label="Service provider" value={profile.service_provider_address} copyable />
        <ProfileValue label="Payee" value={profile.payee_address} copyable />
        <ProfileValue label="Declared service URL" value={profile.service_url} copyable />
        <ProfileValue label="Declared location" value={offering?.location} />
        <ProfileValue label="Minimum piece size" value={formatPieceSize(offering?.min_piece_size_bytes)} />
        <ProfileValue label="Maximum piece size" value={formatPieceSize(offering?.max_piece_size_bytes)} />
        <ProfileValue label="Minimum proving period (epochs)" value={offering?.min_proving_period_epochs} />
        <ProfileValue
          label={`Declared storage price / TiB / day${priceIsUSDFC ? '' : ' (token base units)'}`}
          value={declaredPrice}
          displayValue={priceIsUSDFC ? formatTokenAmount(declaredPrice, 18, 'USDFC') : undefined}
          copyable={priceIsUSDFC}
        />
        <ProfileValue label="Declared payment token" value={offering?.payment_token_address} copyable />
        <ProfileValue label="IPNI piece" value={offering ? (offering.ipni_piece ? 'Yes' : 'No') : undefined} />
        <ProfileValue label="IPNI IPFS" value={offering ? (offering.ipni_ipfs ? 'Yes' : 'No') : undefined} />
        <ProfileValue label="IPNI peer ID" value={offering?.ipni_peer_id} copyable />
        <ProfileValue label="FWSS approved" value={tierStatus(profile.approved, profile.approved_checked_at)} />
        <ProfileValue label="FWSS endorsed" value={tierStatus(profile.endorsed, profile.endorsed_checked_at)} />
      </div>
      {Object.keys(extraCapabilities).length > 0 && (
        <details className="text-xs text-muted-foreground">
          <summary>Additional Registry capabilities ({Object.keys(extraCapabilities).length})</summary>
          <div className="mt-2 space-y-1">
            {Object.entries(extraCapabilities).map(([key, value]) => (
              <div key={key} className="break-all">
                {key}: {value}
              </div>
            ))}
          </div>
        </details>
      )}
    </div>
  )
}

function tierStatus(listed: boolean, checkedAt?: string) {
  if (!checkedAt) return 'Unknown'
  return `${listed ? 'Yes' : 'No'} · checked ${timeAgo(checkedAt)}`
}

function formatPieceSize(raw?: string) {
  if (!raw || !/^\d+$/.test(raw)) return raw
  const bytes = BigInt(raw)
  for (const [size, unit] of [
    [1024n ** 4n, 'TiB'],
    [1024n ** 3n, 'GiB'],
    [1024n ** 2n, 'MiB'],
    [1024n, 'KiB'],
  ] as const) {
    if (bytes >= size && bytes % size === 0n) return `${raw} bytes (${bytes / size} ${unit})`
  }
  return `${raw} bytes`
}

function ProfileValue({
  label,
  value,
  displayValue,
  copyable = false,
}: {
  label: string
  value?: string
  displayValue?: string
  copyable?: boolean
}) {
  return (
    <div className="min-w-0">
      <span className="block text-xs text-muted-foreground">{label}</span>
      {value ? (
        copyable ? (
          <CopyableValue label={label} value={value} displayValue={displayValue} maxLength={30} />
        ) : (
          <span className="break-words">{displayValue ?? value}</span>
        )
      ) : (
        <span>—</span>
      )}
    </div>
  )
}
