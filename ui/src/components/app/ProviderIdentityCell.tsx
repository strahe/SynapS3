import { Link } from '@tanstack/react-router'
import { Info } from 'lucide-react'
import { Fragment, useState } from 'react'

import type { ProviderIdentity } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { cn } from '@/lib/utils'
import { CopyableValue } from './CopyableValue'

export function ProviderIdentityCell({ providerID, identity }: { providerID?: string; identity?: ProviderIdentity }) {
  const [detailsOpen, setDetailsOpen] = useState(false)
  const registryID = identity?.registry_provider_id ?? providerID
  const label = identity?.name?.trim() || (registryID ? `Registry ${registryID}` : '—')

  if (!identity) {
    if (!registryID) {
      return (
        <span className="block max-w-44 truncate font-mono text-xs text-muted-foreground" title={registryID}>
          {label}
        </span>
      )
    }

    return <ProviderTopologyLink providerID={registryID} label={label} className="font-mono text-xs" />
  }

  return (
    <div className="flex min-w-0 items-center gap-1.5">
      {registryID ? <ProviderTopologyLink providerID={registryID} label={label} /> : <span>{label}</span>}
      <Popover open={detailsOpen} onOpenChange={setDetailsOpen}>
        <PopoverTrigger asChild>
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            aria-label={`Provider details for ${label}`}
            aria-expanded={detailsOpen}
          >
            <Info />
          </Button>
        </PopoverTrigger>
        <PopoverContent
          side="top"
          className="max-h-[min(calc(100vh-2rem),32rem)] w-max max-w-[min(calc(100vw-2rem),36rem)] overflow-y-auto whitespace-normal p-3.5 text-left"
        >
          <ProviderIdentityDetails providerID={registryID} identity={identity} />
        </PopoverContent>
      </Popover>
    </div>
  )
}

function ProviderTopologyLink({
  providerID,
  label,
  className,
}: {
  providerID: string
  label: string
  className?: string
}) {
  return (
    <Link
      to="/storage-topology"
      search={{ provider: providerID }}
      className={cn('block min-w-0 max-w-44 truncate font-medium hover:underline', className)}
      title={label}
    >
      {label}
    </Link>
  )
}

function ProviderIdentityDetails({ providerID, identity }: { providerID?: string; identity: ProviderIdentity }) {
  const allFields: Array<[string, string | undefined]> = [
    ['Registry Provider ID', identity.registry_provider_id || providerID],
    ['Actor ID', identity.filecoin_actor_id],
    ['Filecoin address', identity.filecoin_address],
    ['EVM service provider', identity.service_provider_address],
    ['Payee address', identity.payee_address],
    ['Service URL', identity.service_url],
    ['Location', identity.location],
    ['Description', identity.description],
  ]
  const fields = allFields.filter((field): field is [string, string] => Boolean(field[1]))
  const extras = Object.entries(identity.extra_capabilities ?? {}).sort(([a], [b]) => a.localeCompare(b))

  return (
    <div className="flex w-full select-text flex-col gap-3">
      <div className="font-medium">
        {identity.name?.trim() || `Registry ${identity.registry_provider_id || providerID}`}
      </div>
      <div className="grid grid-cols-1 gap-x-3 gap-y-2 text-xs sm:grid-cols-[9rem_minmax(0,1fr)]">
        {fields.map(([label, value]) => (
          <Fragment key={label}>
            <span className="text-muted-foreground">{label}</span>
            <CopyableValue label={label} value={value} monospace maxLength={32} className="leading-relaxed" />
          </Fragment>
        ))}
        {extras.map(([label, value]) => (
          <Fragment key={label}>
            <span className="text-muted-foreground">{label}</span>
            <CopyableValue label={label} value={value} monospace maxLength={32} className="leading-relaxed" />
          </Fragment>
        ))}
      </div>
    </div>
  )
}
