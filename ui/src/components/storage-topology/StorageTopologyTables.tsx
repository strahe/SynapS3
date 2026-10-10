import { Link } from '@tanstack/react-router'
import { Database, Gauge, Loader2 } from 'lucide-react'
import type { ObservabilityDataSetObservation, ObservabilityProviderObservation, ProviderProfile } from '@/api/client'
import { CopyableValue, OptionalCopyableValue } from '@/components/app/CopyableValue'
import { CursorPagination } from '@/components/app/CursorPagination'
import {
  clickableRowProps,
  DataTableFrame,
  RowDetailsButton,
  tableHeaderRowClassName,
} from '@/components/app/DataTable'
import { StatusBadge } from '@/components/app/StatusBadge'
import { RetryButton } from '@/components/tasks/RetryButton'
import { Button } from '@/components/ui/button'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { activePiecesValue } from '@/lib/data-set-storage-health'
import { providerDisplayName, providerLocationLabel, providerRegistryLabel } from '@/lib/provider-display'
import { canTestProviderUploadSpeed } from '@/lib/provider-upload-speed'
import { replicaLabel } from '@/lib/storage-status-labels'
import {
  dataSetChainIDValue,
  formatOptionalTopologyText,
  freshnessLabel,
  localStatusLabel,
  localStatusTone,
  type StorageTopologyProviderRow,
} from '@/lib/storage-topology'
import { cn } from '@/lib/utils'
import { StorageConfirmationsBadge, TopologySignalBadge, UploadSpeedText } from './TopologyStatus'

export function ProvidersTableCard({
  rows,
  total,
  page,
  totalPages,
  loading,
  error,
  contextNote,
  onPageChange,
  onSelect,
  onTestUploadSpeed,
  testingProviderID,
}: {
  rows: StorageTopologyProviderRow[]
  total: number
  page: number
  totalPages: number
  loading?: boolean
  error?: string
  contextNote?: string
  onPageChange: (page: number) => void
  onSelect: (item: StorageTopologyProviderRow) => void
  onTestUploadSpeed: (providerID: string) => void
  testingProviderID?: string
}) {
  return (
    <div className="flex flex-col gap-3">
      <DataTableFrame>
        <Table className="min-w-[820px]">
          <TableHeader>
            <TableRow className={tableHeaderRowClassName}>
              <TableHead className="whitespace-nowrap px-3 py-2">Provider</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Health</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">FWSS</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Service URL</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Checked</TableHead>
              <TableHead className="w-10 px-3 py-2">
                <span className="sr-only">Details</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <InventoryLoadingRow colSpan={6} />
            ) : error ? (
              <InventoryErrorRow colSpan={6} message={error} />
            ) : rows.length > 0 ? (
              rows.map((row) => {
                const name = providerDisplayName(row.providerID, row.provider?.provider_profile?.name)
                return (
                  <TableRow key={row.providerID} {...clickableRowProps(() => onSelect(row))}>
                    <TableCell className="px-3 py-2">
                      <ProviderNameCell providerID={row.providerID} profile={row.provider?.provider_profile} />
                    </TableCell>
                    <TableCell className="px-3 py-2">
                      <ProviderHealthCell
                        row={row}
                        name={name}
                        starting={testingProviderID === row.providerID}
                        onTest={() => onTestUploadSpeed(row.providerID)}
                      />
                    </TableCell>
                    <TableCell className="px-3 py-2">
                      <ProviderTierBadges profile={row.provider?.provider_profile} />
                    </TableCell>
                    <TableCell className="max-w-72 px-3 py-2 text-muted-foreground">
                      <OptionalCopyableValue
                        label="Service URL"
                        value={formatOptionalTopologyText(row.provider?.facts.service_url)}
                        linkHref={row.provider?.facts.service_url}
                        maxLength={36}
                      />
                    </TableCell>
                    <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                      {row.freshness ? freshnessLabel(row.freshness) : '—'}
                    </TableCell>
                    <TableCell className="px-3 py-2 text-right">
                      <RowDetailsButton label={name} onClick={() => onSelect(row)} />
                    </TableCell>
                  </TableRow>
                )
              })
            ) : (
              <InventoryEmptyRow
                colSpan={6}
                title="No providers found"
                description="No provider observations match the current filters."
              />
            )}
          </TableBody>
        </Table>
      </DataTableFrame>
      {contextNote && <div className="text-sm text-muted-foreground">{contextNote}</div>}
      <CursorPagination
        summary={totalPages > 1 ? `Page ${page} of ${totalPages} · ${total} total` : undefined}
        hasPrevious={page > 1}
        hasNext={page < totalPages}
        onPrevious={() => onPageChange(page - 1)}
        onNext={() => onPageChange(page + 1)}
      />
    </div>
  )
}

function ProviderNameCell({
  providerID,
  profile,
  showLocation = true,
}: {
  providerID: string
  profile?: ProviderProfile
  showLocation?: boolean
}) {
  const name = profile?.name?.trim()
  const location = showLocation
    ? providerLocationLabel(profile?.registry_snapshot?.pdp_offering?.location, { compact: true })
    : undefined
  return (
    <div className="flex min-w-0 max-w-64 flex-col">
      {name && <span className="truncate font-medium">{name}</span>}
      <div className={cn('flex min-w-0 items-center gap-1', name ? 'text-xs text-muted-foreground' : 'font-medium')}>
        <CopyableValue label="Registry ID" value={providerID} displayValue={providerRegistryLabel(providerID)} />
        {location && (
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="truncate text-xs text-muted-foreground">· {location}</span>
            </TooltipTrigger>
            <TooltipContent>Provider-declared location</TooltipContent>
          </Tooltip>
        )}
      </div>
    </div>
  )
}

// Fixed slots keep the column width and the speed values aligned across rows,
// whether a provider is testable, being tested, or not testable at all.
function ProviderHealthCell({
  row,
  name,
  starting,
  onTest,
}: {
  row: StorageTopologyProviderRow
  name: string
  starting: boolean
  onTest: () => void
}) {
  const test = row.provider?.upload_speed_test
  const testing = starting || test?.state === 'testing'
  return (
    <div className="grid grid-cols-[6.5rem_1.75rem_5.5rem] items-center gap-1 text-xs text-muted-foreground">
      <div>
        <TopologySignalBadge status={row.status} signal={row.provider?.signal} />
      </div>
      <div>
        {test?.retryable ? (
          <RetryButton taskID={test.retry_task_id} />
        ) : testing ? (
          <Button variant="ghost" size="icon-xs" disabled aria-label={`Testing upload speed for ${name}`}>
            <Loader2 className="animate-spin" />
          </Button>
        ) : (
          canTestProviderUploadSpeed(row.provider) && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  aria-label={`Test upload speed for ${name} (32 MiB)`}
                  onClick={onTest}
                >
                  <Gauge />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Test upload speed with 32 MiB</TooltipContent>
            </Tooltip>
          )
        )}
      </div>
      <UploadSpeedText test={test} testing={testing} hideUntested />
    </div>
  )
}

function ProviderTierBadges({ profile }: { profile?: ProviderProfile }) {
  const tiers = [
    { label: 'Approved', listed: profile?.approved, known: Boolean(profile?.approved_checked_at) },
    { label: 'Endorsed', listed: profile?.endorsed, known: Boolean(profile?.endorsed_checked_at) },
  ]
  if (tiers.every((tier) => !tier.known)) return <span className="text-xs text-muted-foreground">Unknown</span>
  const listed = tiers.filter((tier) => tier.known && tier.listed)
  const unknown = tiers.filter((tier) => !tier.known)
  if (listed.length === 0 && unknown.length === 0) {
    return <span className="text-xs text-muted-foreground">Not listed</span>
  }
  return (
    <div className="flex gap-1">
      {listed.map((tier) => (
        <StatusBadge key={tier.label} tone="success">
          {tier.label}
        </StatusBadge>
      ))}
      {unknown.map((tier) => (
        <StatusBadge key={tier.label}>{tier.label} unknown</StatusBadge>
      ))}
    </div>
  )
}

export function DataSetsTableCard({
  dataSets,
  providersByID,
  storageConfirmations,
  total,
  page,
  totalPages,
  loading,
  error,
  onPageChange,
  onSelect,
}: {
  dataSets: ObservabilityDataSetObservation[]
  providersByID: Map<string, ObservabilityProviderObservation>
  storageConfirmations: Record<string, number>
  total: number
  page: number
  totalPages: number
  loading?: boolean
  error?: string
  onPageChange: (page: number) => void
  onSelect: (item: ObservabilityDataSetObservation) => void
}) {
  return (
    <div className="flex flex-col gap-3">
      <DataTableFrame>
        <Table className="min-w-[860px]">
          <TableHeader>
            <TableRow className={tableHeaderRowClassName}>
              <TableHead className="whitespace-nowrap px-3 py-2">Bucket</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Replica</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Provider</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Data set</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Local status</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Health</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2 text-right">Pieces</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Checked</TableHead>
              <TableHead className="w-10 px-3 py-2">
                <span className="sr-only">Details</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <InventoryLoadingRow colSpan={9} />
            ) : error ? (
              <InventoryErrorRow colSpan={9} message={error} />
            ) : dataSets.length > 0 ? (
              dataSets.map((dataSet) => (
                <TableRow key={dataSet.facts.local_data_set_id} {...clickableRowProps(() => onSelect(dataSet))}>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <Link
                      to="/buckets/$name"
                      params={{ name: dataSet.facts.bucket_name }}
                      className="font-medium hover:underline"
                    >
                      {dataSet.facts.bucket_name}
                    </Link>
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    {replicaLabel(dataSet.facts.copy_index)}
                  </TableCell>
                  <TableCell className="px-3 py-2">
                    <ProviderNameCell
                      providerID={dataSet.facts.provider_id}
                      profile={providersByID.get(dataSet.facts.provider_id)?.provider_profile}
                      showLocation={false}
                    />
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <OptionalCopyableValue label="Chain data set" value={dataSetChainIDValue(dataSet)} />
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <div className="flex items-center gap-2">
                      <StatusBadge tone={localStatusTone(dataSet.facts.local_status)}>
                        {localStatusLabel(dataSet.facts.local_status)}
                      </StatusBadge>
                      <StorageConfirmationsBadge
                        count={storageConfirmations[String(dataSet.facts.local_data_set_id)] ?? 0}
                      />
                    </div>
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <TopologySignalBadge status={dataSet.signal.status} signal={dataSet.signal} />
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2 text-right">
                    {activePiecesValue(dataSet.facts)}
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                    {freshnessLabel(dataSet.signal.freshness)}
                  </TableCell>
                  <TableCell className="px-3 py-2 text-right">
                    <RowDetailsButton
                      label={`${dataSet.facts.bucket_name} ${replicaLabel(dataSet.facts.copy_index)}`}
                      onClick={() => onSelect(dataSet)}
                    />
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <InventoryEmptyRow
                colSpan={9}
                title="No data sets found"
                description="No data set observations match the current filters."
              />
            )}
          </TableBody>
        </Table>
      </DataTableFrame>
      <CursorPagination
        summary={totalPages > 1 ? `Page ${page} of ${totalPages} · ${total} total` : undefined}
        hasPrevious={page > 1}
        hasNext={page < totalPages}
        onPrevious={() => onPageChange(page - 1)}
        onNext={() => onPageChange(page + 1)}
      />
    </div>
  )
}

function InventoryEmptyRow({ colSpan, title, description }: { colSpan: number; title: string; description: string }) {
  return (
    <TableRow>
      <TableCell colSpan={colSpan} className="h-60">
        <Empty className="border-0">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <Database />
            </EmptyMedia>
            <EmptyTitle>{title}</EmptyTitle>
            <EmptyDescription>{description}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      </TableCell>
    </TableRow>
  )
}

function InventoryLoadingRow({ colSpan }: { colSpan: number }) {
  return (
    <TableRow>
      <TableCell colSpan={colSpan} className="h-60">
        <div className="flex flex-col gap-3 p-6">
          <Skeleton className="h-6 w-48" />
          <Skeleton className="h-5 w-full" />
          <Skeleton className="h-5 w-5/6" />
          <Skeleton className="h-5 w-2/3" />
        </div>
      </TableCell>
    </TableRow>
  )
}

function InventoryErrorRow({ colSpan, message }: { colSpan: number; message: string }) {
  return (
    <TableRow>
      <TableCell colSpan={colSpan} className="h-60">
        <Empty className="min-h-56 border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <Database />
            </EmptyMedia>
            <EmptyTitle>Failed to load observations</EmptyTitle>
            <EmptyDescription>{message}</EmptyDescription>
          </EmptyHeader>
        </Empty>
      </TableCell>
    </TableRow>
  )
}
