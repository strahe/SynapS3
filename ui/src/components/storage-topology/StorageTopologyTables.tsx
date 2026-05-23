import { Link } from '@tanstack/react-router'
import { Database, Info } from 'lucide-react'
import type { ReactNode } from 'react'
import type { ObservabilityDataSetObservation, ObservabilityProviderObservation } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Button } from '@/components/ui/button'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Pagination, PaginationContent, PaginationItem } from '@/components/ui/pagination'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { replicaLabel } from '@/lib/storage-status-labels'
import {
  dataSetChainIDValue,
  formatOptionalTopologyText,
  freshnessLabel,
  localStatusTone,
  observabilityStatusTone,
  providerActiveFactBadge,
  providerPDPFactBadge,
  type StorageTopologyProviderRow,
} from '@/lib/storage-topology'
import { formatNumber } from '@/lib/utils'

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
}) {
  return (
    <div className="flex flex-col gap-3">
      <InventoryTableFrame>
        <Table className="min-w-[760px]">
          <TableHeader>
            <TableRow className="bg-muted/50">
              <TableHead className="whitespace-nowrap px-3 py-2">Provider</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Signal</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Registry</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Service URL</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Freshness</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <InventoryLoadingRow colSpan={6} />
            ) : error ? (
              <InventoryErrorRow colSpan={6} message={error} />
            ) : rows.length > 0 ? (
              rows.map((row) => (
                <TableRow key={row.providerID}>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <CopyableValue label="Provider" value={row.providerID} monospace maxLength={16} />
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <StatusBadge tone={observabilityStatusTone(row.status)}>{row.status}</StatusBadge>
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                    {row.provider ? providerFactsSummary(row.provider) : '—'}
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
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <Button variant="ghost" size="sm" onClick={() => onSelect(row)}>
                      <Info data-icon="inline-start" />
                      Details
                    </Button>
                  </TableCell>
                </TableRow>
              ))
            ) : (
              <InventoryEmptyRow
                colSpan={6}
                title="No providers found"
                description="No provider observations match the current filters."
              />
            )}
          </TableBody>
        </Table>
      </InventoryTableFrame>
      {contextNote && <div className="text-sm text-muted-foreground">{contextNote}</div>}
      <TopologyPagination total={total} page={page} totalPages={totalPages} onPageChange={onPageChange} />
    </div>
  )
}

export function DataSetsTableCard({
  dataSets,
  total,
  page,
  totalPages,
  loading,
  error,
  onPageChange,
  onSelect,
}: {
  dataSets: ObservabilityDataSetObservation[]
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
      <InventoryTableFrame>
        <Table className="min-w-[900px]">
          <TableHeader>
            <TableRow className="bg-muted/50">
              <TableHead className="whitespace-nowrap px-3 py-2">Bucket</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Replica</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Provider</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Chain Data Set</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Local Status</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Signal</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2 text-right">Pieces</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Freshness</TableHead>
              <TableHead className="whitespace-nowrap px-3 py-2">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <InventoryLoadingRow colSpan={9} />
            ) : error ? (
              <InventoryErrorRow colSpan={9} message={error} />
            ) : dataSets.length > 0 ? (
              dataSets.map((dataSet) => (
                <TableRow key={dataSet.facts.local_data_set_id}>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <Link
                      to="/buckets/$name"
                      params={{ name: dataSet.facts.bucket_name }}
                      className="font-medium hover:underline"
                    >
                      {dataSet.facts.bucket_name}
                    </Link>
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2 font-mono text-xs">
                    {replicaLabel(dataSet.facts.copy_index)}
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <CopyableValue label="Provider" value={dataSet.facts.provider_id} monospace maxLength={16} />
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <OptionalCopyableValue label="Chain data set" value={dataSetChainIDValue(dataSet)} />
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <StatusBadge tone={localStatusTone(dataSet.facts.local_status)}>
                      {dataSet.facts.local_status}
                    </StatusBadge>
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <StatusBadge tone={observabilityStatusTone(dataSet.signal.status)}>
                      {dataSet.signal.status}
                    </StatusBadge>
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2 text-right">
                    {dataSet.facts.active_piece_count === undefined
                      ? '—'
                      : formatNumber(dataSet.facts.active_piece_count)}
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                    {freshnessLabel(dataSet.signal.freshness)}
                  </TableCell>
                  <TableCell className="whitespace-nowrap px-3 py-2">
                    <Button variant="ghost" size="sm" onClick={() => onSelect(dataSet)}>
                      <Info data-icon="inline-start" />
                      Details
                    </Button>
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
      </InventoryTableFrame>
      <TopologyPagination total={total} page={page} totalPages={totalPages} onPageChange={onPageChange} />
    </div>
  )
}

function InventoryTableFrame({ children }: { children: ReactNode }) {
  return <div className="overflow-hidden rounded-lg border border-border">{children}</div>
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

function providerFactsSummary(provider: ObservabilityProviderObservation) {
  const active = providerActiveFactBadge(provider.facts.active)
  const pdp = providerPDPFactBadge(provider.facts.has_pdp)
  return `${active.label} · ${pdp.label}`
}

function OptionalCopyableValue({
  label,
  value,
  linkHref,
  maxLength,
}: {
  label: string
  value: string
  linkHref?: string
  maxLength?: number
}) {
  if (value === '—') return <span className="font-mono text-xs">—</span>
  return (
    <CopyableValue
      label={label}
      value={value}
      monospace
      maxLength={maxLength}
      linkHref={linkHref}
      external={Boolean(linkHref)}
    />
  )
}

function TopologyPagination({
  total,
  page,
  totalPages,
  onPageChange,
}: {
  total: number
  page: number
  totalPages: number
  onPageChange: (page: number) => void
}) {
  if (totalPages <= 1) {
    return null
  }

  return (
    <div className="flex items-center justify-between">
      <span className="text-sm text-muted-foreground">
        Page {page} of {totalPages} ({total} total)
      </span>
      <Pagination className="mx-0 w-auto justify-end">
        <PaginationContent>
          <PaginationItem>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={page === 1}
              onClick={() => {
                if (page > 1) onPageChange(page - 1)
              }}
            >
              Prev
            </Button>
          </PaginationItem>
          <PaginationItem>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={page >= totalPages}
              onClick={() => {
                if (page < totalPages) onPageChange(page + 1)
              }}
            >
              Next
            </Button>
          </PaginationItem>
        </PaginationContent>
      </Pagination>
    </div>
  )
}
