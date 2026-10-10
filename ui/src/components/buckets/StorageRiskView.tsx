import { useLocation } from '@tanstack/react-router'
import { ArrowLeft, FileIcon, Fingerprint, TriangleAlert } from 'lucide-react'
import { type FormEvent, useEffect, useState } from 'react'

import type {
  BucketStorageRiskDataSet,
  BucketStorageRiskVersion,
  StorageDataSetSummary,
  StorageHealthStatus,
} from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { CursorPagination } from '@/components/app/CursorPagination'
import { DataTableFrame, tableHeaderRowClassName } from '@/components/app/DataTable'
import { EmptyState } from '@/components/app/PageState'
import { ProviderIdentityCell } from '@/components/app/ProviderIdentityCell'
import { RelativeTime } from '@/components/app/RelativeTime'
import { StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { Button } from '@/components/ui/button'
import { Field, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { ScrollArea, ScrollBar } from '@/components/ui/scroll-area'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { dataSetStorageHealthDetailParts } from '@/lib/data-set-storage-health'
import { replicaLabel } from '@/lib/storage-status-labels'
import { localStatusLabel, observabilityStatusLabel } from '@/lib/storage-topology'
import { formatBytes, formatNumber } from '@/lib/utils'

const allRiskDataSetsValue = '__all__'

export type RiskMarkers = [keyMarker: string, versionMarker: string, createdAtMarker: string, staleBefore: string]

export function StorageRiskHeader({ onBack }: { onBack: () => void }) {
  return (
    <div className="flex min-w-0 flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
      <div className="min-w-0">
        <h2 className="truncate text-sm font-semibold">Affected versions</h2>
        <p className="text-sm text-muted-foreground">
          Retained versions with a replica on a data set that needs attention.
        </p>
      </div>
      <Button type="button" variant="outline" size="sm" onClick={onBack} className="w-fit">
        <ArrowLeft data-icon="inline-start" />
        Data sets
      </Button>
    </div>
  )
}

export function StorageRiskView({
  prefix,
  exactKey,
  dataSetID,
  dataSets,
  versions,
  hasMore,
  nextKeyMarker,
  nextVersionMarker,
  nextCreatedAtMarker,
  staleBefore,
  keyMarker,
  versionMarker,
  createdAtMarker,
  staleBeforeMarker,
  navigateToMarker,
  navigateToFilters,
  onOpenProvenance,
}: {
  prefix: string
  exactKey: string
  dataSetID?: number
  dataSets: StorageDataSetSummary[]
  versions: BucketStorageRiskVersion[]
  hasMore: boolean
  nextKeyMarker?: string
  nextVersionMarker?: string
  nextCreatedAtMarker?: string
  staleBefore?: string
  keyMarker: string
  versionMarker: string
  createdAtMarker: string
  staleBeforeMarker: string
  navigateToMarker: (
    keyMarker: string,
    versionMarker: string,
    createdAtMarker: string,
    staleBefore: string,
    history: RiskMarkers[]
  ) => void
  navigateToFilters: (next: { prefix?: string; key?: string; dataSetID?: number }) => void
  onOpenProvenance: (version: BucketStorageRiskVersion) => void
}) {
  const [prefixInput, setPrefixInput] = useState(prefix)
  const [keyInput, setKeyInput] = useState(exactKey)
  const [dataSetInput, setDataSetInput] = useState(dataSetID ? dataSetID.toString() : allRiskDataSetsValue)
  const markerHistory = useLocation({ select: (location) => location.state.bucketRiskMarkers }) ?? []
  const currentMarkers: RiskMarkers = [keyMarker, versionMarker, createdAtMarker, staleBeforeMarker]
  const onFirstPage = currentMarkers.every((marker) => !marker)
  const empty = versions.length === 0
  const filtered = Boolean(prefix || exactKey || dataSetID)

  useEffect(() => {
    setPrefixInput(prefix)
    setKeyInput(exactKey)
    setDataSetInput(dataSetID ? dataSetID.toString() : allRiskDataSetsValue)
  }, [prefix, exactKey, dataSetID])

  const applyFilters = (event?: FormEvent) => {
    event?.preventDefault()
    const key = keyInput
    const nextPrefix = key ? '' : prefixInput
    const parsedDataSetID = dataSetInput === allRiskDataSetsValue ? 0 : Number(dataSetInput)
    navigateToFilters({
      prefix: nextPrefix || undefined,
      key: key || undefined,
      dataSetID: Number.isInteger(parsedDataSetID) && parsedDataSetID > 0 ? parsedDataSetID : undefined,
    })
  }

  const clearFilters = () => {
    setPrefixInput('')
    setKeyInput('')
    setDataSetInput(allRiskDataSetsValue)
    navigateToFilters({})
  }

  return (
    <div className="flex flex-col gap-3">
      <form className="flex flex-col gap-2 sm:flex-row sm:flex-wrap sm:items-end" onSubmit={applyFilters}>
        <Field className="min-w-0 gap-1 sm:w-56">
          <FieldLabel htmlFor="storage-risk-prefix">Key prefix</FieldLabel>
          <Input
            id="storage-risk-prefix"
            value={prefixInput}
            disabled={Boolean(keyInput)}
            onChange={(event) => {
              const next = event.currentTarget.value
              setPrefixInput(next)
              if (next) setKeyInput('')
            }}
            placeholder="objects/"
          />
        </Field>
        <Field className="min-w-0 gap-1 sm:w-56">
          <FieldLabel htmlFor="storage-risk-key">Exact key</FieldLabel>
          <Input
            id="storage-risk-key"
            value={keyInput}
            disabled={Boolean(prefixInput)}
            onChange={(event) => {
              const next = event.currentTarget.value
              setKeyInput(next)
              if (next) setPrefixInput('')
            }}
            placeholder="objects/file.bin"
          />
        </Field>
        <Field className="min-w-0 gap-1 sm:w-56">
          <FieldLabel>Data set</FieldLabel>
          <Select value={dataSetInput} onValueChange={setDataSetInput}>
            <SelectTrigger className="w-full">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value={allRiskDataSetsValue}>All data sets</SelectItem>
                {dataSets.map((dataSet) => (
                  <SelectItem key={dataSet.id} value={dataSet.id.toString()}>
                    {storageRiskDataSetOptionLabel(dataSet)}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </Field>
        <div className="flex gap-2">
          <Button type="submit" variant="outline" size="sm">
            Apply
          </Button>
          <Button type="button" variant="ghost" size="sm" onClick={clearFilters}>
            Clear
          </Button>
        </div>
      </form>

      {empty ? (
        <EmptyState
          icon={<TriangleAlert />}
          title={filtered ? 'No affected versions match these filters' : 'No affected versions'}
        />
      ) : (
        <DataTableFrame>
          <ScrollArea className="w-full">
            <Table className="min-w-[960px]">
              <TableHeader>
                <TableRow className={tableHeaderRowClassName}>
                  <TableHead className="w-[26%] px-4">Object</TableHead>
                  <TableHead className="w-[16%] px-4">Version</TableHead>
                  <TableHead className="w-[8%] px-4 text-right">Size</TableHead>
                  <TableHead className="w-[29%] px-4">Risk data sets</TableHead>
                  <TableHead className="w-[12%] px-4">Readability</TableHead>
                  <TableHead className="w-[9%] px-4">Updated</TableHead>
                  <TableHead className="px-4 text-right">
                    <span className="sr-only">Actions</span>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {versions.map((version) => (
                  <TableRow key={version.version_id}>
                    <TableCell className="px-4">
                      <div className="flex min-w-0 flex-col gap-1">
                        <div className="flex min-w-0 items-center gap-1.5">
                          <FileIcon className="size-4 shrink-0 text-muted-foreground" />
                          <CopyableValue label="Object key" value={version.key} maxLength={64} />
                        </div>
                        <span className="text-xs text-muted-foreground">
                          {version.in_cache ? 'Cached locally' : 'Not cached locally'}
                        </span>
                      </div>
                    </TableCell>
                    <TableCell className="px-4">
                      <div className="flex min-w-0 items-center gap-2">
                        <CopyableValue label="Version" value={version.version_id} monospace maxLength={22} />
                        {version.is_current && (
                          <StatusBadge tone="success" className="shrink-0">
                            Current
                          </StatusBadge>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="px-4 text-right">{formatBytes(version.size)}</TableCell>
                    <TableCell className="px-4">
                      <RiskDataSetsCell dataSets={version.risk_data_sets} />
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground">
                      <span title="Recorded readable alternatives on non-risk data sets">
                        Recorded readable alternatives: {formatNumber(version.readable_alternative_count)}
                      </span>
                    </TableCell>
                    <TableCell className="px-4 text-muted-foreground">
                      <RelativeTime value={version.updated_at} />
                    </TableCell>
                    <TableCell className="px-4 text-right">
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            aria-label={`Open provenance for ${version.version_id}`}
                            onClick={() => onOpenProvenance(version)}
                          >
                            <Fingerprint />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>Provenance</TooltipContent>
                      </Tooltip>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <ScrollBar orientation="horizontal" />
          </ScrollArea>
        </DataTableFrame>
      )}

      <CursorPagination
        hasPrevious={!onFirstPage}
        hasNext={Boolean(hasMore && nextKeyMarker && nextVersionMarker && nextCreatedAtMarker && staleBefore)}
        onPrevious={() => {
          // Without a remembered page (a shared link), Previous returns to the first page.
          const previous = markerHistory[markerHistory.length - 1] ?? ['', '', '', '']
          navigateToMarker(...previous, markerHistory.slice(0, -1))
        }}
        onNext={() => {
          if (!nextKeyMarker || !nextVersionMarker || !nextCreatedAtMarker || !staleBefore) return
          navigateToMarker(nextKeyMarker, nextVersionMarker, nextCreatedAtMarker, staleBefore, [
            ...markerHistory,
            currentMarkers,
          ])
        }}
      />
    </div>
  )
}

function RiskDataSetsCell({ dataSets }: { dataSets: BucketStorageRiskDataSet[] }) {
  if (dataSets.length === 0) return <span className="text-muted-foreground">—</span>

  return (
    <div className="flex min-w-0 flex-col gap-2">
      {dataSets.map((dataSet) => {
        const detailParts = dataSetStorageHealthDetailParts({
          status: dataSet.local_status,
          storage_health: dataSet.storage_health,
        })
        const details = detailParts.join(' · ')

        return (
          <div key={dataSet.local_data_set_id} className="flex min-w-0 flex-col gap-1">
            <div className="flex min-w-0 flex-wrap items-center gap-1.5">
              <StatusBadge tone="neutral">{replicaLabel(dataSet.copy_index)}</StatusBadge>
              <ProviderIdentityCell providerID={dataSet.provider_id} identity={dataSet.provider_identity} />
              <StatusBadge tone={storageHealthStatusTone(dataSet.storage_health.status)}>
                {observabilityStatusLabel(dataSet.storage_health.status)}
              </StatusBadge>
              {dataSet.storage_health.stale && (
                <StatusBadge tone="warning" className="shrink-0">
                  Stale
                </StatusBadge>
              )}
            </div>
            <div className="min-w-0 truncate text-xs text-muted-foreground" title={details}>
              <span className="font-mono">#{dataSet.local_data_set_id}</span>
              {' · '}
              <span>Local status: {localStatusLabel(dataSet.local_status)}</span>
              {dataSet.data_set_id && (
                <>
                  {' · '}
                  <CopyableValue label="Data set ID" value={dataSet.data_set_id} monospace maxLength={18} />
                </>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}

function storageRiskDataSetOptionLabel(dataSet: StorageDataSetSummary) {
  return `${replicaLabel(dataSet.copy_index)} · #${dataSet.id}`
}

function storageHealthStatusTone(status: StorageHealthStatus): StatusTone {
  switch (status) {
    case 'available':
      return 'success'
    case 'degraded':
      return 'warning'
    case 'unavailable':
      return 'danger'
    case 'unknown':
      return 'neutral'
  }
}
