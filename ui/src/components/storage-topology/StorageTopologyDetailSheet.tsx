import { Link } from '@tanstack/react-router'
import { Database } from 'lucide-react'
import type { ReactNode } from 'react'
import type {
  ObservabilityDataSetObservation,
  ObservabilityProviderObservation,
  ObservabilitySignal,
} from '@/api/client'
import { StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { Button } from '@/components/ui/button'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { replicaLabel } from '@/lib/storage-status-labels'
import {
  dataSetDisplayLabel,
  dataSetTopologyPath,
  formatOptionalTopologyText,
  freshnessLabel,
  localStatusTone,
  observabilityStatusTone,
  type ResolvedStorageTopologySelection,
  relatedDataSetsForProviderNode,
  type StorageTopologyGraph,
} from '@/lib/storage-topology'
import { cn, formatNumber } from '@/lib/utils'

export function TopologyDetailSheet({
  selection,
  graph,
  providers,
  dataSets,
  onOpenChange,
}: {
  selection: ResolvedStorageTopologySelection | null
  graph: StorageTopologyGraph
  providers: ObservabilityProviderObservation[]
  dataSets: ObservabilityDataSetObservation[]
  onOpenChange: (open: boolean) => void
}) {
  const title = selection ? topologyDetailTitle(selection) : 'Topology Details'
  const description = selection ? topologyDetailDescription(selection) : undefined

  return (
    <Sheet open={Boolean(selection)} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="data-[side=right]:w-[440px] data-[side=right]:max-w-[calc(100vw-2rem)] data-[side=right]:sm:max-w-[440px]"
      >
        <SheetHeader>
          <div className="flex min-w-0 items-center gap-2 pr-8">
            <SheetTitle className="truncate">{title}</SheetTitle>
            {selection && (
              <StatusBadge tone={topologySelectionTone(selection)}>{topologySelectionBadge(selection)}</StatusBadge>
            )}
          </div>
          {description && <SheetDescription className="break-words font-mono text-xs">{description}</SheetDescription>}
        </SheetHeader>
        <ScrollArea className="min-h-0 flex-1">
          <div className="flex flex-col gap-5 p-4 pt-0">
            {selection && (
              <InspectorContent selection={selection} graph={graph} providers={providers} dataSets={dataSets} />
            )}
          </div>
        </ScrollArea>
      </SheetContent>
    </Sheet>
  )
}

function topologyDetailTitle(selection: ResolvedStorageTopologySelection) {
  if (selection.type === 'edge') return 'Relationship'
  if (selection.type === 'provider') return `Provider #${selection.provider.facts.provider_id}`
  if (selection.type === 'data-set') return dataSetDisplayLabel(selection.dataSet)

  switch (selection.node.kind) {
    case 'bucket':
      return selection.node.data.bucketName ?? selection.node.label
    case 'data-set':
      return dataSetDisplayLabel(selection.node.data)
    case 'provider':
      return `Provider #${selection.node.data.providerID}`
  }
}

function topologyDetailDescription(selection: ResolvedStorageTopologySelection) {
  if (selection.type === 'edge') return selection.edge.data.path
  if (selection.type === 'provider') return `Provider #${selection.provider.facts.provider_id}`
  if (selection.type === 'data-set') return dataSetTopologyPath(selection.dataSet)
  return selection.node.data.path
}

function topologySelectionTone(selection: ResolvedStorageTopologySelection): StatusTone {
  if (selection.type === 'node') return selection.node.tone
  if (selection.type === 'edge') return selection.edge.tone
  if (selection.type === 'provider') return observabilityStatusTone(selection.provider.signal.status)
  return observabilityStatusTone(selection.dataSet.signal.status)
}

function topologySelectionBadge(selection: ResolvedStorageTopologySelection) {
  if (selection.type === 'data-set') return 'data set'
  return selection.type
}

function InspectorContent({
  selection,
  graph,
  providers,
  dataSets,
}: {
  selection: ResolvedStorageTopologySelection
  graph: StorageTopologyGraph
  providers: ObservabilityProviderObservation[]
  dataSets: ObservabilityDataSetObservation[]
}) {
  if (selection.type === 'edge') {
    const edge = selection.edge
    const dataSet = dataSets.find((item) => item.facts.local_data_set_id === edge.data.localDataSetID)
    const provider = providers.find((item) => item.facts.provider_id === edge.data.providerID)
    return (
      <>
        <DetailBlock title="Relationship">
          <DetailRow label="Path" value={edge.data.path} mono />
          <DetailRow label="Edge" value={edge.kind} />
          <DetailRow label="Bucket" value={formatOptionalTopologyText(edge.data.bucketName)} />
          <DetailRow label="Chain data set" value={formatOptionalID(edge.data.chainDataSetID)} mono />
          <DetailRow label="Client data set" value={formatOptionalID(edge.data.clientDataSetID)} mono />
          <DetailRow label="Provider" value={edge.data.providerID ? `#${edge.data.providerID}` : '—'} mono />
        </DetailBlock>
        {dataSet && <DataSetDetailContent dataSet={dataSet} provider={provider} compact />}
      </>
    )
  }

  if (selection.type === 'provider') {
    return <ProviderDetailContent provider={selection.provider} />
  }

  if (selection.type === 'data-set') {
    return <DataSetDetailContent dataSet={selection.dataSet} provider={selection.provider} />
  }

  const node = selection.node
  if (node.kind === 'bucket') {
    const relatedDataSets = dataSets
      .filter((dataSet) => node.data.dataSetIDs?.includes(dataSet.facts.local_data_set_id))
      .sort((left, right) => left.facts.copy_index - right.facts.copy_index)
    return (
      <>
        <DetailBlock title="Bucket">
          <DetailRow label="Name" value={node.data.bucketName ?? node.label} />
          <DetailRow label="Bucket ID" value={`#${node.data.bucketID}`} mono />
          <DetailRow label="Replicas" value={formatNumber(node.data.replicaCount ?? 0)} />
          <DetailRow label="Issues" value={formatNumber(node.data.issueCount ?? 0)} />
        </DetailBlock>
        <RelatedReplicas dataSets={relatedDataSets} />
        {node.data.bucketName && (
          <Button asChild variant="outline" size="sm">
            <Link to="/buckets/$name" params={{ name: node.data.bucketName }}>
              <Database data-icon="inline-start" />
              Open Bucket
            </Link>
          </Button>
        )}
      </>
    )
  }

  if (node.kind === 'data-set') {
    const dataSet = dataSets.find((item) => item.facts.local_data_set_id === node.data.localDataSetID)
    const provider = providers.find((item) => item.facts.provider_id === node.data.providerID)
    return dataSet ? <DataSetDetailContent dataSet={dataSet} provider={provider} /> : null
  }

  const provider = providers.find((item) => item.facts.provider_id === node.data.providerID)
  const relatedDataSets = node.data.providerID
    ? relatedDataSetsForProviderNode(graph, dataSets, node.data.providerID)
    : []
  return (
    <>
      <DetailBlock title="Relationship">
        <DetailRow label="Provider" value={`#${node.data.providerID}`} mono />
        <DetailRow label="Buckets" value={formatRelatedBuckets(relatedDataSets)} />
        <DetailRow label="Data sets" value={formatRelatedDataSets(relatedDataSets)} mono />
      </DetailBlock>
      {provider && <ProviderDetailContent provider={provider} />}
    </>
  )
}

function DataSetDetailContent({
  dataSet,
  provider,
  compact,
}: {
  dataSet: ObservabilityDataSetObservation
  provider?: ObservabilityProviderObservation
  compact?: boolean
}) {
  return (
    <>
      {!compact && (
        <DetailBlock title="Relationship">
          <DetailRow label="Path" value={dataSetTopologyPath(dataSet)} mono />
          <DetailRow
            label="Provider signal"
            value={provider?.signal.status ?? 'unknown'}
            badgeTone={provider ? observabilityStatusTone(provider.signal.status) : 'neutral'}
          />
        </DetailBlock>
      )}
      <DetailBlock title="Facts">
        <DetailRow label="Bucket" value={dataSet.facts.bucket_name} />
        <DetailRow label="Replica" value={replicaLabel(dataSet.facts.copy_index)} />
        <DetailRow label="Provider" value={`#${dataSet.facts.provider_id}`} mono />
        <DetailRow label="Chain data set ID" value={formatOptionalID(dataSet.facts.chain_data_set_id)} mono />
        <DetailRow label="Client data set ID" value={formatOptionalID(dataSet.facts.client_data_set_id)} mono />
        <DetailRow label="Internal local ID" value={`#${dataSet.facts.local_data_set_id}`} mono />
        <DetailRow
          label="Local status"
          value={dataSet.facts.local_status}
          badgeTone={localStatusTone(dataSet.facts.local_status)}
        />
        <DetailRow
          label="Active pieces"
          value={dataSet.facts.active_piece_count === undefined ? '—' : formatNumber(dataSet.facts.active_piece_count)}
        />
      </DetailBlock>
      <SignalDetailBlock signal={dataSet.signal} />
    </>
  )
}

function ProviderDetailContent({ provider }: { provider: ObservabilityProviderObservation }) {
  return (
    <>
      <DetailBlock title="Facts">
        <DetailRow label="Provider" value={`#${provider.facts.provider_id}`} mono />
        <DetailRow label="Active" value={formatOptionalBool(provider.facts.active)} />
        <DetailRow label="Has PDP" value={formatOptionalBool(provider.facts.has_pdp)} />
        <DetailRow label="Health" value={formatOptionalTopologyText(provider.facts.health_status)} />
        <DetailRow label="Service URL" value={formatOptionalTopologyText(provider.facts.service_url)} mono />
      </DetailBlock>
      <SignalDetailBlock signal={provider.signal} />
    </>
  )
}

function RelatedReplicas({ dataSets }: { dataSets: ObservabilityDataSetObservation[] }) {
  if (dataSets.length === 0) {
    return null
  }

  return (
    <section className="flex flex-col gap-3">
      <h3 className="text-sm font-semibold">Replicas</h3>
      <div className="flex flex-col gap-2">
        {dataSets.map((dataSet) => (
          <div key={dataSet.facts.local_data_set_id} className="rounded-md border p-3">
            <div className="flex min-w-0 items-center justify-between gap-2">
              <div className="min-w-0 truncate text-sm font-medium">
                {replicaLabel(dataSet.facts.copy_index)} · Provider #{dataSet.facts.provider_id}
              </div>
              <StatusBadge tone={observabilityStatusTone(dataSet.signal.status)}>{dataSet.signal.status}</StatusBadge>
            </div>
            <div className="mt-2 truncate text-xs text-muted-foreground">{dataSetDisplayLabel(dataSet)}</div>
          </div>
        ))}
      </div>
    </section>
  )
}

function SignalDetailBlock({ signal }: { signal: ObservabilitySignal }) {
  return (
    <DetailBlock title="Signal">
      <DetailRow label="Status" value={signal.status} badgeTone={observabilityStatusTone(signal.status)} />
      <DetailRow label="Freshness" value={freshnessLabel(signal.freshness)} />
      <DetailRow label="Last error" value={formatOptionalTopologyText(signal.last_error)} />
    </DetailBlock>
  )
}

function DetailBlock({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="flex flex-col gap-3">
      <h3 className="text-sm font-semibold">{title}</h3>
      <dl className="grid grid-cols-1 gap-x-4 gap-y-3 text-sm sm:grid-cols-[8rem_minmax(0,1fr)]">{children}</dl>
    </section>
  )
}

function DetailRow({
  label,
  value,
  mono,
  badgeTone,
}: {
  label: string
  value: string
  mono?: boolean
  badgeTone?: StatusTone
}) {
  return (
    <>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={cn('min-w-0 break-words', mono && 'font-mono text-xs')}>
        {badgeTone ? <StatusBadge tone={badgeTone}>{value}</StatusBadge> : value}
      </dd>
    </>
  )
}

function formatRelatedBuckets(dataSets: ObservabilityDataSetObservation[]) {
  const buckets = Array.from(new Set(dataSets.map((dataSet) => dataSet.facts.bucket_name))).sort()
  return buckets.length === 0 ? '—' : buckets.join(', ')
}

function formatRelatedDataSets(dataSets: ObservabilityDataSetObservation[]) {
  if (dataSets.length === 0) return '—'
  return dataSets
    .map(
      (dataSet) =>
        `${dataSet.facts.bucket_name} ${replicaLabel(dataSet.facts.copy_index)} -> ${dataSetDisplayLabel(dataSet)}`
    )
    .join(', ')
}

function formatOptionalBool(value: boolean | undefined) {
  if (value === undefined) return '—'
  return value ? 'yes' : 'no'
}

function formatOptionalID(value: string | undefined) {
  const text = formatOptionalTopologyText(value)
  return text === '—' ? text : `#${text}`
}
