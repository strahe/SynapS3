import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { Database, RefreshCw, TriangleAlert } from 'lucide-react'
import { lazy, Suspense, useEffect, useMemo, useState } from 'react'
import type { ObservabilityDataSetObservation, ObservabilityProviderObservation } from '@/api/client'
import { PageHeader } from '@/components/app/PageHeader'
import { TopologyDetailSheet } from '@/components/storage-topology/StorageTopologyDetailSheet'
import { DataSetsTableCard, ProvidersTableCard } from '@/components/storage-topology/StorageTopologyTables'
import { type StorageTopologyTab, StorageTopologyToolbar } from '@/components/storage-topology/StorageTopologyToolbar'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Skeleton } from '@/components/ui/skeleton'
import { useObservabilityDataSets, useObservabilityProviders } from '@/hooks/queries'
import {
  buildStorageTopologyGraph,
  buildTopologyProviderOptions,
  clampPageForLoadedTotal,
  findDataSetTopologyNodeByLocalID,
  findProviderTopologyNode,
  findStorageTopologySelection,
  mergeTopologyDataSetSnapshots,
  type ObservabilityStatusFilter,
  observabilityStatusOptions,
  paginateItems,
  providerRowsForTopologyContext,
  providerRowsFromInventory,
  reconcileStorageTopologySelectionSearch,
  resolveStorageTopologySelection,
  type SourcedStorageTopologySelection,
  type StorageTopologyFilters,
  type StorageTopologyGraph,
  type StorageTopologyPinnedContext,
  type StorageTopologyProviderRow,
  type StorageTopologySelection,
  snapshotPageIsPartial,
  storageTopologyAllFilterValue,
  storageTopologyDataSetSelectionSearch,
  storageTopologyPinnedContextForSelection,
  topologySummaryLabel,
} from '@/lib/storage-topology'
import {
  cleanStorageTopologySearch,
  clearStorageTopologySelectionSearch,
  hasStorageTopologySelectionSearch,
  nonEmptyStorageTopologySearchString,
  nonNegativeStorageTopologySearchInteger,
} from '@/lib/storage-topology-search'

const TopologyGraphCanvas = lazy(() => import('@/components/storage-topology/TopologyGraphCanvas'))

type TopologyTab = StorageTopologyTab
type TopologySelectionState = SourcedStorageTopologySelection
type PinnedTopologyContext = StorageTopologyPinnedContext & { source: 'route' | 'local' }
type StorageTopologySearch = {
  tab?: TopologyTab
  status?: Exclude<ObservabilityStatusFilter, 'all'>
  provider?: string
  bucket?: string
  chain_data_set_id?: string
  local_data_set_id?: number
  selection_provider?: string
  selection_bucket?: string
}

const allStatusFilterValue = 'all'
const allEntityFilterValue = storageTopologyAllFilterValue
const pageSize = 20
const graphSnapshotLimit = 500
const emptyProviderObservations: ObservabilityProviderObservation[] = []
const emptyDataSetObservations: ObservabilityDataSetObservation[] = []
const topologyTabValues = new Set<TopologyTab>(['topology', 'providers', 'data-sets'])
const topologyStatusSearchValues = new Set<string>(observabilityStatusOptions.filter((status) => status !== 'all'))

export const Route = createFileRoute('/storage-topology')({
  validateSearch: (search: Record<string, unknown>): StorageTopologySearch => {
    const tab =
      typeof search.tab === 'string' && topologyTabValues.has(search.tab as TopologyTab) ? search.tab : undefined
    const status =
      typeof search.status === 'string' && topologyStatusSearchValues.has(search.status) ? search.status : undefined
    const provider = nonEmptyStorageTopologySearchString(search.provider)
    const bucket = nonEmptyStorageTopologySearchString(search.bucket)
    const chainDataSetID = nonEmptyStorageTopologySearchString(search.chain_data_set_id)
    const localDataSetID = nonNegativeStorageTopologySearchInteger(search.local_data_set_id)
    const selectionProvider = nonEmptyStorageTopologySearchString(search.selection_provider)
    const selectionBucket = nonEmptyStorageTopologySearchString(search.selection_bucket)

    return {
      tab: tab as TopologyTab | undefined,
      status: status as Exclude<ObservabilityStatusFilter, 'all'> | undefined,
      provider,
      bucket,
      chain_data_set_id: chainDataSetID,
      local_data_set_id: localDataSetID,
      selection_provider: selectionProvider,
      selection_bucket: selectionBucket,
    }
  },
  component: StorageTopologyPage,
})

function StorageTopologyPage() {
  const search = Route.useSearch()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const tab = search.tab ?? 'topology'
  const filters = useMemo<StorageTopologyFilters>(
    () => ({
      status: search.status ?? allStatusFilterValue,
      provider: search.provider ?? allEntityFilterValue,
      bucket: search.bucket ?? allEntityFilterValue,
    }),
    [search.bucket, search.provider, search.status]
  )
  const hasSearchSelection = hasStorageTopologySelectionSearch(search)
  const [providerPage, setProviderPage] = useState(1)
  const [dataSetPage, setDataSetPage] = useState(1)
  const [selection, setSelection] = useState<TopologySelectionState | null>(null)
  const [pinnedContext, setPinnedContext] = useState<PinnedTopologyContext | null>(null)
  const selectionProviderID = search.selection_provider ?? observabilityProviderParam(filters.provider)
  const selectionBucketName = search.selection_bucket ?? observabilityBucketParam(filters.bucket)

  const providerSnapshot = useObservabilityProviders({ limit: graphSnapshotLimit, offset: 0 })
  const dataSetSnapshot = useObservabilityDataSets({ limit: graphSnapshotLimit, offset: 0 })
  const scopedDeepLinkSnapshotEnabled = hasSearchSelection && Boolean(selectionProviderID || selectionBucketName)
  const scopedDeepLinkDataSetSnapshot = useObservabilityDataSets(
    {
      provider_id: selectionProviderID,
      bucket: selectionBucketName,
      limit: graphSnapshotLimit,
      offset: 0,
    },
    scopedDeepLinkSnapshotEnabled
  )
  const providerInventoryUsesSnapshot = filters.bucket !== allEntityFilterValue
  const providerInventoryEnabled = tab === 'providers' && !providerInventoryUsesSnapshot
  const dataSetInventoryEnabled = tab === 'data-sets'
  const providerInventory = useObservabilityProviders(
    {
      status: observabilityStatusParam(filters.status),
      provider_id: observabilityProviderParam(filters.provider),
      limit: pageSize,
      offset: (providerPage - 1) * pageSize,
    },
    providerInventoryEnabled
  )
  const dataSetInventory = useObservabilityDataSets(
    {
      status: observabilityStatusParam(filters.status),
      provider_id: observabilityProviderParam(filters.provider),
      bucket: observabilityBucketParam(filters.bucket),
      limit: pageSize,
      offset: (dataSetPage - 1) * pageSize,
    },
    dataSetInventoryEnabled
  )

  const providers = useMemo(
    () => providerSnapshot.data?.items ?? emptyProviderObservations,
    [providerSnapshot.data?.items]
  )
  const baseDataSets = useMemo(
    () => dataSetSnapshot.data?.items ?? emptyDataSetObservations,
    [dataSetSnapshot.data?.items]
  )
  const scopedDataSets = useMemo(
    () =>
      scopedDeepLinkSnapshotEnabled
        ? (scopedDeepLinkDataSetSnapshot.data?.items ?? emptyDataSetObservations)
        : emptyDataSetObservations,
    [scopedDeepLinkDataSetSnapshot.data?.items, scopedDeepLinkSnapshotEnabled]
  )
  const pinnedProviders = pinnedContext?.providers ?? emptyProviderObservations
  const pinnedDataSets = pinnedContext?.dataSets ?? emptyDataSetObservations
  const graphProviders = useMemo(
    () => mergeProviderObservations(providers, [], pinnedProviders),
    [pinnedProviders, providers]
  )
  const dataSets = useMemo(
    () =>
      mergeDataSetObservations(
        pinnedDataSets,
        mergeTopologyDataSetSnapshots(baseDataSets, scopedDataSets, scopedDeepLinkSnapshotEnabled)
      ),
    [baseDataSets, pinnedDataSets, scopedDataSets, scopedDeepLinkSnapshotEnabled]
  )
  const providerOptions = useMemo(() => buildTopologyProviderOptions(dataSets), [dataSets])
  const bucketOptions = useMemo(
    () => Array.from(new Set(dataSets.map((dataSet) => dataSet.facts.bucket_name))).sort(),
    [dataSets]
  )
  const graph = useMemo(
    () => buildStorageTopologyGraph(graphProviders, dataSets, filters),
    [dataSets, filters, graphProviders]
  )
  const providerContextFilters = useMemo<StorageTopologyFilters>(
    () => ({ ...filters, status: allStatusFilterValue }),
    [filters]
  )
  const providerContextGraph = useMemo(
    () => buildStorageTopologyGraph(graphProviders, dataSets, providerContextFilters),
    [graphProviders, dataSets, providerContextFilters]
  )
  const providerContextRows = useMemo(
    () => providerRowsForTopologyContext(providerContextGraph, graphProviders, filters.status),
    [filters.status, providerContextGraph, graphProviders]
  )
  const snapshotProviderPageData = useMemo(
    () => paginateItems(providerContextRows, { page: providerPage, pageSize }),
    [providerContextRows, providerPage]
  )
  const providerInventoryRows = useMemo(
    () =>
      providerInventoryEnabled
        ? (providerInventory.data?.items ?? emptyProviderObservations)
        : emptyProviderObservations,
    [providerInventory.data?.items, providerInventoryEnabled]
  )
  const providerTableData = useMemo(
    () =>
      providerInventoryUsesSnapshot
        ? snapshotProviderPageData.items
        : providerRowsFromInventory(providerInventoryRows, graph),
    [graph, providerInventoryRows, providerInventoryUsesSnapshot, snapshotProviderPageData.items]
  )
  const providerTableTotal = providerInventoryUsesSnapshot
    ? providerContextRows.length
    : (providerInventory.data?.total ?? 0)
  const providerTablePage = providerInventoryUsesSnapshot ? snapshotProviderPageData.page : providerPage
  const providerTableTotalPages = providerInventoryUsesSnapshot
    ? snapshotProviderPageData.totalPages
    : totalPagesFor(providerInventory.data?.total ?? 0, pageSize)
  const dataSetTableData = useMemo(
    () =>
      dataSetInventoryEnabled ? (dataSetInventory.data?.items ?? emptyDataSetObservations) : emptyDataSetObservations,
    [dataSetInventory.data?.items, dataSetInventoryEnabled]
  )
  const dataSetTableTotal = dataSetInventory.data?.total ?? 0
  const dataSetTableTotalPages = totalPagesFor(dataSetTableTotal, pageSize)
  const unpinnedDetailProviders = useMemo(
    () => mergeProviderObservations(providers, providerTableData),
    [providerTableData, providers]
  )
  const unpinnedDetailDataSets = useMemo(
    () => mergeDataSetObservations(dataSets, dataSetTableData),
    [dataSetTableData, dataSets]
  )
  const detailProviders = useMemo(
    () => mergeProviderObservations(unpinnedDetailProviders, [], pinnedProviders),
    [pinnedProviders, unpinnedDetailProviders]
  )
  const detailDataSets = useMemo(
    () => mergeDataSetObservations(pinnedDataSets, unpinnedDetailDataSets),
    [pinnedDataSets, unpinnedDetailDataSets]
  )
  const snapshotLoading =
    providerSnapshot.isLoading ||
    dataSetSnapshot.isLoading ||
    (scopedDeepLinkSnapshotEnabled && scopedDeepLinkDataSetSnapshot.isLoading)
  const snapshotError =
    providerSnapshot.error ||
    dataSetSnapshot.error ||
    (scopedDeepLinkSnapshotEnabled ? scopedDeepLinkDataSetSnapshot.error : null)
  const snapshotPartial =
    (providerSnapshot.data ? snapshotPageIsPartial(providerSnapshot.data) : false) ||
    (dataSetSnapshot.data ? snapshotPageIsPartial(dataSetSnapshot.data) : false) ||
    (scopedDeepLinkSnapshotEnabled && scopedDeepLinkDataSetSnapshot.data
      ? snapshotPageIsPartial(scopedDeepLinkDataSetSnapshot.data)
      : false)
  const resolvedSelection = useMemo(
    () => resolveStorageTopologySelection(selection?.selection ?? null, graph, detailProviders, detailDataSets),
    [selection, graph, detailProviders, detailDataSets]
  )
  const refreshing =
    providerSnapshot.isFetching ||
    dataSetSnapshot.isFetching ||
    (scopedDeepLinkSnapshotEnabled && scopedDeepLinkDataSetSnapshot.isFetching) ||
    (providerInventoryEnabled && providerInventory.isFetching) ||
    (dataSetInventoryEnabled && dataSetInventory.isFetching)

  useEffect(() => {
    if (providerInventoryUsesSnapshot) return
    if (!providerInventory.data) return
    const nextPage = clampPageForLoadedTotal(providerPage, providerInventory.data.total, pageSize)
    if (nextPage !== providerPage) setProviderPage(nextPage)
  }, [providerInventory.data, providerInventoryUsesSnapshot, providerPage])

  useEffect(() => {
    if (!dataSetInventory.data) return
    const nextPage = clampPageForLoadedTotal(dataSetPage, dataSetInventory.data.total, pageSize)
    if (nextPage !== dataSetPage) setDataSetPage(nextPage)
  }, [dataSetInventory.data, dataSetPage])

  useEffect(() => {
    if (hasSearchSelection) return
    setSelection((current) => reconcileStorageTopologySelectionSearch(current, false))
    setPinnedContext((current) => (current?.source === 'route' ? null : current))
  }, [hasSearchSelection])

  useEffect(() => {
    if (snapshotLoading || !hasSearchSelection) return
    const topologySelection = findStorageTopologySelection(graph, {
      chainDataSetID: search.chain_data_set_id,
      providerID: selectionProviderID,
      bucketName: selectionBucketName,
      localDataSetID: search.local_data_set_id,
    })
    const nextSelection =
      topologySelection ??
      (search.local_data_set_id != null
        ? { type: 'data-set' as const, localDataSetID: search.local_data_set_id }
        : null)
    const nextState = nextSelection ? { source: 'route' as const, selection: nextSelection } : null
    setSelection((current) => (sameStorageTopologySelectionState(current, nextState) ? current : nextState))
    const nextPinnedContext = nextSelection
      ? {
          source: 'route' as const,
          ...storageTopologyPinnedContextForSelection(
            nextSelection,
            graph,
            unpinnedDetailProviders,
            unpinnedDetailDataSets
          ),
        }
      : null
    setPinnedContext((current) => (samePinnedTopologyContext(current, nextPinnedContext) ? current : nextPinnedContext))
  }, [
    graph,
    hasSearchSelection,
    search.chain_data_set_id,
    search.local_data_set_id,
    selectionBucketName,
    selectionProviderID,
    snapshotLoading,
    unpinnedDetailDataSets,
    unpinnedDetailProviders,
  ])

  useEffect(() => {
    if (!selection || resolvedSelection || snapshotLoading) return
    setSelection(null)
    setPinnedContext(null)
  }, [resolvedSelection, selection, snapshotLoading])

  function pinSelectionContext(
    source: PinnedTopologyContext['source'],
    nextSelection: StorageTopologySelection | null
  ) {
    const nextPinnedContext = nextSelection
      ? {
          source,
          ...storageTopologyPinnedContextForSelection(
            nextSelection,
            graph,
            unpinnedDetailProviders,
            unpinnedDetailDataSets
          ),
        }
      : null
    setPinnedContext((current) => (samePinnedTopologyContext(current, nextPinnedContext) ? current : nextPinnedContext))
  }

  function updateFilters(next: Partial<StorageTopologyFilters>) {
    setProviderPage(1)
    setDataSetPage(1)
    setSelection(null)
    setPinnedContext(null)
    updateSearch(
      clearStorageTopologySelectionSearch({
        ...search,
        status: next.status === undefined ? search.status : searchStatusValue(next.status),
        provider: next.provider === undefined ? search.provider : searchFilterValue(next.provider),
        bucket: next.bucket === undefined ? search.bucket : searchFilterValue(next.bucket),
      }) as StorageTopologySearch
    )
  }

  function selectProvider(row: StorageTopologyProviderRow) {
    const node = row.node ?? findProviderTopologyNode(graph, row.providerID)
    const nextSelection = node
      ? { type: 'node' as const, id: node.id, kind: node.kind }
      : { type: 'provider' as const, providerID: row.providerID }
    setSelection({ source: 'local', selection: nextSelection })
    pinSelectionContext('local', nextSelection)
    clearSelectionSearchInRoute()
  }

  function selectDataSet(dataSet: ObservabilityDataSetObservation) {
    const node = findDataSetTopologyNodeByLocalID(graph, dataSet.facts.local_data_set_id)
    const nextSelection = node
      ? { type: 'node' as const, id: node.id, kind: node.kind }
      : { type: 'data-set' as const, localDataSetID: dataSet.facts.local_data_set_id }
    setSelection({ source: 'route', selection: nextSelection })
    pinSelectionContext('route', nextSelection)
    updateSearch({ ...search, ...storageTopologyDataSetSelectionSearch(dataSet) })
  }

  function selectTopologySelection(nextSelection: StorageTopologySelection | null) {
    setSelection(nextSelection ? { source: 'local', selection: nextSelection } : null)
    pinSelectionContext('local', nextSelection)
    clearSelectionSearchInRoute()
  }

  function updateTab(nextTab: string) {
    const next = nextTab === 'providers' || nextTab === 'data-sets' ? nextTab : 'topology'
    setProviderPage(1)
    setDataSetPage(1)
    setSelection(null)
    setPinnedContext(null)
    updateSearch(
      clearStorageTopologySelectionSearch({
        ...search,
        tab: next === 'topology' ? undefined : next,
      }) as StorageTopologySearch
    )
  }

  function updateSearch(next: StorageTopologySearch) {
    navigate({ to: '/storage-topology', search: cleanStorageTopologySearch(next) as StorageTopologySearch })
  }

  function clearSelectionSearchInRoute() {
    if (hasStorageTopologySelectionSearch(search) || search.selection_provider || search.selection_bucket) {
      updateSearch(clearStorageTopologySelectionSearch(search) as StorageTopologySearch)
    }
  }

  function refreshObservability() {
    qc.invalidateQueries({ queryKey: ['observabilityProviders'] })
    qc.invalidateQueries({ queryKey: ['observabilityDataSets'] })
  }

  const pageClassName =
    tab === 'topology'
      ? 'flex h-[calc(100svh-3.5rem)] min-h-0 min-w-0 flex-col gap-4 overflow-hidden px-6 pt-6 pb-0 md:h-svh'
      : 'flex min-h-[calc(100vh-3rem)] min-w-0 flex-col gap-4 p-6'

  return (
    <div className={pageClassName}>
      <PageHeader
        className="shrink-0"
        title="Storage Topology"
        actions={
          <div className="flex items-center gap-3">
            <div className="text-sm text-muted-foreground">{topologySummaryLabel(graph)}</div>
            <Button variant="outline" size="sm" onClick={refreshObservability} disabled={refreshing}>
              <RefreshCw data-icon="inline-start" className={refreshing ? 'animate-spin' : undefined} />
              Refresh
            </Button>
          </div>
        }
      />

      <div className="shrink-0">
        <StorageTopologyToolbar
          tab={tab}
          filters={filters}
          providerOptions={providerOptions}
          bucketOptions={bucketOptions}
          onTabChange={updateTab}
          onChange={updateFilters}
        />
      </div>

      {!snapshotLoading && !snapshotError && snapshotPartial && (
        <div className="shrink-0">
          <TopologyPartialAlert />
        </div>
      )}

      {snapshotLoading ? (
        <TopologyLoading />
      ) : snapshotError ? (
        <TopologyError message={snapshotError.message} onRetry={refreshObservability} />
      ) : tab === 'topology' ? (
        <div className="min-h-0 flex-1">
          <TopologyWorkbench
            graph={graph}
            selection={selection?.selection ?? null}
            onSelectionChange={selectTopologySelection}
          />
        </div>
      ) : tab === 'providers' ? (
        <ProvidersTableCard
          rows={providerTableData}
          total={providerTableTotal}
          page={providerTablePage}
          totalPages={providerTableTotalPages}
          loading={!providerInventoryUsesSnapshot && providerInventory.isLoading}
          error={!providerInventoryUsesSnapshot ? providerInventory.error?.message : undefined}
          contextNote={
            providerInventoryUsesSnapshot
              ? 'Bucket filter shows providers connected to the loaded topology snapshot.'
              : undefined
          }
          onPageChange={setProviderPage}
          onSelect={selectProvider}
        />
      ) : (
        <DataSetsTableCard
          dataSets={dataSetTableData}
          total={dataSetTableTotal}
          page={dataSetPage}
          totalPages={dataSetTableTotalPages}
          loading={dataSetInventory.isLoading}
          error={dataSetInventory.error?.message}
          onPageChange={setDataSetPage}
          onSelect={selectDataSet}
        />
      )}

      <TopologyDetailSheet
        selection={resolvedSelection}
        graph={graph}
        providers={detailProviders}
        dataSets={detailDataSets}
        onOpenChange={(open) => {
          if (open) return
          setSelection(null)
          setPinnedContext(null)
          if (hasStorageTopologySelectionSearch(search)) {
            updateSearch(clearStorageTopologySelectionSearch(search) as StorageTopologySearch)
          }
        }}
      />
    </div>
  )
}

function TopologyPartialAlert() {
  return (
    <Alert className="py-2">
      <TriangleAlert className="size-4" />
      <AlertTitle>Partial topology snapshot</AlertTitle>
      <AlertDescription>
        Topology is based on the first 500 observations. Some relationships and filter options may be missing.
      </AlertDescription>
    </Alert>
  )
}

function TopologyWorkbench({
  graph,
  selection,
  onSelectionChange,
}: {
  graph: StorageTopologyGraph
  selection: StorageTopologySelection | null
  onSelectionChange: (selection: StorageTopologySelection | null) => void
}) {
  return (
    <div className="h-full min-h-0 overflow-hidden rounded-md border bg-card">
      <Suspense fallback={<TopologyGraphFallback />}>
        <TopologyGraphCanvas graph={graph} selection={selection} onSelectionChange={onSelectionChange} />
      </Suspense>
    </div>
  )
}

function TopologyGraphFallback() {
  return (
    <div className="grid h-full min-h-0 grid-cols-3 gap-12 p-6">
      <div className="flex flex-col gap-6">
        <Skeleton className="h-8" />
        <Skeleton className="h-24" />
      </div>
      <div className="flex flex-col gap-4">
        <Skeleton className="h-8" />
        <Skeleton className="h-24" />
        <Skeleton className="h-24" />
      </div>
      <div className="flex flex-col gap-6">
        <Skeleton className="h-8" />
        <Skeleton className="h-24" />
      </div>
    </div>
  )
}

function mergeProviderObservations(
  providers: ObservabilityProviderObservation[],
  rows: StorageTopologyProviderRow[],
  pinnedProviders: ObservabilityProviderObservation[] = []
) {
  const providerMap = new Map(pinnedProviders.map((provider) => [provider.facts.provider_id, provider]))
  for (const provider of providers) {
    providerMap.set(provider.facts.provider_id, provider)
  }
  for (const row of rows) {
    if (row.provider) {
      providerMap.set(row.provider.facts.provider_id, row.provider)
    }
  }
  return Array.from(providerMap.values())
}

function mergeDataSetObservations(...dataSetCollections: ObservabilityDataSetObservation[][]) {
  const dataSetMap = new Map<number, ObservabilityDataSetObservation>()
  for (const collection of dataSetCollections) {
    for (const dataSet of collection) {
      dataSetMap.set(dataSet.facts.local_data_set_id, dataSet)
    }
  }
  return Array.from(dataSetMap.values())
}

function TopologyLoading() {
  return (
    <div className="flex min-h-0 flex-1 flex-col gap-4 rounded-md border bg-card p-6">
      <div className="grid grid-cols-3 gap-6">
        <Skeleton className="h-8" />
        <Skeleton className="h-8" />
        <Skeleton className="h-8" />
      </div>
      <div className="grid flex-1 grid-cols-3 gap-12">
        <div className="flex flex-col gap-6">
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
        </div>
        <div className="flex flex-col gap-4">
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
        </div>
        <div className="flex flex-col gap-6">
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
          <Skeleton className="h-24" />
        </div>
      </div>
    </div>
  )
}

function TopologyError({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="flex min-h-0 flex-1 items-center justify-center rounded-md border bg-card p-6">
      <Empty className="border-0">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <Database />
          </EmptyMedia>
          <EmptyTitle>Failed to load topology</EmptyTitle>
          <EmptyDescription>{message}</EmptyDescription>
        </EmptyHeader>
        <Button variant="outline" size="sm" onClick={onRetry}>
          <RefreshCw data-icon="inline-start" />
          Retry
        </Button>
      </Empty>
    </div>
  )
}

function sameStorageTopologySelectionState(left: TopologySelectionState | null, right: TopologySelectionState | null) {
  if (left === right) return true
  if (!left || !right || left.source !== right.source) return false
  return sameStorageTopologySelection(left.selection, right.selection)
}

function sameStorageTopologySelection(left: StorageTopologySelection | null, right: StorageTopologySelection | null) {
  if (left === right) return true
  if (!left || !right || left.type !== right.type) return false

  switch (left.type) {
    case 'node':
      return right.type === 'node' && left.id === right.id && left.kind === right.kind
    case 'edge':
      return right.type === 'edge' && left.id === right.id && left.kind === right.kind
    case 'provider':
      return right.type === 'provider' && left.providerID === right.providerID
    case 'data-set':
      return right.type === 'data-set' && left.localDataSetID === right.localDataSetID
  }
}

function samePinnedTopologyContext(left: PinnedTopologyContext | null, right: PinnedTopologyContext | null) {
  if (left === right) return true
  if (!left || !right || left.source !== right.source) return false
  return (
    sameStringList(
      left.providers.map((provider) => provider.facts.provider_id),
      right.providers.map((provider) => provider.facts.provider_id)
    ) &&
    sameNumberList(
      left.dataSets.map((dataSet) => dataSet.facts.local_data_set_id),
      right.dataSets.map((dataSet) => dataSet.facts.local_data_set_id)
    )
  )
}

function sameStringList(left: string[], right: string[]) {
  if (left.length !== right.length) return false
  return left.every((value, index) => value === right[index])
}

function sameNumberList(left: number[], right: number[]) {
  if (left.length !== right.length) return false
  return left.every((value, index) => value === right[index])
}

function searchStatusValue(status: ObservabilityStatusFilter | undefined) {
  if (!status || status === allStatusFilterValue) return undefined
  return status as Exclude<ObservabilityStatusFilter, 'all'>
}

function searchFilterValue(value: string | undefined) {
  if (!value || value === allEntityFilterValue) return undefined
  return value
}

function observabilityStatusParam(status: ObservabilityStatusFilter) {
  return status === allStatusFilterValue ? undefined : status
}

function observabilityProviderParam(provider: string) {
  return provider === allEntityFilterValue ? undefined : provider
}

function observabilityBucketParam(bucket: string) {
  return bucket === allEntityFilterValue ? undefined : bucket
}

function totalPagesFor(total: number, pageSizeValue: number) {
  return Math.max(1, Math.ceil(total / pageSizeValue))
}
