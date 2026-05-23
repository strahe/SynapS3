import type {
  ObservabilityDataSetObservation,
  ObservabilityFreshness,
  ObservabilityListResponse,
  ObservabilityProviderObservation,
  ObservabilitySignal,
  ObservabilitySignalLevel,
  ObservabilityStatus,
  ObservabilitySummary,
  StorageDataSetSummary,
} from '../api/client.ts'
import { replicaLabel } from './storage-status-labels.ts'
import { formatNumber, timeAgo } from './utils.ts'

export const observabilityStatusOptions = ['all', 'available', 'degraded', 'unavailable', 'unknown'] as const
export const storageTopologyAllFilterValue = '__all__'

export type ObservabilityStatusFilter = (typeof observabilityStatusOptions)[number]
export type StorageTopologyTone = 'success' | 'warning' | 'danger' | 'info' | 'neutral'

export interface StorageTopologyFilters {
  status: ObservabilityStatusFilter
  provider: string
  bucket: string
}

export interface PaginationState {
  page: number
  pageSize: number
}

export type StorageTopologyNodeKind = 'bucket' | 'data-set' | 'provider'
export type StorageTopologyEdgeKind = 'bucket-data-set' | 'data-set-provider'

export interface StorageTopologyNodeData {
  path: string
  status?: ObservabilityStatus
  level?: ObservabilitySignalLevel
  signal?: ObservabilitySignal
  bucketID?: number
  bucketName?: string
  replicaCount?: number
  issueCount?: number
  dataSetIDs?: number[]
  providerIDs?: string[]
  localDataSetID?: number
  copyIndex?: number
  localStatus?: string
  chainDataSetID?: string
  clientDataSetID?: string
  activePieceCount?: number
  providerID?: string
  active?: boolean
  hasPDP?: boolean
  serviceURL?: string
  healthStatus?: string
  observation?: ObservabilityProviderObservation | ObservabilityDataSetObservation
}

export interface StorageTopologyNode {
  id: string
  kind: StorageTopologyNodeKind
  label: string
  tone: StorageTopologyTone
  x: number
  y: number
  data: StorageTopologyNodeData
}

export interface StorageTopologyEdge {
  id: string
  kind: StorageTopologyEdgeKind
  source: string
  target: string
  tone: StorageTopologyTone
  data: {
    path: string
    bucketID?: number
    bucketName?: string
    localDataSetID: number
    chainDataSetID?: string
    clientDataSetID?: string
    providerID?: string
  }
}

export interface StorageTopologyGraph {
  nodes: StorageTopologyNode[]
  edges: StorageTopologyEdge[]
  buckets: StorageTopologyNode[]
  dataSets: StorageTopologyNode[]
  providers: StorageTopologyNode[]
}

export type StorageTopologySelection =
  | { type: 'node'; id: string; kind: StorageTopologyNodeKind }
  | { type: 'edge'; id: string; kind: StorageTopologyEdgeKind }
  | { type: 'provider'; providerID: string }
  | { type: 'data-set'; localDataSetID: number }

export type StorageTopologySelectionSource = 'route' | 'local'

export interface SourcedStorageTopologySelection {
  source: StorageTopologySelectionSource
  selection: StorageTopologySelection
}

export type ResolvedStorageTopologySelection =
  | { type: 'node'; node: StorageTopologyNode }
  | { type: 'edge'; edge: StorageTopologyEdge }
  | { type: 'provider'; provider: ObservabilityProviderObservation }
  | { type: 'data-set'; dataSet: ObservabilityDataSetObservation; provider?: ObservabilityProviderObservation }

export interface StorageTopologyProviderRow {
  providerID: string
  provider?: ObservabilityProviderObservation
  node?: StorageTopologyNode
  status: ObservabilityStatus
  freshness?: ObservabilityFreshness
  isSnapshotOnly: boolean
}

export interface StorageTopologySelectionLookup {
  chainDataSetID?: string | null
  providerID?: string
  bucketName?: string
  localDataSetID?: number | null
}

export interface StorageTopologyDataSetSelectionSearch {
  chain_data_set_id?: string
  local_data_set_id?: number
  selection_provider?: string
  selection_bucket?: string
}

export interface BucketStorageDataSetTopologyLinkModel {
  label: string
  copyValue?: string
  search: StorageTopologyDataSetSelectionSearch & {
    bucket: string
    provider: string
  }
}

export interface StorageTopologyPinnedContext {
  providers: ObservabilityProviderObservation[]
  dataSets: ObservabilityDataSetObservation[]
}

interface DataSetLayoutRow {
  dataSet: ObservabilityDataSetObservation
  y: number
}

interface ProviderLayoutRow {
  providerID: string
  provider?: ObservabilityProviderObservation
  connectedYs: number[]
  minY: number
  averageY: number
}

type DataSetDisplaySource =
  | ObservabilityDataSetObservation
  | { chainDataSetID?: string }
  | { chain_data_set_id?: string }

export const storageTopologyGraphLayout = {
  bucketX: 0,
  dataSetX: 460,
  providerX: 920,
  rowY: 124,
  providerMinGap: 124,
} as const

export function observabilityStatusTone(status: ObservabilityStatus): StorageTopologyTone {
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

export function localStatusTone(status: string): StorageTopologyTone {
  switch (status) {
    case 'ready':
      return 'success'
    case 'creating':
    case 'pending':
      return 'warning'
    case 'failed':
    case 'unavailable':
      return 'danger'
    default:
      return 'neutral'
  }
}

export function freshnessLabel(freshness: ObservabilityFreshness): string {
  if (freshness.warnings.includes('no_state_recorded')) {
    return 'No state recorded'
  }
  if (!freshness.last_checked_at) {
    return freshness.stale ? 'Stale' : 'No state recorded'
  }
  const checked = timeAgo(freshness.last_checked_at)
  return freshness.stale ? `Stale · ${checked}` : checked
}

export function dataSetDisplayLabel(dataSet: DataSetDisplaySource) {
  const chainDataSetID = dataSetChainID(dataSet)
  return chainDataSetID ? `Data Set #${chainDataSetID}` : 'No chain data set'
}

export function dataSetChainIDValue(dataSet: DataSetDisplaySource) {
  const chainDataSetID = dataSetChainID(dataSet)
  return formatOptionalTopologyID(chainDataSetID)
}

export function dataSetTopologyPath(dataSet: ObservabilityDataSetObservation) {
  return `${dataSet.facts.bucket_name} -> ${replicaLabel(dataSet.facts.copy_index)} -> ${dataSetDisplayLabel(dataSet)} -> Provider #${dataSet.facts.provider_id}`
}

export function bucketStorageDataSetTopologyLinkModel(
  bucketName: string,
  dataSet: Pick<StorageDataSetSummary, 'id' | 'bucket_name' | 'provider_id' | 'data_set_id'>
): BucketStorageDataSetTopologyLinkModel {
  const chainDataSetID = nonEmptyTopologyText(dataSet.data_set_id)

  return {
    label: chainDataSetID ?? 'No chain data set',
    copyValue: chainDataSetID,
    search: {
      bucket: bucketName,
      provider: dataSet.provider_id,
      chain_data_set_id: chainDataSetID,
      local_data_set_id: dataSet.id,
      selection_provider: dataSet.provider_id,
      selection_bucket: bucketName,
    },
  }
}

export function buildTopologyProviderOptions(dataSets: ObservabilityDataSetObservation[]) {
  return Array.from(new Set(dataSets.map((dataSet) => dataSet.facts.provider_id))).sort()
}

export function topologyGraphSummary(graph: StorageTopologyGraph) {
  return {
    buckets: graph.buckets.length,
    dataSets: graph.dataSets.length,
    providers: graph.providers.length,
  }
}

export function topologySummaryLabel(graph: StorageTopologyGraph) {
  const summary = topologyGraphSummary(graph)
  return `${formatNumber(summary.buckets)} buckets · ${formatNumber(summary.dataSets)} data sets · ${formatNumber(summary.providers)} providers`
}

export function formatOptionalTopologyText(value: string | null | undefined) {
  return nonEmptyTopologyText(value) ?? '—'
}

export function formatOptionalTopologyID(value: string | number | null | undefined) {
  return formatOptionalTopologyText(value === undefined || value === null ? undefined : String(value))
}

export function reconcileStorageTopologySelectionSearch(
  selection: SourcedStorageTopologySelection | null,
  hasSearchSelection: boolean
) {
  if (!selection) return null
  if (!hasSearchSelection && selection.source === 'route') return null
  return selection
}

export function snapshotPageIsPartial(page: Pick<ObservabilityListResponse<unknown>, 'items' | 'offset' | 'total'>) {
  return page.total > page.offset + page.items.length
}

export function bucketIssueTone(issueCount: number, bucketTone: StorageTopologyTone) {
  return issueCount > 0 ? bucketTone : 'success'
}

export function mergeTopologyDataSetSnapshots(
  baseDataSets: ObservabilityDataSetObservation[],
  scopedDataSets: ObservabilityDataSetObservation[],
  scopedEnabled: boolean
) {
  const dataSetMap = new Map(baseDataSets.map((dataSet) => [dataSet.facts.local_data_set_id, dataSet]))
  if (scopedEnabled) {
    for (const dataSet of scopedDataSets) {
      dataSetMap.set(dataSet.facts.local_data_set_id, dataSet)
    }
  }
  return Array.from(dataSetMap.values())
}

export function storageTopologyPinnedContextForSelection(
  selection: StorageTopologySelection | null,
  graph: StorageTopologyGraph,
  providers: ObservabilityProviderObservation[],
  dataSets: ObservabilityDataSetObservation[]
): StorageTopologyPinnedContext {
  if (!selection) return { providers: [], dataSets: [] }

  if (selection.type === 'node') {
    const node = graph.nodes.find((item) => item.id === selection.id && item.kind === selection.kind)
    if (!node) return { providers: [], dataSets: [] }

    if (node.kind === 'bucket') {
      const ids = new Set(node.data.dataSetIDs ?? [])
      const pinnedDataSets = dataSets.filter((dataSet) => ids.has(dataSet.facts.local_data_set_id))
      return {
        providers: providersForDataSets(providers, pinnedDataSets),
        dataSets: pinnedDataSets,
      }
    }

    if (node.kind === 'data-set') {
      const dataSet = dataSets.find((item) => item.facts.local_data_set_id === node.data.localDataSetID)
      return {
        providers: dataSet ? providersForDataSets(providers, [dataSet]) : [],
        dataSets: dataSet ? [dataSet] : [],
      }
    }

    const providerID = node.data.providerID
    const provider = providerID ? providers.find((item) => item.facts.provider_id === providerID) : undefined
    const pinnedDataSets = providerID ? relatedDataSetsForProviderNode(graph, dataSets, providerID) : []
    return {
      providers: provider ? [provider] : [],
      dataSets: pinnedDataSets,
    }
  }

  if (selection.type === 'edge') {
    const edge = graph.edges.find((item) => item.id === selection.id && item.kind === selection.kind)
    const dataSet = edge
      ? dataSets.find((item) => item.facts.local_data_set_id === edge.data.localDataSetID)
      : undefined
    return {
      providers: dataSet ? providersForDataSets(providers, [dataSet]) : [],
      dataSets: dataSet ? [dataSet] : [],
    }
  }

  if (selection.type === 'provider') {
    const provider = providers.find((item) => item.facts.provider_id === selection.providerID)
    return {
      providers: provider ? [provider] : [],
      dataSets: [],
    }
  }

  const dataSet = dataSets.find((item) => item.facts.local_data_set_id === selection.localDataSetID)
  return {
    providers: dataSet ? providersForDataSets(providers, [dataSet]) : [],
    dataSets: dataSet ? [dataSet] : [],
  }
}

export function storageTopologyDataSetSelectionSearch(
  dataSet: ObservabilityDataSetObservation
): StorageTopologyDataSetSelectionSearch {
  return {
    chain_data_set_id: dataSet.facts.chain_data_set_id,
    local_data_set_id: dataSet.facts.local_data_set_id,
    selection_provider: dataSet.facts.provider_id,
    selection_bucket: dataSet.facts.bucket_name,
  }
}

export function providerActiveFactBadge(active: boolean | undefined) {
  if (active === undefined) return { label: 'unknown', tone: 'neutral' as const }
  return active ? { label: 'active', tone: 'success' as const } : { label: 'inactive', tone: 'warning' as const }
}

export function providerPDPFactBadge(hasPDP: boolean | undefined, missingLabel = 'missing PDP') {
  if (hasPDP === undefined) return { label: 'unknown', tone: 'neutral' as const }
  return hasPDP ? { label: 'PDP', tone: 'success' as const } : { label: missingLabel, tone: 'warning' as const }
}

export function providerRowsForTopologyContext(
  graph: StorageTopologyGraph,
  providers: ObservabilityProviderObservation[],
  statusFilter: ObservabilityStatusFilter = 'all'
) {
  const providerMap = new Map(providers.map((provider) => [provider.facts.provider_id, provider]))

  return graph.providers
    .map<StorageTopologyProviderRow>((node) => {
      const providerID = node.data.providerID ?? ''
      const provider = providerMap.get(providerID)
      return {
        providerID,
        provider,
        node,
        status: provider?.signal.status ?? node.data.status ?? 'unknown',
        freshness: provider?.signal.freshness,
        isSnapshotOnly: !provider,
      }
    })
    .filter((row) => statusFilter === 'all' || row.status === statusFilter)
}

export function providerRowsFromInventory(providers: ObservabilityProviderObservation[], graph: StorageTopologyGraph) {
  return providers.map<StorageTopologyProviderRow>((provider) => ({
    providerID: provider.facts.provider_id,
    provider,
    node: findProviderTopologyNode(graph, provider.facts.provider_id),
    status: provider.signal.status,
    freshness: provider.signal.freshness,
    isSnapshotOnly: false,
  }))
}

export function relatedDataSetsForProviderNode(
  graph: StorageTopologyGraph,
  dataSets: ObservabilityDataSetObservation[],
  providerID: string
) {
  const dataSetMap = new Map(dataSets.map((dataSet) => [dataSet.facts.local_data_set_id, dataSet]))
  return graph.dataSets
    .filter((node) => node.data.providerID === providerID)
    .map((node) => (node.data.localDataSetID === undefined ? undefined : dataSetMap.get(node.data.localDataSetID)))
    .filter((dataSet): dataSet is ObservabilityDataSetObservation => Boolean(dataSet))
}

export function buildStorageTopologyGraph(
  providers: ObservabilityProviderObservation[],
  dataSets: ObservabilityDataSetObservation[],
  filters: StorageTopologyFilters
): StorageTopologyGraph {
  const providerMap = new Map(providers.map((provider) => [provider.facts.provider_id, provider]))
  const visibleDataSets = dataSets.filter((dataSet) => dataSetMatchesFilters(dataSet, filters)).sort(compareDataSets)
  const dataSetRows = visibleDataSets.map<DataSetLayoutRow>((dataSet, index) => ({
    dataSet,
    y: index * storageTopologyGraphLayout.rowY,
  }))
  const bucketGroups = buildBucketGraphGroups(dataSetRows)
  const providerRows = buildProviderLayoutRows(dataSetRows, providerMap)

  const buckets = bucketGroups.map<StorageTopologyNode>((group) => {
    const summary = summaryFromStatuses(group.dataSets.map((dataSet) => dataSet.signal.status))
    return {
      id: bucketNodeID(group.bucketID),
      kind: 'bucket',
      label: group.bucketName,
      tone: summaryTone(summary),
      x: storageTopologyGraphLayout.bucketX,
      y: group.y,
      data: {
        path: group.bucketName,
        bucketID: group.bucketID,
        bucketName: group.bucketName,
        replicaCount: group.dataSets.length,
        issueCount: summaryIssueCount(summary),
        dataSetIDs: group.dataSets.map((dataSet) => dataSet.facts.local_data_set_id),
        providerIDs: Array.from(new Set(group.dataSets.map((dataSet) => dataSet.facts.provider_id))).sort(),
      },
    }
  })

  const dataSetNodes = dataSetRows.map<StorageTopologyNode>(({ dataSet, y }) => ({
    id: dataSetNodeID(dataSet.facts.local_data_set_id),
    kind: 'data-set',
    label: replicaLabel(dataSet.facts.copy_index),
    tone: observabilityStatusTone(dataSet.signal.status),
    x: storageTopologyGraphLayout.dataSetX,
    y,
    data: {
      path: dataSetTopologyPath(dataSet),
      status: dataSet.signal.status,
      level: dataSet.signal.level,
      signal: dataSet.signal,
      bucketID: dataSet.facts.bucket_id,
      bucketName: dataSet.facts.bucket_name,
      localDataSetID: dataSet.facts.local_data_set_id,
      copyIndex: dataSet.facts.copy_index,
      localStatus: dataSet.facts.local_status,
      chainDataSetID: dataSet.facts.chain_data_set_id,
      clientDataSetID: dataSet.facts.client_data_set_id,
      activePieceCount: dataSet.facts.active_piece_count,
      providerID: dataSet.facts.provider_id,
      observation: dataSet,
    },
  }))

  const providerNodes = layoutProviderRows(providerRows).map<StorageTopologyNode>(({ row, y }) =>
    providerNode(row.providerID, row.provider, y)
  )

  const edges = visibleDataSets.flatMap<StorageTopologyEdge>((dataSet) => {
    const chainPath = dataSetTopologyPath(dataSet)
    const replicaPath = `${dataSet.facts.bucket_name} -> ${replicaLabel(dataSet.facts.copy_index)} -> ${dataSetDisplayLabel(dataSet)}`
    return [
      {
        id: `bucket-data-set:${dataSet.facts.bucket_id}:${dataSet.facts.local_data_set_id}`,
        kind: 'bucket-data-set',
        source: bucketNodeID(dataSet.facts.bucket_id),
        target: dataSetNodeID(dataSet.facts.local_data_set_id),
        tone: observabilityStatusTone(dataSet.signal.status),
        data: {
          path: replicaPath,
          bucketID: dataSet.facts.bucket_id,
          bucketName: dataSet.facts.bucket_name,
          localDataSetID: dataSet.facts.local_data_set_id,
          chainDataSetID: dataSet.facts.chain_data_set_id,
          clientDataSetID: dataSet.facts.client_data_set_id,
          providerID: dataSet.facts.provider_id,
        },
      },
      {
        id: `data-set-provider:${dataSet.facts.local_data_set_id}:${dataSet.facts.provider_id}`,
        kind: 'data-set-provider',
        source: dataSetNodeID(dataSet.facts.local_data_set_id),
        target: providerNodeID(dataSet.facts.provider_id),
        tone: observabilityStatusTone(dataSet.signal.status),
        data: {
          path: chainPath,
          bucketID: dataSet.facts.bucket_id,
          bucketName: dataSet.facts.bucket_name,
          localDataSetID: dataSet.facts.local_data_set_id,
          chainDataSetID: dataSet.facts.chain_data_set_id,
          clientDataSetID: dataSet.facts.client_data_set_id,
          providerID: dataSet.facts.provider_id,
        },
      },
    ]
  })

  return {
    nodes: [...buckets, ...dataSetNodes, ...providerNodes],
    edges,
    buckets,
    dataSets: dataSetNodes,
    providers: providerNodes,
  }
}

export function findProviderTopologyNode(graph: StorageTopologyGraph, providerID: string) {
  return graph.providers.find((node) => node.data.providerID === providerID)
}

export function findDataSetTopologyNode(graph: StorageTopologyGraph, localDataSetID: number) {
  return findDataSetTopologyNodeByLocalID(graph, localDataSetID)
}

export function findDataSetTopologyNodeByChainID(
  graph: StorageTopologyGraph,
  chainDataSetID: string,
  providerID?: string,
  bucketName?: string
) {
  const candidates = graph.dataSets.filter((node) => node.data.chainDataSetID === chainDataSetID)
  const filtered = candidates.filter((node) => {
    if (providerID && node.data.providerID !== providerID) return false
    if (bucketName && node.data.bucketName !== bucketName) return false
    return true
  })
  return filtered.length === 1 ? filtered[0] : undefined
}

export function findDataSetTopologyNodeByLocalID(graph: StorageTopologyGraph, localDataSetID: number) {
  return graph.dataSets.find((node) => node.data.localDataSetID === localDataSetID)
}

export function findStorageTopologySelection(
  graph: StorageTopologyGraph,
  lookup: StorageTopologySelectionLookup
): StorageTopologySelection | null {
  const hasDataSetLookup = Boolean(lookup.chainDataSetID) || lookup.localDataSetID != null
  if (lookup.chainDataSetID) {
    const node = findDataSetTopologyNodeByChainID(graph, lookup.chainDataSetID, lookup.providerID, lookup.bucketName)
    if (node) return { type: 'node', id: node.id, kind: node.kind }
  }
  if (lookup.localDataSetID != null) {
    const node = findDataSetTopologyNodeByLocalID(graph, lookup.localDataSetID)
    if (node) return { type: 'node', id: node.id, kind: node.kind }
  }
  if (hasDataSetLookup) return null
  if (lookup.providerID) {
    const node = findProviderTopologyNode(graph, lookup.providerID)
    if (!node) return null
    return { type: 'node', id: node.id, kind: node.kind }
  }
  return null
}

export function resolveStorageTopologySelection(
  selection: StorageTopologySelection | null,
  graph: StorageTopologyGraph,
  providers: ObservabilityProviderObservation[],
  dataSets: ObservabilityDataSetObservation[]
): ResolvedStorageTopologySelection | null {
  if (!selection) return null

  if (selection.type === 'node') {
    const node = graph.nodes.find((item) => item.id === selection.id && item.kind === selection.kind)
    return node ? { type: 'node', node } : null
  }

  if (selection.type === 'edge') {
    const edge = graph.edges.find((item) => item.id === selection.id && item.kind === selection.kind)
    return edge ? { type: 'edge', edge } : null
  }

  if (selection.type === 'provider') {
    const provider = providers.find((item) => item.facts.provider_id === selection.providerID)
    return provider ? { type: 'provider', provider } : null
  }

  const dataSet = dataSets.find((item) => item.facts.local_data_set_id === selection.localDataSetID)
  if (!dataSet) return null
  return {
    type: 'data-set',
    dataSet,
    provider: providers.find((item) => item.facts.provider_id === dataSet.facts.provider_id),
  }
}

export function clampPageForTotal(page: number, total: number, pageSize: number) {
  const safePageSize = Math.max(1, pageSize)
  const totalPages = Math.max(1, Math.ceil(Math.max(0, total) / safePageSize))
  return Math.min(Math.max(1, page), totalPages)
}

export function clampPageForLoadedTotal(page: number, total: number | undefined, pageSize: number) {
  if (total === undefined) return Math.max(1, page)
  return clampPageForTotal(page, total, pageSize)
}

export function paginateItems<T>(items: T[], pagination: PaginationState) {
  const pageSize = Math.max(1, pagination.pageSize)
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize))
  const page = clampPageForTotal(pagination.page, items.length, pageSize)
  const offset = (page - 1) * pageSize
  return {
    items: items.slice(offset, offset + pageSize),
    page,
    pageSize,
    totalPages,
    offset,
    limit: pageSize,
  }
}

export function summaryIssueCount(summary: ObservabilitySummary) {
  return summary.degraded + summary.unavailable + summary.unknown
}

export function summaryFromStatuses(statuses: ObservabilityStatus[]): ObservabilitySummary {
  return statuses.reduce<ObservabilitySummary>(
    (summary, status) => {
      summary.total += 1
      summary[status] += 1
      return summary
    },
    {
      total: 0,
      available: 0,
      degraded: 0,
      unavailable: 0,
      unknown: 0,
    }
  )
}

function dataSetMatchesFilters(dataSet: ObservabilityDataSetObservation, filters: StorageTopologyFilters) {
  if (filters.status !== 'all' && dataSet.signal.status !== filters.status) return false
  if (!isAllEntityFilter(filters.provider) && dataSet.facts.provider_id !== filters.provider) return false
  if (!isAllEntityFilter(filters.bucket) && dataSet.facts.bucket_name !== filters.bucket) return false
  return true
}

function isAllEntityFilter(value: string) {
  return value === storageTopologyAllFilterValue
}

function buildBucketGraphGroups(dataSetRows: DataSetLayoutRow[]) {
  const groups = new Map<
    number,
    {
      bucketID: number
      bucketName: string
      dataSets: ObservabilityDataSetObservation[]
      ys: number[]
      y: number
    }
  >()

  for (const row of dataSetRows) {
    const group = groups.get(row.dataSet.facts.bucket_id) ?? {
      bucketID: row.dataSet.facts.bucket_id,
      bucketName: row.dataSet.facts.bucket_name,
      dataSets: [],
      ys: [],
      y: 0,
    }
    group.dataSets.push(row.dataSet)
    group.ys.push(row.y)
    groups.set(group.bucketID, group)
  }

  return Array.from(groups.values()).map((group) => ({
    ...group,
    y: average(group.ys),
  }))
}

function buildProviderLayoutRows(
  dataSetRows: DataSetLayoutRow[],
  providerMap: Map<string, ObservabilityProviderObservation>
) {
  const connectedProviderYs = new Map<string, number[]>()
  for (const row of dataSetRows) {
    const values = connectedProviderYs.get(row.dataSet.facts.provider_id) ?? []
    values.push(row.y)
    connectedProviderYs.set(row.dataSet.facts.provider_id, values)
  }

  return Array.from(connectedProviderYs.keys())
    .map<ProviderLayoutRow>((providerID) => {
      const connectedYs = connectedProviderYs.get(providerID) ?? []
      return {
        providerID,
        provider: providerMap.get(providerID),
        connectedYs,
        minY: connectedYs.length > 0 ? Math.min(...connectedYs) : Number.POSITIVE_INFINITY,
        averageY: connectedYs.length > 0 ? average(connectedYs) : Number.POSITIVE_INFINITY,
      }
    })
    .sort(compareProviderRows)
}

function layoutProviderRows(rows: ProviderLayoutRow[]) {
  let previousY = -storageTopologyGraphLayout.providerMinGap

  return rows.map((row) => {
    const targetY = Number.isFinite(row.minY) ? row.minY : 0
    const y = Math.max(targetY, previousY + storageTopologyGraphLayout.providerMinGap)
    previousY = y
    return { row, y }
  })
}

function providerNode(
  providerID: string,
  provider: ObservabilityProviderObservation | undefined,
  y: number
): StorageTopologyNode {
  const status = provider?.signal.status ?? 'unknown'
  return {
    id: providerNodeID(providerID),
    kind: 'provider',
    label: `Provider #${providerID}`,
    tone: provider ? observabilityStatusTone(provider.signal.status) : 'neutral',
    x: storageTopologyGraphLayout.providerX,
    y,
    data: {
      path: `Provider #${providerID}`,
      providerID,
      status,
      level: provider?.signal.level,
      signal: provider?.signal,
      active: provider?.facts.active,
      hasPDP: provider?.facts.has_pdp,
      serviceURL: provider?.facts.service_url,
      healthStatus: provider?.facts.health_status,
      observation: provider,
    },
  }
}

function compareProviderRows(left: ProviderLayoutRow, right: ProviderLayoutRow) {
  if (left.minY !== right.minY) return left.minY - right.minY
  if (left.averageY !== right.averageY) return left.averageY - right.averageY
  return left.providerID.localeCompare(right.providerID)
}

function providersForDataSets(
  providers: ObservabilityProviderObservation[],
  dataSets: ObservabilityDataSetObservation[]
) {
  const providerIDs = new Set(dataSets.map((dataSet) => dataSet.facts.provider_id))
  return providers.filter((provider) => providerIDs.has(provider.facts.provider_id))
}

function compareDataSets(left: ObservabilityDataSetObservation, right: ObservabilityDataSetObservation) {
  const bucketDiff = left.facts.bucket_name.localeCompare(right.facts.bucket_name)
  if (bucketDiff !== 0) return bucketDiff
  const copyDiff = left.facts.copy_index - right.facts.copy_index
  if (copyDiff !== 0) return copyDiff
  return left.facts.local_data_set_id - right.facts.local_data_set_id
}

function average(values: number[]) {
  if (values.length === 0) return 0
  return values.reduce((sum, value) => sum + value, 0) / values.length
}

function summaryTone(summary: ObservabilitySummary): StorageTopologyTone {
  if (summary.unavailable > 0) return 'danger'
  if (summary.degraded > 0 || summary.unknown > 0) return 'warning'
  return 'success'
}

function bucketNodeID(bucketID: number) {
  return `bucket:${bucketID}`
}

function dataSetNodeID(localDataSetID: number) {
  return `data-set:${localDataSetID}`
}

function providerNodeID(providerID: string) {
  return `provider:${providerID}`
}

function dataSetChainID(dataSet: DataSetDisplaySource) {
  if ('facts' in dataSet) return nonEmptyTopologyText(dataSet.facts.chain_data_set_id)
  return nonEmptyTopologyText(
    (dataSet as { chainDataSetID?: string }).chainDataSetID ??
      (dataSet as { chain_data_set_id?: string }).chain_data_set_id
  )
}

function nonEmptyTopologyText(value: string | null | undefined) {
  const trimmed = value?.trim()
  return trimmed ? trimmed : undefined
}
