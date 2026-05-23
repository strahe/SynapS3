import assert from 'node:assert/strict'
import test from 'node:test'

import type { ObservabilityDataSetObservation, ObservabilityProviderObservation } from '../src/api/client.ts'
import {
  bucketIssueTone,
  bucketStorageDataSetTopologyLinkModel,
  buildStorageTopologyGraph,
  buildTopologyProviderOptions,
  clampPageForLoadedTotal,
  clampPageForTotal,
  dataSetChainIDValue,
  dataSetDisplayLabel,
  dataSetTopologyPath,
  findDataSetTopologyNodeByChainID,
  findDataSetTopologyNodeByLocalID,
  findProviderTopologyNode,
  findStorageTopologySelection,
  formatOptionalTopologyID,
  formatOptionalTopologyText,
  freshnessLabel,
  mergeTopologyDataSetSnapshots,
  paginateItems,
  providerRowsForTopologyContext,
  reconcileStorageTopologySelectionSearch,
  relatedDataSetsForProviderNode,
  resolveStorageTopologySelection,
  snapshotPageIsPartial,
  storageTopologyAllFilterValue,
  storageTopologyDataSetSelectionSearch,
  storageTopologyPinnedContextForSelection,
  topologyGraphSummary,
  topologySummaryLabel,
} from '../src/lib/storage-topology.ts'
import {
  clearStorageTopologySelectionSearch,
  nonNegativeStorageTopologySearchInteger,
} from '../src/lib/storage-topology-search.ts'

const allEntityFilterValue = storageTopologyAllFilterValue

const provider101: ObservabilityProviderObservation = {
  facts: { provider_id: '101', active: true, has_pdp: true },
  signal: {
    status: 'available',
    level: 'ok',
    reason_codes: [],
    freshness: { last_checked_at: '2026-05-21T08:00:00Z', stale: false, warnings: [] },
  },
}

const provider202: ObservabilityProviderObservation = {
  facts: { provider_id: '202', active: true, has_pdp: true },
  signal: {
    status: 'degraded',
    level: 'warning',
    reason_codes: ['provider_http_unreachable'],
    freshness: { last_checked_at: '2026-05-21T08:00:00Z', stale: false, warnings: [] },
  },
}

const provider606: ObservabilityProviderObservation = {
  facts: {
    provider_id: '606',
    active: true,
    has_pdp: true,
    service_url: 'https://zeta-empty.example',
    health_status: 'reachable',
  },
  signal: {
    status: 'available',
    level: 'ok',
    reason_codes: [],
    freshness: { last_checked_at: '2026-05-21T08:00:00Z', stale: false, warnings: [] },
  },
}

const mediaDataSet: ObservabilityDataSetObservation = {
  facts: {
    local_data_set_id: 11,
    bucket_id: 1,
    bucket_name: 'media-prod',
    copy_index: 0,
    provider_id: '101',
    chain_data_set_id: '51001',
    client_data_set_id: '90001',
    local_status: 'ready',
  },
  signal: provider101.signal,
}

const mediaReplicaDataSet: ObservabilityDataSetObservation = {
  facts: {
    local_data_set_id: 12,
    bucket_id: 1,
    bucket_name: 'media-prod',
    copy_index: 1,
    provider_id: '202',
    chain_data_set_id: '51002',
    client_data_set_id: '90002',
    local_status: 'ready',
    active_piece_count: 779,
  },
  signal: provider202.signal,
}

const logsDataSet: ObservabilityDataSetObservation = {
  facts: {
    local_data_set_id: 21,
    bucket_id: 2,
    bucket_name: 'logs-archive',
    copy_index: 0,
    provider_id: '202',
    chain_data_set_id: '61001',
    client_data_set_id: '91001',
    local_status: 'ready',
  },
  signal: provider202.signal,
}

const missingChainDataSet: ObservabilityDataSetObservation = {
  facts: {
    local_data_set_id: 31,
    bucket_id: 3,
    bucket_name: 'research-data',
    copy_index: 0,
    provider_id: '101',
    local_status: 'ready',
  },
  signal: provider101.signal,
}

test('storage topology formats freshness without inventing new state', () => {
  assert.equal(freshnessLabel({ stale: false, warnings: ['no_state_recorded'] }), 'No state recorded')
  assert.equal(freshnessLabel({ stale: true, warnings: [] }), 'Stale')

  const originalNow = Date.now
  Date.now = () => new Date('2026-05-21T09:00:00Z').getTime()
  try {
    assert.equal(
      freshnessLabel({
        last_checked_at: '2026-05-21T08:00:00Z',
        stale: true,
        warnings: ['stale_state'],
      }),
      'Stale · 1h ago'
    )
  } finally {
    Date.now = originalNow
  }
})

test('storage topology provider context rows keep graph-only providers without inventing facts', () => {
  const graph = buildStorageTopologyGraph([provider101], [mediaDataSet, mediaReplicaDataSet], {
    status: 'all',
    provider: allEntityFilterValue,
    bucket: 'media-prod',
  })

  const rows = providerRowsForTopologyContext(graph, [provider101])

  assert.deepEqual(
    rows.map((row) => ({
      providerID: row.providerID,
      hasProvider: Boolean(row.provider),
      status: row.status,
      isSnapshotOnly: row.isSnapshotOnly,
    })),
    [
      { providerID: '101', hasProvider: true, status: 'available', isSnapshotOnly: false },
      { providerID: '202', hasProvider: false, status: 'unknown', isSnapshotOnly: true },
    ]
  )

  assert.deepEqual(
    providerRowsForTopologyContext(
      buildStorageTopologyGraph([provider101, provider202], [mediaDataSet, mediaReplicaDataSet], {
        status: 'all',
        provider: allEntityFilterValue,
        bucket: 'media-prod',
      }),
      [provider101, provider202],
      'available'
    ).map((row) => row.providerID),
    ['101']
  )
})

test('storage topology provider relationships come from the current graph context', () => {
  const graph = buildStorageTopologyGraph(
    [provider101, provider202],
    [mediaDataSet, mediaReplicaDataSet, logsDataSet],
    { status: 'all', provider: '202', bucket: 'media-prod' }
  )

  assert.deepEqual(
    relatedDataSetsForProviderNode(graph, [mediaDataSet, mediaReplicaDataSet, logsDataSet], '202').map(
      (dataSet) => dataSet.facts.local_data_set_id
    ),
    [12]
  )
})

test('storage topology pins detail context from the active selection', () => {
  const graph = buildStorageTopologyGraph(
    [provider101, provider202],
    [mediaDataSet, mediaReplicaDataSet, logsDataSet],
    { status: 'all', provider: allEntityFilterValue, bucket: allEntityFilterValue }
  )

  assert.deepEqual(
    storageTopologyPinnedContextForSelection(
      { type: 'node', id: 'data-set:12', kind: 'data-set' },
      graph,
      [provider101, provider202],
      [mediaDataSet, mediaReplicaDataSet, logsDataSet]
    ).dataSets.map((dataSet) => dataSet.facts.local_data_set_id),
    [12]
  )
  assert.deepEqual(
    storageTopologyPinnedContextForSelection(
      { type: 'edge', id: 'bucket-data-set:1:11', kind: 'bucket-data-set' },
      graph,
      [provider101, provider202],
      [mediaDataSet, mediaReplicaDataSet, logsDataSet]
    ).providers.map((provider) => provider.facts.provider_id),
    ['101']
  )
  assert.deepEqual(
    storageTopologyPinnedContextForSelection(
      { type: 'node', id: 'bucket:1', kind: 'bucket' },
      graph,
      [provider101, provider202],
      [mediaDataSet, mediaReplicaDataSet, logsDataSet]
    ).dataSets.map((dataSet) => dataSet.facts.local_data_set_id),
    [11, 12]
  )
  assert.deepEqual(
    storageTopologyPinnedContextForSelection(
      { type: 'node', id: 'provider:202', kind: 'provider' },
      graph,
      [provider101, provider202],
      [mediaDataSet, mediaReplicaDataSet, logsDataSet]
    ).dataSets.map((dataSet) => dataSet.facts.local_data_set_id),
    [21, 12]
  )
  assert.deepEqual(
    storageTopologyPinnedContextForSelection(
      { type: 'provider', providerID: '202' },
      graph,
      [provider101, provider202],
      [mediaDataSet]
    ).providers.map((provider) => provider.facts.provider_id),
    ['202']
  )
})

test('storage topology pinned data keeps detail resolution stable after page or scoped data changes', () => {
  const scopedGraph = buildStorageTopologyGraph([provider101, provider202], [mediaDataSet, logsDataSet], {
    status: 'all',
    provider: allEntityFilterValue,
    bucket: allEntityFilterValue,
  })
  const selection = { type: 'node' as const, id: 'data-set:21', kind: 'data-set' as const }
  const pinned = storageTopologyPinnedContextForSelection(
    selection,
    scopedGraph,
    [provider101, provider202],
    [mediaDataSet, logsDataSet]
  )
  const graphAfterScopedSearchClears = buildStorageTopologyGraph(
    [provider101, provider202],
    mergeTopologyDataSetSnapshots([mediaDataSet], pinned.dataSets, true),
    { status: 'all', provider: allEntityFilterValue, bucket: allEntityFilterValue }
  )

  assert.equal(
    resolveStorageTopologySelection(
      selection,
      graphAfterScopedSearchClears,
      [provider101, provider202],
      [mediaDataSet, ...pinned.dataSets]
    )?.type,
    'node'
  )
  assert.equal(
    resolveStorageTopologySelection(
      { type: 'data-set', localDataSetID: 12 },
      buildStorageTopologyGraph([provider101], [mediaDataSet], {
        status: 'all',
        provider: allEntityFilterValue,
        bucket: allEntityFilterValue,
      }),
      [provider101],
      [mediaDataSet]
    ),
    null
  )
  assert.equal(
    resolveStorageTopologySelection(
      { type: 'data-set', localDataSetID: 12 },
      buildStorageTopologyGraph([provider101, provider202], [mediaDataSet, mediaReplicaDataSet], {
        status: 'all',
        provider: allEntityFilterValue,
        bucket: allEntityFilterValue,
      }),
      [provider101, provider202],
      [mediaDataSet, mediaReplicaDataSet]
    )?.type,
    'data-set'
  )
})

test('storage topology keeps the all bucket name distinct from the no-filter sentinel', () => {
  const allBucketDataSet: ObservabilityDataSetObservation = {
    ...mediaDataSet,
    facts: {
      ...mediaDataSet.facts,
      local_data_set_id: 41,
      bucket_id: 4,
      bucket_name: 'all',
      chain_data_set_id: '81001',
    },
  }

  assert.deepEqual(
    buildStorageTopologyGraph([provider101], [mediaDataSet, allBucketDataSet], {
      status: 'all',
      provider: allEntityFilterValue,
      bucket: allEntityFilterValue,
    }).buckets.map((node) => node.data.bucketName),
    ['all', 'media-prod']
  )
  assert.deepEqual(
    buildStorageTopologyGraph([provider101], [mediaDataSet, allBucketDataSet], {
      status: 'all',
      provider: allEntityFilterValue,
      bucket: 'all',
    }).buckets.map((node) => node.data.bucketName),
    ['all']
  )
})

test('storage topology detects partial loaded snapshots from page boundaries', () => {
  assert.equal(snapshotPageIsPartial({ items: [provider101], total: 2, offset: 0 }), true)
  assert.equal(snapshotPageIsPartial({ items: [provider101], total: 1, offset: 0 }), false)
  assert.equal(snapshotPageIsPartial({ items: [provider101], total: 2, offset: 1 }), false)
})

test('storage topology merges scoped data sets only when scoped snapshot is enabled', () => {
  assert.deepEqual(mergeTopologyDataSetSnapshots([mediaDataSet], [logsDataSet], false), [mediaDataSet])
  assert.deepEqual(mergeTopologyDataSetSnapshots([mediaDataSet], [logsDataSet], true), [mediaDataSet, logsDataSet])
})

test('storage topology data set selection search keeps chain, local fallback, and scope', () => {
  assert.deepEqual(storageTopologyDataSetSelectionSearch(mediaDataSet), {
    chain_data_set_id: '51001',
    local_data_set_id: 11,
    selection_provider: '101',
    selection_bucket: 'media-prod',
  })
  assert.deepEqual(storageTopologyDataSetSelectionSearch(missingChainDataSet), {
    chain_data_set_id: undefined,
    local_data_set_id: 31,
    selection_provider: '101',
    selection_bucket: 'research-data',
  })
})

test('storage topology bucket data set links keep local fallback and raw chain copy values', () => {
  assert.deepEqual(
    bucketStorageDataSetTopologyLinkModel('media-prod', {
      id: 11,
      provider_id: '101',
      data_set_id: '51001',
    }),
    {
      label: '51001',
      copyValue: '51001',
      search: {
        bucket: 'media-prod',
        provider: '101',
        chain_data_set_id: '51001',
        local_data_set_id: 11,
        selection_provider: '101',
        selection_bucket: 'media-prod',
      },
    }
  )

  assert.deepEqual(
    bucketStorageDataSetTopologyLinkModel('research-data', {
      id: 31,
      provider_id: '101',
    }),
    {
      label: 'No chain data set',
      copyValue: undefined,
      search: {
        bucket: 'research-data',
        provider: '101',
        chain_data_set_id: undefined,
        local_data_set_id: 31,
        selection_provider: '101',
        selection_bucket: 'research-data',
      },
    }
  )

  assert.deepEqual(
    bucketStorageDataSetTopologyLinkModel('research-data', {
      id: 32,
      provider_id: '101',
      data_set_id: '   ',
    }),
    {
      label: 'No chain data set',
      copyValue: undefined,
      search: {
        bucket: 'research-data',
        provider: '101',
        chain_data_set_id: undefined,
        local_data_set_id: 32,
        selection_provider: '101',
        selection_bucket: 'research-data',
      },
    }
  )
})

test('storage topology clears selection search without changing visible filters', () => {
  assert.deepEqual(
    clearStorageTopologySelectionSearch({
      tab: 'data-sets',
      status: 'degraded',
      provider: '101',
      bucket: 'media-prod',
      chain_data_set_id: '51001',
      local_data_set_id: 11,
      selection_provider: '101',
      selection_bucket: 'media-prod',
    }),
    {
      tab: 'data-sets',
      status: 'degraded',
      provider: '101',
      bucket: 'media-prod',
      chain_data_set_id: undefined,
      local_data_set_id: undefined,
      selection_provider: undefined,
      selection_bucket: undefined,
    }
  )
})

test('storage topology parses local data set search ids without treating blanks as zero', () => {
  assert.equal(nonNegativeStorageTopologySearchInteger(''), undefined)
  assert.equal(nonNegativeStorageTopologySearchInteger('   '), undefined)
  assert.equal(nonNegativeStorageTopologySearchInteger('0'), 0)
  assert.equal(nonNegativeStorageTopologySearchInteger(0), 0)
  assert.equal(nonNegativeStorageTopologySearchInteger('001'), 1)
  assert.equal(nonNegativeStorageTopologySearchInteger('1e2'), undefined)
  assert.equal(nonNegativeStorageTopologySearchInteger('1.5'), undefined)
  assert.equal(nonNegativeStorageTopologySearchInteger('-1'), undefined)
})

test('storage topology clears only route-owned selection when search selection disappears', () => {
  const routeSelection = {
    source: 'route' as const,
    selection: { type: 'data-set' as const, localDataSetID: 11 },
  }
  const localSelection = {
    source: 'local' as const,
    selection: { type: 'node' as const, id: 'provider:101', kind: 'provider' as const },
  }

  assert.equal(reconcileStorageTopologySelectionSearch(routeSelection, false), null)
  assert.deepEqual(reconcileStorageTopologySelectionSearch(localSelection, false), localSelection)
  assert.deepEqual(reconcileStorageTopologySelectionSearch(routeSelection, true), routeSelection)
})

test('storage topology optional text formatter treats blank values as missing', () => {
  assert.equal(formatOptionalTopologyText(undefined), '—')
  assert.equal(formatOptionalTopologyText(null), '—')
  assert.equal(formatOptionalTopologyText(''), '—')
  assert.equal(formatOptionalTopologyText('   '), '—')
  assert.equal(formatOptionalTopologyText('reachable'), 'reachable')
})

test('storage topology uses raw ids for value fields and prefixed ids for labels and paths', () => {
  assert.equal(formatOptionalTopologyID(undefined), '—')
  assert.equal(formatOptionalTopologyID(null), '—')
  assert.equal(formatOptionalTopologyID(''), '—')
  assert.equal(formatOptionalTopologyID(' 51002 '), '51002')
  assert.equal(dataSetDisplayLabel(mediaReplicaDataSet), 'Data Set #51002')
  assert.equal(dataSetChainIDValue(mediaReplicaDataSet), '51002')
  assert.equal(dataSetChainIDValue(missingChainDataSet), '—')
  assert.equal(dataSetDisplayLabel(missingChainDataSet), 'No chain data set')
  assert.equal(dataSetTopologyPath(mediaReplicaDataSet), 'media-prod -> Replica 2 -> Data Set #51002 -> Provider #202')
  assert.equal(
    dataSetTopologyPath(missingChainDataSet),
    'research-data -> Replica 1 -> No chain data set -> Provider #101'
  )
})

test('storage topology graph builds unique bucket, replica, and used provider nodes', () => {
  const graph = buildStorageTopologyGraph(
    [provider101, provider202, provider606],
    [mediaDataSet, mediaReplicaDataSet, logsDataSet],
    { status: 'all', provider: allEntityFilterValue, bucket: allEntityFilterValue }
  )

  assert.deepEqual(sortedIDs(graph.nodes), [
    'bucket:1',
    'bucket:2',
    'data-set:11',
    'data-set:12',
    'data-set:21',
    'provider:101',
    'provider:202',
  ])
  assert.equal(graph.nodes.find((node) => node.id === 'bucket:1')?.data.replicaCount, 2)
  assert.equal(graph.nodes.filter((node) => node.id === 'provider:202').length, 1)
  assert.deepEqual(topologyGraphSummary(graph), { buckets: 2, dataSets: 3, providers: 2 })
  assert.equal(topologySummaryLabel(graph), '2 buckets · 3 data sets · 2 providers')
  assert.deepEqual(buildTopologyProviderOptions([mediaDataSet, mediaReplicaDataSet, logsDataSet]), ['101', '202'])
  assert.equal(bucketIssueTone(graph.buckets[0]?.data.issueCount ?? 0, graph.buckets[0]?.tone ?? 'neutral'), 'warning')
  assert.equal(
    graph.nodes.find((node) => node.id === 'provider:606'),
    undefined
  )
})

test('storage topology graph connects bucket to replicas and replicas to providers', () => {
  const graph = buildStorageTopologyGraph(
    [provider101, provider202, provider606],
    [mediaDataSet, mediaReplicaDataSet],
    { status: 'all', provider: allEntityFilterValue, bucket: allEntityFilterValue }
  )

  assert.deepEqual(
    graph.edges.map((edge) => [edge.id, edge.source, edge.target]).sort(([left], [right]) => left.localeCompare(right)),
    [
      ['bucket-data-set:1:11', 'bucket:1', 'data-set:11'],
      ['bucket-data-set:1:12', 'bucket:1', 'data-set:12'],
      ['data-set-provider:11:101', 'data-set:11', 'provider:101'],
      ['data-set-provider:12:202', 'data-set:12', 'provider:202'],
    ]
  )
  assert.equal(graph.edges[0]?.data.path, 'media-prod -> Replica 1 -> Data Set #51001')
  assert.equal(graph.edges[1]?.data.path, 'media-prod -> Replica 1 -> Data Set #51001 -> Provider #101')
  assert.equal(graph.edges[1]?.data.chainDataSetID, '51001')
  assert.equal(graph.edges[1]?.data.clientDataSetID, '90001')
})

test('storage topology graph filters bucket and provider while retaining connected context', () => {
  const providers = [provider101, provider202, provider606]
  const dataSets = [mediaDataSet, mediaReplicaDataSet, logsDataSet]

  const bucketGraph = buildStorageTopologyGraph(providers, dataSets, {
    status: 'all',
    provider: allEntityFilterValue,
    bucket: 'media-prod',
  })
  assert.deepEqual(sortedIDs(bucketGraph.nodes), [
    'bucket:1',
    'data-set:11',
    'data-set:12',
    'provider:101',
    'provider:202',
  ])

  const providerGraph = buildStorageTopologyGraph(providers, dataSets, {
    status: 'all',
    provider: '202',
    bucket: allEntityFilterValue,
  })
  assert.deepEqual(sortedIDs(providerGraph.nodes), [
    'bucket:1',
    'bucket:2',
    'data-set:12',
    'data-set:21',
    'provider:202',
  ])

  const statusGraph = buildStorageTopologyGraph(providers, dataSets, {
    status: 'degraded',
    provider: allEntityFilterValue,
    bucket: allEntityFilterValue,
  })
  assert.deepEqual(sortedIDs(statusGraph.nodes), ['bucket:1', 'bucket:2', 'data-set:12', 'data-set:21', 'provider:202'])
  assert.equal(
    statusGraph.nodes.find((node) => node.id === 'provider:606'),
    undefined
  )
})

test('storage topology finds detail nodes by chain id first and local id fallback', () => {
  const graph = buildStorageTopologyGraph(
    [provider101, provider202, provider606],
    [mediaDataSet, mediaReplicaDataSet, logsDataSet],
    { status: 'all', provider: allEntityFilterValue, bucket: allEntityFilterValue }
  )

  assert.equal(findProviderTopologyNode(graph, '202')?.id, 'provider:202')
  assert.equal(findProviderTopologyNode(graph, 'missing'), undefined)
  assert.equal(findDataSetTopologyNodeByChainID(graph, '51002')?.id, 'data-set:12')
  assert.equal(findDataSetTopologyNodeByChainID(graph, 'missing'), undefined)
  assert.equal(findDataSetTopologyNodeByLocalID(graph, 12)?.id, 'data-set:12')
  assert.equal(findDataSetTopologyNodeByLocalID(graph, 999), undefined)
  assert.deepEqual(findStorageTopologySelection(graph, { chainDataSetID: '51002', localDataSetID: 11 }), {
    type: 'node',
    id: 'data-set:12',
    kind: 'data-set',
  })
  assert.deepEqual(findStorageTopologySelection(graph, { localDataSetID: 12 }), {
    type: 'node',
    id: 'data-set:12',
    kind: 'data-set',
  })
  assert.deepEqual(findStorageTopologySelection(graph, { providerID: '202' }), {
    type: 'node',
    id: 'provider:202',
    kind: 'provider',
  })
  assert.equal(findStorageTopologySelection(graph, { localDataSetID: 999 }), null)
})

test('storage topology selection lookup keeps local data set id zero as a valid fallback', () => {
  const zeroDataSet: ObservabilityDataSetObservation = {
    ...mediaDataSet,
    facts: {
      ...mediaDataSet.facts,
      local_data_set_id: 0,
      chain_data_set_id: undefined,
    },
  }
  const graph = buildStorageTopologyGraph([provider101], [zeroDataSet], {
    status: 'all',
    provider: allEntityFilterValue,
    bucket: allEntityFilterValue,
  })

  assert.deepEqual(findStorageTopologySelection(graph, { localDataSetID: 0 }), {
    type: 'node',
    id: 'data-set:0',
    kind: 'data-set',
  })
})

test('storage topology chain data set lookup requires disambiguation when providers share a chain id', () => {
  const duplicateChainDataSet: ObservabilityDataSetObservation = {
    ...mediaReplicaDataSet,
    facts: {
      ...mediaReplicaDataSet.facts,
      chain_data_set_id: mediaDataSet.facts.chain_data_set_id,
    },
  }
  const graph = buildStorageTopologyGraph([provider101, provider202], [mediaDataSet, duplicateChainDataSet], {
    status: 'all',
    provider: allEntityFilterValue,
    bucket: allEntityFilterValue,
  })

  assert.equal(findStorageTopologySelection(graph, { chainDataSetID: '51001' }), null)
  assert.deepEqual(findStorageTopologySelection(graph, { chainDataSetID: '51001', providerID: '101' }), {
    type: 'node',
    id: 'data-set:11',
    kind: 'data-set',
  })
  assert.deepEqual(findStorageTopologySelection(graph, { chainDataSetID: '51001', providerID: '202' }), {
    type: 'node',
    id: 'data-set:12',
    kind: 'data-set',
  })
  assert.deepEqual(findStorageTopologySelection(graph, { chainDataSetID: '51001', localDataSetID: 12 }), {
    type: 'node',
    id: 'data-set:12',
    kind: 'data-set',
  })
})

test('storage topology resolves stable selections against current observations', () => {
  const graph = buildStorageTopologyGraph([provider101, provider202], [mediaDataSet, mediaReplicaDataSet], {
    status: 'all',
    provider: allEntityFilterValue,
    bucket: allEntityFilterValue,
  })

  assert.equal(
    resolveStorageTopologySelection(
      { type: 'node', id: 'data-set:12', kind: 'data-set' },
      graph,
      [provider101, provider202],
      [mediaDataSet, mediaReplicaDataSet]
    )?.type,
    'node'
  )
  assert.equal(
    resolveStorageTopologySelection(
      { type: 'node', id: 'data-set:12', kind: 'data-set' },
      buildStorageTopologyGraph([provider101], [mediaDataSet], {
        status: 'all',
        provider: allEntityFilterValue,
        bucket: allEntityFilterValue,
      }),
      [provider101],
      [mediaDataSet]
    ),
    null
  )
  assert.equal(
    resolveStorageTopologySelection(
      { type: 'data-set', localDataSetID: 12 },
      graph,
      [provider101, provider202],
      [mediaDataSet, mediaReplicaDataSet]
    )?.type,
    'data-set'
  )
})

test('storage topology paginates with clamped pages', () => {
  assert.deepEqual(paginateItems([1, 2, 3, 4], { page: 3, pageSize: 2 }), {
    items: [3, 4],
    page: 2,
    pageSize: 2,
    totalPages: 2,
    offset: 2,
    limit: 2,
  })
  assert.equal(clampPageForTotal(3, 1, 20), 1)
  assert.equal(clampPageForTotal(2, 21, 20), 2)
  assert.equal(clampPageForTotal(0, 0, 20), 1)
  assert.equal(clampPageForLoadedTotal(3, undefined, 20), 3)
  assert.equal(clampPageForLoadedTotal(3, 1, 20), 1)
})

function sortedIDs(items: Array<{ id: string }>) {
  return items.map((item) => item.id).sort()
}
