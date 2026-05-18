import assert from 'node:assert/strict'
import test from 'node:test'
import type { ObservabilityFreshness, ObservabilitySummary, OverviewData } from '../src/api/client.ts'
import {
  attentionDisplayRows,
  filecoinStorageHealthCheckedLabel,
  filecoinStorageHealthFreshnessLabel,
  filecoinStorageHealthLevelLabel,
  filecoinStorageHealthLevelStyle,
  filecoinStorageHealthLevelTone,
  filecoinStorageHealthPartialErrorRows,
  filecoinStorageHealthReadyPercent,
  filecoinStorageHealthStatusLabel,
  filecoinStorageHealthSummaryRow,
  overviewPipelineRows,
  workerHealthRows,
} from '../src/lib/overview.ts'

test('worker health rows use stable product labels and ordering', () => {
  assert.deepEqual(workerHealthRows({ wallet_operations: true, unknown_worker: false, uploader: true }), [
    { key: 'uploader', label: 'Upload', healthy: true },
    { key: 'wallet_operations', label: 'Wallet Operations', healthy: true },
    { key: 'unknown_worker', label: 'Unknown Worker', healthy: false },
  ])
})

test('attention rows stay quiet when nothing needs attention', () => {
  const rows = attentionDisplayRows({
    objects: { needs_attention: 0, unavailable: 0 },
    tasks: { failed: 0, exhausted: 0 },
  })

  assert.deepEqual(rows, [])
})

test('attention rows show only nonzero attention items', () => {
  const rows = attentionDisplayRows({
    objects: { needs_attention: 2, unavailable: 1 },
    tasks: { failed: 3, exhausted: 4 },
  })

  assert.deepEqual(rows, [
    { key: 'object_failures', label: 'Object failures', value: 2, tone: 'warning', target: 'buckets' },
    { key: 'unavailable', label: 'Unavailable objects', value: 1, tone: 'danger', target: 'buckets' },
    { key: 'failed_tasks', label: 'Failed tasks', value: 3, tone: 'danger', target: 'tasks', taskStatus: 'failed' },
    {
      key: 'exhausted_tasks',
      label: 'Retry limit reached',
      value: 4,
      tone: 'danger',
      target: 'tasks',
      taskStatus: 'exhausted',
    },
  ])
})

test('pipeline rows keep fixed order and active status breakdown', () => {
  const rows = overviewPipelineRows([
    { pipeline: 'sync', total: 5, by_status: { queued: 2, waiting: 3 } },
    { pipeline: 'upload', total: 1, by_status: { running: 1 } },
  ])

  assert.deepEqual(rows, [
    { key: 'prepare', label: 'Prepare', total: 0, queued: 0, scheduled: 0, waiting: 0, running: 0 },
    { key: 'upload', label: 'Upload', total: 1, queued: 0, scheduled: 0, waiting: 0, running: 1 },
    { key: 'commit', label: 'Commit', total: 0, queued: 0, scheduled: 0, waiting: 0, running: 0 },
    { key: 'sync', label: 'Sync', total: 5, queued: 2, scheduled: 0, waiting: 3, running: 0 },
    { key: 'evict', label: 'Evict', total: 0, queued: 0, scheduled: 0, waiting: 0, running: 0 },
    { key: 'cleanup', label: 'Cleanup', total: 0, queued: 0, scheduled: 0, waiting: 0, running: 0 },
  ])
})

test('filecoin storage health levels use stable labels and tones', () => {
  assert.equal(filecoinStorageHealthLevelLabel('ok'), 'Healthy')
  assert.equal(filecoinStorageHealthLevelTone('ok'), 'success')
  assert.equal(filecoinStorageHealthLevelLabel('warning'), 'Warning')
  assert.equal(filecoinStorageHealthLevelTone('warning'), 'warning')
  assert.equal(filecoinStorageHealthLevelLabel('blocking'), 'Blocked')
  assert.equal(filecoinStorageHealthLevelTone('blocking'), 'danger')
})

test('filecoin storage health level styles follow signal severity', () => {
  assert.deepEqual(filecoinStorageHealthLevelStyle('ok'), {
    textClassName: 'text-[color:var(--status-success)]',
    progressClassName: 'bg-[var(--status-success)]',
  })
  assert.deepEqual(filecoinStorageHealthLevelStyle('warning'), {
    textClassName: 'text-[color:var(--status-warning)]',
    progressClassName: 'bg-[var(--status-warning)]',
  })
  assert.deepEqual(filecoinStorageHealthLevelStyle('blocking'), {
    textClassName: 'text-[color:var(--status-danger)]',
    progressClassName: 'bg-[var(--status-danger)]',
  })
})

test('filecoin storage health freshness labels no-state stale and checked states', () => {
  assert.equal(
    filecoinStorageHealthFreshnessLabel({ stale: false, warnings: ['no_state_recorded'] }),
    'No state recorded'
  )
  assert.equal(
    filecoinStorageHealthFreshnessLabel({
      stale: true,
      warnings: ['stale_state'],
      last_checked_at: '9999-01-01T00:00:00Z',
    }),
    'Stale · just now'
  )
  assert.equal(
    filecoinStorageHealthFreshnessLabel({ stale: false, warnings: [], last_checked_at: '9999-01-01T00:00:00Z' }),
    'just now'
  )
})

test('filecoin storage health checked label uses worst provider or storage freshness', () => {
  assert.equal(
    filecoinStorageHealthCheckedLabel(
      filecoinStorageHealthFixture(
        {},
        {
          providerFreshness: { stale: true, warnings: ['stale_state'], last_checked_at: '9999-01-01T00:00:00Z' },
          dataSetFreshness: { stale: false, warnings: [], last_checked_at: '9999-01-02T00:00:00Z' },
        }
      )
    ),
    'Stale · just now'
  )
  assert.equal(
    filecoinStorageHealthCheckedLabel(
      filecoinStorageHealthFixture(
        {},
        {
          providerFreshness: { stale: false, warnings: ['no_state_recorded'] },
          dataSetFreshness: { stale: false, warnings: [], last_checked_at: '9999-01-02T00:00:00Z' },
        }
      )
    ),
    'No state recorded'
  )
})

test('filecoin storage health summary row exposes compact observability counts without reinterpreting them', () => {
  const row = filecoinStorageHealthSummaryRow('providers', 'Providers', {
    summary: { total: 10, available: 7, degraded: 1, unavailable: 1, unknown: 1 },
    summary_signal: {
      level: 'warning',
      freshness: { stale: false, warnings: [], last_checked_at: '9999-01-01T00:00:00Z' },
    },
  })

  assert.deepEqual(row, {
    key: 'providers',
    label: 'Providers',
    total: 10,
    available: 7,
    degraded: 1,
    unavailable: 1,
    unknown: 1,
    readyPercent: 70,
    stateRecorded: true,
    freshness: 'just now',
    level: 'warning',
    tone: 'warning',
  })
  assert.deepEqual(filecoinStorageHealthSummaryRow('data_sets', 'Data Sets', null), {
    key: 'data_sets',
    label: 'Data Sets',
    total: null,
    available: null,
    degraded: null,
    unavailable: null,
    unknown: null,
    readyPercent: null,
    stateRecorded: false,
    freshness: 'Summary unavailable',
    level: 'warning',
    tone: 'warning',
  })
})

test('filecoin storage health summary row does not render missing observations as zero counts', () => {
  assert.deepEqual(
    filecoinStorageHealthSummaryRow('data_sets', 'Data Sets', {
      summary: { total: 0, available: 0, degraded: 0, unavailable: 0, unknown: 0 },
      summary_signal: { level: 'warning', freshness: { stale: false, warnings: ['no_state_recorded'] } },
    }),
    {
      key: 'data_sets',
      label: 'Data Sets',
      total: null,
      available: null,
      degraded: null,
      unavailable: null,
      unknown: null,
      readyPercent: null,
      stateRecorded: false,
      freshness: 'No state recorded',
      level: 'warning',
      tone: 'warning',
    }
  )
})

test('filecoin storage health ready percent handles normal and empty totals', () => {
  assert.equal(filecoinStorageHealthReadyPercent(10, 11), 91)
  assert.equal(filecoinStorageHealthReadyPercent(0, 0), 0)
  assert.equal(filecoinStorageHealthReadyPercent(3, null), 0)
})

test('filecoin storage health status label prefers concrete storage and provider states', () => {
  assert.equal(
    filecoinStorageHealthStatusLabel(
      filecoinStorageHealthFixture({ dataSets: { total: 0 } }, { stateRecorded: false })
    ),
    'Checking'
  )
  assert.equal(filecoinStorageHealthStatusLabel(filecoinStorageHealthFixture({ level: 'ok' })), 'Healthy')
  assert.equal(
    filecoinStorageHealthStatusLabel(
      filecoinStorageHealthFixture({ dataSets: { total: 2, available: 1, degraded: 1 } })
    ),
    'Degraded'
  )
  assert.equal(
    filecoinStorageHealthStatusLabel(
      filecoinStorageHealthFixture({ dataSets: { total: 2, available: 1, unknown: 1 } })
    ),
    'Degraded'
  )
  assert.equal(
    filecoinStorageHealthStatusLabel(
      filecoinStorageHealthFixture({ providers: { total: 2, available: 1, unavailable: 1 } })
    ),
    'Provider degraded'
  )
  assert.equal(
    filecoinStorageHealthStatusLabel(
      filecoinStorageHealthFixture({ level: 'blocking', dataSets: { total: 2, available: 1, unavailable: 1 } })
    ),
    'Blocked'
  )
})

test('filecoin storage health partial error rows use fixed labels', () => {
  assert.deepEqual(filecoinStorageHealthPartialErrorRows({ observability_providers: 'health query failed' }), [
    { key: 'observability_providers', label: 'Provider summary unavailable', message: 'health query failed' },
  ])
})

function filecoinStorageHealthFixture(
  {
    level = 'warning',
    providers = { total: 1, available: 1 },
    dataSets = { total: 1, available: 1 },
  }: {
    level?: OverviewData['filecoin_storage_health']['level']
    providers?: Partial<ObservabilitySummary>
    dataSets?: Partial<ObservabilitySummary>
  } = {},
  options: {
    stateRecorded?: boolean
    providerFreshness?: ObservabilityFreshness
    dataSetFreshness?: ObservabilityFreshness
  } = {}
): OverviewData['filecoin_storage_health'] {
  const freshness =
    options.stateRecorded === false
      ? { stale: false, warnings: ['no_state_recorded'] }
      : { last_checked_at: '9999-01-01T00:00:00Z', stale: false, warnings: [] }
  return {
    level,
    providers: {
      summary: filecoinStorageHealthSummaryFixture(providers),
      summary_signal: {
        level: providers.unavailable || providers.degraded || providers.unknown ? 'warning' : 'ok',
        freshness: options.providerFreshness ?? freshness,
      },
    },
    data_sets: {
      summary: filecoinStorageHealthSummaryFixture(dataSets),
      summary_signal: {
        level: dataSets.unavailable || dataSets.degraded || dataSets.unknown ? 'warning' : 'ok',
        freshness: options.dataSetFreshness ?? freshness,
      },
    },
    partial_errors: {},
  }
}

function filecoinStorageHealthSummaryFixture(summary: Partial<ObservabilitySummary>): ObservabilitySummary {
  return {
    total: 0,
    available: 0,
    degraded: 0,
    unavailable: 0,
    unknown: 0,
    ...summary,
  }
}
