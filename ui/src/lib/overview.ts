import type { ObservabilityFreshness, OverviewData } from '@/api/client'
import type { StatusTone } from '@/components/app/StatusBadge'
import { timeAgo } from './utils.ts'

export type AttentionTone = 'warning' | 'danger'
export type FilecoinStorageHealthLevel = OverviewData['filecoin_storage_health']['level']

export interface AttentionDisplayRow {
  key: string
  label: string
  value: number
  tone: AttentionTone
  target: 'buckets' | 'tasks'
  taskStatus?: 'failed' | 'exhausted'
}

export interface PipelineDisplayRow {
  key: string
  label: string
  total: number
  queued: number
  scheduled: number
  waiting: number
  running: number
}

export interface FilecoinStorageHealthSummaryRow {
  key: string
  label: string
  total: number | null
  available: number | null
  degraded: number | null
  unavailable: number | null
  unknown: number | null
  readyPercent: number | null
  stateRecorded: boolean
  freshness: string
  level: FilecoinStorageHealthLevel
  tone: StatusTone
}

export interface FilecoinStorageHealthLevelStyle {
  textClassName: string
  progressClassName: string
}

const filecoinStorageHealthLevelLabels: Record<FilecoinStorageHealthLevel, string> = {
  ok: 'Healthy',
  warning: 'Warning',
  blocking: 'Blocked',
}

const filecoinStorageHealthLevelTones: Record<FilecoinStorageHealthLevel, StatusTone> = {
  ok: 'success',
  warning: 'warning',
  blocking: 'danger',
}

const filecoinStorageHealthLevelStyles: Record<FilecoinStorageHealthLevel, FilecoinStorageHealthLevelStyle> = {
  ok: {
    textClassName: 'text-[color:var(--status-success)]',
    progressClassName: 'bg-[var(--status-success)]',
  },
  warning: {
    textClassName: 'text-[color:var(--status-warning)]',
    progressClassName: 'bg-[var(--status-warning)]',
  },
  blocking: {
    textClassName: 'text-[color:var(--status-danger)]',
    progressClassName: 'bg-[var(--status-danger)]',
  },
}

const workerOrder = ['uploader', 'evictor', 'storage_cleanup', 'wallet_operations']
const workerLabels: Record<string, string> = {
  uploader: 'Upload',
  evictor: 'Cache Evictor',
  storage_cleanup: 'Replica Cleanup',
  wallet_operations: 'Wallet Operations',
}

const pipelineOrder = ['prepare', 'upload', 'commit', 'sync', 'evict', 'cleanup'] as const
type PipelineKey = (typeof pipelineOrder)[number]

const pipelineLabels: Record<PipelineKey, string> = {
  prepare: 'Prepare',
  upload: 'Upload',
  commit: 'Commit',
  sync: 'Sync',
  evict: 'Evict',
  cleanup: 'Cleanup',
}

export function workerHealthRows(workers: Record<string, boolean>) {
  return Object.entries(workers)
    .map(([key, healthy]) => ({
      key,
      label: workerLabels[key] ?? titleCaseEnum(key),
      healthy,
      order: workerOrder.includes(key) ? workerOrder.indexOf(key) : workerOrder.length,
    }))
    .sort((a, b) => a.order - b.order || a.label.localeCompare(b.label))
    .map(({ key, label, healthy }) => ({ key, label, healthy }))
}

export function attentionDisplayRows(attention: {
  objects: OverviewData['objects']['attention']
  tasks: OverviewData['tasks']['attention']
}): AttentionDisplayRow[] {
  return [
    {
      key: 'object_failures',
      label: 'Object failures',
      value: attention.objects.needs_attention,
      tone: 'warning' as const,
      target: 'buckets' as const,
    },
    {
      key: 'unavailable',
      label: 'Unavailable objects',
      value: attention.objects.unavailable,
      tone: 'danger' as const,
      target: 'buckets' as const,
    },
    {
      key: 'failed_tasks',
      label: 'Failed tasks',
      value: attention.tasks.failed,
      tone: 'danger' as const,
      target: 'tasks' as const,
      taskStatus: 'failed' as const,
    },
    {
      key: 'exhausted_tasks',
      label: 'Retry limit reached',
      value: attention.tasks.exhausted,
      tone: 'danger' as const,
      target: 'tasks' as const,
      taskStatus: 'exhausted' as const,
    },
  ].filter((row) => row.value > 0)
}

export function overviewPipelineRows(activePipeline: OverviewData['tasks']['active_pipeline']): PipelineDisplayRow[] {
  const byPipeline = new Map(activePipeline.map((row) => [row.pipeline, row]))
  return pipelineOrder.map((key) => {
    const row = byPipeline.get(key)
    return {
      key,
      label: pipelineLabels[key],
      total: row?.total ?? 0,
      queued: row?.by_status.queued ?? 0,
      scheduled: row?.by_status.scheduled ?? 0,
      waiting: row?.by_status.waiting ?? 0,
      running: row?.by_status.running ?? 0,
    }
  })
}

export function filecoinStorageHealthLevelLabel(level: FilecoinStorageHealthLevel) {
  return filecoinStorageHealthLevelLabels[level]
}

export function filecoinStorageHealthLevelTone(level: FilecoinStorageHealthLevel) {
  return filecoinStorageHealthLevelTones[level]
}

export function filecoinStorageHealthLevelStyle(level: FilecoinStorageHealthLevel) {
  return filecoinStorageHealthLevelStyles[level]
}

export function filecoinStorageHealthFreshnessLabel(
  freshness: NonNullable<OverviewData['filecoin_storage_health']['providers']>['summary_signal']['freshness']
) {
  if (freshness.warnings.includes('no_state_recorded')) {
    return 'No state recorded'
  }
  if (!freshness.last_checked_at) {
    return freshness.stale ? 'Stale' : 'No state recorded'
  }
  const checked = timeAgo(freshness.last_checked_at)
  return freshness.stale ? `Stale · ${checked}` : checked
}

export function filecoinStorageHealthCheckedLabel(health: OverviewData['filecoin_storage_health']) {
  const freshnesses = [health.providers?.summary_signal.freshness, health.data_sets?.summary_signal.freshness].filter(
    (freshness): freshness is ObservabilityFreshness => Boolean(freshness)
  )
  if (freshnesses.length === 0) return 'Summary unavailable'
  return filecoinStorageHealthFreshnessLabel(worstFilecoinStorageHealthFreshness(freshnesses))
}

export function filecoinStorageHealthSummaryRow(
  key: string,
  label: string,
  section: OverviewData['filecoin_storage_health']['providers']
): FilecoinStorageHealthSummaryRow {
  if (!section) {
    return {
      key,
      label,
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
    }
  }
  const stateRecorded = filecoinStorageHealthStateRecorded(section)
  if (!stateRecorded) {
    return {
      key,
      label,
      total: null,
      available: null,
      degraded: null,
      unavailable: null,
      unknown: null,
      readyPercent: null,
      stateRecorded,
      freshness: filecoinStorageHealthFreshnessLabel(section.summary_signal.freshness),
      level: section.summary_signal.level,
      tone: filecoinStorageHealthLevelTone(section.summary_signal.level),
    }
  }
  return {
    key,
    label,
    total: section.summary.total,
    available: section.summary.available,
    degraded: section.summary.degraded,
    unavailable: section.summary.unavailable,
    unknown: section.summary.unknown,
    readyPercent: filecoinStorageHealthReadyPercent(section.summary.available, section.summary.total),
    stateRecorded,
    freshness: filecoinStorageHealthFreshnessLabel(section.summary_signal.freshness),
    level: section.summary_signal.level,
    tone: filecoinStorageHealthLevelTone(section.summary_signal.level),
  }
}

export function filecoinStorageHealthReadyPercent(available: number, total: number | null | undefined) {
  if (!total || total <= 0) return 0
  return Math.max(0, Math.min(100, Math.round((available / total) * 100)))
}

export function filecoinStorageHealthStatusLabel(health: OverviewData['filecoin_storage_health']) {
  if (Object.keys(health.partial_errors ?? {}).length > 0) return filecoinStorageHealthLevelLabel(health.level)
  if (health.level === 'blocking') return filecoinStorageHealthLevelLabel(health.level)
  if (hasFilecoinStorageHealthIssue(health.data_sets)) return 'Degraded'
  if (hasFilecoinStorageHealthIssue(health.providers)) return 'Provider degraded'
  if (!filecoinStorageHealthStateRecorded(health.data_sets) || !filecoinStorageHealthStateRecorded(health.providers))
    return 'Checking'
  return filecoinStorageHealthLevelLabel(health.level)
}

export function filecoinStorageHealthPartialErrorRows(partialErrors: Record<string, string>) {
  const labels: Record<string, string> = {
    observability: 'Observability unavailable',
    observability_providers: 'Provider summary unavailable',
    observability_data_sets: 'Data set summary unavailable',
  }
  return Object.entries(partialErrors).map(([key, message]) => ({
    key,
    label: labels[key] ?? titleCaseEnum(key),
    message,
  }))
}

function hasFilecoinStorageHealthIssue(section: OverviewData['filecoin_storage_health']['providers']) {
  if (!section || !filecoinStorageHealthStateRecorded(section)) return false
  return section.summary.degraded > 0 || section.summary.unavailable > 0 || section.summary.unknown > 0
}

function filecoinStorageHealthStateRecorded(section: OverviewData['filecoin_storage_health']['providers']) {
  if (!section) return false
  const freshness = section.summary_signal.freshness
  return Boolean(freshness.last_checked_at) && !freshness.warnings.includes('no_state_recorded')
}

function worstFilecoinStorageHealthFreshness(freshnesses: ObservabilityFreshness[]) {
  const noState = freshnesses.find((freshness) => freshness.warnings.includes('no_state_recorded'))
  if (noState) return noState
  const stale = freshnesses.filter((freshness) => freshness.stale)
  return oldestFilecoinStorageHealthFreshness(stale.length > 0 ? stale : freshnesses)
}

function oldestFilecoinStorageHealthFreshness(freshnesses: ObservabilityFreshness[]) {
  return freshnesses.reduce((worst, current) =>
    filecoinStorageHealthFreshnessTime(current) < filecoinStorageHealthFreshnessTime(worst) ? current : worst
  )
}

function filecoinStorageHealthFreshnessTime(freshness: ObservabilityFreshness) {
  if (!freshness.last_checked_at) return Number.NEGATIVE_INFINITY
  const value = Date.parse(freshness.last_checked_at)
  return Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY
}

function titleCaseEnum(value: string) {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}
