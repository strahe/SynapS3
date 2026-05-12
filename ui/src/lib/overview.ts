import type { OverviewData } from '@/api/client'

export type AttentionTone = 'warning' | 'danger'

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

function titleCaseEnum(value: string) {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}
