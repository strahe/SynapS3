import type { ObjectState, ObjectStatus, ObjectUploadStatus } from '@/api/client'
import type { StatusTone } from '@/components/app/StatusBadge'
import { titleCaseEnum } from './utils.ts'

export const taskStageOptions = [
  'all',
  'prepare_upload',
  'ensure_dataset',
  'ingress_store',
  'ingress_commit',
  'peer_pull',
  'peer_commit',
] as const

export type TaskStageOption = (typeof taskStageOptions)[number]

const taskStageLabels: Record<Exclude<TaskStageOption, 'all'> | '', string> = {
  prepare_upload: 'Prepare Filecoin storage',
  ensure_dataset: 'Prepare replica target',
  ingress_store: 'Upload source replica',
  ingress_commit: 'Register source replica on-chain',
  peer_pull: 'Sync peer replica',
  peer_commit: 'Register peer replica on-chain',
  '': 'Upload',
}

export function taskTypeLabel(type?: string) {
  switch (type) {
    case 'all':
      return 'All'
    case 'upload':
      return 'Upload'
    case 'evict_cache':
      return 'Evict Cache'
    case 'storage_cleanup':
      return 'Replica Cleanup'
    default:
      return titleCaseEnum(type)
  }
}

export function taskOperationOptionLabel(stage: TaskStageOption) {
  if (stage === 'all') return 'All'
  return taskStageLabels[stage]
}

export function taskOperationLabel(task: { type?: string; stage?: string }) {
  const stage = task.stage ?? ''
  return taskOperationBaseLabel(task.type, stage)
}

export function taskReplicaLabel(task: { copy_index?: number; copyIndex?: number }) {
  const copyIndex = task.copy_index ?? task.copyIndex
  if (typeof copyIndex !== 'number') return '—'
  return replicaLabel(copyIndex)
}

export function taskHasByteTransfer(task: { type?: string; stage?: string }) {
  if (task.type !== 'upload') return false
  return task.stage === 'ingress_store' || task.stage === ''
}

export function storageCleanupStatusLabel(copies: Array<{ status?: string }>) {
  if (copies.length === 0) return 'No remote replicas to delete'
  if (copies.some((copy) => copy.status === 'failed' || copy.status === 'unsupported')) return 'Needs attention'
  if (copies.every((copy) => copy.status === 'removed')) return 'Remote replicas deleted'
  if (copies.some((copy) => copy.status === 'delete_scheduled')) return 'Replica deletion scheduled'
  return 'Waiting to delete replicas'
}

export function storageCleanupCopyStatusLabel(status?: string) {
  switch (status) {
    case 'pending':
      return 'Waiting'
    case 'delete_scheduled':
      return 'Scheduled'
    case 'removed':
      return 'Removed'
    case 'failed':
      return 'Failed'
    case 'unsupported':
      return 'Unsupported'
    default:
      return titleCaseEnum(status) || 'Unknown'
  }
}

export function storageCleanupCopyStatusTone(status?: string): StatusTone {
  switch (status) {
    case 'removed':
      return 'success'
    case 'delete_scheduled':
      return 'info'
    case 'failed':
    case 'unsupported':
      return 'danger'
    case 'pending':
      return 'neutral'
    default:
      return 'neutral'
  }
}

function taskOperationBaseLabel(type: string | undefined, stage: string) {
  const stageLabel = taskStageLabels[stage as keyof typeof taskStageLabels]
  if (stageLabel && stage !== '') return stageLabel
  switch (type) {
    case 'evict_cache':
      return 'Evict local cache'
    case 'storage_cleanup':
      return 'Delete remote replicas'
    case 'upload':
      return 'Upload object'
    default:
      return titleCaseEnum(type) || 'Run task'
  }
}

export function replicaLabel(copyIndex: number) {
  return `Replica ${copyIndex + 1}`
}

export function transferMethodLabel(method?: string) {
  switch (method) {
    case 'ingress':
      return 'Ingress upload'
    case 'peer_pull':
      return 'Peer sync'
    default:
      return method || '—'
  }
}

export function uploadStatusLabel(uploadStatus: ObjectUploadStatus, progressPercent: number | null = null) {
  switch (uploadStatus) {
    case 'running':
      return progressPercent === null ? 'Preparing Filecoin storage' : `Uploading to Filecoin ${progressPercent}%`
    case 'ingress_ready':
      return 'Registering storage record on-chain'
    case 'readable':
      return 'Available, syncing replicas'
    case 'complete':
      return 'Stored on Filecoin · On-chain verified'
    case 'failed':
      return 'Needs attention'
    case 'rejected':
      return 'Upload rejected'
    case 'superseded':
      return 'Replaced by newer version'
  }
}

export function objectStatusLabel(status: ObjectStatus) {
  switch (status) {
    case 'success':
      return 'Success'
    case 'warning':
      return 'Warning'
    case 'unavailable':
      return 'Unavailable'
    case 'syncing':
      return 'Syncing'
    case 'uploading':
      return 'Uploading'
  }
}

export function objectStateLabel(
  state: ObjectState | undefined,
  status: ObjectStatus,
  uploadStatus?: ObjectUploadStatus,
  progressPercent: number | null = null
) {
  if (uploadStatus) return uploadStatusLabel(uploadStatus, progressPercent)
  switch (state) {
    case 'cached':
      return 'Stored in cache'
    case 'uploading':
      return 'Uploading'
    case 'committing':
      return 'Registering storage record on-chain'
    case 'replicating':
      return 'Syncing replicas'
    case 'stored':
      return 'Stored'
    case 'cache_evicted':
      return 'Stored remotely'
    case 'failed':
      return 'Needs attention'
    default:
      return objectStatusLabel(status)
  }
}
