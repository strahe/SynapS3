import type { ObjectState, ObjectStatus } from '@/api/client'
import type { StatusTone } from '@/components/app/StatusBadge'
import { titleCaseEnum } from './utils.ts'

export function taskReplicaLabel(task: { copy_index?: number; copyIndex?: number }) {
  const copyIndex = task.copy_index ?? task.copyIndex
  if (typeof copyIndex !== 'number') return '—'
  return replicaLabel(copyIndex)
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
  progressPercent: number | null = null
) {
  switch (state) {
    case 'cached':
      return 'Stored in cache'
    case 'uploading':
      return progressPercent === null ? 'Uploading' : `Uploading to Filecoin ${progressPercent}%`
    case 'committing':
      return 'Registering storage record on-chain'
    case 'replicating':
      return 'Syncing replicas'
    case 'stored':
      // Distinct from a single replica's "Stored" so the object summary and the
      // per-copy rows stay tellable apart.
      return 'Stored on Filecoin'
    case 'failed':
      return 'Needs attention'
    default:
      return objectStatusLabel(status)
  }
}
