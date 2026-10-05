import type { TaskItem, TaskSubjectInfo, TaskSubjectProvider } from '../api/client.ts'
import { replicaLabel } from './storage-status-labels.ts'
import { formatBytes, formatTokenAmount } from './utils.ts'

export const taskOperationLabels: Record<string, string> = {
  bucket_provision: 'Prepare bucket',
  upload_plan: 'Prepare upload',
  storage_dataset_ensure: 'Create dataset',
  storage_transfer_plan: 'Prepare copy',
  storage_store: 'Upload data',
  storage_pull: 'Copy data',
  storage_commit: 'Confirm storage',
  provider_replacement_coordinate: 'Replace provider',
  cache_evict: 'Clear cache',
  cache_reconcile_durability: 'Apply cache policy',
  storage_cleanup: 'Delete remote copies',
  storage_dataset_retire: 'Close dataset',
  wallet_operation: 'Wallet operation',
  provider_upload_speed_test: 'Test upload speed',
  cache_capacity_reconcile: 'Manage cache',
  observability_refresh: 'Refresh health',
  approved_provider_refresh: 'Refresh approved providers',
  endorsed_provider_refresh: 'Refresh endorsed providers',
  task_gc: 'Clean task history',
}

export function taskOperationLabel(type: string) {
  return taskOperationLabels[type] ?? 'Background operation'
}

const subjectLabels: Record<string, string> = {
  storage_content: 'Content',
  storage_copy: 'Copy',
  storage_data_set: 'Dataset',
  bucket: 'Bucket',
  provider: 'Provider',
  storage_replacement: 'Replacement',
  wallet_operation: 'Wallet operation',
  storage_commit_request: 'Storage registration',
}

export function taskSubjectLabel(task: Pick<TaskItem, 'subject_type' | 'subject_key'>) {
  if (task.subject_type === 'system') return 'System'
  if (!task.subject_type || !task.subject_key) return '—'
  if (task.subject_type === 'storage_commit_request') return 'Storage registration'
  return `${subjectLabels[task.subject_type] ?? 'Resource'} #${task.subject_key}`
}

export function isTaskSubjectType(value: string): value is TaskSubjectInfo['subject_type'] {
  return Object.keys(subjectLabels).includes(value)
}

export function taskSystemDescription(type: string) {
  const descriptions: Record<string, string> = {
    cache_capacity_reconcile: 'Keeps local cache usage within its limit.',
    observability_refresh: 'Updates storage health information.',
    approved_provider_refresh: 'Updates approved providers.',
    endorsed_provider_refresh: 'Updates endorsed providers.',
    task_gc: 'Removes task history after its retention period.',
  }
  return descriptions[type] ?? 'Runs maintenance for this node.'
}

export function taskDetailsView(task: Pick<TaskItem, 'status' | 'last_error' | 'status_message'>) {
  const failed = task.status === 'failed'
  const value = failed ? task.last_error || task.status_message : task.status_message
  return {
    value,
    label: failed && task.last_error ? 'Task error' : 'Task details',
    lastError: !failed && task.last_error !== value ? task.last_error : undefined,
  }
}

export function taskTook(task: Pick<TaskItem, 'status' | 'started_at' | 'finished_at'>) {
  if (task.status === 'pending' || task.status === 'running' || !task.started_at || !task.finished_at) return '—'
  const milliseconds = Date.parse(task.finished_at) - Date.parse(task.started_at)
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return '—'
  if (milliseconds < 1000) return '<1s'
  const seconds = Math.floor(milliseconds / 1000)
  const units = [
    [86_400, 'd'],
    [3600, 'h'],
    [60, 'm'],
    [1, 's'],
  ] as const
  const index = units.findIndex(([size]) => seconds >= size)
  const unit = units[index]
  if (!unit) return '—'
  const [size, label] = unit
  const parts = [`${Math.floor(seconds / size)}${label}`]
  const next = units[index + 1]
  if (next) {
    const amount = Math.floor((seconds % size) / next[0])
    if (amount) parts.push(`${amount}${next[1]}`)
  }
  return parts.join(' ')
}

export interface TaskSubjectField {
  label: string
  value: string
  note?: string
}

function providerLabel(provider?: TaskSubjectProvider) {
  return provider ? (provider.name ? `${provider.name} (#${provider.id})` : `Provider #${provider.id}`) : 'Unavailable'
}

export function taskSubjectFields(info: TaskSubjectInfo): TaskSubjectField[] {
  const bucket = { label: 'Bucket', value: info.bucket || 'Unavailable' }
  const replica = info.copy_index === undefined ? 'Unavailable' : replicaLabel(info.copy_index)
  const provider = { label: 'Provider', value: providerLabel(info.provider) }
  const file: TaskSubjectField = {
    label: 'File',
    value: info.file?.key ?? 'File information unavailable',
    note: [
      info.file?.source === 'deleted' ? 'Deleted' : info.file?.source === 'historical' ? 'Historical version' : '',
      info.file?.other_versions
        ? `${info.file.other_versions} other linked version${info.file.other_versions === 1 ? '' : 's'}`
        : '',
    ]
      .filter(Boolean)
      .join(' · '),
  }
  const size = { label: 'Size', value: info.size === undefined ? 'Unavailable' : formatBytes(info.size) }
  switch (info.subject_type) {
    case 'storage_content':
      return [file, bucket, size]
    case 'storage_copy':
      return [file, bucket, size, { label: 'Replica / Provider', value: `${replica} · ${provider.value}` }]
    case 'storage_data_set':
      return [
        bucket,
        { label: 'Replica', value: replica },
        provider,
        { label: 'On-chain Dataset ID', value: info.data_set_id ?? 'Not available yet' },
      ]
    case 'bucket':
      return [{ label: 'Name', value: info.bucket || 'Unavailable' }]
    case 'provider':
      return [
        { label: 'Name', value: info.provider?.name || 'Unavailable' },
        { label: 'Provider ID', value: info.provider?.id ?? info.subject_key },
        { label: 'Service address', value: info.provider?.service_url || 'Unavailable' },
      ]
    case 'storage_replacement':
      return [
        bucket,
        { label: 'Replica', value: replica },
        { label: 'From', value: providerLabel(info.source_provider) },
        { label: 'To', value: providerLabel(info.target_provider) },
      ]
    case 'wallet_operation': {
      const operation = info.wallet?.operation
      const labels: Record<string, string> = { fund: 'Fund account', withdraw: 'Withdraw', approve: 'Approve FWSS' }
      return [
        { label: 'Operation', value: labels[operation ?? ''] ?? 'Wallet operation' },
        operation === 'approve'
          ? { label: 'Authorized service', value: 'FWSS' }
          : { label: 'Amount', value: formatTokenAmount(info.wallet?.amount, 18, 'USDFC') },
      ]
    }
    case 'storage_commit_request':
      return [
        bucket,
        {
          label: 'Dataset',
          value: info.local_data_set_id === undefined ? 'Unavailable' : `Local #${info.local_data_set_id}`,
          note: info.data_set_id === undefined ? undefined : `On-chain #${info.data_set_id}`,
        },
        { label: 'Replica / Provider', value: `${replica} · ${provider.value}` },
        { label: 'Contents', value: info.content_count === undefined ? 'Unavailable' : String(info.content_count) },
      ]
  }
}
