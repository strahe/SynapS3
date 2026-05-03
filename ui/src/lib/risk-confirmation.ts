import type { SettingsEditableConfig, SettingsFieldMetadata } from '@/api/client'

export type SettingsRiskSeverity = 'medium' | 'high'
export type SettingsRiskConfirmation = 'review' | 'strong'

export interface SettingsRiskChange {
  field: string
  label: string
  from: string
  to: string
  severity: SettingsRiskSeverity
  reason: string
}

export function confirmationMatches(input: string, target: string) {
  return input === target
}

export function classifySettingsRisk(change: SettingsRiskChange): SettingsRiskConfirmation {
  return change.severity === 'high' ? 'strong' : 'review'
}

export function settingsRiskNeedsStrongConfirmation(changes: SettingsRiskChange[]) {
  return changes.some((change) => classifySettingsRisk(change) === 'strong')
}

export function collectSettingsRiskChanges(
  initial: SettingsEditableConfig,
  next: SettingsEditableConfig,
  envManaged: Record<string, string>,
  metadata: Record<string, SettingsFieldMetadata>
) {
  const changes: SettingsRiskChange[] = []

  const addChanged = (
    field: string,
    from: string | number | boolean,
    to: string | number | boolean,
    severity: SettingsRiskSeverity,
    reason: string
  ) => {
    pushRiskChange(changes, metadata, envManaged, field, from, to, severity, reason)
  }

  addChanged('server.port', initial.server.port, next.server.port, 'medium', 'Changes the S3 API listener address.')
  addChanged(
    'server.tls.enabled',
    initial.server.tls.enabled,
    next.server.tls.enabled,
    'medium',
    'Changes TLS behavior for the S3 API listener.'
  )
  addChanged(
    'server.tls.cert_file',
    initial.server.tls.cert_file,
    next.server.tls.cert_file,
    'medium',
    'Changes the TLS certificate used by the S3 API listener.'
  )
  addChanged(
    'server.tls.key_file',
    initial.server.tls.key_file,
    next.server.tls.key_file,
    'medium',
    'Changes the TLS private key used by the S3 API listener.'
  )
  addChanged('s3.region', initial.s3.region, next.s3.region, 'medium', 'Changes the S3 region reported to clients.')

  addChanged(
    'filecoin.network',
    initial.filecoin.network,
    next.filecoin.network,
    'high',
    'Switches the Filecoin network used for storage operations.'
  )
  addChanged(
    'filecoin.rpc_url',
    initial.filecoin.rpc_url,
    next.filecoin.rpc_url,
    'medium',
    'Changes the Filecoin RPC endpoint used by background work.'
  )
  if (!initial.filecoin.allow_private_networks && next.filecoin.allow_private_networks) {
    addChanged(
      'filecoin.allow_private_networks',
      initial.filecoin.allow_private_networks,
      next.filecoin.allow_private_networks,
      'high',
      'Allows private-network retrieval URLs; enable only in trusted environments.'
    )
  }
  addChanged(
    'filecoin.default_copies',
    initial.filecoin.default_copies,
    next.filecoin.default_copies,
    'medium',
    'Changes the default Filecoin copy count for buckets without an explicit policy.'
  )

  addChanged('cache.dir', initial.cache.dir, next.cache.dir, 'medium', 'Changes where cached object data is stored.')
  if (next.cache.max_size_gb < initial.cache.max_size_gb) {
    addChanged(
      'cache.max_size_gb',
      initial.cache.max_size_gb,
      next.cache.max_size_gb,
      'medium',
      'Reduces available cache capacity.'
    )
  }
  addChanged(
    'cache.eviction_policy',
    initial.cache.eviction_policy,
    next.cache.eviction_policy,
    'medium',
    'Changes local cache eviction behavior.'
  )

  addWorkerRiskChanges(changes, initial, next, envManaged, metadata, 'upload')
  addWorkerRiskChanges(changes, initial, next, envManaged, metadata, 'evictor')

  return changes
}

function addWorkerRiskChanges(
  changes: SettingsRiskChange[],
  initial: SettingsEditableConfig,
  next: SettingsEditableConfig,
  envManaged: Record<string, string>,
  metadata: Record<string, SettingsFieldMetadata>,
  pool: 'upload' | 'evictor'
) {
  const prefix = `worker.${pool}`
  const initialPool = initial.worker[pool]
  const nextPool = next.worker[pool]

  if (nextPool.concurrency > initialPool.concurrency) {
    pushRiskChange(
      changes,
      metadata,
      envManaged,
      `${prefix}.concurrency`,
      initialPool.concurrency,
      nextPool.concurrency,
      'medium',
      'Increases concurrent background work.'
    )
  }
  pushRiskChange(
    changes,
    metadata,
    envManaged,
    `${prefix}.poll_interval`,
    initialPool.poll_interval,
    nextPool.poll_interval,
    'medium',
    'Changes how often background work is polled.'
  )
  if (nextPool.max_retries > initialPool.max_retries) {
    pushRiskChange(
      changes,
      metadata,
      envManaged,
      `${prefix}.max_retries`,
      initialPool.max_retries,
      nextPool.max_retries,
      'medium',
      'Increases retry attempts for failed background work.'
    )
  }
}

function pushRiskChange(
  changes: SettingsRiskChange[],
  metadata: Record<string, SettingsFieldMetadata>,
  envManaged: Record<string, string>,
  field: string,
  from: string | number | boolean,
  to: string | number | boolean,
  severity: SettingsRiskSeverity,
  reason: string
) {
  if (envManaged[field] || from === to) return
  changes.push({
    field,
    label: metadata[field]?.label ?? field,
    from: String(from),
    to: String(to),
    severity,
    reason,
  })
}
