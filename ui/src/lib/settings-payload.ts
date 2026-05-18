import type { SettingsEditableConfig, SettingsUpdatePayload } from '../api/client'

export function buildSettingsPayload(
  form: SettingsEditableConfig,
  initial: SettingsEditableConfig,
  envManaged: Record<string, string>
): SettingsUpdatePayload {
  const include = (field: string) => !envManaged[field]
  const payload: SettingsUpdatePayload = {}

  payload.server = {}
  if (include('server.port')) payload.server.port = form.server.port
  if (include('server.max_connections')) payload.server.max_connections = form.server.max_connections
  if (include('server.max_requests')) payload.server.max_requests = form.server.max_requests
  payload.server.tls = {}
  if (include('server.tls.enabled')) payload.server.tls.enabled = form.server.tls.enabled
  if (include('server.tls.cert_file')) payload.server.tls.cert_file = form.server.tls.cert_file
  if (include('server.tls.key_file')) payload.server.tls.key_file = form.server.tls.key_file

  payload.s3 = {}
  if (include('s3.region')) payload.s3.region = form.s3.region

  payload.filecoin = {}
  if (include('filecoin.network')) payload.filecoin.network = form.filecoin.network
  if (include('filecoin.rpc_url')) payload.filecoin.rpc_url = form.filecoin.rpc_url
  if (include('filecoin.source')) payload.filecoin.source = form.filecoin.source
  if (include('filecoin.default_copies')) payload.filecoin.default_copies = form.filecoin.default_copies
  if (include('filecoin.with_cdn')) payload.filecoin.with_cdn = form.filecoin.with_cdn
  if (include('filecoin.allow_private_networks'))
    payload.filecoin.allow_private_networks = form.filecoin.allow_private_networks
  payload.filecoin.observability = {}
  if (include('filecoin.observability.interval'))
    payload.filecoin.observability.interval = form.filecoin.observability.interval
  if (include('filecoin.observability.timeout'))
    payload.filecoin.observability.timeout = form.filecoin.observability.timeout
  if (include('filecoin.observability.concurrency'))
    payload.filecoin.observability.concurrency = form.filecoin.observability.concurrency

  payload.cache = {}
  if (include('cache.dir') && form.cache.dir !== initial.cache.dir) payload.cache.dir = form.cache.dir
  if (include('cache.max_size_gb')) payload.cache.max_size_gb = form.cache.max_size_gb
  if (include('cache.eviction_policy')) payload.cache.eviction_policy = form.cache.eviction_policy

  const upload: NonNullable<NonNullable<SettingsUpdatePayload['worker']>['upload']> = {}
  const evictor: NonNullable<NonNullable<SettingsUpdatePayload['worker']>['evictor']> = {}
  const storageCleanup: NonNullable<NonNullable<SettingsUpdatePayload['worker']>['storage_cleanup']> = {}
  if (include('worker.upload.concurrency')) upload.concurrency = form.worker.upload.concurrency
  if (include('worker.upload.poll_interval')) upload.poll_interval = form.worker.upload.poll_interval
  if (include('worker.upload.max_retries')) upload.max_retries = form.worker.upload.max_retries
  if (include('worker.evictor.concurrency')) evictor.concurrency = form.worker.evictor.concurrency
  if (include('worker.evictor.poll_interval')) evictor.poll_interval = form.worker.evictor.poll_interval
  if (include('worker.evictor.max_retries')) evictor.max_retries = form.worker.evictor.max_retries
  if (include('worker.storage_cleanup.concurrency'))
    storageCleanup.concurrency = form.worker.storage_cleanup.concurrency
  if (include('worker.storage_cleanup.poll_interval'))
    storageCleanup.poll_interval = form.worker.storage_cleanup.poll_interval
  if (include('worker.storage_cleanup.max_retries'))
    storageCleanup.max_retries = form.worker.storage_cleanup.max_retries
  payload.worker = { upload, evictor, storage_cleanup: storageCleanup }

  payload.logging = {}
  if (include('logging.level')) payload.logging.level = form.logging.level
  if (include('logging.format')) payload.logging.format = form.logging.format
  payload.logging.s3_access = {}
  if (include('logging.s3_access.enabled')) payload.logging.s3_access.enabled = form.logging.s3_access.enabled
  if (include('logging.s3_access.level')) payload.logging.s3_access.level = form.logging.s3_access.level

  return payload
}
