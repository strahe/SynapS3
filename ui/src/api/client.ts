const BASE = '/api/v1'

export const internalRootOwnerAccessKey = '__internal_root__'
export const minFOCUploadSize = 127
export const maxFOCUploadSize = 1_065_353_216

export function validateFOCUploadSize(size: number) {
  if (size < minFOCUploadSize) {
    return `EntityTooSmall: object size must be at least ${minFOCUploadSize} B`
  }
  if (size > maxFOCUploadSize) {
    return `EntityTooLarge: object size must be no more than ${maxFOCUploadSize} bytes`
  }
  return null
}

async function fetchJSON<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers)
  if (init?.body !== undefined && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json')
  }

  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers,
  })
  if (!res.ok) {
    const body = await res.json().catch(() => ({}) as Record<string, unknown>)
    const errorBody = body as { error?: string; fields?: SettingsFieldError[] }
    const fieldText = Array.isArray(errorBody.fields)
      ? errorBody.fields.map((field) => `${field.field}: ${field.message}`).join(', ')
      : ''
    throw new Error([errorBody.error || `API error: ${res.status}`, fieldText].filter(Boolean).join(' - '))
  }
  if (res.status === 204) return undefined as T
  return res.json() as Promise<T>
}

type APIRequestOptions = Pick<RequestInit, 'signal'>

export interface OverviewData {
  buckets: { total: number; by_status: Record<string, number> }
  objects: {
    total: number
    total_size_bytes: number
    by_state: Record<string, number>
    attention: { needs_attention: number; unavailable: number }
  }
  tasks: {
    by_status: Record<string, number>
    attention: { failed: number; exhausted: number }
    active_pipeline: Array<{ pipeline: string; by_status: Record<string, number>; total: number }>
  }
  cache: { used_bytes: number; max_bytes: number }
  workers: Record<string, boolean>
  system: { version: string; commit: string; build_date: string; uptime_seconds: number }
  filecoin_storage_health: OverviewFilecoinStorageHealth
}

export type ObservabilitySignalLevel = 'ok' | 'warning' | 'blocking'
export type ObservabilityStatus = 'available' | 'degraded' | 'unavailable' | 'unknown'

export interface ObservabilitySummary {
  total: number
  available: number
  degraded: number
  unavailable: number
  unknown: number
}

export interface ObservabilityFreshness {
  last_checked_at?: string
  stale: boolean
  warnings: string[]
}

export interface ObservabilitySummarySignal {
  level: ObservabilitySignalLevel
  freshness: ObservabilityFreshness
}

export interface ObservabilitySignal {
  status: ObservabilityStatus
  level: ObservabilitySignalLevel
  reason_codes: string[]
  last_error?: string
  freshness: ObservabilityFreshness
}

export interface ObservabilityProviderFacts {
  provider_id: string
  active?: boolean
  has_pdp?: boolean
  service_url?: string
  health_status?: string
}

export interface ObservabilityProviderObservation {
  facts: ObservabilityProviderFacts
  signal: ObservabilitySignal
}

export interface ObservabilityDataSetFacts {
  local_data_set_id: number
  bucket_id: number
  bucket_name: string
  copy_index: number
  provider_id: string
  chain_data_set_id?: string
  client_data_set_id?: string
  local_status: string
  active_piece_count?: number
}

export interface ObservabilityDataSetObservation {
  facts: ObservabilityDataSetFacts
  signal: ObservabilitySignal
}

export interface OverviewFilecoinStorageHealthSection {
  summary: ObservabilitySummary
  summary_signal: ObservabilitySummarySignal
}

export interface OverviewFilecoinStorageHealth {
  level: ObservabilitySignalLevel
  providers: OverviewFilecoinStorageHealthSection | null
  data_sets: OverviewFilecoinStorageHealthSection | null
  partial_errors: Record<string, string>
}

export interface BucketItem {
  id: number
  name: string
  owner_access_key: string | null
  default_copies: number | null
  effective_copies: number
  status: string
  object_count: number
  total_size_bytes: number
  storage_health: BucketStorageHealthSummary
  created_at: string
}

export interface StorageDataSetSummary {
  id: number
  bucket_id: number
  bucket_name?: string
  copy_index: number
  provider_id: string
  provider_identity?: ProviderIdentity
  data_set_id?: string
  client_data_set_id?: string
  status: string
  created_by_upload_id?: number
  last_used_upload_id?: number
  committed_copies: number
  readable_copies: number
  physical_bytes: number
  referenced_version_count: number
  current_version_count: number
  created_at: string
  updated_at: string
  storage_health?: DataSetStorageHealthInfo
}

export type StorageHealthStatus = ObservabilityStatus
export type CopyHealthStatus = ObservabilityStatus

export interface DataSetStorageHealthInfo {
  status: StorageHealthStatus
  reason_codes: string[] | null
  active_piece_count?: number
  last_checked_at?: string
  last_error?: string
  stale: boolean
}

export interface BucketStorageHealthSummary {
  status: StorageHealthStatus
  reason_codes: string[]
  stale: boolean
  last_checked_at?: string
  last_error?: string
  abnormal_data_sets: number
  affected_versions_capped: number
  affected_versions_cap: number
  affected_versions_exceeds_cap: boolean
}

export interface CopyHealthInfo {
  status: CopyHealthStatus
  reason_codes: string[]
  stale: boolean
  last_checked_at?: string
  last_error?: string
}

export interface CopyHealthSummary extends CopyHealthInfo {
  total_objects: number
  unhealthy_objects: number
  requested_copies: number
  readable_copies: number
  pending_copies: number
  failed_copies: number
  unknown_copies: number
}

export interface ObservabilityListResponse<T> {
  items: T[]
  summary: ObservabilitySummary
  summary_signal: ObservabilitySummarySignal
  total: number
  limit: number
  offset: number
}

export interface ObservabilityListParams {
  status?: ObservabilityStatus | ''
  provider_id?: string
  bucket?: string
  bucket_id?: number
  limit?: number
  offset?: number
}

export interface BucketDetail extends BucketItem {
  updated_at: string
  versioning_status: string
  versioning_enforced: boolean
  data_sets: StorageDataSetSummary[]
}

export interface BucketMutationResponse {
  id: number
  name: string
  owner_access_key: string | null
  default_copies: number | null
  effective_copies: number
  status: string
}

export interface ObjectLocation {
  cache: boolean
  filecoin: boolean
}

export type ObjectStatus = 'uploading' | 'syncing' | 'success' | 'warning' | 'unavailable'
export type ObjectState = 'cached' | 'uploading' | 'committing' | 'replicating' | 'stored' | 'cache_evicted' | 'failed'
export type ObjectUploadStatus =
  | 'running'
  | 'ingress_ready'
  | 'readable'
  | 'complete'
  | 'failed'
  | 'rejected'
  | 'superseded'

export interface UploadTransferProgress {
  scope: 'ingress_store'
  attempt: number
  uploaded_bytes: number
  total_bytes: number
  percent?: number
  done: boolean
  updated_at: string
}

export interface ObjectItem {
  id: number
  key: string
  current_version_id: string
  size: number
  state: ObjectState
  status: ObjectStatus
  upload_status?: ObjectUploadStatus
  progress?: UploadTransferProgress
  location: ObjectLocation
  content_type: string
  etag: string
  piece_cid?: string
  created_at: string
  updated_at: string
}

export interface ObjectFolderItem {
  name: string
  prefix: string
}

export interface ObjectListResponse {
  folders: ObjectFolderItem[]
  objects: ObjectItem[]
  has_more: boolean
  next_marker?: string
}

export interface ObjectDeleteMarkerResponse {
  key: string
  delete_marker_version_id: string
  deleted_at: string
}

export interface ObjectUploadResponse {
  key: string
  version_id: string
  etag: string
  size: number
  content_type: string
}

export interface ObjectUploadClientProgress {
  loaded: number
  total: number
  percent: number
}

export interface DeletedObjectItem {
  key: string
  delete_marker_version_id: string
  deleted_at: string
  restore_version_id: string
  restore_size: number
  restore_content_type: string
  restore_etag: string
}

export interface DeletedObjectListResponse {
  objects: DeletedObjectItem[]
  has_more: boolean
  next_marker?: string
}

export interface RestoreObjectResponse {
  key: string
  restored_version_id: string
}

export interface PermanentDeleteObjectResponse {
  key: string
  version_id: string
  cache_cleanup_status: string
  storage_cleanup_task_id?: number
}

export interface PermanentDeleteDeletedObjectResponse {
  key: string
  delete_marker_version_id: string
  data_versions_deleted: number
  delete_markers_deleted: number
  cache_cleanup_failed_count: number
  storage_cleanup_task_ids: number[]
}

export interface ObjectVersionItem {
  version_id: string
  key: string
  size: number
  state: ObjectState
  status: ObjectStatus
  is_delete_marker: boolean
  upload_status?: ObjectUploadStatus
  progress?: UploadTransferProgress
  location: ObjectLocation
  content_type: string
  etag: string
  piece_cid?: string
  created_at: string
  updated_at: string
  is_current: boolean
}

export interface ObjectVersionListResponse {
  versions: ObjectVersionItem[]
  has_more: boolean
  next_version_marker?: string
}

export interface BucketStorageRiskDataSet {
  local_data_set_id: number
  bucket_id: number
  copy_index: number
  provider_id: string
  provider_identity?: ProviderIdentity
  data_set_id?: string
  client_data_set_id?: string
  local_status: string
  storage_health: DataSetStorageHealthInfo
}

export interface BucketStorageRiskVersion {
  key: string
  version_id: string
  size: number
  state: ObjectState
  is_current: boolean
  in_cache: boolean
  content_type: string
  etag: string
  created_at: string
  updated_at: string
  readable_alternative_count: number
  has_readable_alternative: boolean
  risk_data_sets: BucketStorageRiskDataSet[]
}

export interface BucketStorageRiskVersionsResponse {
  versions: BucketStorageRiskVersion[]
  has_more: boolean
  next_key_marker?: string
  next_version_marker?: string
  next_created_at_marker?: string
  stale_before: string
}

export interface ObjectStatusDetail {
  version_id: string
  state: ObjectState
  status: ObjectStatus
  upload_status?: ObjectUploadStatus
  progress?: UploadTransferProgress
  failed_at_state?: string
  message?: string
  updated_at: string
}

export type ObjectUploadCopyStatus = 'pending' | 'piece_ready' | 'committing' | 'committed' | 'failed'

export interface ProviderIdentity {
  registry_provider_id: string
  name?: string
  description?: string
  service_provider_address?: string
  payee_address?: string
  filecoin_address?: string
  filecoin_actor_id?: string
  service_url?: string
  location?: string
  extra_capabilities?: Record<string, string>
}

export interface ObjectProvenanceCopy {
  copy_index: number
  status: ObjectUploadCopyStatus
  health: CopyHealthInfo
  provider_id?: string
  provider_identity?: ProviderIdentity
  data_set_id?: string
  piece_id?: string
  transfer_method: string
  retrieval_url?: string
  is_new_data_set: boolean
}

export interface ObjectProvenanceFailure {
  attempt_index: number
  provider_id?: string
  provider_identity?: ProviderIdentity
  transfer_method: string
  stage?: string
  error?: string
}

export interface ObjectProvenance {
  version_id: string
  state: ObjectState
  status: ObjectStatus
  upload_status?: ObjectUploadStatus
  progress?: UploadTransferProgress
  piece_cid?: string
  requested_copies: number
  success_copies: number
  copy_health: CopyHealthSummary
  copies: ObjectProvenanceCopy[]
  failures: ObjectProvenanceFailure[]
  updated_at: string
}

export interface TaskItem {
  id: number
  type: string
  stage?: string
  upload_id?: number
  copy_index?: number
  ref_type: string
  ref_id: number
  ref_version_id: string
  status: string
  progress?: UploadTransferProgress
  retry_count: number
  max_retries: number
  last_error?: string
  status_message?: string
  wait_reason?: string
  scheduled_at: string
  claimed_at?: string
  completed_at?: string
}

export interface TaskListResponse {
  tasks: TaskItem[]
  total: number
  limit: number
  offset: number
}

export type TaskDiagnosticCurrentState =
  | 'not_applicable'
  | 'preparing'
  | 'transferring'
  | 'waiting_for_chain'
  | 'confirmed'
  | 'rejected'
  | 'mismatch'
  | 'unavailable'
  | 'unknown'

export type TaskDiagnosticNextAction =
  | 'none'
  | 'wait'
  | 'retry_task'
  | 'check_wallet_funds'
  | 'check_wallet_approval'
  | 'inspect_provider'
  | 'inspect_task'

export type TaskDiagnosticOperation = 'none' | 'prepare_upload' | 'transfer_piece' | 'create_data_set' | 'add_pieces'

export interface TaskDiagnosticTaskFacts {
  id?: number
  type: string
  stage?: string
  status: string
  retry_count?: number
  max_retries?: number
  last_error?: string
  status_message?: string
  wait_reason?: string
  scheduled_at?: string
}

export interface TaskDiagnosticUploadFacts {
  id?: number
  status?: string
  requested_copies?: number
  error_message?: string
  accept_error?: string
}

export interface TaskDiagnosticCopyFacts {
  upload_id?: number
  copy_index?: number
  status?: string
  provider_id?: string
  storage_data_set_id?: number
  chain_data_set_id?: string
  piece_id?: string
  transfer_method?: string
  commit_transaction_id?: string
  last_error?: string
}

export interface TaskDiagnosticDataSetFacts {
  id?: number
  status?: string
  provider_id?: string
  copy_index?: number
  chain_data_set_id?: string
  client_data_set_id?: string
  create_transaction_id?: string
  create_status_url?: string
  last_error?: string
}

export interface TaskDiagnosticProviderFacts {
  provider_id?: string
  status?: ObservabilityStatus
  reason_codes?: string[]
  service_url?: string
  health_status?: string
  last_error?: string
}

export interface TaskDiagnosticTransactionFacts {
  kind: TaskDiagnosticOperation
  status_url?: string
  service_url?: string
  data_set_id?: string
  transaction_id?: string
  piece_count?: number
}

export type TaskDiagnosticLiveState =
  | 'skipped'
  | 'pending'
  | 'confirmed'
  | 'rejected'
  | 'mismatch'
  | 'unavailable'
  | 'unknown'

export interface TaskDiagnosticLiveCheck {
  state: TaskDiagnosticLiveState
  status_url?: string
  tx_status?: string
  data_set_id?: string
  data_set_created?: boolean
  pieces_added?: boolean
  piece_count?: number
  confirmed_piece_ids?: string[]
  error?: string
}

export interface TaskDiagnosticEvidence {
  task: TaskDiagnosticTaskFacts
  upload?: TaskDiagnosticUploadFacts
  copy?: TaskDiagnosticCopyFacts
  data_set?: TaskDiagnosticDataSetFacts
  provider?: TaskDiagnosticProviderFacts
  transaction?: TaskDiagnosticTransactionFacts
  live_check?: TaskDiagnosticLiveCheck
  operation: TaskDiagnosticOperation
}

export interface TaskDiagnostic {
  checked_at: string
  current_state: TaskDiagnosticCurrentState
  signal: ObservabilitySignal
  reason_codes: string[]
  next_action: TaskDiagnosticNextAction
  evidence: TaskDiagnosticEvidence
}

export interface TaskStatusCount {
  type: string
  status: string
  count: number
}

export interface TaskRefObjectDetail {
  bucket_name: string
  key: string
  version_id: string
  size: number
  state: ObjectState
  status: ObjectStatus
  upload_status?: ObjectUploadStatus
  progress?: UploadTransferProgress
  location: ObjectLocation
  content_type: string
  updated_at: string
}

export interface TaskStorageCleanupCopyDetail {
  copy_index: number
  provider_id?: string
  data_set_id?: string
  client_data_set_id?: string
  piece_id?: string
  piece_cid: string
  status: string
  delete_tx_hash?: string
  last_error?: string
}

export interface TaskStorageCleanupDeletedVersionDetail {
  bucket_name: string
  key: string
  version_id: string
  size: number
  deleted_at: string
}

export interface TaskStorageCleanupDetail {
  upload_id: number
  deleted_versions: TaskStorageCleanupDeletedVersionDetail[]
  copies: TaskStorageCleanupCopyDetail[]
}

export interface TaskRefDetail {
  ref_type: string
  ref_id: number
  ref_version_id: string
  object: TaskRefObjectDetail | null
  storage_cleanup?: TaskStorageCleanupDetail
}

export interface PaymentAccountData {
  funds: string | null
  available_funds: string | null
  lockup_current: string | null
  lockup_rate: string | null
  lockup_last_settled_at: string | null
  funded_until_epoch: string | null
  funded_until_time?: string
  runway_seconds?: number
  lockup_rate_per_day: string | null
  lockup_rate_per_month: string | null
  no_active_spend: boolean
}

export interface WalletBusiness {
  data_set_count: number
  onchain_tasks_pending: number
  onchain_tasks_completed: number
}

export interface WalletData {
  configured: boolean
  identity?: {
    address: string
    nonce: number | null
  }
  chain?: {
    network: string
    chain_id: number
    current_epoch: string | null
    epoch_duration_seconds: number
  }
  wallet_balances?: {
    fil_gas: string | null
    usdfc: string | null
  }
  payment_account?: PaymentAccountData | null
  contracts?: {
    payments_address: string
    usdfc_address: string
    usdfc_decimals: number
  }
  business?: WalletBusiness
  partial_errors?: Record<string, string>
}

export type FilecoinReadinessStatus = 'ready' | 'warning' | 'blocked' | 'unknown'

export interface FilecoinReadinessCheck {
  id: string
  status: FilecoinReadinessStatus
  message: string
  action?: string
}

export interface FilecoinReadinessData {
  status: FilecoinReadinessStatus
  mode: 'runtime' | 'draft'
  checked_at: string
  checks: FilecoinReadinessCheck[]
  partial_errors?: Record<string, string>
}

export type WalletOperationType = 'fund' | 'withdraw'
export type WalletOperationStatus = 'pending' | 'running' | 'submitted' | 'confirmed' | 'failed' | 'unknown'

export interface WalletOperation {
  id: number
  type: WalletOperationType
  client_request_id: string
  amount: string
  status: WalletOperationStatus
  tx_hash?: string
  last_error?: string
  lease_until?: string
  started_at?: string
  submitted_at?: string
  completed_at?: string
  created_at: string
  updated_at: string
}

export interface WalletOperationResponse {
  operation: WalletOperation
}

export interface WalletOperationsResponse {
  operations: WalletOperation[]
}

export interface SettingsFieldError {
  field: string
  message: string
}

export interface SettingsData {
  mode: 'ready' | 'setup'
  config_path: string
  writable: boolean
  runtime_available?: boolean
  restart_required: boolean
  s3_users: SettingsS3UsersStatus
  config: SettingsEditableConfig
  manual: SettingsManualConfig
  secrets: SettingsSecretStatus
  metadata: Record<string, SettingsFieldMetadata>
  defaults: SettingsDefaults
  env_managed: Record<string, string>
  validation_errors?: SettingsFieldError[]
  write_header: string
  write_header_value: string
}

export interface SettingsValidationData {
  validation_errors: SettingsFieldError[]
}

export interface SettingsFieldMetadata {
  label: string
  description: string
  env?: string
  editable: boolean
  secret: boolean
}

export interface SettingsDefaults {
  filecoin_rpc_urls: Record<string, string>
}

export interface SettingsS3UsersStatus {
  available: boolean
  reason?: string
}

export interface SettingsEditableConfig {
  server: SettingsServerConfig
  s3: SettingsS3Config
  filecoin: SettingsFilecoinConfig
  cache: SettingsCacheConfig
  worker: SettingsWorkerConfig
  logging: SettingsLoggingConfig
}

export interface SettingsServerConfig {
  port: string
  tls: SettingsTLSConfig
  max_connections: number
  max_requests: number
}

export interface SettingsTLSConfig {
  enabled: boolean
  cert_file: string
  key_file: string
}

export interface SettingsS3Config {
  region: string
}

export interface SettingsFilecoinConfig {
  network: string
  rpc_url: string
  source: string
  with_cdn: boolean
  allow_private_networks: boolean
  default_copies: number
  observability: SettingsObservabilityConfig
}

export interface SettingsObservabilityConfig {
  interval: string
  timeout: string
  concurrency: number
}

export interface SettingsCacheConfig {
  dir: string
  max_size_gb: number
  eviction_policy: string
}

export interface SettingsWorkerConfig {
  upload: SettingsWorkerPoolConfig
  evictor: SettingsWorkerPoolConfig
  storage_cleanup: SettingsWorkerPoolConfig
}

export interface SettingsWorkerPoolConfig {
  concurrency: number
  poll_interval: string
  max_retries: number
}

export interface SettingsLoggingConfig {
  level: string
  format: string
  s3_access: SettingsS3AccessLoggingConfig
}

export interface SettingsS3AccessLoggingConfig {
  enabled: boolean
  level: string
}

export interface SettingsManualField {
  configured: boolean
  field: string
  env?: string
}

export interface SettingsManualConfig {
  database: {
    driver: string
    dsn: string
    dsn_configured: boolean
    max_open_conns: number
    max_idle_conns: number
  }
  admin: { addr_configured: boolean }
  filecoin_private_key: SettingsManualField
  config_doc: string
}

export interface SettingsSecretStatus {
  filecoin_private_key_configured: boolean
}

export interface SettingsS3Credentials {
  access_key: string
  secret_key: string
  role?: S3UserRole
}

export type S3UserRole = 'user' | 'userplus' | 'admin'

export interface S3User {
  access_key: string
  role: S3UserRole
  bucket_count: number
}

export interface S3UserCredentials extends SettingsS3Credentials {
  role: S3UserRole
}

export type SettingsUpdatePayload = Partial<{
  server: Partial<{
    port: string
    tls: Partial<SettingsTLSConfig>
    max_connections: number
    max_requests: number
  }>
  s3: Partial<SettingsS3Config>
  filecoin: Partial<Omit<SettingsFilecoinConfig, 'observability'>> & {
    observability?: Partial<SettingsObservabilityConfig>
  }
  cache: Partial<SettingsCacheConfig>
  worker: Partial<{
    upload: Partial<SettingsWorkerPoolConfig>
    evictor: Partial<SettingsWorkerPoolConfig>
    storage_cleanup: Partial<SettingsWorkerPoolConfig>
  }>
  logging: Partial<Pick<SettingsLoggingConfig, 'level' | 'format'>> & {
    s3_access?: Partial<SettingsS3AccessLoggingConfig>
  }
}>

export interface FilecoinReadinessPreflightPayload {
  filecoin: Partial<Omit<SettingsFilecoinConfig, 'observability'>> & {
    observability?: Partial<SettingsObservabilityConfig>
  }
}

export const api = {
  getOverview: () => fetchJSON<OverviewData>('/overview'),
  getBuckets: () => fetchJSON<BucketItem[]>('/buckets'),
  getBucket: (name: string) => fetchJSON<BucketDetail>(`/buckets/${encodeURIComponent(name)}`),
  createBucket: (payload: { name: string; owner_access_key: string; default_copies?: number | null }) =>
    fetchJSON<BucketMutationResponse>('/buckets', {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  updateBucketOwner: (name: string, ownerAccessKey: string) =>
    fetchJSON<BucketMutationResponse>(`/buckets/${encodeURIComponent(name)}/owner`, {
      method: 'PUT',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify({ owner_access_key: ownerAccessKey }),
    }),
  updateBucketCopyPolicy: (name: string, defaultCopies: number | null) =>
    fetchJSON<BucketMutationResponse>(`/buckets/${encodeURIComponent(name)}/copy-policy`, {
      method: 'PUT',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify({ default_copies: defaultCopies }),
    }),
  deleteBucket: (name: string, params: { recursive?: boolean } = {}) => {
    const sp = new URLSearchParams()
    if (params.recursive) sp.set('recursive', 'true')
    const qs = sp.toString()
    return fetchJSON<BucketMutationResponse>(`/buckets/${encodeURIComponent(name)}${qs ? `?${qs}` : ''}`, {
      method: 'DELETE',
    })
  },
  getBucketObjects: (name: string, params: { prefix?: string; delimiter?: string; after?: string; limit?: number }) => {
    const sp = new URLSearchParams()
    if (params.prefix) sp.set('prefix', params.prefix)
    if (params.delimiter) sp.set('delimiter', params.delimiter)
    if (params.after) sp.set('after', params.after)
    if (params.limit) sp.set('limit', params.limit.toString())
    const qs = sp.toString()
    return fetchJSON<ObjectListResponse>(`/buckets/${encodeURIComponent(name)}/objects${qs ? `?${qs}` : ''}`)
  },
  deleteBucketObject: (name: string, key: string) => {
    const sp = new URLSearchParams()
    sp.set('key', key)
    return fetchJSON<ObjectDeleteMarkerResponse>(`/buckets/${encodeURIComponent(name)}/objects?${sp.toString()}`, {
      method: 'DELETE',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
    })
  },
  uploadBucketObject: (
    name: string,
    params: { key: string; file: File; onProgress?: (progress: ObjectUploadClientProgress) => void }
  ) =>
    new Promise<ObjectUploadResponse>((resolve, reject) => {
      const sizeError = validateFOCUploadSize(params.file.size)
      if (sizeError) {
        reject(new Error(sizeError))
        return
      }
      const xhr = new XMLHttpRequest()
      xhr.open(
        'POST',
        `${BASE}/buckets/${encodeURIComponent(name)}/objects/upload?key=${encodeURIComponent(params.key)}`
      )
      xhr.setRequestHeader('X-SynapS3-Settings-Write', '1')
      xhr.setRequestHeader('Content-Type', params.file.type || 'application/octet-stream')
      xhr.timeout = 60 * 60 * 1000
      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable || event.total <= 0) return
        params.onProgress?.({
          loaded: event.loaded,
          total: event.total,
          percent: Math.round((event.loaded / event.total) * 100),
        })
      }
      xhr.onload = () => {
        let body: unknown
        try {
          body = xhr.responseText ? JSON.parse(xhr.responseText) : undefined
        } catch {
          reject(new Error(`API error: ${xhr.status}`))
          return
        }
        if (xhr.status >= 200 && xhr.status < 300) {
          resolve(body as ObjectUploadResponse)
          return
        }
        const errorBody = body as { error?: string } | undefined
        reject(new Error(errorBody?.error || `API error: ${xhr.status}`))
      }
      xhr.onerror = () => reject(new Error('Network error'))
      xhr.ontimeout = () => reject(new Error('Upload timed out'))
      xhr.send(params.file)
    }),
  getBucketDeletedObjects: (name: string, params: { prefix?: string; after?: string; limit?: number }) => {
    const sp = new URLSearchParams()
    if (params.prefix) sp.set('prefix', params.prefix)
    if (params.after) sp.set('after', params.after)
    if (params.limit) sp.set('limit', params.limit.toString())
    const qs = sp.toString()
    return fetchJSON<DeletedObjectListResponse>(
      `/buckets/${encodeURIComponent(name)}/objects/deleted${qs ? `?${qs}` : ''}`
    )
  },
  restoreBucketObject: (name: string, payload: { key: string; delete_marker_version_id: string }) =>
    fetchJSON<RestoreObjectResponse>(`/buckets/${encodeURIComponent(name)}/objects/restore`, {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  permanentlyDeleteBucketObjectVersion: (name: string, payload: { key: string; version_id: string }) =>
    fetchJSON<PermanentDeleteObjectResponse>(`/buckets/${encodeURIComponent(name)}/objects/permanent-delete`, {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  permanentlyDeleteDeletedBucketObject: (name: string, payload: { key: string; delete_marker_version_id: string }) =>
    fetchJSON<PermanentDeleteDeletedObjectResponse>(
      `/buckets/${encodeURIComponent(name)}/objects/deleted/permanent-delete`,
      {
        method: 'POST',
        headers: {
          'X-SynapS3-Settings-Write': '1',
        },
        body: JSON.stringify(payload),
      }
    ),
  getBucketObjectVersions: (name: string, params: { key: string; limit?: number; version_marker?: string }) => {
    const sp = new URLSearchParams()
    sp.set('key', params.key)
    if (params.limit) sp.set('limit', params.limit.toString())
    if (params.version_marker) sp.set('version_marker', params.version_marker)
    return fetchJSON<ObjectVersionListResponse>(
      `/buckets/${encodeURIComponent(name)}/objects/versions?${sp.toString()}`
    )
  },
  getBucketStorageRiskVersions: (
    name: string,
    params: {
      prefix?: string
      key?: string
      local_data_set_id?: number
      key_marker?: string
      version_marker?: string
      created_at_marker?: string
      stale_before?: string
      limit?: number
    }
  ) => {
    const sp = new URLSearchParams()
    if (params.prefix) sp.set('prefix', params.prefix)
    if (params.key) sp.set('key', params.key)
    if (params.local_data_set_id) sp.set('local_data_set_id', params.local_data_set_id.toString())
    if (params.key_marker) sp.set('key_marker', params.key_marker)
    if (params.version_marker) sp.set('version_marker', params.version_marker)
    if (params.created_at_marker) sp.set('created_at_marker', params.created_at_marker)
    if (params.stale_before) sp.set('stale_before', params.stale_before)
    if (params.limit) sp.set('limit', params.limit.toString())
    const qs = sp.toString()
    return fetchJSON<BucketStorageRiskVersionsResponse>(
      `/buckets/${encodeURIComponent(name)}/storage-health/affected-versions${qs ? `?${qs}` : ''}`
    )
  },
  getObjectStatusDetail: (name: string, versionId: string) =>
    fetchJSON<ObjectStatusDetail>(
      `/buckets/${encodeURIComponent(name)}/objects/status-detail?version_id=${encodeURIComponent(versionId)}`
    ),
  getObjectProvenance: (name: string, versionId: string) =>
    fetchJSON<ObjectProvenance>(
      `/buckets/${encodeURIComponent(name)}/objects/provenance?version_id=${encodeURIComponent(versionId)}`
    ),
  getObjectDownloadUrl: (name: string, key: string, versionId?: string) => {
    const params = [`key=${encodeURIComponent(key)}`]
    if (versionId) params.push(`version_id=${encodeURIComponent(versionId)}`)
    return `${BASE}/buckets/${encodeURIComponent(name)}/objects/download?${params.join('&')}`
  },
  getTasks: (params: { type?: string; stage?: string; status?: string; limit?: number; offset?: number }) => {
    const sp = new URLSearchParams()
    if (params.type) sp.set('type', params.type)
    if (params.stage) sp.set('stage', params.stage)
    if (params.status) sp.set('status', params.status)
    if (params.limit) sp.set('limit', params.limit.toString())
    if (params.offset) sp.set('offset', params.offset.toString())
    const qs = sp.toString()
    return fetchJSON<TaskListResponse>(`/tasks${qs ? `?${qs}` : ''}`)
  },
  getTaskStats: () => fetchJSON<TaskStatusCount[]>('/tasks/stats'),
  getTaskRefDetail: (id: number) => fetchJSON<TaskRefDetail>(`/tasks/${id}/ref-detail`),
  getTaskDiagnostic: (id: number, options?: APIRequestOptions) =>
    fetchJSON<TaskDiagnostic>(`/tasks/${id}/diagnostic`, options),
  refreshTaskDiagnostic: (id: number, options?: APIRequestOptions) =>
    fetchJSON<TaskDiagnostic>(`/tasks/${id}/diagnostic/refresh`, { method: 'POST', ...options }),
  retryTask: (id: number) => fetchJSON(`/tasks/${id}/retry`, { method: 'POST' }),
  getSystemInfo: () => fetchJSON<OverviewData['system']>('/system/info'),
  getWorkers: () => fetchJSON<{ workers: Record<string, boolean> }>('/workers'),
  getCacheStats: () => fetchJSON<{ used_bytes: number; max_bytes: number }>('/cache/stats'),
  getWallet: () => fetchJSON<WalletData>('/wallet'),
  getFilecoinReadiness: () => fetchJSON<FilecoinReadinessData>('/filecoin/readiness'),
  getObservabilityProviders: (
    params: Pick<ObservabilityListParams, 'status' | 'provider_id' | 'limit' | 'offset'> = {}
  ) =>
    fetchJSON<ObservabilityListResponse<ObservabilityProviderObservation>>(
      `/observability/providers${observabilityListQuery(params)}`
    ),
  getObservabilityDataSets: (params: ObservabilityListParams = {}) =>
    fetchJSON<ObservabilityListResponse<ObservabilityDataSetObservation>>(
      `/observability/data-sets${observabilityListQuery(params)}`
    ),
  refreshDataSetStorageHealth: (params: { bucket?: string; bucket_id?: number } = {}) => {
    const sp = new URLSearchParams()
    if (params.bucket) sp.set('bucket', params.bucket)
    if (params.bucket_id !== undefined) sp.set('bucket_id', params.bucket_id.toString())
    const qs = sp.toString()
    return fetchJSON<ObservabilityListResponse<unknown>>(`/observability/data-sets/refresh${qs ? `?${qs}` : ''}`, {
      method: 'POST',
      headers: {
        'X-SynapS3-Observability-Refresh': '1',
      },
    })
  },
  preflightFilecoin: (payload: FilecoinReadinessPreflightPayload) =>
    fetchJSON<FilecoinReadinessData>('/filecoin/readiness/preflight', {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  getWalletOperations: (limit = 20) => fetchJSON<WalletOperationsResponse>(`/wallet/operations?limit=${limit}`),
  fundWallet: (payload: { client_request_id: string; amount: string }) =>
    fetchJSON<WalletOperationResponse>('/wallet/fund', {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  withdrawWallet: (payload: { client_request_id: string; amount: string }) =>
    fetchJSON<WalletOperationResponse>('/wallet/withdraw', {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  getSettings: () => fetchJSON<SettingsData>('/settings'),
  updateSettings: (payload: SettingsUpdatePayload) =>
    fetchJSON<SettingsData>('/settings', {
      method: 'PUT',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  validateSettings: (payload: SettingsUpdatePayload) =>
    fetchJSON<SettingsValidationData>('/settings/validate', {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  getS3Users: () => fetchJSON<S3User[]>('/s3-users'),
  createS3User: (payload: { role?: S3UserRole } = {}) =>
    fetchJSON<S3UserCredentials>('/s3-users', {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  updateS3User: (accessKey: string, payload: { role: S3UserRole }) =>
    fetchJSON<S3User>(`/s3-users/${encodeURIComponent(accessKey)}`, {
      method: 'PUT',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify(payload),
    }),
  rotateS3UserSecret: (accessKey: string) =>
    fetchJSON<S3UserCredentials>(`/s3-users/${encodeURIComponent(accessKey)}/secret`, {
      method: 'POST',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
      body: JSON.stringify({}),
    }),
  deleteS3User: (accessKey: string) =>
    fetchJSON<void>(`/s3-users/${encodeURIComponent(accessKey)}`, {
      method: 'DELETE',
      headers: {
        'X-SynapS3-Settings-Write': '1',
      },
    }),
}

function observabilityListQuery(params: ObservabilityListParams) {
  const sp = new URLSearchParams()
  if (params.status) sp.set('status', params.status)
  if (params.provider_id) sp.set('provider_id', params.provider_id)
  if (params.bucket) sp.set('bucket', params.bucket)
  if (params.bucket_id !== undefined) sp.set('bucket_id', params.bucket_id.toString())
  if (params.limit !== undefined) sp.set('limit', params.limit.toString())
  if (params.offset !== undefined) sp.set('offset', params.offset.toString())
  const qs = sp.toString()
  return qs ? `?${qs}` : ''
}
