const BASE = '/api/v1'

export const internalRootOwnerAccessKey = '__internal_root__'

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

export interface OverviewData {
  buckets: { total: number; by_status: Record<string, number> }
  objects: { total: number; total_size_bytes: number; by_state: Record<string, number> }
  tasks: { by_status: Record<string, number> }
  cache: { used_bytes: number; max_bytes: number }
  workers: Record<string, boolean>
  system: { version: string; commit: string; build_date: string; uptime_seconds: number }
}

export interface BucketItem {
  id: number
  name: string
  owner_access_key: string | null
  status: string
  proof_set_id: string | null
  object_count: number
  total_size_bytes: number
  created_at: string
}

export interface BucketDetail extends BucketItem {
  updated_at: string
}

export interface BucketMutationResponse {
  id: number
  name: string
  owner_access_key: string | null
  status: string
}

export interface ObjectItem {
  id: number
  key: string
  size: number
  state: string
  content_type: string
  etag: string
  piece_cid?: string
  created_at: string
  updated_at: string
}

export interface ObjectListResponse {
  objects: ObjectItem[]
  has_more: boolean
  next_marker?: string
}

export interface TaskItem {
  id: number
  type: string
  ref_type: string
  ref_id: number
  ref_generation: number
  status: string
  retry_count: number
  max_retries: number
  last_error?: string
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

export interface TaskStatusCount {
  type: string
  status: string
  count: number
}

export interface TokenAccountData {
  funds: string | null
  available_funds: string | null
  lockup_current: string | null
  lockup_rate: string | null
  lockup_last_settled: string | null
  funded_until_epoch: string | null
  current_lockup_rate: string | null
}

export interface WalletBusiness {
  proof_set_count: number
  onchain_tasks_pending: number
  onchain_tasks_completed: number
}

export interface WalletData {
  configured: boolean
  address?: string
  network?: string
  chain_id?: number
  nonce: number | null
  payments_address?: string
  usdfc_address?: string
  usdfc_decimals: number
  fil_balance: string | null
  usdfc_balance: string | null
  fil_account: TokenAccountData | null
  usdfc_account: TokenAccountData | null
  business?: WalletBusiness
  partial_errors?: Record<string, string>
}

export interface SettingsFieldError {
  field: string
  message: string
}

export interface SettingsData {
  mode: 'ready' | 'setup'
  config_path: string
  writable: boolean
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
}

export interface SettingsCacheConfig {
  dir: string
  max_size_gb: number
  eviction_policy: string
}

export interface SettingsWorkerConfig {
  upload: SettingsWorkerPoolConfig
  evictor: SettingsWorkerPoolConfig
}

export interface SettingsWorkerPoolConfig {
  concurrency: number
  poll_interval: string
  max_retries: number
}

export interface SettingsLoggingConfig {
  level: string
  format: string
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
  filecoin: Partial<SettingsFilecoinConfig>
  cache: Partial<SettingsCacheConfig>
  worker: Partial<{
    upload: Partial<SettingsWorkerPoolConfig>
    evictor: Partial<SettingsWorkerPoolConfig>
  }>
  logging: Partial<SettingsLoggingConfig>
}>

export const api = {
  getOverview: () => fetchJSON<OverviewData>('/overview'),
  getBuckets: () => fetchJSON<BucketItem[]>('/buckets'),
  getBucket: (name: string) => fetchJSON<BucketDetail>(`/buckets/${encodeURIComponent(name)}`),
  createBucket: (payload: { name: string; owner_access_key: string }) =>
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
  deleteBucket: (name: string, params: { recursive?: boolean } = {}) => {
    const sp = new URLSearchParams()
    if (params.recursive) sp.set('recursive', 'true')
    const qs = sp.toString()
    return fetchJSON<BucketMutationResponse>(`/buckets/${encodeURIComponent(name)}${qs ? `?${qs}` : ''}`, {
      method: 'DELETE',
    })
  },
  getBucketObjects: (name: string, params: { prefix?: string; after?: string; limit?: number }) => {
    const sp = new URLSearchParams()
    if (params.prefix) sp.set('prefix', params.prefix)
    if (params.after) sp.set('after', params.after)
    if (params.limit) sp.set('limit', params.limit.toString())
    const qs = sp.toString()
    return fetchJSON<ObjectListResponse>(`/buckets/${encodeURIComponent(name)}/objects${qs ? `?${qs}` : ''}`)
  },
  getTasks: (params: { type?: string; status?: string; limit?: number; offset?: number }) => {
    const sp = new URLSearchParams()
    if (params.type) sp.set('type', params.type)
    if (params.status) sp.set('status', params.status)
    if (params.limit) sp.set('limit', params.limit.toString())
    if (params.offset) sp.set('offset', params.offset.toString())
    const qs = sp.toString()
    return fetchJSON<TaskListResponse>(`/tasks${qs ? `?${qs}` : ''}`)
  },
  getTaskStats: () => fetchJSON<TaskStatusCount[]>('/tasks/stats'),
  retryTask: (id: number) => fetchJSON(`/tasks/${id}/retry`, { method: 'POST' }),
  getSystemInfo: () => fetchJSON<OverviewData['system']>('/system/info'),
  getWorkers: () => fetchJSON<{ workers: Record<string, boolean> }>('/workers'),
  getCacheStats: () => fetchJSON<{ used_bytes: number; max_bytes: number }>('/cache/stats'),
  getWallet: () => fetchJSON<WalletData>('/wallet'),
  getSettings: () => fetchJSON<SettingsData>('/settings'),
  updateSettings: (payload: SettingsUpdatePayload) =>
    fetchJSON<SettingsData>('/settings', {
      method: 'PUT',
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
