const BASE = '/api/v1'

async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) {
    const body = await res.json().catch(() => ({} as Record<string, string>))
    throw new Error((body as Record<string, string>).error || `API error: ${res.status}`)
  }
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
  status: string
  proof_set_id: string | null
  object_count: number
  total_size_bytes: number
  created_at: string
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

export const api = {
  getOverview: () => fetchJSON<OverviewData>('/overview'),
  getBuckets: () => fetchJSON<BucketItem[]>('/buckets'),
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
  retryTask: async (id: number) => {
    const res = await fetch(`${BASE}/tasks/${id}/retry`, { method: 'POST' })
    if (!res.ok) {
      const body = await res.json().catch(() => ({} as Record<string, string>))
      throw new Error((body as Record<string, string>).error || `API error: ${res.status}`)
    }
    return res.json()
  },
  getSystemInfo: () => fetchJSON<OverviewData['system']>('/system/info'),
  getWorkers: () => fetchJSON<{ workers: Record<string, boolean> }>('/workers'),
  getCacheStats: () => fetchJSON<{ used_bytes: number; max_bytes: number }>('/cache/stats'),
  getWallet: () => fetchJSON<WalletData>('/wallet'),
}
