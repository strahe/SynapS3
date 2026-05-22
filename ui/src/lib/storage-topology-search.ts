export interface StorageTopologySearchParams {
  tab?: string
  status?: string
  provider?: string
  bucket?: string
  chain_data_set_id?: string
  local_data_set_id?: number
  selection_provider?: string
  selection_bucket?: string
}

export function nonEmptyStorageTopologySearchString(value: unknown) {
  if (typeof value !== 'string') return undefined
  const trimmed = value.trim()
  return trimmed || undefined
}

export function nonNegativeStorageTopologySearchInteger(value: unknown) {
  if (typeof value !== 'string' && typeof value !== 'number') return undefined
  const raw = typeof value === 'string' ? value.trim() : value
  if (raw === '') return undefined
  if (typeof raw === 'string' && !/^\d+$/.test(raw)) return undefined
  const parsed = Number(raw)
  if (!Number.isInteger(parsed) || parsed < 0) return undefined
  return parsed
}

export function cleanStorageTopologySearch(search: StorageTopologySearchParams): StorageTopologySearchParams {
  const chainDataSetID = nonEmptyStorageTopologySearchString(search.chain_data_set_id)
  const localDataSetID = nonNegativeStorageTopologySearchInteger(search.local_data_set_id)
  const hasSelection = Boolean(chainDataSetID || localDataSetID != null)
  return {
    tab: search.tab && search.tab !== 'topology' ? search.tab : undefined,
    status: nonEmptyStorageTopologySearchString(search.status),
    provider: nonEmptyStorageTopologySearchString(search.provider),
    bucket: nonEmptyStorageTopologySearchString(search.bucket),
    chain_data_set_id: chainDataSetID,
    local_data_set_id: localDataSetID,
    selection_provider: hasSelection ? nonEmptyStorageTopologySearchString(search.selection_provider) : undefined,
    selection_bucket: hasSelection ? nonEmptyStorageTopologySearchString(search.selection_bucket) : undefined,
  }
}

export function clearStorageTopologySelectionSearch(search: StorageTopologySearchParams): StorageTopologySearchParams {
  return {
    ...search,
    chain_data_set_id: undefined,
    local_data_set_id: undefined,
    selection_provider: undefined,
    selection_bucket: undefined,
  }
}

export function hasStorageTopologySelectionSearch(search: StorageTopologySearchParams) {
  return Boolean(search.chain_data_set_id || search.local_data_set_id != null)
}
