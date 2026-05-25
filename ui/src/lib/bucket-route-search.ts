export type BucketRouteSearch = {
  prefix?: string
  marker?: string
  version_marker?: string
  risk_prefix?: string
  risk_dataset?: string
  risk_key?: string
  risk_key_marker?: string
  risk_version_marker?: string
  risk_created_at_marker?: string
  risk_stale_before?: string
  view?: 'objects' | 'deleted' | 'storage-risk'
}

export function normalizeBucketRouteSearch(search: Record<string, unknown>): BucketRouteSearch {
  return {
    prefix: normalizePrefixSearch(search.prefix),
    marker: normalizeSearchString(search.marker),
    version_marker: normalizeSearchString(search.version_marker),
    risk_prefix: normalizeSearchString(search.risk_prefix),
    risk_dataset: normalizePositiveIntegerSearch(search.risk_dataset),
    risk_key: normalizeSearchString(search.risk_key),
    risk_key_marker: normalizeSearchString(search.risk_key_marker),
    risk_version_marker: normalizeSearchString(search.risk_version_marker),
    risk_created_at_marker: normalizeSearchString(search.risk_created_at_marker),
    risk_stale_before: normalizeSearchString(search.risk_stale_before),
    view: search.view === 'deleted' || search.view === 'storage-risk' ? search.view : undefined,
  }
}

function normalizeSearchString(value: unknown) {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function normalizePrefixSearch(value: unknown) {
  const prefix = normalizeSearchString(value)
  if (!prefix) return undefined
  return prefix.endsWith('/') ? prefix : `${prefix}/`
}

function normalizePositiveIntegerSearch(value: unknown) {
  if (typeof value !== 'string') return undefined
  const raw = value.trim()
  if (!raw || !/^\d+$/.test(raw)) return undefined
  return Number(raw) > 0 ? raw : undefined
}
