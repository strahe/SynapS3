export const bucketTabs = ['objects', 'trash', 'storage', 'settings'] as const
export type BucketTab = (typeof bucketTabs)[number]

export type BucketRouteSearch = {
  tab?: BucketTab
  prefix?: string
  marker?: string
  /** Shows the Storage tab's affected-versions view instead of its data sets. */
  risk?: true
  risk_prefix?: string
  risk_dataset?: string
  risk_key?: string
  risk_key_marker?: string
  risk_version_marker?: string
  risk_created_at_marker?: string
  risk_stale_before?: string
}

export function normalizeBucketRouteSearch(search: Record<string, unknown>): BucketRouteSearch {
  // Links written before the bucket page had tabs used `view` and `details`.
  const legacyRisk = search.view === 'storage-risk'
  const legacyTab: BucketTab | undefined =
    search.view === 'deleted' ? 'trash' : legacyRisk || search.details === 'storage' ? 'storage' : undefined
  const tab = bucketTabs.find((candidate) => candidate === search.tab) ?? legacyTab
  const risk = tab === 'storage' && (search.risk === true || search.risk === 'true' || legacyRisk) ? true : undefined

  return {
    tab: tab === 'objects' ? undefined : tab,
    prefix: normalizePrefixSearch(search.prefix),
    marker: normalizeSearchString(search.marker),
    risk,
    risk_prefix: normalizeSearchString(search.risk_prefix),
    risk_dataset: normalizePositiveIntegerSearch(search.risk_dataset),
    risk_key: normalizeSearchString(search.risk_key),
    risk_key_marker: normalizeSearchString(search.risk_key_marker),
    risk_version_marker: normalizeSearchString(search.risk_version_marker),
    risk_created_at_marker: normalizeSearchString(search.risk_created_at_marker),
    risk_stale_before: normalizeSearchString(search.risk_stale_before),
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
