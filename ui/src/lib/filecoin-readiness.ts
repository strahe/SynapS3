import type {
  FilecoinReadinessCheck,
  FilecoinReadinessData,
  FilecoinReadinessPreflightPayload,
  FilecoinReadinessStatus,
  SettingsFilecoinConfig,
} from '@/api/client'
import type { StatusTone } from '@/components/app/StatusBadge'

interface BrowserStorage {
  getItem(key: string): string | null
  removeItem(key: string): void
  setItem(key: string, value: string): void
}

const statusLabels: Record<FilecoinReadinessStatus, string> = {
  ready: 'Ready',
  warning: 'Warning',
  blocked: 'Blocked',
  unknown: 'Unknown',
}

const statusDescriptions: Record<FilecoinReadinessStatus, string> = {
  ready: 'All Filecoin upload prerequisites are satisfied.',
  warning: 'Filecoin uploads can proceed, but review the highlighted risk.',
  blocked: 'Filecoin uploads are blocked until the highlighted issue is fixed.',
  unknown: 'SynapS3 could not verify one or more Filecoin upload prerequisites.',
}

const statusTones: Record<FilecoinReadinessStatus, StatusTone> = {
  ready: 'success',
  warning: 'warning',
  blocked: 'danger',
  unknown: 'warning',
}

const checkImpactRank: Record<FilecoinReadinessStatus, number> = {
  blocked: 0,
  unknown: 1,
  warning: 2,
  ready: 3,
}

const dismissibleCheckIds = ['private_networks'] as const
const dismissStoragePrefix = 'synaps3.filecoin-readiness.dismissed.'
const emptyDismissedChecks = new Set<string>()

const filecoinPayloadKeys = [
  'network',
  'rpc_url',
  'with_cdn',
  'allow_private_networks',
  'default_copies',
] as const satisfies Array<keyof SettingsFilecoinConfig>

const filecoinPayloadFieldPaths: Record<(typeof filecoinPayloadKeys)[number], string> = {
  network: 'filecoin.network',
  rpc_url: 'filecoin.rpc_url',
  with_cdn: 'filecoin.with_cdn',
  allow_private_networks: 'filecoin.allow_private_networks',
  default_copies: 'filecoin.default_copies',
}

const filecoinObservabilityPayloadKeys = ['interval', 'timeout', 'concurrency'] as const satisfies Array<
  keyof SettingsFilecoinConfig['observability']
>

const filecoinObservabilityPayloadFieldPaths: Record<(typeof filecoinObservabilityPayloadKeys)[number], string> = {
  interval: 'filecoin.observability.interval',
  timeout: 'filecoin.observability.timeout',
  concurrency: 'filecoin.observability.concurrency',
}

const checkTitles: Record<string, string> = {
  config_private_key: 'Private key',
  config_rpc_url: 'RPC URL',
  config_network: 'Network selection',
  config_default_copies: 'Default copy count',
  private_networks: 'Private network access',
  sdk_client: 'Filecoin SDK client',
  network_match: 'RPC network',
  wallet_fil_gas: 'FIL gas balance',
  wallet_usdfc: 'USDFC wallet balance',
  payment_account: 'Payment account',
  payment_runway: 'Payment runway',
  providers: 'Storage providers',
  storage_cost: 'Storage cost estimate',
  payment_funding: 'Payment funding',
  fwss_approval: 'FWSS approval',
}

export function filecoinReadinessStatusLabel(status: FilecoinReadinessStatus) {
  return statusLabels[status]
}

export function filecoinReadinessStatusDescription(status: FilecoinReadinessStatus) {
  return statusDescriptions[status]
}

export function filecoinReadinessStatusTone(status: FilecoinReadinessStatus) {
  return statusTones[status]
}

export function filecoinReadinessCheckTitle(id: string) {
  return checkTitles[id] ?? id.replace(/_/g, ' ')
}

export function isDismissibleFilecoinReadinessCheck(id: string) {
  return (dismissibleCheckIds as readonly string[]).includes(id)
}

export function importantFilecoinReadinessChecks(
  checks: FilecoinReadinessCheck[],
  dismissedCheckIds: ReadonlySet<string> = emptyDismissedChecks
) {
  return checks
    .filter((check) => check.status !== 'ready' && !dismissedCheckIds.has(check.id))
    .sort((a, b) => checkImpactRank[a.status] - checkImpactRank[b.status] || a.id.localeCompare(b.id))
}

export function filecoinReadinessSummary(
  data?: FilecoinReadinessData,
  dismissedCheckIds: ReadonlySet<string> = emptyDismissedChecks
) {
  if (!data) return 'Readiness has not been checked.'
  const important = importantFilecoinReadinessChecks(data.checks, dismissedCheckIds)
  if (important[0]) return important[0].message
  if (data.status === 'ready') return 'Filecoin readiness checks passed.'
  return filecoinReadinessStatusLabel(data.status)
}

export function readDismissedFilecoinReadinessChecks(storage: BrowserStorage | null | undefined = browserStorage()) {
  const dismissed = new Set<string>()
  if (!storage) return dismissed

  for (const id of dismissibleCheckIds) {
    try {
      if (storage.getItem(filecoinReadinessDismissStorageKey(id)) === '1') {
        dismissed.add(id)
      }
    } catch {
      return dismissed
    }
  }
  return dismissed
}

export function writeDismissedFilecoinReadinessCheck(
  id: string,
  dismissed: boolean,
  storage: BrowserStorage | null | undefined = browserStorage()
) {
  if (!storage || !isDismissibleFilecoinReadinessCheck(id)) return

  try {
    const key = filecoinReadinessDismissStorageKey(id)
    if (dismissed) storage.setItem(key, '1')
    else storage.removeItem(key)
  } catch {
    return
  }
}

export function buildFilecoinPreflightPayload(
  filecoin: Partial<SettingsFilecoinConfig> | Record<string, unknown>,
  envManaged: Readonly<Record<string, string>> = {}
): FilecoinReadinessPreflightPayload {
  const out: FilecoinReadinessPreflightPayload['filecoin'] = {}
  const source = filecoin as Record<string, unknown>
  for (const key of filecoinPayloadKeys) {
    if (Object.keys(envManaged).includes(filecoinPayloadFieldPaths[key])) continue
    const value = source[key]
    if (value !== undefined) {
      Object.assign(out, { [key]: value })
    }
  }
  const observabilitySource = source.observability
  if (observabilitySource && typeof observabilitySource === 'object' && !Array.isArray(observabilitySource)) {
    const observability: NonNullable<FilecoinReadinessPreflightPayload['filecoin']['observability']> = {}
    const observabilityRecord = observabilitySource as Record<string, unknown>
    for (const key of filecoinObservabilityPayloadKeys) {
      if (Object.keys(envManaged).includes(filecoinObservabilityPayloadFieldPaths[key])) continue
      const value = observabilityRecord[key]
      if (value !== undefined) {
        Object.assign(observability, { [key]: value })
      }
    }
    if (Object.keys(observability).length > 0) {
      out.observability = observability
    }
  }
  return { filecoin: out }
}

export function filecoinPreflightPayloadKey(payload: FilecoinReadinessPreflightPayload) {
  return JSON.stringify(payload)
}

function filecoinReadinessDismissStorageKey(id: string) {
  return `${dismissStoragePrefix}${id}`
}

function browserStorage() {
  if (typeof window === 'undefined') return null

  try {
    return window.localStorage
  } catch {
    return null
  }
}
