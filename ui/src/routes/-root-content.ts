import type { FilecoinReadinessCheck, FilecoinReadinessData, FilecoinReadinessStatus } from '../api/client.ts'
import { importantFilecoinReadinessChecks } from '../lib/filecoin-readiness.ts'

export type RootSettingsState = {
  mode: 'ready' | 'setup'
  runtime_available?: boolean
}

export type RootContentKind = 'outlet' | 'setup-required'
export type GlobalFilecoinReadinessAlertState =
  | { show: false }
  | {
      show: true
      title: string
      summary: string
      status: FilecoinReadinessStatus
      primaryCheck?: FilecoinReadinessCheck
      details?: FilecoinReadinessData
      failed: boolean
    }

const provisioningCheckIds = new Set(['providers', 'storage_cost', 'payment_funding', 'fwss_approval'])

export function rootUsesSetupShell(settings: RootSettingsState | undefined) {
  return settings?.mode === 'setup' || settings?.runtime_available === false
}

export function rootContentKind(settings: RootSettingsState | undefined, pathname: string): RootContentKind {
  if (rootUsesSetupShell(settings) && pathname !== '/settings') {
    return 'setup-required'
  }
  return 'outlet'
}

export function fullRuntimeAvailable(settings: RootSettingsState | undefined, settingsLoading: boolean) {
  return !settingsLoading && settings?.mode === 'ready' && settings.runtime_available !== false
}

export function filecoinRuntimeReadinessEnabled(settings: RootSettingsState | undefined, settingsLoading: boolean) {
  return fullRuntimeAvailable(settings, settingsLoading)
}

export function globalFilecoinReadinessAlertState({
  enabled,
  data,
  error,
  dismissedCheckIds = new Set<string>(),
}: {
  enabled: boolean
  data?: FilecoinReadinessData
  error?: unknown
  dismissedCheckIds?: ReadonlySet<string>
}): GlobalFilecoinReadinessAlertState {
  if (!enabled) return { show: false }

  if (error instanceof Error) {
    return {
      show: true,
      title: 'Filecoin readiness could not be checked',
      summary: error.message,
      status: 'unknown',
      failed: true,
    }
  }

  if (!data) return { show: false }

  const checks = data.checks.filter((check) => !provisioningCheckIds.has(check.id))
  const visibleChecks = importantFilecoinReadinessChecks(checks, dismissedCheckIds)
  const primaryCheck = visibleChecks[0]
  if (primaryCheck?.status === 'blocked' || primaryCheck?.status === 'unknown') {
    return {
      show: true,
      title: filecoinReadinessAlertTitle(primaryCheck.status),
      summary: primaryCheck.message,
      status: primaryCheck.status,
      primaryCheck,
      details: { ...data, status: primaryCheck.status, checks },
      failed: false,
    }
  }

  return { show: false }
}

function filecoinReadinessAlertTitle(status: Extract<FilecoinReadinessStatus, 'blocked' | 'unknown'>) {
  return status === 'blocked' ? 'Filecoin uploads blocked' : 'Filecoin readiness unknown'
}
