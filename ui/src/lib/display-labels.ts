import type { WalletOperationStatus, WalletOperationType } from '../api/client.ts'
import { enumLabel } from './utils.ts'

const bucketStatusLabels: Record<string, string> = {
  ready: 'Ready',
  provisioning: 'Provisioning',
  deleting: 'Deleting',
  create_failed: 'Creation failed',
  delete_failed: 'Deletion failed',
}

export function bucketStatusLabel(status: string) {
  return bucketStatusLabels[status] ?? (enumLabel(status) || 'Unknown')
}

export function versioningStatusLabel(status: string) {
  return enumLabel(status) || 'Unknown'
}

const walletOperationTypeLabels: Record<WalletOperationType, string> = {
  fund: 'Fund',
  withdraw: 'Withdraw',
  approve: 'Approve FWSS',
}

export function walletOperationTypeLabel(type: WalletOperationType) {
  return walletOperationTypeLabels[type] ?? (enumLabel(type) || 'Unknown')
}

export function walletOperationStatusLabel(status: WalletOperationStatus) {
  return enumLabel(status) || 'Unknown'
}

/** Display names for configuration values whose stored form is not readable as-is. */
const settingsOptionLabels: Record<string, Record<string, string>> = {
  'filecoin.network': { calibration: 'Calibration', mainnet: 'Mainnet' },
  'filecoin.anchor_provider_tier': { approved: 'Approved', endorsed: 'Endorsed', none: 'None' },
  'logging.level': { debug: 'Debug', info: 'Info', warn: 'Warning', error: 'Error' },
  'logging.s3_access.level': { debug: 'Debug', info: 'Info', warn: 'Warning', error: 'Error' },
  'logging.format': { json: 'JSON', text: 'Text' },
}

export function settingsOptions(field: string, values: readonly string[]) {
  return values.map((value) => ({ value, label: settingsOptionLabels[field]?.[value] ?? value }))
}

const providerSelectionStrategyLabels: Record<string, string> = {
  distribution: 'Distribution first',
  speed: 'Speed first',
}

export function providerSelectionStrategyLabel(strategy: string) {
  return providerSelectionStrategyLabels[strategy] ?? (enumLabel(strategy) || 'Unknown')
}
