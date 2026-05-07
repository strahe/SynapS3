import type { PaymentAccountData, WalletOperation, WalletOperationType } from '@/api/client'
import { formatTokenAmount } from './utils.ts'

export type WalletRunwayTone = 'danger' | 'warning' | 'neutral'

export interface WalletOperationConfirmation {
  title: string
  description: string
  actionLabel: string
  amount: string
}

export interface WalletOperationDraft {
  type: WalletOperationType
  amount: string
  clientRequestID: string
  confirmation: WalletOperationConfirmation
}

export type WalletOperationDialogCloseReason = 'dismiss' | 'success'

export function walletOperationDialogShouldClearDraft(reason: WalletOperationDialogCloseReason, isMutating: boolean) {
  if (reason === 'success') return true
  return !isMutating
}

export function createWalletOperationDraft({
  type,
  amountBaseUnits,
  decimals,
  requestPrefix = type,
}: {
  type: WalletOperationType
  amountBaseUnits: string
  decimals: number
  requestPrefix?: string
}): WalletOperationDraft {
  return {
    type,
    amount: amountBaseUnits,
    clientRequestID: requestID(requestPrefix),
    confirmation: buildWalletOperationConfirmation({ type, amountBaseUnits, decimals }),
  }
}

export function walletOperationPayload(draft: WalletOperationDraft) {
  return {
    client_request_id: draft.clientRequestID,
    amount: draft.amount,
  }
}

export function walletOperationMutationError(
  activeType: WalletOperationType | null | undefined,
  fundError: unknown,
  withdrawError: unknown
) {
  if (activeType === 'fund') return errorMessage(fundError)
  if (activeType === 'withdraw') return errorMessage(withdrawError)
  return null
}

export function buildWalletOperationConfirmation({
  type,
  amountBaseUnits,
  decimals,
}: {
  type: WalletOperationType
  amountBaseUnits: string
  decimals: number
}): WalletOperationConfirmation {
  const verb = type === 'withdraw' ? 'withdraw' : 'fund'
  return {
    title: `Confirm ${verb}`,
    description: 'This will broadcast an on-chain transaction from the configured wallet.',
    actionLabel: `Confirm ${verb}`,
    amount: formatTokenAmount(amountBaseUnits, decimals, 'USDFC'),
  }
}

export function walletOperationDetail(operation: WalletOperation) {
  if (operation.last_error) return operation.last_error
  if (operation.status === 'failed') return 'Failed without a recorded reason'
  if (operation.status === 'unknown') return 'Operation state is unknown'
  return ''
}

export function decimalToBaseUnits(
  input: string,
  decimals: number
): { ok: true; value: bigint } | { ok: false; error: string } {
  const value = input.trim()
  if (value === '') return { ok: false, error: 'Amount is required' }
  if (value.startsWith('-')) return { ok: false, error: 'Amount must be positive' }
  if (!/^(?:\d+|\d*\.\d+)$/.test(value)) return { ok: false, error: 'Amount must be a decimal number' }

  const [integerPart, fractionPart = ''] = value.split('.')
  if (fractionPart.length > decimals) return { ok: false, error: `USDFC supports up to ${decimals} decimal places` }

  const integer = integerPart === '' ? '0' : integerPart
  const raw = `${integer}${fractionPart.padEnd(decimals, '0')}`.replace(/^0+/, '') || '0'
  const amount = BigInt(raw)
  if (amount <= 0n) return { ok: false, error: 'Amount must be greater than 0' }
  return { ok: true, value: amount }
}

export function baseUnitsToDecimal(raw: string, decimals: number) {
  const amount = raw.replace(/^0+/, '') || '0'
  if (decimals <= 0) return amount
  const padded = amount.padStart(decimals + 1, '0')
  const integer = padded.slice(0, -decimals) || '0'
  const fraction = padded.slice(-decimals).replace(/0+$/, '')
  return fraction ? `${integer}.${fraction}` : integer
}

export function topUpNeeded(account: PaymentAccountData, targetEpochs: number) {
  if (account.no_active_spend) return 0n
  const lockupRate = parseBaseUnitBigInt(account.lockup_rate)
  const available = parseBaseUnitBigInt(account.available_funds)
  if (lockupRate == null || available == null || lockupRate <= 0n) return 0n
  const needed = lockupRate * BigInt(targetEpochs) - available
  return needed > 0n ? needed : 0n
}

export function fundedUntilTone(account: PaymentAccountData): WalletRunwayTone {
  if (account.no_active_spend || account.runway_seconds == null) return 'neutral'
  const runwaySeconds = Math.max(0, account.runway_seconds)
  if (runwaySeconds < 7 * 86_400) return 'danger'
  if (runwaySeconds < 30 * 86_400) return 'warning'
  return 'neutral'
}

export function fundedUntilCaption(account: PaymentAccountData) {
  if (account.no_active_spend || account.runway_seconds == null) return undefined
  const runwaySeconds = Math.max(0, account.runway_seconds)
  if (runwaySeconds <= 0) return 'Expired'
  if (runwaySeconds < 7 * 86_400) return 'Under 7 days'
  if (runwaySeconds < 30 * 86_400) return 'Under 1 month'
  return undefined
}

export function requestID(prefix: string) {
  const random =
    typeof crypto !== 'undefined' && 'randomUUID' in crypto ? crypto.randomUUID() : Math.random().toString(36).slice(2)
  return `${prefix}-${Date.now()}-${random}`
}

function parseBaseUnitBigInt(raw: string | null | undefined) {
  if (!raw || !/^\d+$/.test(raw)) return null
  return BigInt(raw)
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : null
}
