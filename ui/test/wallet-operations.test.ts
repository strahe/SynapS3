import assert from 'node:assert/strict'
import test from 'node:test'

import type { WalletOperation } from '../src/api/client.ts'
import {
  buildWalletOperationConfirmation,
  createWalletOperationDraft,
  decimalToBaseUnits,
  fundedUntilCaption,
  fundedUntilTone,
  topUpNeeded,
  walletOperationDetail,
  walletOperationDialogShouldClearDraft,
  walletOperationMutationError,
  walletOperationPayload,
} from '../src/lib/wallet-operations.ts'

test('wallet fund action requires an explicit confirmation summary', () => {
  const confirmation = buildWalletOperationConfirmation({
    type: 'fund',
    amountBaseUnits: '1000000000000000000',
    decimals: 18,
  })

  assert.equal(confirmation.title, 'Confirm fund')
  assert.equal(confirmation.amount, '1 USDFC')
  assert.equal(confirmation.actionLabel, 'Confirm fund')
})

test('wallet approve action has no transfer amount', () => {
  const draft = createWalletOperationDraft({
    type: 'approve',
    amountBaseUnits: '0',
    decimals: 18,
    requestPrefix: 'approve-fwss',
  })

  assert.equal(draft.confirmation.title, 'Confirm FWSS approval')
  assert.equal(draft.confirmation.amount, 'No USDFC transfer')
  assert.deepEqual(walletOperationPayload(draft), { client_request_id: draft.clientRequestID })
})

test('wallet operation draft reuses the same client request id for retry', () => {
  const draft = createWalletOperationDraft({
    type: 'fund',
    amountBaseUnits: '1000000000000000000',
    decimals: 18,
    requestPrefix: 'fund',
  })

  assert.equal(walletOperationPayload(draft).client_request_id, draft.clientRequestID)
  assert.equal(walletOperationPayload(draft).client_request_id, walletOperationPayload(draft).client_request_id)
  assert.equal(walletOperationPayload(draft).amount, '1000000000000000000')
})

test('wallet operation confirmation keeps the draft while submit is pending', () => {
  assert.equal(walletOperationDialogShouldClearDraft('dismiss', true), false)
  assert.equal(walletOperationDialogShouldClearDraft('dismiss', false), true)
  assert.equal(walletOperationDialogShouldClearDraft('success', false), true)
})

test('wallet operation mutation error follows the active operation type', () => {
  const fundError = new Error('fund failed')
  const withdrawError = new Error('withdraw failed')
  const approveError = new Error('approve failed')

  assert.equal(walletOperationMutationError('fund', fundError, withdrawError, approveError), 'fund failed')
  assert.equal(walletOperationMutationError('withdraw', fundError, withdrawError, approveError), 'withdraw failed')
  assert.equal(walletOperationMutationError('approve', fundError, withdrawError, approveError), 'approve failed')
  assert.equal(walletOperationMutationError('withdraw', fundError, null, approveError), null)
  assert.equal(walletOperationMutationError(null, fundError, withdrawError, approveError), null)
})

test('wallet operation detail exposes failure reason', () => {
  const operation: WalletOperation = {
    id: 1,
    type: 'fund',
    client_request_id: 'request-1',
    amount: '1',
    status: 'failed',
    last_error: 'receipt lookup failed: rpc timeout',
    created_at: '2026-05-06T00:00:00Z',
    updated_at: '2026-05-06T00:00:00Z',
  }

  assert.equal(walletOperationDetail(operation), 'receipt lookup failed: rpc timeout')
})

test('wallet operation detail keeps long failure text intact', () => {
  const longError = [
    'receipt lookup failed: rpc timeout after 30s',
    'provider: calibration-usdfc-payments',
    'tx: 0x1234567890abcdef'.repeat(12),
  ].join('\n')
  const operation: WalletOperation = {
    id: 1,
    type: 'fund',
    client_request_id: 'request-1',
    amount: '1',
    status: 'failed',
    last_error: longError,
    created_at: '2026-05-06T00:00:00Z',
    updated_at: '2026-05-06T00:00:00Z',
  }

  assert.equal(walletOperationDetail(operation), longError)
})

test('wallet operation detail identifies no-op approve', () => {
  const operation: WalletOperation = {
    id: 1,
    type: 'approve',
    client_request_id: 'approve-1',
    amount: '0',
    status: 'confirmed',
    created_at: '2026-05-06T00:00:00Z',
    updated_at: '2026-05-06T00:00:00Z',
  }

  assert.equal(walletOperationDetail(operation), 'Already approved')
})

test('wallet decimal input rejects more than 18 fractional digits', () => {
  const parsed = decimalToBaseUnits('1.1234567890123456789', 18)

  assert.equal(parsed.ok, false)
})

test('wallet runway top-up uses target shortfall rather than a direct multiple', () => {
  const account = {
    funds: '100',
    available_funds: '80000',
    lockup_current: '20',
    lockup_rate: '1',
    lockup_last_settled_at: '1',
    funded_until_epoch: '2',
    lockup_rate_per_day: '2880',
    lockup_rate_per_month: '86400',
    no_active_spend: false,
  }

  assert.equal(topUpNeeded(account, 86_400).toString(), '6400')
  assert.equal(topUpNeeded(account, 518_400).toString(), '438400')
})

test('wallet runway top-up is not needed when there is no active spend', () => {
  const account = {
    funds: '100',
    available_funds: '1',
    lockup_current: '20',
    lockup_rate: '1',
    lockup_last_settled_at: '1',
    funded_until_epoch: '115792089237316195423570985008687907853269984665640564039457584007913129639935',
    lockup_rate_per_day: '2880',
    lockup_rate_per_month: '86400',
    no_active_spend: true,
  }

  assert.equal(topUpNeeded(account, 86_400).toString(), '0')
})

test('funded until tone highlights low runway ranges', () => {
  const base = {
    funds: '100',
    available_funds: '20',
    lockup_current: '80',
    lockup_rate: '2',
    lockup_last_settled_at: '1',
    funded_until_epoch: '2',
    lockup_rate_per_day: '5760',
    lockup_rate_per_month: '172800',
    no_active_spend: false,
  }

  assert.equal(fundedUntilTone({ ...base, runway_seconds: 6 * 86_400 }), 'danger')
  assert.equal(fundedUntilCaption({ ...base, runway_seconds: 6 * 86_400 }), 'Under 7 days')
  assert.equal(fundedUntilTone({ ...base, runway_seconds: 20 * 86_400 }), 'warning')
  assert.equal(fundedUntilCaption({ ...base, runway_seconds: 20 * 86_400 }), 'Under 1 month')
  assert.equal(fundedUntilTone({ ...base, runway_seconds: 45 * 86_400 }), 'neutral')
  assert.equal(fundedUntilTone({ ...base, no_active_spend: true, runway_seconds: undefined }), 'neutral')
})
