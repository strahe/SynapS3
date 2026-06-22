import assert from 'node:assert/strict'
import test from 'node:test'
import { QueryClient } from '@tanstack/react-query'

import type { WalletOperation, WalletOperationsResponse } from '../src/api/client.ts'
import { applyWalletOperationEventData } from '../src/lib/wallet-operation-events.ts'

function walletOperation(id: number, createdAt: string, overrides: Partial<WalletOperation> = {}): WalletOperation {
  return {
    id,
    type: 'fund',
    client_request_id: `request-${id}`,
    amount: '1',
    status: 'submitted',
    created_at: createdAt,
    updated_at: createdAt,
    ...overrides,
  }
}

test('wallet operation events preserve the selected operation count', () => {
  const qc = new QueryClient()
  const key = ['walletOperations', 2]
  qc.setQueryData<WalletOperationsResponse>(key, {
    operations: [walletOperation(2, '2026-05-06T00:02:00Z'), walletOperation(1, '2026-05-06T00:01:00Z')],
  })

  applyWalletOperationEventData(
    qc,
    JSON.stringify({
      topic: 'wallet_operation_updated',
      operation: walletOperation(3, '2026-05-06T00:03:00Z'),
    })
  )

  const data = qc.getQueryData<WalletOperationsResponse>(key)
  assert.deepEqual(
    data?.operations.map((operation) => operation.id),
    [3, 2]
  )
})

test('wallet operation events grow up to the selected operation count', () => {
  const qc = new QueryClient()
  const key = ['walletOperations', 10]
  qc.setQueryData<WalletOperationsResponse>(key, {
    operations: [
      walletOperation(3, '2026-05-06T00:03:00Z'),
      walletOperation(2, '2026-05-06T00:02:00Z'),
      walletOperation(1, '2026-05-06T00:01:00Z'),
    ],
  })

  applyWalletOperationEventData(
    qc,
    JSON.stringify({
      topic: 'wallet_operation_updated',
      operation: walletOperation(4, '2026-05-06T00:04:00Z'),
    })
  )

  const data = qc.getQueryData<WalletOperationsResponse>(key)
  assert.deepEqual(
    data?.operations.map((operation) => operation.id),
    [4, 3, 2, 1]
  )
})

test('confirmed approve events invalidate Filecoin readiness', () => {
  const qc = new QueryClient()
  const readinessKey = ['filecoinReadiness']
  qc.setQueryData(readinessKey, { status: 'blocked' })

  applyWalletOperationEventData(
    qc,
    JSON.stringify({
      topic: 'wallet_operation_updated',
      operation: walletOperation(1, '2026-06-22T00:00:00Z', {
        type: 'approve',
        amount: '0',
      }),
    })
  )
  assert.equal(qc.getQueryState(readinessKey)?.isInvalidated, false)

  applyWalletOperationEventData(
    qc,
    JSON.stringify({
      topic: 'wallet_operation_updated',
      operation: walletOperation(1, '2026-06-22T00:00:00Z', {
        type: 'approve',
        amount: '0',
        status: 'confirmed',
      }),
    })
  )
  assert.equal(qc.getQueryState(readinessKey)?.isInvalidated, true)
})
