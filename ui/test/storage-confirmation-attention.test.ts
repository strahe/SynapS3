import assert from 'node:assert/strict'
import test from 'node:test'
import { storageConfirmationAttentionView } from '../src/lib/storage-confirmation-attention.ts'

test('known confirmation reasons use operator-facing labels', () => {
  const cases = [
    ['submission_mismatch', 'Confirmation does not match'],
    ['data_set_unavailable', 'Data set is unavailable'],
    ['confirmation_timeout', 'Confirmation timed out'],
  ] as const

  for (const [reasonCode, label] of cases) {
    assert.deepEqual(storageConfirmationAttentionView(reasonCode), {
      known: true,
      label,
      reasonCode,
    })
  }
})

test('unknown confirmation reasons preserve the raw code behind a safe fallback', () => {
  assert.deepEqual(storageConfirmationAttentionView('future_provider_result'), {
    known: false,
    label: 'Unknown confirmation issue',
    reasonCode: 'future_provider_result',
  })
  assert.deepEqual(storageConfirmationAttentionView('constructor'), {
    known: false,
    label: 'Unknown confirmation issue',
    reasonCode: 'constructor',
  })
})
