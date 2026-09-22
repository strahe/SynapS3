import assert from 'node:assert/strict'
import test from 'node:test'
import {
  storageConfirmationAttentionView,
  storageConfirmationListCommand,
  storageConfirmationReleaseWarning,
} from '../src/lib/storage-confirmation-attention.ts'

test('known confirmation reasons use operator-facing labels', () => {
  const cases = [
    ['attempt_only_ambiguous', 'Submission result unknown'],
    ['unattributed_piece', 'Piece ownership unknown'],
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

test('confirmation recovery copy names the supported command and duplicate storage risk', () => {
  assert.equal(storageConfirmationListCommand, 'synaps3 admin storage-confirmation list')
  assert.match(storageConfirmationReleaseWarning, /only if/)
  assert.match(storageConfirmationReleaseWarning, /duplicate paid storage/)
})
