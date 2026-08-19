import assert from 'node:assert/strict'
import test from 'node:test'

import {
  bucketCopyPolicyEffectNote,
  bucketCopyPolicyInheritOptionLabel,
  bucketCopyPolicySavedMessage,
  clampMinimumDurableCopiesValue,
  minimumDurableCopiesLabel,
  minimumDurableCopiesOptions,
  minimumDurableCopiesValue,
  minimumDurableCopiesWarning,
  selectedTargetCopies,
  strictMinimumDurableCopiesValue,
} from '../src/lib/bucket-copy-policy.ts'

test('bucket copy policy text explains save result and future upload scope', () => {
  assert.equal(bucketCopyPolicySavedMessage(), 'Replica policy saved.')
  assert.equal(
    bucketCopyPolicyEffectNote(),
    'Target replicas apply to new uploads. Cache release applies to retained cache for current and future uploads.'
  )
  assert.equal(
    minimumDurableCopiesWarning(),
    'Lowering this value can release local cache before every target replica is ready. Raising it cannot restore cache that has already been deleted.'
  )
})

test('minimum durable copy choices follow the selected runtime or explicit target', () => {
  assert.equal(selectedTargetCopies('inherit', 3), 3)
  assert.equal(selectedTargetCopies('inherit'), null)
  assert.equal(selectedTargetCopies('4', 3), 4)
  assert.deepEqual(minimumDurableCopiesOptions(3), [1, 2, 3])
  assert.deepEqual(minimumDurableCopiesOptions(null), [])
  assert.equal(clampMinimumDurableCopiesValue('3', 2), strictMinimumDurableCopiesValue)
  assert.equal(clampMinimumDurableCopiesValue('2', 3), '2')
})

test('inherited target labels use the current runtime rather than the saved next-start value', () => {
  assert.equal(
    bucketCopyPolicyInheritOptionLabel(
      {
        default_copies: 4,
        effective_copies: 4,
        minimum_durable_copies: null,
        effective_minimum_durable_copies: 4,
      },
      3
    ),
    'Inherit current runtime default (3 copies)'
  )
  assert.equal(
    bucketCopyPolicyInheritOptionLabel({
      default_copies: null,
      effective_copies: 3,
      minimum_durable_copies: null,
      effective_minimum_durable_copies: 3,
    }),
    'Inherit current runtime default (3 copies)'
  )
})

test('minimum durable copy labels distinguish strict and explicit policies', () => {
  assert.equal(
    minimumDurableCopiesValue({ minimum_durable_copies: null, effective_copies: 3 }),
    strictMinimumDurableCopiesValue
  )
  assert.equal(
    minimumDurableCopiesValue({ minimum_durable_copies: 5, effective_copies: 2 }),
    strictMinimumDurableCopiesValue
  )
  assert.equal(
    minimumDurableCopiesLabel({
      default_copies: null,
      effective_copies: 3,
      minimum_durable_copies: null,
      effective_minimum_durable_copies: 3,
    }),
    'All replicas (strict)'
  )
  assert.equal(
    minimumDurableCopiesLabel({
      default_copies: 3,
      effective_copies: 3,
      minimum_durable_copies: 2,
      effective_minimum_durable_copies: 2,
    }),
    '2 of 3 replicas'
  )
})
