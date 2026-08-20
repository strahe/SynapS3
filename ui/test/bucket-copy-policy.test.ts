import assert from 'node:assert/strict'
import test from 'node:test'

import {
  bucketCopyPolicyInheritOptionLabel,
  bucketCopyPolicySavedMessage,
  clampMinimumDurableCopiesValue,
  minimumDurableCopiesChoiceNote,
  minimumDurableCopiesFixedCountNote,
  minimumDurableCopiesLabel,
  minimumDurableCopiesOptionLabel,
  minimumDurableCopiesOptions,
  minimumDurableCopiesValue,
  minimumDurableCopiesWarning,
  persistMinimumDurableCopies,
  replicaTargetChoiceNote,
  selectedTargetCopies,
  showsMinimumDurableCopiesWarning,
  strictMinimumDurableCopiesValue,
} from '../src/lib/bucket-copy-policy.ts'

test('bucket copy policy text explains save result and future upload scope', () => {
  assert.equal(
    replicaTargetChoiceNote(),
    'Applies to new uploads. Existing objects keep the replica target they started with.'
  )
  assert.equal(
    bucketCopyPolicySavedMessage(),
    'Saved. New uploads use this replica target. Cache can be released after the selected count.'
  )
  assert.equal(
    minimumDurableCopiesChoiceNote(),
    'Keeps cache until every replica of that upload is stored, including later Replicas increases.'
  )
  assert.equal(minimumDurableCopiesFixedCountNote(), 'Keeps this count if Replicas later increases.')
  assert.equal(minimumDurableCopiesOptionLabel(3), '3 replicas')
  assert.equal(minimumDurableCopiesOptionLabel(3, 3), '3 replicas (fixed count)')
  assert.equal(minimumDurableCopiesOptionLabel(2, 3), '2 replicas')
  assert.equal(
    minimumDurableCopiesWarning(),
    'Cache may be removed before every target replica is ready. Raising this later cannot restore deleted cache.'
  )
  assert.equal(showsMinimumDurableCopiesWarning(strictMinimumDurableCopiesValue, 3), false)
  assert.equal(showsMinimumDurableCopiesWarning('3', 3), false)
  assert.equal(showsMinimumDurableCopiesWarning('2', 3), true)
})

test('minimum durable copy choices follow the selected runtime or explicit target', () => {
  assert.equal(selectedTargetCopies('inherit', 3), 3)
  assert.equal(selectedTargetCopies('inherit'), null)
  assert.equal(selectedTargetCopies('4', 3), 4)
  assert.deepEqual(minimumDurableCopiesOptions(3), [1, 2, 3])
  assert.deepEqual(minimumDurableCopiesOptions(null), [])
  assert.equal(clampMinimumDurableCopiesValue('3', 2), '2')
  assert.equal(clampMinimumDurableCopiesValue('2', 3), '2')
  assert.equal(clampMinimumDurableCopiesValue(strictMinimumDurableCopiesValue, 2), strictMinimumDurableCopiesValue)
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
  assert.equal(minimumDurableCopiesValue({ minimum_durable_copies: 5, effective_copies: 2 }), '2')
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
      minimum_durable_copies: 3,
      effective_minimum_durable_copies: 3,
    }),
    '3 of 3 replicas'
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
  assert.equal(
    minimumDurableCopiesLabel({
      default_copies: null,
      effective_copies: 2,
      minimum_durable_copies: 5,
      effective_minimum_durable_copies: 2,
    }),
    '2 of 2 replicas'
  )
})

test('persisted minimum keeps an explicit count instead of clearing to all replicas', () => {
  assert.equal(persistMinimumDurableCopies(strictMinimumDurableCopiesValue, null, 3), undefined)
  assert.equal(persistMinimumDurableCopies(strictMinimumDurableCopiesValue, 2, 3), null)
  assert.equal(persistMinimumDurableCopies('2', 2, 3), undefined)
  assert.equal(persistMinimumDurableCopies('2', 5, 2), 2)
  assert.equal(persistMinimumDurableCopies('5', 5, 8), undefined)
  assert.equal(persistMinimumDurableCopies('1', 2, 3), 1)
})
