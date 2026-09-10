import assert from 'node:assert/strict'
import test from 'node:test'

import {
  bucketCopyPolicyLabel,
  bucketCopyPolicySavedMessage,
  bucketCopyPolicyValue,
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
  replicaTargetLocked,
  replicaTargetLockNote,
  selectedTargetCopies,
  showsMinimumDurableCopiesWarning,
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
  assert.equal(minimumDurableCopiesChoiceNote(), 'Keeps cache until every replica of that upload is stored.')
  assert.equal(minimumDurableCopiesFixedCountNote(), 'Keeps this count if Replicas later increases.')
  assert.equal(minimumDurableCopiesOptionLabel(3), '3 replicas')
  assert.equal(minimumDurableCopiesOptionLabel(3, 3), '3 replicas (all replicas)')
  assert.equal(minimumDurableCopiesOptionLabel(2, 3), '2 replicas')
  assert.equal(
    minimumDurableCopiesWarning(),
    'Cache may be removed before every target replica is ready. Raising this later cannot restore deleted cache.'
  )
  assert.equal(showsMinimumDurableCopiesWarning('3', 3), false)
  assert.equal(showsMinimumDurableCopiesWarning('2', 3), true)
})

test('minimum durable copy choices follow the selected target', () => {
  assert.equal(selectedTargetCopies('4'), 4)
  assert.equal(selectedTargetCopies(''), null)
  assert.equal(selectedTargetCopies('9'), null)
  assert.deepEqual(minimumDurableCopiesOptions(3), [1, 2, 3])
  assert.deepEqual(minimumDurableCopiesOptions(null), [])
  assert.equal(clampMinimumDurableCopiesValue('3', 2), '2')
  assert.equal(clampMinimumDurableCopiesValue('2', 3), '2')
  assert.equal(clampMinimumDurableCopiesValue('', 2), '2')
})

test('replica targets below the stored one are offered but locked', () => {
  assert.equal(replicaTargetLockNote(), 'Lowering the replica target is not supported yet.')
  assert.equal(replicaTargetLocked(2, 3), true)
  assert.equal(replicaTargetLocked(3, 3), false)
  assert.equal(replicaTargetLocked(4, 3), false)
  // A bucket at the smallest target locks nothing.
  assert.equal(replicaTargetLocked(1, 1), false)
})

test('a bucket reports the replica target it stores rather than one to resolve later', () => {
  assert.equal(bucketCopyPolicyValue({ default_copies: 3 }), '3')
  assert.equal(bucketCopyPolicyLabel({ default_copies: 1 }), '1 copy')
  assert.equal(bucketCopyPolicyLabel({ default_copies: 3 }), '3 copies')
})

test('minimum durable copy labels read against the stored target', () => {
  assert.equal(minimumDurableCopiesValue({ default_copies: 3, minimum_durable_copies: 3 }), '3')
  assert.equal(minimumDurableCopiesValue({ default_copies: 2, minimum_durable_copies: 5 }), '2')
  assert.equal(minimumDurableCopiesLabel({ default_copies: 3, minimum_durable_copies: 3 }), '3 of 3 replicas')
  assert.equal(minimumDurableCopiesLabel({ default_copies: 3, minimum_durable_copies: 2 }), '2 of 3 replicas')
  assert.equal(minimumDurableCopiesLabel({ default_copies: 1, minimum_durable_copies: 1 }), '1 of 1 replica')
})

test('persisted minimum sends only a changed explicit count', () => {
  assert.equal(persistMinimumDurableCopies('2', 2, 3), undefined)
  assert.equal(persistMinimumDurableCopies('2', 5, 2), 2)
  assert.equal(persistMinimumDurableCopies('5', 5, 8), undefined)
  assert.equal(persistMinimumDurableCopies('1', 2, 3), 1)
  assert.equal(persistMinimumDurableCopies('', 2, 3), undefined)
})
