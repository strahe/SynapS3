import assert from 'node:assert/strict'
import test from 'node:test'

import { bucketCopyPolicyEffectNote, bucketCopyPolicySavedMessage } from '../src/lib/bucket-copy-policy.ts'

test('bucket copy policy text explains save result and future upload scope', () => {
  assert.equal(bucketCopyPolicySavedMessage(), 'Replica policy saved.')
  assert.equal(
    bucketCopyPolicyEffectNote(),
    'Applies only to uploads started after this change. Existing objects and in-flight uploads keep their current replica plan.'
  )
})
