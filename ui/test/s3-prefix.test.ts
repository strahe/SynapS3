import assert from 'node:assert/strict'
import test from 'node:test'

import { bucketPrefixCrumbs } from '../src/lib/s3-prefix.ts'

test('bucketPrefixCrumbs preserves slash-only path segments', () => {
  assert.deepEqual(bucketPrefixCrumbs('a//'), [
    { label: 'a', prefix: 'a/' },
    { label: '/', prefix: 'a//' },
  ])
})
