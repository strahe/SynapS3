import assert from 'node:assert/strict'
import test from 'node:test'

import { bucketPrefixCrumbs, duplicateObjectUploadKeys, objectUploadKey } from '../src/lib/s3-prefix.ts'

test('bucketPrefixCrumbs preserves slash-only path segments', () => {
  assert.deepEqual(bucketPrefixCrumbs('a//'), [
    { label: 'a', prefix: 'a/' },
    { label: '/', prefix: 'a//' },
  ])
})

test('objectUploadKey joins file names under normalized prefixes', () => {
  assert.equal(objectUploadKey('', 'file.txt'), 'file.txt')
  assert.equal(objectUploadKey('dir', 'file.txt'), 'dir/file.txt')
  assert.equal(objectUploadKey('dir/', 'file.txt'), 'dir/file.txt')
})

test('duplicateObjectUploadKeys reports repeated upload targets once', () => {
  assert.deepEqual(duplicateObjectUploadKeys(['dir/a.txt', 'dir/b.txt', 'dir/a.txt', 'dir/a.txt']), ['dir/a.txt'])
})
