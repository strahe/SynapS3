import assert from 'node:assert/strict'
import test from 'node:test'

import { internalRootOwnerAccessKey } from '../src/api/client.ts'
import { ownerLabel, s3UserLabel } from '../src/lib/s3-owner.ts'

test('S3 user labels prefer the name and keep a short access key', () => {
  const user = { access_key: 'access-key-abcdef', name: '备份客户端' }
  assert.equal(s3UserLabel(user), '备份客户端 (…abcdef)')
  assert.equal(ownerLabel(user.access_key, [user]), '备份客户端 (…abcdef)')
})

test('owner labels fall back when no named user is available', () => {
  const user = { access_key: 'access-key-abcdef', name: '' }
  assert.equal(s3UserLabel(user), user.access_key)
  assert.equal(ownerLabel(user.access_key, [user]), user.access_key)
  assert.equal(ownerLabel(user.access_key), user.access_key)
  assert.equal(ownerLabel(internalRootOwnerAccessKey), 'Internal root')
  assert.equal(ownerLabel(null), 'Unassigned')
})
