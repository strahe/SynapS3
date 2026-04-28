import assert from 'node:assert/strict'
import test from 'node:test'

import { api } from '../src/api/client.ts'

test('bucket write mutations send settings write header', async () => {
  const originalFetch = globalThis.fetch
  const calls: Array<{ headers: Headers; method?: string }> = []
  globalThis.fetch = (async (_input, init) => {
    calls.push({ headers: new Headers(init?.headers), method: init?.method })
    return new Response(JSON.stringify({ id: 1, name: 'bucket-a', owner_access_key: 'owner-a', status: 'active' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }) as typeof fetch

  try {
    await api.createBucket({ name: 'bucket-a', owner_access_key: 'owner-a' })
    await api.updateBucketOwner('bucket-a', 'owner-b')
  } finally {
    globalThis.fetch = originalFetch
  }

  assert.equal(calls.length, 2)
  assert.equal(calls[0]?.method, 'POST')
  assert.equal(calls[0]?.headers.get('X-SynapS3-Settings-Write'), '1')
  assert.equal(calls[1]?.method, 'PUT')
  assert.equal(calls[1]?.headers.get('X-SynapS3-Settings-Write'), '1')
})
