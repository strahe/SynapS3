import assert from 'node:assert/strict'
import test from 'node:test'

import { api } from '../src/api/client.ts'

test('admin write mutations send settings write header', async () => {
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
    await api.deleteBucketObject('bucket-a', 'folder/file.txt')
    await api.restoreBucketObject('bucket-a', {
      key: 'folder/file.txt',
      delete_marker_version_id: 'marker-1',
    })
    await api.permanentlyDeleteDeletedBucketObject('bucket-a', {
      key: 'folder/file.txt',
      delete_marker_version_id: 'marker-1',
    })
  } finally {
    globalThis.fetch = originalFetch
  }

  assert.equal(calls.length, 5)
  assert.equal(calls[0]?.method, 'POST')
  assert.equal(calls[0]?.headers.get('X-SynapS3-Settings-Write'), '1')
  assert.equal(calls[1]?.method, 'PUT')
  assert.equal(calls[1]?.headers.get('X-SynapS3-Settings-Write'), '1')
  assert.equal(calls[2]?.method, 'DELETE')
  assert.equal(calls[2]?.headers.get('X-SynapS3-Settings-Write'), '1')
  assert.equal(calls[3]?.method, 'POST')
  assert.equal(calls[3]?.headers.get('X-SynapS3-Settings-Write'), '1')
  assert.equal(calls[4]?.method, 'POST')
  assert.equal(calls[4]?.headers.get('X-SynapS3-Settings-Write'), '1')
})

test('object download URL encodes bucket name and object key', () => {
  assert.equal(
    api.getObjectDownloadUrl('bucket-a', 'reports/April summary.txt'),
    '/api/v1/buckets/bucket-a/objects/download?key=reports%2FApril%20summary.txt'
  )
})

test('bucket object listing sends folder browser query parameters', async () => {
  const originalFetch = globalThis.fetch
  let requestedURL = ''
  globalThis.fetch = (async (input) => {
    requestedURL = input.toString()
    return new Response(JSON.stringify({ folders: [], objects: [], has_more: false }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }) as typeof fetch

  try {
    await api.getBucketObjects('bucket-a', {
      prefix: 'reports/',
      delimiter: '/',
      after: 'reports/2026/a.txt',
      limit: 50,
    })
  } finally {
    globalThis.fetch = originalFetch
  }

  assert.equal(
    requestedURL,
    '/api/v1/buckets/bucket-a/objects?prefix=reports%2F&delimiter=%2F&after=reports%2F2026%2Fa.txt&limit=50'
  )
})
