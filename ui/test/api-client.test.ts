import assert from 'node:assert/strict'
import test from 'node:test'

import {
  addAuthInvalidationListener,
  api,
  maxFOCUploadSize,
  minFOCUploadSize,
  setAdminCSRFToken,
  validateFOCUploadSize,
} from '../src/api/client.ts'

class FakeXMLHttpRequest {
  static instances: FakeXMLHttpRequest[] = []

  upload: { onprogress: ((event: ProgressEvent) => void) | null } = { onprogress: null }
  method = ''
  url = ''
  headers = new Headers()
  body: XMLHttpRequestBodyInit | null = null
  status = 0
  responseText = ''
  timeout = 0
  withCredentials = false
  onload: (() => void) | null = null
  onerror: (() => void) | null = null
  ontimeout: (() => void) | null = null

  open(method: string, url: string) {
    this.method = method
    this.url = url
  }

  setRequestHeader(name: string, value: string) {
    this.headers.set(name, value)
  }

  send(body?: XMLHttpRequestBodyInit | null) {
    this.body = body ?? null
    FakeXMLHttpRequest.instances.push(this)
  }

  load(status: number, responseText: string) {
    this.status = status
    this.responseText = responseText
    this.onload?.()
  }
}

function installFakeXMLHttpRequest() {
  const original = globalThis.XMLHttpRequest
  FakeXMLHttpRequest.instances = []
  globalThis.XMLHttpRequest = FakeXMLHttpRequest as unknown as typeof XMLHttpRequest
  return () => {
    globalThis.XMLHttpRequest = original
  }
}

test('admin write mutations send csrf header', async () => {
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
    setAdminCSRFToken('csrf-test-token')
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
    setAdminCSRFToken('')
    globalThis.fetch = originalFetch
  }

  assert.equal(calls.length, 5)
  assert.equal(calls[0]?.method, 'POST')
  assert.equal(calls[0]?.headers.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(calls[0]?.headers.get('X-SynapS3-Settings-Write'), null)
  assert.equal(calls[1]?.method, 'PUT')
  assert.equal(calls[1]?.headers.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(calls[1]?.headers.get('X-SynapS3-Settings-Write'), null)
  assert.equal(calls[2]?.method, 'DELETE')
  assert.equal(calls[2]?.headers.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(calls[2]?.headers.get('X-SynapS3-Settings-Write'), null)
  assert.equal(calls[3]?.method, 'POST')
  assert.equal(calls[3]?.headers.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(calls[3]?.headers.get('X-SynapS3-Settings-Write'), null)
  assert.equal(calls[4]?.method, 'POST')
  assert.equal(calls[4]?.headers.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(calls[4]?.headers.get('X-SynapS3-Settings-Write'), null)
})

test('401 responses notify auth invalidation and clear csrf token', async () => {
  const originalFetch = globalThis.fetch
  let invalidations = 0
  const removeListener = addAuthInvalidationListener(() => {
    invalidations++
  })
  const calls: Array<{ headers: Headers; method?: string }> = []
  globalThis.fetch = (async (_input, init) => {
    calls.push({ headers: new Headers(init?.headers), method: init?.method })
    if (calls.length === 1) {
      return new Response(JSON.stringify({ error: 'admin authentication required' }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      })
    }
    return new Response(JSON.stringify({ id: 1, name: 'bucket-a', owner_access_key: 'owner-a', status: 'active' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }) as typeof fetch

  try {
    setAdminCSRFToken('csrf-test-token')
    await assert.rejects(() => api.getOverview(), /admin authentication required/)
    await api.createBucket({ name: 'bucket-a', owner_access_key: 'owner-a' })
  } finally {
    removeListener()
    setAdminCSRFToken('')
    globalThis.fetch = originalFetch
  }

  assert.equal(invalidations, 1)
  assert.equal(calls[1]?.headers.get('X-SynapS3-CSRF'), null)
})

test('object upload 401 notifies auth invalidation and clears csrf token', async () => {
  const restore = installFakeXMLHttpRequest()
  const originalFetch = globalThis.fetch
  const file = new File(['x'.repeat(minFOCUploadSize)], 'expired.bin')
  const calls: Array<{ headers: Headers }> = []
  let invalidations = 0
  const removeListener = addAuthInvalidationListener(() => {
    invalidations++
  })
  globalThis.fetch = (async (_input, init) => {
    calls.push({ headers: new Headers(init?.headers) })
    return new Response(JSON.stringify({ id: 1, name: 'bucket-a', owner_access_key: 'owner-a', status: 'active' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }) as typeof fetch

  try {
    setAdminCSRFToken('csrf-test-token')
    const promise = api.uploadBucketObject('bucket-a', {
      key: 'expired.bin',
      file,
    })

    const xhr = FakeXMLHttpRequest.instances[0]
    assert.ok(xhr)
    xhr.load(401, JSON.stringify({ error: 'admin authentication required' }))

    await assert.rejects(promise, /admin authentication required/)
    await api.createBucket({ name: 'bucket-a', owner_access_key: 'owner-a' })
  } finally {
    removeListener()
    setAdminCSRFToken('')
    globalThis.fetch = originalFetch
    restore()
  }

  assert.equal(invalidations, 1)
  assert.equal(calls[0]?.headers.get('X-SynapS3-CSRF'), null)
})

test('logout clears csrf token even when server logout fails', async () => {
  const originalFetch = globalThis.fetch
  const calls: Array<{ headers: Headers; method?: string }> = []
  globalThis.fetch = (async (_input, init) => {
    calls.push({ headers: new Headers(init?.headers), method: init?.method })
    if (calls.length === 1) {
      return new Response(JSON.stringify({ error: 'admin CSRF token required' }), {
        status: 403,
        headers: { 'Content-Type': 'application/json' },
      })
    }
    return new Response(JSON.stringify({ id: 1, name: 'bucket-a', owner_access_key: 'owner-a', status: 'active' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }) as typeof fetch

  try {
    setAdminCSRFToken('csrf-test-token')
    await assert.rejects(() => api.logout(), /admin CSRF token required/)
    await api.createBucket({ name: 'bucket-a', owner_access_key: 'owner-a' })
  } finally {
    setAdminCSRFToken('')
    globalThis.fetch = originalFetch
  }

  assert.equal(calls[0]?.headers.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(calls[1]?.headers.get('X-SynapS3-CSRF'), null)
})

test('filecoin preflight sends csrf header and payload', async () => {
  const originalFetch = globalThis.fetch
  let requestedURL = ''
  let requestedMethod = ''
  let requestedHeaders = new Headers()
  let requestedBody: unknown
  globalThis.fetch = (async (input, init) => {
    requestedURL = input.toString()
    requestedMethod = init?.method ?? ''
    requestedHeaders = new Headers(init?.headers)
    requestedBody = JSON.parse(init?.body?.toString() ?? '{}') as unknown
    return new Response(
      JSON.stringify({
        status: 'ready',
        mode: 'draft',
        checked_at: '2026-05-16T12:00:00Z',
        checks: [],
      }),
      {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }) as typeof fetch

  try {
    setAdminCSRFToken('csrf-test-token')
    await api.preflightFilecoin({
      filecoin: {
        network: 'calibration',
        default_copies: 2,
      },
    })
  } finally {
    setAdminCSRFToken('')
    globalThis.fetch = originalFetch
  }

  assert.equal(requestedURL, '/api/v1/filecoin/readiness/preflight')
  assert.equal(requestedMethod, 'POST')
  assert.equal(requestedHeaders.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(requestedHeaders.get('X-SynapS3-Settings-Write'), null)
  assert.deepEqual(requestedBody, {
    filecoin: {
      network: 'calibration',
      default_copies: 2,
    },
  })
})

test('settings validate sends csrf header and payload', async () => {
  const originalFetch = globalThis.fetch
  let requestedURL = ''
  let requestedMethod = ''
  let requestedHeaders = new Headers()
  let requestedBody: unknown
  globalThis.fetch = (async (input, init) => {
    requestedURL = input.toString()
    requestedMethod = init?.method ?? ''
    requestedHeaders = new Headers(init?.headers)
    requestedBody = JSON.parse(init?.body?.toString() ?? '{}') as unknown
    return new Response(JSON.stringify({ validation_errors: [] }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }) as typeof fetch

  try {
    setAdminCSRFToken('csrf-test-token')
    await api.validateSettings({
      server: {
        port: ':8080',
      },
    })
  } finally {
    setAdminCSRFToken('')
    globalThis.fetch = originalFetch
  }

  assert.equal(requestedURL, '/api/v1/settings/validate')
  assert.equal(requestedMethod, 'POST')
  assert.equal(requestedHeaders.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(requestedHeaders.get('X-SynapS3-Settings-Write'), null)
  assert.deepEqual(requestedBody, {
    server: {
      port: ':8080',
    },
  })
})

test('data set storage health refresh sends csrf header', async () => {
  const originalFetch = globalThis.fetch
  let requestedURL = ''
  let requestedMethod = ''
  let requestedHeaders = new Headers()
  globalThis.fetch = (async (input, init) => {
    requestedURL = input.toString()
    requestedMethod = init?.method ?? ''
    requestedHeaders = new Headers(init?.headers)
    return new Response(JSON.stringify({ items: [], summary: {}, warnings: [], total: 0, limit: 100, offset: 0 }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }) as typeof fetch

  try {
    setAdminCSRFToken('csrf-test-token')
    await api.refreshDataSetStorageHealth({ bucket: 'bucket-a' })
  } finally {
    setAdminCSRFToken('')
    globalThis.fetch = originalFetch
  }

  assert.equal(requestedURL, '/api/v1/observability/data-sets/refresh?bucket=bucket-a')
  assert.equal(requestedMethod, 'POST')
  assert.equal(requestedHeaders.get('X-SynapS3-CSRF'), 'csrf-test-token')
  assert.equal(requestedHeaders.get('X-SynapS3-Observability-Refresh'), null)
  assert.equal(requestedHeaders.get('X-SynapS3-Settings-Write'), null)
})

test('observability list APIs encode supported query parameters', async () => {
  const originalFetch = globalThis.fetch
  const requestedURLs: string[] = []
  globalThis.fetch = (async (input) => {
    requestedURLs.push(input.toString())
    return new Response(
      JSON.stringify({
        items: [],
        summary: { total: 0, available: 0, degraded: 0, unavailable: 0, unknown: 0 },
        summary_signal: { level: 'ok', freshness: { stale: false, warnings: [] } },
        total: 0,
        limit: 100,
        offset: 0,
      }),
      {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }) as typeof fetch

  try {
    await api.getObservabilityProviders({
      status: 'degraded',
      provider_id: '101',
      limit: 20,
      offset: 40,
    })
    await api.getObservabilityDataSets({
      status: 'available',
      provider_id: '202',
      bucket: 'bucket-a',
      limit: 50,
      offset: 100,
    })
    await api.getObservabilityDataSets({
      bucket_id: 3,
    })
  } finally {
    globalThis.fetch = originalFetch
  }

  assert.deepEqual(requestedURLs, [
    '/api/v1/observability/providers?status=degraded&provider_id=101&limit=20&offset=40',
    '/api/v1/observability/data-sets?status=available&provider_id=202&bucket=bucket-a&limit=50&offset=100',
    '/api/v1/observability/data-sets?bucket_id=3',
  ])
})

test('observability list APIs omit query string when params are empty', async () => {
  const originalFetch = globalThis.fetch
  const requestedURLs: string[] = []
  globalThis.fetch = (async (input) => {
    requestedURLs.push(input.toString())
    return new Response(
      JSON.stringify({
        items: [],
        summary: { total: 0, available: 0, degraded: 0, unavailable: 0, unknown: 0 },
        summary_signal: { level: 'ok', freshness: { stale: false, warnings: [] } },
        total: 0,
        limit: 100,
        offset: 0,
      }),
      {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }) as typeof fetch

  try {
    await api.getObservabilityProviders()
    await api.getObservabilityDataSets()
  } finally {
    globalThis.fetch = originalFetch
  }

  assert.deepEqual(requestedURLs, ['/api/v1/observability/providers', '/api/v1/observability/data-sets'])
})

test('task diagnostic APIs use task diagnostic endpoints without write header', async () => {
  const originalFetch = globalThis.fetch
  const controller = new AbortController()
  const calls: Array<{ url: string; method: string; headers: Headers; signal?: AbortSignal | null }> = []
  globalThis.fetch = (async (input, init) => {
    calls.push({
      url: input.toString(),
      method: init?.method ?? 'GET',
      headers: new Headers(init?.headers),
      signal: init?.signal,
    })
    return new Response(
      JSON.stringify({
        checked_at: '2026-05-22T10:00:00Z',
        current_state: 'waiting_for_chain',
        signal: {
          status: 'degraded',
          level: 'warning',
          reason_codes: ['task_chain_pending'],
          freshness: { stale: false, warnings: [] },
        },
        reason_codes: ['task_chain_pending'],
        next_action: 'wait',
        evidence: { task: { id: 7, type: 'upload', status: 'waiting' }, operation: 'add_pieces' },
      }),
      {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }
    )
  }) as typeof fetch

  try {
    await api.getTaskDiagnostic(7, { signal: controller.signal })
    await api.refreshTaskDiagnostic(7, { signal: controller.signal })
  } finally {
    globalThis.fetch = originalFetch
  }

  assert.deepEqual(
    calls.map((call) => [call.url, call.method, call.headers.get('X-SynapS3-Settings-Write'), call.signal]),
    [
      ['/api/v1/tasks/7/diagnostic', 'GET', null, controller.signal],
      ['/api/v1/tasks/7/diagnostic/refresh', 'POST', null, controller.signal],
    ]
  )
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

test('bucket storage risk versions sends independent marker parameters', async () => {
  const originalFetch = globalThis.fetch
  let requestedURL = ''
  globalThis.fetch = (async (input) => {
    requestedURL = input.toString()
    return new Response(JSON.stringify({ versions: [], has_more: false }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    })
  }) as typeof fetch

  try {
    await api.getBucketStorageRiskVersions('bucket-a', {
      prefix: ' reports/ ',
      local_data_set_id: 42,
      key_marker: ' reports/a.txt ',
      version_marker: '01JMARKER',
      created_at_marker: '2026-05-24T12:00:00.123456789Z',
      stale_before: '2026-05-24T11:59:00.123456789Z',
      limit: 50,
    })
  } finally {
    globalThis.fetch = originalFetch
  }

  assert.equal(
    requestedURL,
    '/api/v1/buckets/bucket-a/storage-health/affected-versions?prefix=+reports%2F+&local_data_set_id=42&key_marker=+reports%2Fa.txt+&version_marker=01JMARKER&created_at_marker=2026-05-24T12%3A00%3A00.123456789Z&stale_before=2026-05-24T11%3A59%3A00.123456789Z&limit=50'
  )
})

test('object upload uses XHR with csrf header and upload progress', async () => {
  const restore = installFakeXMLHttpRequest()
  const file = new File(['x'.repeat(minFOCUploadSize)], 'report summary.txt', { type: 'text/plain' })
  const progress: Array<{ loaded: number; total: number; percent: number }> = []

  try {
    setAdminCSRFToken('csrf-test-token')
    const promise = api.uploadBucketObject('bucket a', {
      key: 'reports/report summary.txt',
      file,
      onProgress: (next) => progress.push(next),
    })

    const xhr = FakeXMLHttpRequest.instances[0]
    assert.ok(xhr)
    assert.equal(xhr.method, 'POST')
    assert.equal(xhr.url, '/api/v1/buckets/bucket%20a/objects/upload?key=reports%2Freport%20summary.txt')
    assert.equal(xhr.withCredentials, true)
    assert.equal(xhr.headers.get('X-SynapS3-CSRF'), 'csrf-test-token')
    assert.equal(xhr.headers.get('X-SynapS3-Settings-Write'), null)
    assert.equal(xhr.headers.get('Content-Type'), 'text/plain')
    assert.equal(xhr.timeout, 60 * 60 * 1000)
    assert.equal(xhr.body, file)

    xhr.upload.onprogress?.({ lengthComputable: true, loaded: 6, total: 12 } as ProgressEvent)
    xhr.load(
      200,
      JSON.stringify({
        key: 'reports/report summary.txt',
        version_id: 'version-1',
        etag: '"etag-1"',
        size: 12,
        content_type: 'text/plain',
      })
    )

    assert.deepEqual(await promise, {
      key: 'reports/report summary.txt',
      version_id: 'version-1',
      etag: '"etag-1"',
      size: 12,
      content_type: 'text/plain',
    })
    assert.deepEqual(progress, [{ loaded: 6, total: 12, percent: 50 }])
  } finally {
    setAdminCSRFToken('')
    restore()
  }
})

test('object upload surfaces timeout errors', async () => {
  const restore = installFakeXMLHttpRequest()
  const file = new File(['x'.repeat(minFOCUploadSize)], 'slow.bin')

  try {
    const promise = api.uploadBucketObject('bucket-a', {
      key: 'slow.bin',
      file,
    })

    const xhr = FakeXMLHttpRequest.instances[0]
    assert.ok(xhr)
    assert.equal(typeof xhr.ontimeout, 'function')
    xhr.ontimeout?.()

    await assert.rejects(promise, /Upload timed out/)
  } finally {
    restore()
  }
})

test('object upload surfaces JSON error responses', async () => {
  const restore = installFakeXMLHttpRequest()
  const file = new File(['x'.repeat(minFOCUploadSize)], 'large.bin')

  try {
    const promise = api.uploadBucketObject('bucket-a', {
      key: 'large.bin',
      file,
      onProgress: () => {},
    })

    const xhr = FakeXMLHttpRequest.instances[0]
    assert.ok(xhr)
    xhr.load(507, JSON.stringify({ error: 'cache capacity exceeded' }))

    await assert.rejects(promise, /cache capacity exceeded/)
  } finally {
    restore()
  }
})

test('object upload rejects files outside FOC size limits before XHR', async () => {
  const restore = installFakeXMLHttpRequest()

  try {
    await assert.rejects(
      api.uploadBucketObject('bucket-a', {
        key: 'empty.bin',
        file: new File([], 'empty.bin'),
      }),
      /EntityTooSmall/
    )

    const hugeFile = new File(['x'], 'huge.bin')
    Object.defineProperty(hugeFile, 'size', { value: maxFOCUploadSize + 1 })
    await assert.rejects(
      api.uploadBucketObject('bucket-a', {
        key: 'huge.bin',
        file: hugeFile,
      }),
      /EntityTooLarge/
    )

    assert.equal(validateFOCUploadSize(minFOCUploadSize), null)
    assert.equal(FakeXMLHttpRequest.instances.length, 0)
  } finally {
    restore()
  }
})
