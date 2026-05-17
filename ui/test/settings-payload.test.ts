import assert from 'node:assert/strict'
import test from 'node:test'

import type { SettingsEditableConfig } from '../src/api/client.ts'
import { buildSettingsPayload } from '../src/lib/settings-payload.ts'

function baseConfig(): SettingsEditableConfig {
  return {
    server: {
      port: ':8080',
      max_connections: 4096,
      max_requests: 512,
      tls: { enabled: false, cert_file: '', key_file: '' },
    },
    s3: { region: 'us-east-1' },
    filecoin: {
      network: 'calibration',
      rpc_url: 'https://rpc.example.invalid',
      source: 'synaps3',
      with_cdn: false,
      allow_private_networks: false,
      default_copies: 2,
      availability: { interval: '5m0s', timeout: '5s', concurrency: 8 },
    },
    cache: {
      dir: '/tmp/cache',
      max_size_gb: 100,
      eviction_policy: 'lru',
    },
    worker: {
      upload: { concurrency: 4, poll_interval: '5s', max_retries: 5 },
      evictor: { concurrency: 2, poll_interval: '1m0s', max_retries: 3 },
      storage_cleanup: { concurrency: 2, poll_interval: '1m0s', max_retries: 5 },
    },
    logging: {
      level: 'info',
      format: 'text',
      s3_access: { enabled: true, level: 'info' },
    },
  }
}

test('settings payload includes editable s3 access logging fields', () => {
  const initial = baseConfig()
  const form = baseConfig()
  form.logging.s3_access.enabled = false
  form.logging.s3_access.level = 'debug'

  const payload = buildSettingsPayload(form, initial, {})

  assert.deepEqual(payload.logging?.s3_access, { enabled: false, level: 'debug' })
})

test('settings payload omits env-managed s3 access logging fields', () => {
  const initial = baseConfig()
  const form = baseConfig()
  form.logging.s3_access.enabled = false
  form.logging.s3_access.level = 'debug'

  const payload = buildSettingsPayload(form, initial, {
    'logging.s3_access.enabled': 'SYNAPS3_LOGGING_S3_ACCESS_ENABLED',
    'logging.s3_access.level': 'SYNAPS3_LOGGING_S3_ACCESS_LEVEL',
  })

  assert.deepEqual(payload.logging?.s3_access, {})
})

test('settings payload includes editable filecoin availability fields', () => {
  const initial = baseConfig()
  const form = baseConfig()
  form.filecoin.availability.interval = '10m0s'
  form.filecoin.availability.timeout = '10s'
  form.filecoin.availability.concurrency = 4

  const payload = buildSettingsPayload(form, initial, {})

  assert.deepEqual(payload.filecoin?.availability, {
    interval: '10m0s',
    timeout: '10s',
    concurrency: 4,
  })
})

test('settings payload omits env-managed filecoin availability fields', () => {
  const initial = baseConfig()
  const form = baseConfig()
  form.filecoin.availability.interval = '10m0s'
  form.filecoin.availability.timeout = '10s'
  form.filecoin.availability.concurrency = 4

  const payload = buildSettingsPayload(form, initial, {
    'filecoin.availability.interval': 'SYNAPS3_FILECOIN_AVAILABILITY_INTERVAL',
    'filecoin.availability.timeout': 'SYNAPS3_FILECOIN_AVAILABILITY_TIMEOUT',
    'filecoin.availability.concurrency': 'SYNAPS3_FILECOIN_AVAILABILITY_CONCURRENCY',
  })

  assert.deepEqual(payload.filecoin?.availability, {})
})
