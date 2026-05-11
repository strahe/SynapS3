import assert from 'node:assert/strict'
import test from 'node:test'

import type { SettingsEditableConfig, SettingsFieldMetadata } from '../src/api/client.ts'
import {
  classifySettingsRisk,
  collectSettingsRiskChanges,
  confirmationMatches,
  settingsRiskNeedsStrongConfirmation,
} from '../src/lib/risk-confirmation.ts'

const metadata: Record<string, SettingsFieldMetadata> = {
  'server.tls.enabled': meta('TLS Enabled'),
  'server.tls.cert_file': meta('TLS Cert File'),
  'server.tls.key_file': meta('TLS Key File'),
  'server.port': meta('S3 Port'),
  's3.region': meta('Region'),
  'filecoin.network': meta('Filecoin Network'),
  'filecoin.rpc_url': meta('Filecoin RPC URL'),
  'filecoin.allow_private_networks': meta('Allow Private Networks'),
  'filecoin.default_copies': meta('Default Copies'),
  'cache.dir': meta('Cache Directory'),
  'cache.max_size_gb': meta('Cache Max Size'),
  'cache.eviction_policy': meta('Cache Eviction Policy'),
  'worker.upload.concurrency': meta('Upload Concurrency'),
  'worker.upload.max_retries': meta('Upload Max Retries'),
  'worker.upload.poll_interval': meta('Upload Poll Interval'),
  'worker.evictor.concurrency': meta('Evictor Concurrency'),
  'worker.evictor.max_retries': meta('Evictor Max Retries'),
  'worker.evictor.poll_interval': meta('Evictor Poll Interval'),
  'worker.storage_cleanup.concurrency': meta('Replica Cleanup Concurrency'),
  'worker.storage_cleanup.max_retries': meta('Replica Cleanup Max Retries'),
  'worker.storage_cleanup.poll_interval': meta('Replica Cleanup Poll Interval'),
}

function meta(label: string): SettingsFieldMetadata {
  return { label, description: label, editable: true, secret: false }
}

function baseConfig(): SettingsEditableConfig {
  return {
    server: {
      port: ':8080',
      max_connections: 250000,
      max_requests: 100000,
      tls: {
        enabled: true,
        cert_file: '/certs/current.pem',
        key_file: '/certs/current.key',
      },
    },
    s3: {
      region: 'us-east-1',
    },
    filecoin: {
      network: 'calibration',
      rpc_url: 'https://api.calibration.node.glif.io/rpc/v1',
      source: 'synaps3',
      with_cdn: false,
      allow_private_networks: false,
      default_copies: 2,
    },
    cache: {
      dir: '/var/lib/synaps3/cache',
      max_size_gb: 100,
      eviction_policy: 'lru',
    },
    worker: {
      upload: {
        concurrency: 4,
        poll_interval: '5s',
        max_retries: 5,
      },
      evictor: {
        concurrency: 2,
        poll_interval: '1m',
        max_retries: 3,
      },
      storage_cleanup: {
        concurrency: 2,
        poll_interval: '1m',
        max_retries: 5,
      },
    },
    logging: {
      level: 'info',
      format: 'text',
      s3_access: {
        enabled: true,
        level: 'info',
      },
    },
  }
}

test('confirmation matching is exact', () => {
  assert.equal(confirmationMatches('access-key', 'access-key'), true)
  assert.equal(confirmationMatches(' access-key', 'access-key'), false)
  assert.equal(confirmationMatches('ACCESS-KEY', 'access-key'), false)
  assert.equal(confirmationMatches('', 'access-key'), false)
})

test('settings risk collection ignores env-managed fields and ordinary logging edits', () => {
  const initial = baseConfig()
  const next = baseConfig()
  next.filecoin.network = 'mainnet'
  next.logging.level = 'debug'

  const changes = collectSettingsRiskChanges(
    initial,
    next,
    { 'filecoin.network': 'SYNAPS3_FILECOIN_NETWORK' },
    metadata
  )

  assert.deepEqual(changes, [])
})

test('settings risk collection classifies high-risk security boundary changes', () => {
  const initial = baseConfig()
  const next = baseConfig()
  next.filecoin.network = 'mainnet'
  next.filecoin.allow_private_networks = true

  const changes = collectSettingsRiskChanges(initial, next, {}, metadata)

  assert.deepEqual(
    changes.map((change) => [change.field, change.severity, classifySettingsRisk(change)]),
    [
      ['filecoin.network', 'high', 'strong'],
      ['filecoin.allow_private_networks', 'high', 'strong'],
    ]
  )
  assert.equal(settingsRiskNeedsStrongConfirmation(changes), true)
})

test('settings risk collection reports review-level infrastructure changes', () => {
  const initial = baseConfig()
  const next = baseConfig()
  next.server.port = ':9443'
  next.server.tls.enabled = false
  next.server.tls.cert_file = '/certs/next.pem'
  next.server.tls.key_file = '/certs/next.key'
  next.s3.region = 'us-west-2'
  next.filecoin.rpc_url = 'https://rpc.example.invalid'
  next.filecoin.default_copies = 3
  next.cache.dir = '/data/cache'
  next.cache.max_size_gb = 50
  next.cache.eviction_policy = 'manual'
  next.worker.upload.concurrency = 8
  next.worker.upload.max_retries = 7
  next.worker.upload.poll_interval = '1s'
  next.worker.evictor.concurrency = 3
  next.worker.evictor.max_retries = 4
  next.worker.evictor.poll_interval = '30s'

  const changes = collectSettingsRiskChanges(initial, next, {}, metadata)

  assert.deepEqual(
    changes.map((change) => [change.field, change.label, change.from, change.to, change.severity]),
    [
      ['server.port', 'S3 Port', ':8080', ':9443', 'medium'],
      ['server.tls.enabled', 'TLS Enabled', 'true', 'false', 'medium'],
      ['server.tls.cert_file', 'TLS Cert File', '/certs/current.pem', '/certs/next.pem', 'medium'],
      ['server.tls.key_file', 'TLS Key File', '/certs/current.key', '/certs/next.key', 'medium'],
      ['s3.region', 'Region', 'us-east-1', 'us-west-2', 'medium'],
      [
        'filecoin.rpc_url',
        'Filecoin RPC URL',
        'https://api.calibration.node.glif.io/rpc/v1',
        'https://rpc.example.invalid',
        'medium',
      ],
      ['filecoin.default_copies', 'Default Copies', '2', '3', 'medium'],
      ['cache.dir', 'Cache Directory', '/var/lib/synaps3/cache', '/data/cache', 'medium'],
      ['cache.max_size_gb', 'Cache Max Size', '100', '50', 'medium'],
      ['cache.eviction_policy', 'Cache Eviction Policy', 'lru', 'manual', 'medium'],
      ['worker.upload.concurrency', 'Upload Concurrency', '4', '8', 'medium'],
      ['worker.upload.poll_interval', 'Upload Poll Interval', '5s', '1s', 'medium'],
      ['worker.upload.max_retries', 'Upload Max Retries', '5', '7', 'medium'],
      ['worker.evictor.concurrency', 'Evictor Concurrency', '2', '3', 'medium'],
      ['worker.evictor.poll_interval', 'Evictor Poll Interval', '1m', '30s', 'medium'],
      ['worker.evictor.max_retries', 'Evictor Max Retries', '3', '4', 'medium'],
    ]
  )
  assert.deepEqual([...new Set(changes.map(classifySettingsRisk))], ['review'])
  assert.equal(settingsRiskNeedsStrongConfirmation(changes), false)
})
