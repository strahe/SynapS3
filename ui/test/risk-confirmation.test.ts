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
  'server.max_connections': meta('Max Connections'),
  'server.max_requests': meta('Max Requests'),
  's3.region': meta('Region'),
  'filecoin.network': meta('Filecoin Network'),
  'filecoin.rpc_url': meta('Filecoin RPC URL'),
  'filecoin.allow_private_networks': meta('Allow Private Networks'),
  'filecoin.default_copies': meta('Default Copies'),
  'cache.dir': meta('Cache Directory'),
  'cache.max_size_gb': meta('Cache Max Size'),
  'cache.eviction_policy': meta('Cache Eviction Policy'),
  'cache.lru_high_watermark_percent': meta('LRU High Watermark'),
  'cache.lru_low_watermark_percent': meta('LRU Low Watermark'),
  'worker.tasks.concurrency': meta('Background Work Concurrency'),
  'worker.tasks.poll_interval': meta('Background Work Poll Interval'),
  'worker.tasks.lease_duration': meta('Recovery Lease Duration'),
  'worker.tasks.max_retries': meta('Background Work Retry Limit'),
  'worker.tasks.retention': meta('Finished Work Retention'),
  'worker.tasks.provider_mutation_concurrency': meta('Remote Storage Concurrency'),
  'worker.tasks.destructive_mutation_concurrency': meta('Remote Cleanup Concurrency'),
}

function meta(label: string): SettingsFieldMetadata {
  return { label, description: label, editable: true, secret: false }
}

function baseConfig(): SettingsEditableConfig {
  return {
    server: {
      port: ':8080',
      max_connections: 4096,
      max_requests: 512,
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
      with_cdn: false,
      allow_private_networks: false,
      default_copies: 2,
      observability: { interval: '5m0s', timeout: '5s', concurrency: 8 },
    },
    cache: {
      dir: '/var/lib/synaps3/cache',
      max_size_gb: 100,
      eviction_policy: 'lru',
      lru_high_watermark_percent: 90,
      lru_low_watermark_percent: 80,
    },
    worker: {
      tasks: {
        concurrency: 12,
        poll_interval: '5s',
        lease_duration: '5m0s',
        max_retries: 5,
        retention: '168h0m0s',
        provider_mutation_concurrency: 4,
        destructive_mutation_concurrency: 2,
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
  const privateNetworks = changes.find((change) => change.field === 'filecoin.allow_private_networks')
  assert.match(privateNetworks?.reason ?? '', /storage provider operations, retrieval, and diagnostics/i)
  assert.equal(settingsRiskNeedsStrongConfirmation(changes), true)
})

test('settings risk collection reports review-level infrastructure changes', () => {
  const initial = baseConfig()
  const next = baseConfig()
  next.server.port = ':9443'
  next.server.tls.enabled = false
  next.server.tls.cert_file = '/certs/next.pem'
  next.server.tls.key_file = '/certs/next.key'
  next.server.max_connections = 8192
  next.server.max_requests = 1024
  next.s3.region = 'us-west-2'
  next.filecoin.rpc_url = 'https://rpc.example.invalid'
  next.filecoin.default_copies = 3
  next.cache.dir = '/data/cache'
  next.cache.max_size_gb = 50
  next.cache.eviction_policy = 'after_upload'
  next.cache.lru_high_watermark_percent = 85
  next.cache.lru_low_watermark_percent = 70
  next.worker.tasks.concurrency = 16
  next.worker.tasks.poll_interval = '1s'
  next.worker.tasks.lease_duration = '2m0s'
  next.worker.tasks.max_retries = 7
  next.worker.tasks.retention = '336h0m0s'
  next.worker.tasks.provider_mutation_concurrency = 6
  next.worker.tasks.destructive_mutation_concurrency = 3

  const changes = collectSettingsRiskChanges(initial, next, {}, metadata)

  assert.deepEqual(
    changes.map((change) => [change.field, change.label, change.from, change.to, change.severity]),
    [
      ['server.port', 'S3 Port', ':8080', ':9443', 'medium'],
      ['server.tls.enabled', 'TLS Enabled', 'true', 'false', 'medium'],
      ['server.tls.cert_file', 'TLS Cert File', '/certs/current.pem', '/certs/next.pem', 'medium'],
      ['server.tls.key_file', 'TLS Key File', '/certs/current.key', '/certs/next.key', 'medium'],
      ['server.max_connections', 'Max Connections', '4096', '8192', 'medium'],
      ['server.max_requests', 'Max Requests', '512', '1024', 'medium'],
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
      ['cache.eviction_policy', 'Cache Eviction Policy', 'lru', 'after_upload', 'medium'],
      ['cache.lru_high_watermark_percent', 'LRU High Watermark', '90', '85', 'medium'],
      ['cache.lru_low_watermark_percent', 'LRU Low Watermark', '80', '70', 'medium'],
      ['worker.tasks.concurrency', 'Background Work Concurrency', '12', '16', 'medium'],
      ['worker.tasks.poll_interval', 'Background Work Poll Interval', '5s', '1s', 'medium'],
      ['worker.tasks.lease_duration', 'Recovery Lease Duration', '5m0s', '2m0s', 'medium'],
      ['worker.tasks.max_retries', 'Background Work Retry Limit', '5', '7', 'medium'],
      ['worker.tasks.retention', 'Finished Work Retention', '168h0m0s', '336h0m0s', 'medium'],
      ['worker.tasks.provider_mutation_concurrency', 'Remote Storage Concurrency', '4', '6', 'medium'],
      ['worker.tasks.destructive_mutation_concurrency', 'Remote Cleanup Concurrency', '2', '3', 'medium'],
    ]
  )
  assert.deepEqual([...new Set(changes.map(classifySettingsRisk))], ['review'])
  assert.equal(settingsRiskNeedsStrongConfirmation(changes), false)
})

test('settings risk collection ignores lowered server capacity limits', () => {
  const initial = baseConfig()
  const next = baseConfig()
  next.server.max_connections = 2048
  next.server.max_requests = 256

  const changes = collectSettingsRiskChanges(initial, next, {}, metadata)

  assert.deepEqual(changes, [])
})
