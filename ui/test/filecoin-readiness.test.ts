import assert from 'node:assert/strict'
import test from 'node:test'

import type { FilecoinReadinessCheck, FilecoinReadinessData } from '../src/api/client.ts'
import {
  buildFilecoinPreflightPayload,
  filecoinReadinessCheckTitle,
  filecoinReadinessStatusDescription,
  filecoinReadinessStatusLabel,
  filecoinReadinessStatusTone,
  filecoinReadinessSummary,
  importantFilecoinReadinessChecks,
  readDismissedFilecoinReadinessChecks,
  writeDismissedFilecoinReadinessCheck,
} from '../src/lib/filecoin-readiness.ts'

test('filecoin readiness status labels and tones are stable', () => {
  for (const [status, label, tone] of [
    ['ready', 'Ready', 'success'],
    ['warning', 'Warning', 'warning'],
    ['blocked', 'Blocked', 'danger'],
    ['unknown', 'Unknown', 'warning'],
  ] as const) {
    assert.equal(filecoinReadinessStatusLabel(status), label)
    assert.equal(filecoinReadinessStatusTone(status), tone)
    assert.notEqual(filecoinReadinessStatusDescription(status), '')
  }
})

test('filecoin readiness check titles hide implementation ids', () => {
  assert.equal(filecoinReadinessCheckTitle('private_networks'), 'Private network access')
  assert.equal(filecoinReadinessCheckTitle('payment_runway'), 'Payment runway')
  assert.equal(filecoinReadinessCheckTitle('wallet_fil_gas'), 'FIL gas balance')
  assert.equal(filecoinReadinessCheckTitle('custom_check'), 'custom check')
})

test('important filecoin readiness checks exclude ready checks and sort by impact', () => {
  const checks: FilecoinReadinessCheck[] = [
    { id: 'payment_runway', status: 'warning', message: 'Payment account runway is under 30 days.' },
    { id: 'network_match', status: 'ready', message: 'RPC network matches settings.' },
    { id: 'storage_cost', status: 'unknown', message: 'Storage cost estimate could not be calculated.' },
    { id: 'wallet_fil_gas', status: 'blocked', message: 'FIL gas balance is empty.' },
  ]

  assert.deepEqual(
    importantFilecoinReadinessChecks(checks).map((check) => check.id),
    ['wallet_fil_gas', 'storage_cost', 'payment_runway']
  )
})

test('dismissed filecoin readiness checks are excluded locally', () => {
  const storage = new MemoryStorage()
  writeDismissedFilecoinReadinessCheck('wallet_fil_gas', true, storage)
  assert.deepEqual([...readDismissedFilecoinReadinessChecks(storage)], [])

  writeDismissedFilecoinReadinessCheck('private_networks', true, storage)
  const dismissed = readDismissedFilecoinReadinessChecks(storage)
  assert.deepEqual([...dismissed], ['private_networks'])

  const checks: FilecoinReadinessCheck[] = [
    { id: 'private_networks', status: 'warning', message: 'Private network downloads are allowed.' },
    { id: 'payment_runway', status: 'warning', message: 'Payment account runway is under 30 days.' },
  ]
  assert.deepEqual(
    importantFilecoinReadinessChecks(checks, dismissed).map((check) => check.id),
    ['payment_runway']
  )

  writeDismissedFilecoinReadinessCheck('private_networks', false, storage)
  assert.deepEqual([...readDismissedFilecoinReadinessChecks(storage)], [])
})

test('filecoin readiness summary uses the first non-ready check', () => {
  const data: FilecoinReadinessData = {
    status: 'warning',
    mode: 'runtime',
    checked_at: '2026-05-16T12:00:00Z',
    checks: [
      { id: 'network_match', status: 'ready', message: 'RPC network matches settings.' },
      {
        id: 'payment_runway',
        status: 'warning',
        message: 'Payment account runway is under 30 days.',
        action: 'Top up the payment account to maintain at least 30 days of runway.',
      },
    ],
    partial_errors: {
      storage_cost: 'RPC call failed',
    },
  }

  assert.equal(filecoinReadinessSummary(data), 'Payment account runway is under 30 days.')
  assert.equal(filecoinReadinessSummary(undefined), 'Readiness has not been checked.')
  assert.equal(
    filecoinReadinessSummary({
      status: 'ready',
      mode: 'draft',
      checked_at: '2026-05-16T12:00:00Z',
      checks: [{ id: 'network_match', status: 'ready', message: 'RPC network matches settings.' }],
    }),
    'Filecoin readiness checks passed.'
  )
})

test('preflight payload includes editable filecoin fields and excludes private key or env-managed fields', () => {
  const payload = buildFilecoinPreflightPayload({
    network: 'calibration',
    rpc_url: 'https://rpc.example.invalid',
    source: 'synaps3',
    with_cdn: true,
    allow_private_networks: false,
    default_copies: 2,
    private_key: 'raw-private-key',
    ignored: 'value',
  } as Record<string, unknown>)

  assert.deepEqual(payload, {
    filecoin: {
      network: 'calibration',
      rpc_url: 'https://rpc.example.invalid',
      source: 'synaps3',
      with_cdn: true,
      allow_private_networks: false,
      default_copies: 2,
    },
  })
  assert.equal('private_key' in payload.filecoin, false)

  const envManagedPayload = buildFilecoinPreflightPayload(
    {
      network: 'calibration',
      rpc_url: 'https://env.example.invalid',
      source: 'synaps3',
      with_cdn: true,
      allow_private_networks: false,
      default_copies: 2,
    },
    {
      'filecoin.rpc_url': 'SYNAPS3_FILECOIN_RPC_URL',
      'filecoin.default_copies': '',
    }
  )
  assert.deepEqual(envManagedPayload, {
    filecoin: {
      network: 'calibration',
      source: 'synaps3',
      with_cdn: true,
      allow_private_networks: false,
    },
  })
})

class MemoryStorage {
  private readonly items = new Map<string, string>()

  getItem(key: string) {
    return this.items.get(key) ?? null
  }

  removeItem(key: string) {
    this.items.delete(key)
  }

  setItem(key: string, value: string) {
    this.items.set(key, value)
  }
}
