import assert from 'node:assert/strict'
import test from 'node:test'

import type { FilecoinReadinessData, FilecoinReadinessStatus } from '../src/api/client.ts'
import {
  filecoinRuntimeReadinessEnabled,
  globalFilecoinReadinessAlertState,
  rootContentKind,
  rootUsesSetupShell,
} from '../src/routes/-root-content.ts'

test('root content renders outlet while settings are loading', () => {
  assert.equal(rootContentKind(undefined, '/wallet'), 'outlet')
})

test('root content blocks wallet in setup mode', () => {
  assert.equal(rootContentKind({ mode: 'setup' }, '/wallet'), 'setup-required')
})

test('root content allows settings in setup mode', () => {
  assert.equal(rootContentKind({ mode: 'setup' }, '/settings'), 'outlet')
})

test('root content blocks runtime pages until full runtime is available', () => {
  const settings = { mode: 'ready' as const, runtime_available: false }

  assert.equal(rootContentKind(settings, '/wallet'), 'setup-required')
  assert.equal(rootContentKind(settings, '/settings'), 'outlet')
})

test('root setup shell uses setup mode or explicit unavailable runtime', () => {
  assert.equal(rootUsesSetupShell({ mode: 'setup' }), true)
  assert.equal(rootUsesSetupShell({ mode: 'ready', runtime_available: false }), true)
  assert.equal(rootUsesSetupShell({ mode: 'ready' }), false)
  assert.equal(rootUsesSetupShell({ mode: 'ready', runtime_available: true }), false)
})

test('filecoin runtime readiness only runs in full runtime with valid settings', () => {
  assert.equal(filecoinRuntimeReadinessEnabled(undefined, true), false)
  assert.equal(filecoinRuntimeReadinessEnabled({ mode: 'ready', runtime_available: true }, true), false)
  assert.equal(filecoinRuntimeReadinessEnabled({ mode: 'ready', runtime_available: false }, false), false)
  assert.equal(filecoinRuntimeReadinessEnabled({ mode: 'setup', runtime_available: true }, false), false)
  assert.equal(filecoinRuntimeReadinessEnabled({ mode: 'ready', runtime_available: true }, false), true)
  assert.equal(filecoinRuntimeReadinessEnabled({ mode: 'ready' }, false), true)
})

test('global filecoin readiness alert stays quiet for ready and warning states', () => {
  assert.equal(
    globalFilecoinReadinessAlertState({
      enabled: true,
      data: readinessData('ready', [
        { id: 'network_match', status: 'ready', message: 'RPC network matches settings.' },
      ]),
    }).show,
    false
  )
  assert.equal(
    globalFilecoinReadinessAlertState({
      enabled: true,
      data: readinessData('warning', [
        { id: 'private_networks', status: 'warning', message: 'Private network provider URLs are allowed.' },
      ]),
    }).show,
    false
  )
})

test('global filecoin readiness alert shows blocked checks first', () => {
  const alert = globalFilecoinReadinessAlertState({
    enabled: true,
    data: readinessData('blocked', [
      { id: 'providers', status: 'blocked', message: 'Only 0 approved providers are available.' },
      { id: 'wallet_fil_gas', status: 'blocked', message: 'FIL gas balance is empty.' },
    ]),
  })

  assert.equal(alert.show, true)
  assert.equal(alert.title, 'Filecoin uploads blocked')
  assert.equal(alert.status, 'blocked')
  assert.equal(alert.summary, 'FIL gas balance is empty.')
  assert.equal(alert.primaryCheck?.id, 'wallet_fil_gas')
  assert.deepEqual(
    alert.details?.checks.map((check) => check.id),
    ['wallet_fil_gas']
  )
})

test('global filecoin readiness alert shows unknown checks without blocked checks', () => {
  const alert = globalFilecoinReadinessAlertState({
    enabled: true,
    data: readinessData('warning', [
      { id: 'payment_runway', status: 'warning', message: 'Payment runway is low.' },
      { id: 'payment_account', status: 'unknown', message: 'Payment account could not be checked.' },
    ]),
  })

  assert.equal(alert.show, true)
  assert.equal(alert.title, 'Filecoin readiness unknown')
  assert.equal(alert.status, 'unknown')
  assert.equal(alert.summary, 'Payment account could not be checked.')
  assert.equal(alert.primaryCheck?.id, 'payment_account')
})

test('global filecoin readiness alert ignores provider selection and its dependent checks', () => {
  const alert = globalFilecoinReadinessAlertState({
    enabled: true,
    data: readinessData('blocked', [
      { id: 'providers', status: 'blocked', message: 'Only 0 approved providers are available.' },
      { id: 'storage_cost', status: 'unknown', message: 'Storage cost could not be estimated.' },
      { id: 'payment_funding', status: 'unknown', message: 'Payment funding could not be checked.' },
      { id: 'fwss_approval', status: 'unknown', message: 'FWSS approval could not be checked.' },
    ]),
  })

  assert.equal(alert.show, false)
})

test('global filecoin readiness alert shows query failures and respects disabled state', () => {
  const errorAlert = globalFilecoinReadinessAlertState({
    enabled: true,
    error: new Error('readiness unavailable'),
  })

  assert.equal(errorAlert.show, true)
  assert.equal(errorAlert.title, 'Filecoin readiness could not be checked')
  assert.equal(errorAlert.summary, 'readiness unavailable')

  assert.equal(
    globalFilecoinReadinessAlertState({
      enabled: false,
      data: readinessData('blocked', [
        { id: 'wallet_fil_gas', status: 'blocked', message: 'FIL gas balance is empty.' },
      ]),
    }).show,
    false
  )
})

function readinessData(
  status: FilecoinReadinessStatus,
  checks: FilecoinReadinessData['checks']
): FilecoinReadinessData {
  return {
    status,
    mode: 'runtime',
    checked_at: '2026-05-16T12:00:00Z',
    checks,
  }
}
