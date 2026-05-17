import assert from 'node:assert/strict'
import test from 'node:test'

import { filecoinRuntimeReadinessEnabled, rootContentKind, rootUsesSetupShell } from '../src/routes/-root-content.ts'

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
