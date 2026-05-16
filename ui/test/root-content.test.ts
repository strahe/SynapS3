import assert from 'node:assert/strict'
import test from 'node:test'

import { filecoinRuntimeReadinessEnabled, rootContentKind } from '../src/routes/-root-content.ts'

test('root content renders outlet while settings are loading', () => {
  assert.equal(rootContentKind(undefined, '/wallet'), 'outlet')
})

test('root content blocks wallet in setup mode', () => {
  assert.equal(rootContentKind({ mode: 'setup' }, '/wallet'), 'setup-required')
})

test('root content allows settings in setup mode', () => {
  assert.equal(rootContentKind({ mode: 'setup' }, '/settings'), 'outlet')
})

test('filecoin runtime readiness only runs in full runtime with valid settings', () => {
  assert.equal(filecoinRuntimeReadinessEnabled(undefined, true), false)
  assert.equal(filecoinRuntimeReadinessEnabled({ mode: 'ready', runtime_available: false }, false), false)
  assert.equal(filecoinRuntimeReadinessEnabled({ mode: 'setup', runtime_available: true }, false), false)
  assert.equal(filecoinRuntimeReadinessEnabled({ mode: 'ready', runtime_available: true }, false), true)
})
