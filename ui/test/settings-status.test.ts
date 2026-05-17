import assert from 'node:assert/strict'
import test from 'node:test'

import {
  settingsRuntimeRestartBannerVisible,
  settingsSavedBannerVisible,
  settingsSetupBannerVisible,
} from '../src/lib/settings-status.ts'

test('settings setup banner is visible when runtime availability is missing', () => {
  assert.equal(settingsSetupBannerVisible({ mode: 'setup' }), true)
})

test('settings runtime restart banner is visible when settings are ready but runtime is unavailable', () => {
  assert.equal(settingsRuntimeRestartBannerVisible({ mode: 'ready', runtime_available: false }), true)
})

test('settings saved banner is hidden while full runtime is unavailable', () => {
  assert.equal(settingsSavedBannerVisible({ restart_required: true, runtime_available: false }), false)
})

test('settings saved banner remains visible in full runtime', () => {
  assert.equal(settingsSavedBannerVisible({ restart_required: true, runtime_available: true }), true)
})

test('settings saved banner is hidden when settings do not require restart', () => {
  assert.equal(settingsSavedBannerVisible({ restart_required: false, runtime_available: true }), false)
})
