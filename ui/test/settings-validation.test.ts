import assert from 'node:assert/strict'
import test from 'node:test'

import type { SettingsFieldError } from '../src/api/client.ts'
import {
  activeSettingsValidationErrors,
  settingsDraftValidationEnabled,
  settingsValidationPayloadKey,
} from '../src/lib/settings-validation.ts'

test('settings validation draft errors replace matching base errors', () => {
  const baseErrors: SettingsFieldError[] = [{ field: 'server.port', message: 'port must be between 1 and 65535' }]
  const payloadKey = settingsValidationPayloadKey({ server: { port: ':8080' } })

  const errors = activeSettingsValidationErrors(baseErrors, { payloadKey, validation_errors: [] }, payloadKey)

  assert.deepEqual(errors, [])
})

test('settings validation ignores stale draft errors', () => {
  const baseErrors: SettingsFieldError[] = [{ field: 'server.port', message: 'port must be between 1 and 65535' }]
  const stalePayloadKey = settingsValidationPayloadKey({ server: { port: ':8080' } })
  const currentPayloadKey = settingsValidationPayloadKey({ server: { port: ':80801' } })

  const errors = activeSettingsValidationErrors(
    baseErrors,
    {
      payloadKey: stalePayloadKey,
      validation_errors: [{ field: 'server.max_requests', message: 'must not exceed server.max_connections' }],
    },
    currentPayloadKey
  )

  assert.deepEqual(errors, baseErrors)
})

test('settings draft validation is skipped until the form is dirty', () => {
  const payload = { server: { port: ':8080' } }
  const payloadKey = settingsValidationPayloadKey(payload)

  assert.equal(settingsDraftValidationEnabled({ writable: true, formDirty: false, payload, payloadKey }), false)
  assert.equal(settingsDraftValidationEnabled({ writable: true, formDirty: true, payload, payloadKey }), true)
  assert.equal(settingsDraftValidationEnabled({ writable: false, formDirty: true, payload, payloadKey }), false)
  assert.equal(settingsDraftValidationEnabled({ writable: true, formDirty: true, payload: null, payloadKey }), false)
  assert.equal(settingsDraftValidationEnabled({ writable: true, formDirty: true, payload, payloadKey: null }), false)
})
