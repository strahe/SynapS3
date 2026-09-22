import assert from 'node:assert/strict'
import test from 'node:test'

import type { ObservabilityProviderObservation } from '../src/api/client.ts'
import { canTestProviderUploadSpeed, providerUploadSpeedLabel } from '../src/lib/provider-upload-speed.ts'

const available: ObservabilityProviderObservation = {
  facts: { provider_id: '101', active: true, has_pdp: true, service_url: 'https://provider.example' },
  signal: { status: 'available', level: 'ok', reason_codes: [], freshness: { stale: false, warnings: [] } },
}

test('upload speed action requires a fresh available provider and no active test', () => {
  assert.equal(canTestProviderUploadSpeed(available), true)
  assert.equal(
    canTestProviderUploadSpeed({ ...available, upload_speed_test: { state: 'testing', sample_bytes: 32 << 20 } }),
    false
  )
  assert.equal(
    canTestProviderUploadSpeed({
      ...available,
      signal: { ...available.signal, freshness: { stale: true, warnings: [] } },
    }),
    false
  )
  assert.equal(canTestProviderUploadSpeed({ ...available, facts: { ...available.facts, has_pdp: false } }), false)
})

test('upload speed labels show only a current successful measurement as a number', () => {
  assert.equal(
    providerUploadSpeedLabel({ state: 'succeeded', sample_bytes: 32 << 20, bytes_per_second: 10 << 20 }),
    '10.0 MiB/s'
  )
  assert.match(providerUploadSpeedLabel({ state: 'stale', sample_bytes: 32 << 20 }), /test again/i)
  assert.equal(
    providerUploadSpeedLabel({ state: 'failed', sample_bytes: 32 << 20, failure_code: 'timeout' }),
    'Upload test timed out'
  )
})
