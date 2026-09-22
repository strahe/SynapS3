import assert from 'node:assert/strict'
import test from 'node:test'

import type { ObservabilityProviderObservation } from '../src/api/client.ts'
import {
  canTestProviderUploadSpeed,
  providerUploadSampleSize,
  providerUploadSpeedLabel,
} from '../src/lib/provider-upload-speed.ts'

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
  assert.equal(
    canTestProviderUploadSpeed({ ...available, upload_speed_test: { state: 'stale', sample_bytes: 32 << 20 } }),
    true
  )
})

test('upload speed labels distinguish results from failed or outdated tests', () => {
  assert.equal(
    providerUploadSpeedLabel({ state: 'succeeded', sample_bytes: 32 << 20, bytes_per_second: 10 << 20 }),
    '10.0 MiB/s'
  )
  assert.equal(
    providerUploadSpeedLabel({ state: 'stale', sample_bytes: 32 << 20 }),
    'Outdated — Service URL no longer matches'
  )
  for (const [failureCode, label] of [
    ['timeout', 'Upload test timed out'],
    ['interrupted', 'Upload test interrupted — test again'],
    ['unavailable', 'Upload test could not run'],
    ['provider_changed', 'Provider no longer ready for this test'],
    ['record_failed', 'Upload speed not recorded'],
    ['upload_failed', 'Upload test failed'],
  ]) {
    assert.equal(
      providerUploadSpeedLabel({ state: 'failed', sample_bytes: 32 << 20, failure_code: failureCode }),
      label
    )
  }
  assert.equal(providerUploadSpeedLabel({ state: 'succeeded', sample_bytes: 32 << 20 }), 'Upload speed not recorded')
})

test('sample size describes the saved test rather than an unstarted test', () => {
  assert.equal(providerUploadSampleSize(), '—')
  assert.equal(providerUploadSampleSize({ state: 'succeeded', sample_bytes: 64 << 20 }), '64 MiB')
  assert.equal(providerUploadSampleSize({ state: 'testing', sample_bytes: 3 << 19 }), '1.5 MiB')
})
