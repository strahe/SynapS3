import assert from 'node:assert/strict'
import test from 'node:test'

import type { ObservabilityProviderObservation } from '../src/api/client.ts'
import {
  canTestProviderUploadSpeed,
  providerUploadSampleSize,
  providerUploadSpeedLabel,
  providerUploadSpeedShortLabel,
  providerUploadSpeedUnavailableReason,
} from '../src/lib/provider-upload-speed.ts'

const available: ObservabilityProviderObservation = {
  upload_speed_testable: true,
  facts: { provider_id: '101', active: true, has_pdp: true, service_url: 'https://provider.example' },
  signal: { status: 'available', level: 'ok', reason_codes: [], freshness: { stale: false, warnings: [] } },
}

test('upload speed actions follow server eligibility and explain unavailable or unknown providers', () => {
  assert.equal(canTestProviderUploadSpeed(available), true)
  assert.equal(
    canTestProviderUploadSpeed({ ...available, upload_speed_test: { state: 'testing', sample_bytes: 32 << 20 } }),
    false
  )
  for (const reason of [
    'Health information is out of date. Refresh this provider.',
    'The provider service URL changed. Refresh this provider.',
    'This provider is inactive.',
    'This provider does not support PDP.',
    'This provider has no service URL.',
  ]) {
    const provider = { ...available, upload_speed_testable: false, upload_speed_unavailable_reason: reason }
    assert.equal(canTestProviderUploadSpeed(provider), false)
    assert.equal(providerUploadSpeedUnavailableReason(provider), reason)
  }
  const unknown = { ...available, upload_speed_testable: undefined }
  assert.equal(canTestProviderUploadSpeed(unknown), false)
  assert.equal(canTestProviderUploadSpeed(), false)
  assert.equal(
    providerUploadSpeedUnavailableReason(unknown),
    'Provider availability could not be checked. Refresh this provider.'
  )
  assert.equal(providerUploadSpeedUnavailableReason(available), undefined)
  assert.equal(
    canTestProviderUploadSpeed({ ...available, upload_speed_test: { state: 'stale', sample_bytes: 32 << 20 } }),
    true
  )
  const unavailableForReplacement = {
    ...available,
    manual_selectable: false,
    manual_block_reason: 'already_serves_bucket',
  }
  assert.equal(canTestProviderUploadSpeed(unavailableForReplacement), true)
  assert.equal(
    providerUploadSpeedUnavailableReason({
      ...available,
      upload_speed_test: { state: 'testing', sample_bytes: 32 << 20 },
    }),
    'A test is already running.'
  )
})

test('upload speed labels distinguish results from failed or outdated tests', () => {
  for (const label of [providerUploadSpeedLabel, providerUploadSpeedShortLabel]) {
    assert.equal(
      label({ state: 'testing', sample_bytes: 32 << 20, bytes_per_second: 10 << 20 }),
      'Testing · last 10.0 MiB/s'
    )
  }
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

test('short upload speed labels keep a result to a few words', () => {
  assert.equal(providerUploadSpeedShortLabel(), 'Not tested')
  assert.equal(providerUploadSpeedShortLabel({ state: 'testing', sample_bytes: 32 << 20 }), 'Testing…')
  assert.equal(
    providerUploadSpeedShortLabel({ state: 'succeeded', sample_bytes: 32 << 20, bytes_per_second: 10 << 20 }),
    '10.0 MiB/s'
  )
  assert.equal(providerUploadSpeedShortLabel({ state: 'succeeded', sample_bytes: 32 << 20 }), 'Not recorded')
  assert.equal(
    providerUploadSpeedShortLabel({ state: 'failed', sample_bytes: 32 << 20, failure_code: 'timeout' }),
    'Test failed'
  )
  assert.equal(providerUploadSpeedShortLabel({ state: 'stale', sample_bytes: 32 << 20 }), 'Outdated')
})

test('sample size describes the saved test rather than an unstarted test', () => {
  assert.equal(providerUploadSampleSize(), '—')
  assert.equal(providerUploadSampleSize({ state: 'succeeded', sample_bytes: 64 << 20 }), '64 MiB')
  assert.equal(providerUploadSampleSize({ state: 'testing', sample_bytes: 3 << 19 }), '1.5 MiB')
})
