import type { ObservabilityProviderObservation, ProviderUploadSpeedTest } from '../api/client.ts'

export function canTestProviderUploadSpeed(provider?: ObservabilityProviderObservation) {
  return Boolean(
    provider?.signal.status === 'available' &&
      !provider.signal.freshness.stale &&
      provider.facts.active &&
      provider.facts.has_pdp &&
      provider.facts.service_url &&
      provider.upload_speed_test?.state !== 'testing'
  )
}

export function providerUploadSpeedLabel(test?: ProviderUploadSpeedTest): string {
  if (!test) return 'Not tested'
  switch (test.state) {
    case 'testing':
      return 'Testing upload speed…'
    case 'stale':
      return 'Outdated — Service URL no longer matches'
    case 'failed':
      switch (test.failure_code) {
        case 'timeout':
          return 'Upload test timed out'
        case 'interrupted':
          return 'Upload test interrupted — test again'
        case 'unavailable':
          return 'Upload test could not run'
        case 'provider_changed':
          return 'Provider no longer ready for this test'
        case 'record_failed':
          return 'Upload speed not recorded'
        default:
          return 'Upload test failed'
      }
    case 'succeeded':
      if (!test.bytes_per_second) return 'Upload speed not recorded'
      return `${(test.bytes_per_second / (1024 * 1024)).toFixed(1)} MiB/s`
  }
}

export function providerUploadSampleSize(test?: ProviderUploadSpeedTest): string {
  if (!test) return '—'
  return `${Number((test.sample_bytes / (1024 * 1024)).toFixed(1))} MiB`
}
