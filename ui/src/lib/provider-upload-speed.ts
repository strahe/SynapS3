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
      return 'Previous service address — test again when available'
    case 'failed':
      return test.failure_code === 'timeout' ? 'Upload test timed out' : 'Upload test failed'
    case 'succeeded':
      if (!test.bytes_per_second) return 'Upload test unavailable'
      return `${(test.bytes_per_second / (1024 * 1024)).toFixed(1)} MiB/s`
  }
}
