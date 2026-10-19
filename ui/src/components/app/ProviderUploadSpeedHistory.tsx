import type { ProviderUploadSpeedTest } from '@/api/client'
import { providerUploadSpeedLastSuccessLabel } from '@/lib/provider-upload-speed'
import { timeAgo } from '@/lib/utils'

export function ProviderUploadSpeedHistory({ test }: { test?: ProviderUploadSpeedTest }) {
  const label = providerUploadSpeedLastSuccessLabel(test)
  if (!label || !test?.last_success) return null
  const testedAt = test.last_success.tested_at
  return (
    <span className="block text-xs text-muted-foreground">
      {label} ·{' '}
      <time dateTime={testedAt} title={new Date(testedAt).toLocaleString()}>
        {timeAgo(testedAt)}
      </time>{' '}
      (reference)
    </span>
  )
}
