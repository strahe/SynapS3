import { Link } from '@tanstack/react-router'
import { AlertTriangle, BellOff, Info, Loader2 } from 'lucide-react'
import { useState } from 'react'
import { Banner } from '@/components/app/Banner'
import { Button } from '@/components/ui/button'
import { useStartAllUploadSpeedTests, useUploadSpeedSummary } from '@/hooks/queries'
import { providerSpeedNoticeState } from '@/lib/upload-speed-setup'

const dismissStorageKey = 'synaps3.provider-speed-notice.dismissed'

function readDismissed() {
  try {
    return localStorage.getItem(dismissStorageKey) === 'true'
  } catch {
    return false
  }
}

export function ProviderSpeedNotice({ enabled }: { enabled: boolean }) {
  const summary = useUploadSpeedSummary(enabled)
  const startTests = useStartAllUploadSpeedTests()
  const [dismissed, setDismissed] = useState(readDismissed)
  const notice = providerSpeedNoticeState({ enabled, summary: summary.data, dismissed })

  function dismiss() {
    try {
      localStorage.setItem(dismissStorageKey, 'true')
    } catch {
      // Dismissal remains available when browser storage is disabled.
    }
    setDismissed(true)
  }

  if (enabled && summary.isError && !summary.data) {
    return (
      <div className="w-full min-w-0 shrink-0 px-6 pt-6">
        <Banner tone="warning" icon={AlertTriangle} title="Could not check provider speeds">
          <Button variant="outline" size="sm" onClick={() => summary.refetch()} disabled={summary.isFetching}>
            Retry
          </Button>
        </Banner>
      </div>
    )
  }
  if (notice.kind === 'hidden') return null

  return (
    <div className="w-full min-w-0 shrink-0 px-6 pt-6">
      <Banner tone={notice.tone} icon={notice.kind === 'shortage' ? AlertTriangle : Info} title={notice.title}>
        <div className="flex min-w-0 flex-wrap items-center justify-between gap-3">
          <span className="min-w-0 flex-1 break-words">{notice.message}</span>
          <div className="flex shrink-0 flex-wrap items-center gap-2">
            {summary.data?.can_test_remaining && (
              <Button variant="outline" size="sm" onClick={() => startTests.mutate()} disabled={startTests.isPending}>
                {startTests.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
                Test remaining providers
              </Button>
            )}
            <Button variant="outline" size="sm" asChild>
              <Link to="/storage-topology" search={{ tab: 'providers' }}>
                Storage
              </Link>
            </Button>
            {notice.dismissible && (
              <Button variant="outline" size="sm" onClick={dismiss}>
                <BellOff data-icon="inline-start" />
                Dismiss
              </Button>
            )}
          </div>
        </div>
        {startTests.isError && <p role="alert">Could not start provider tests. Try again.</p>}
      </Banner>
    </div>
  )
}
