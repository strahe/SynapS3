import type { UploadSpeedSummary } from '../api/client.ts'

type VisibleNotice = {
  kind: 'trusted_testing' | 'shortage' | 'remaining' | 'other_testing'
  title: string
  message: string
  tone: 'info' | 'warning'
  dismissible: boolean
}

export type ProviderSpeedNoticeState = { kind: 'hidden' } | VisibleNotice

export function providerSpeedNoticeState({
  enabled,
  summary,
  dismissed = false,
}: {
  enabled: boolean
  summary?: UploadSpeedSummary
  dismissed?: boolean
}): ProviderSpeedNoticeState {
  if (!enabled || !summary) return { kind: 'hidden' }
  if (!summary.ready || summary.trusted.testing > 0) {
    return {
      kind: 'trusted_testing',
      title: 'Testing trusted providers',
      message: `${summary.trusted.succeeded + summary.trusted.failed} of ${summary.trusted.total} trusted provider tests finished.`,
      tone: 'info',
      dismissible: false,
    }
  }
  if (!summary.can_provision_default) {
    return {
      kind: 'shortage',
      title: 'Providers need attention',
      message:
        summary.trusted.failed + summary.other.failed > 0
          ? 'Retry failed speed tests in Storage or Tasks.'
          : 'The default replica requirements could not be confirmed.',
      tone: 'warning',
      dismissible: false,
    }
  }
  if (dismissed) return { kind: 'hidden' }
  if (summary.can_test_remaining) {
    return {
      kind: 'remaining',
      title: 'More providers available',
      message: 'Test the remaining providers to expand the selection pool.',
      tone: 'info',
      dismissible: true,
    }
  }
  if (summary.other.testing > 0) {
    return {
      kind: 'other_testing',
      title: 'Testing providers',
      message: `${summary.other.succeeded + summary.other.failed} of ${summary.other.total} provider tests finished.`,
      tone: 'info',
      dismissible: true,
    }
  }
  return { kind: 'hidden' }
}
