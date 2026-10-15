import assert from 'node:assert/strict'
import test from 'node:test'
import type { UploadSpeedSummary } from '../src/api/client.ts'
import { providerSpeedNoticeState } from '../src/lib/upload-speed-setup.ts'

function summary(updates: Partial<UploadSpeedSummary> = {}): UploadSpeedSummary {
  return {
    auto_scope: 'trusted',
    trusted: { total: 3, succeeded: 3, failed: 0, testing: 0, untested: 0 },
    other: { total: 2, succeeded: 0, failed: 0, testing: 0, untested: 2 },
    selectable_providers: 3,
    required_copies: 3,
    ready: true,
    can_provision_default: true,
    can_test_remaining: true,
    ...updates,
  }
}

test('provider notice follows trusted tests through shortage and optional testing', () => {
  const trustedTesting = summary({
    ready: false,
    can_provision_default: false,
    trusted: { total: 3, succeeded: 1, failed: 0, testing: 2, untested: 0 },
  })
  const shortage = summary({ can_provision_default: false, selectable_providers: 1 })
  const failedTests = summary({
    can_provision_default: false,
    selectable_providers: 2,
    can_test_remaining: false,
    trusted: { total: 3, succeeded: 2, failed: 1, testing: 0, untested: 0 },
  })
  const otherTesting = summary({
    can_test_remaining: false,
    other: { total: 2, succeeded: 0, failed: 1, testing: 1, untested: 0 },
  })
  for (const [data, kind, dismissible] of [
    [trustedTesting, 'trusted_testing', false],
    [summary({ ready: false }), 'trusted_testing', false],
    [summary({ trusted: { total: 3, succeeded: 2, failed: 0, testing: 1, untested: 0 } }), 'trusted_testing', false],
    [shortage, 'shortage', false],
    [failedTests, 'shortage', false],
    [summary(), 'remaining', true],
    [otherTesting, 'other_testing', true],
  ] as const) {
    const notice = providerSpeedNoticeState({ enabled: true, summary: data })
    assert.equal(notice.kind, kind)
    if (notice.kind === 'hidden') assert.fail('notice must be visible')
    assert.equal(notice.dismissible, dismissible)
    assert.equal(
      providerSpeedNoticeState({ enabled: true, summary: data, dismissed: true }).kind,
      dismissible ? 'hidden' : kind
    )
  }
  const failureNotice = providerSpeedNoticeState({ enabled: true, summary: failedTests, dismissed: true })
  assert.notEqual(failureNotice.kind, 'hidden')
  if (failureNotice.kind === 'hidden') assert.fail('failed tests require attention')
  assert.match(failureNotice.message, /Retry.*Storage or Tasks/)
  assert.equal(
    providerSpeedNoticeState({ enabled: true, summary: summary({ can_test_remaining: false }) }).kind,
    'hidden'
  )
  assert.equal(providerSpeedNoticeState({ enabled: false, summary: trustedTesting }).kind, 'hidden')
  assert.equal(providerSpeedNoticeState({ enabled: true }).kind, 'hidden')
})
