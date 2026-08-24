import assert from 'node:assert/strict'
import test from 'node:test'
import type { ProviderReplacementProgress } from '../src/api/client.ts'
import { providerReplacementProgressView } from '../src/lib/provider-replacement-progress.ts'

function progress(overrides: Partial<ProviderReplacementProgress> = {}): ProviderReplacementProgress {
  return {
    scope: 'provider_replacement',
    phase: 'migrate',
    seeding_complete: true,
    items_total: 10,
    items_processed: 4,
    items_copied: 3,
    items_no_longer_needed: 1,
    items_pending: 2,
    items_active: 1,
    items_retrying: 1,
    items_waiting_source: 1,
    items_failed: 1,
    percent: 40,
    ...overrides,
  }
}

test('discovering progress is indeterminate and names discovered work', () => {
  const view = providerReplacementProgressView(
    progress({ seeding_complete: false, items_total: 7, items_processed: 2, percent: undefined })
  )
  assert.equal(view.indeterminate, true)
  assert.equal(view.value, null)
  assert.equal(view.summary, 'Discovering stored content · 7 found · 2 processed')
})

test('prepare phase does not describe undiscovered migration work', () => {
  const view = providerReplacementProgressView(
    progress({ phase: 'prepare', seeding_complete: false, items_total: 0, items_processed: 0, percent: undefined }),
    'Waiting for wallet funds'
  )
  assert.equal(view.indeterminate, true)
  assert.equal(view.value, null)
  assert.equal(view.summary, 'Preparing the new storage service')
  assert.equal(view.activity, 'Waiting for wallet funds')
})

test('determinate progress exposes active recovery states without raw errors', () => {
  const view = providerReplacementProgressView(progress())
  assert.equal(view.indeterminate, false)
  assert.equal(view.value, 40)
  assert.equal(view.summary, '4 of 10 processed · 40%')
  assert.equal(view.activity, '1 copying · 1 retrying · 1 waiting for source · 1 need attention')
})

test('completed progress separates copied and no-longer-needed items at 100 percent', () => {
  const view = providerReplacementProgressView(
    progress({
      phase: 'retire',
      items_processed: 10,
      items_copied: 8,
      items_no_longer_needed: 2,
      items_pending: 0,
      items_active: 0,
      items_retrying: 0,
      items_waiting_source: 0,
      items_failed: 0,
      percent: 100,
    })
  )
  assert.equal(view.summary, '10 of 10 processed · 100%')
  assert.equal(view.activity, '8 copied · 2 no longer needed')
})
