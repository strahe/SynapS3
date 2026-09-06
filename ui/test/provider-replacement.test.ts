import assert from 'node:assert/strict'
import test from 'node:test'
import {
  APIError,
  type ProviderReplacement,
  type ReplacementProviderCandidate,
  type StorageDataSetSummary,
} from '../src/api/client.ts'
import {
  activeReplacements,
  dataSetGenerationLabel,
  dataSetGenerationTone,
  dataSetReplaceable,
  providerCandidateDisabledReason,
  providerCandidateLabel,
  providerCandidateMatches,
  providerCandidateNote,
  providerCandidateRegistryLine,
  replacementConfirmationSummary,
  replacementErrorMessage,
  replacementNextStep,
  replacementRetryable,
  replacementStatusLabel,
  replacementStatusTone,
} from '../src/lib/provider-replacement.ts'

function dataSet(overrides: Partial<StorageDataSetSummary> = {}): StorageDataSetSummary {
  return {
    id: 1,
    bucket_id: 1,
    copy_index: 0,
    generation: 1,
    is_current: true,
    replaceable: true,
    provider_id: '101',
    status: 'ready',
    committed_copies: 3,
    readable_copies: 3,
    physical_bytes: 1024 * 1024,
    referenced_version_count: 12,
    current_version_count: 4,
    created_at: '2026-08-01T00:00:00Z',
    updated_at: '2026-08-01T00:00:00Z',
    ...overrides,
  }
}

function replacement(overrides: Partial<ProviderReplacement> = {}): ProviderReplacement {
  return {
    id: 1,
    bucket_name: 'bucket',
    copy_index: 0,
    status: 'migrating',
    selection_mode: 'automatic',
    source: { id: 1, generation: 1, is_current: false, status: 'draining', provider_id: '101', data_set_id: '1001' },
    target: { id: 2, generation: 2, is_current: true, status: 'ready', provider_id: '202', data_set_id: '2002' },
    items_total: 10,
    items_copied: 3,
    progress: {
      scope: 'provider_replacement',
      phase: 'migrate',
      seeding_complete: true,
      items_total: 10,
      items_processed: 3,
      items_copied: 3,
      items_no_longer_needed: 0,
      items_pending: 7,
      items_active: 0,
      items_retrying: 0,
      items_waiting_source: 0,
      items_failed: 0,
      percent: 30,
    },
    last_error: null,
    termination_epoch: null,
    created_at: '2026-08-01T00:00:00Z',
    updated_at: '2026-08-01T00:00:00Z',
    ...overrides,
  }
}

// A replica already being replaced must not offer a second confirmation.
test('a data set in flight is not replaceable again', () => {
  const set = dataSet()
  assert.equal(dataSetReplaceable(set, []), true)
  assert.equal(dataSetReplaceable(set, [replacement({ source: { ...replacement().source, id: set.id } })]), false)
  // A finished replacement releases the slot.
  assert.equal(dataSetReplaceable(set, [replacement({ status: 'completed' })]), true)
})

test('a historical generation is never offered for replacement', () => {
  assert.equal(dataSetReplaceable(dataSet({ is_current: false, replaceable: false }), []), false)
})

test('confirmation uses referenced versions and bytes', () => {
  assert.equal(replacementConfirmationSummary(dataSet()), '12 versions · 1 MB')
})

test('only operator-owned states are retryable', () => {
  assert.equal(replacementRetryable(replacement({ status: 'failed' })), true)
  assert.equal(replacementRetryable(replacement({ status: 'cleanup_attention' })), true)
  assert.equal(replacementRetryable(replacement({ status: 'failed', failure_reason: 'target_in_use' })), false)
  for (const status of ['preparing_target', 'migrating', 'waiting', 'retiring', 'completed', 'superseded'] as const) {
    assert.equal(replacementRetryable(replacement({ status })), false, status)
  }
})

test('a permanent target conflict stays visible and allows a new provider choice', () => {
  const source = dataSet({ id: 1, is_current: true, replaceable: true })
  const stopped = replacement({
    status: 'failed',
    failure_reason: 'target_in_use',
    source: { ...replacement().source, id: 1, is_current: true, status: 'ready' },
    target: { ...replacement().target, id: 2, is_current: false },
  })
  assert.deepEqual(
    activeReplacements([stopped]).map((row) => row.id),
    [stopped.id]
  )
  assert.equal(replacementRetryable(stopped), false)
  assert.equal(dataSetReplaceable(source, [stopped]), true)
})

test('every replacement that still needs something is surfaced', () => {
  assert.deepEqual(activeReplacements([]), [])
  assert.deepEqual(activeReplacements([replacement({ status: 'completed' })]), [])
  assert.deepEqual(activeReplacements([replacement({ status: 'superseded' })]), [])
  assert.deepEqual(
    activeReplacements([
      replacement({ id: 7, status: 'waiting' }),
      replacement({ id: 8, status: 'cleanup_attention' }),
    ]).map((row) => row.id),
    [7, 8]
  )
})

test('states needing attention are visually distinct from progress', () => {
  assert.equal(replacementStatusTone('failed'), 'danger')
  assert.equal(replacementStatusTone('cleanup_attention'), 'danger')
  assert.equal(replacementStatusTone('waiting'), 'warning')
  assert.equal(replacementStatusTone('completed'), 'success')
  assert.equal(replacementStatusTone('migrating'), 'info')
})

test('generation labels describe the slot without internal state names', () => {
  assert.equal(dataSetGenerationLabel(dataSet()), 'Current')
  assert.equal(dataSetGenerationLabel(dataSet({ is_current: false, status: 'draining' })), 'Outgoing')
  assert.equal(dataSetGenerationLabel(dataSet({ is_current: false, status: 'retired' })), 'Retired')
  assert.equal(dataSetGenerationLabel(dataSet({ is_current: false, status: 'ready' })), 'Historical')
})

// The replica column is narrow. A label longer than about "Historical" is cut
// mid-word, which reads as nothing at all.
test('generation labels are short enough for the replica column', () => {
  const statuses = ['ready', 'pending', 'creating', 'failed', 'draining', 'retired']
  for (const status of statuses) {
    for (const isCurrent of [true, false]) {
      const label = dataSetGenerationLabel(dataSet({ is_current: isCurrent, status }))
      assert.ok(label.length <= 10, `${status}/${isCurrent} → ${label} (${label.length} chars)`)
    }
  }
})

// The replacement target has no service yet, so it is not current. Falling
// through to "Historical" pointed at the past for the generation that is
// arriving, and coloured ordinary progress as something to act on.
test('the generation being prepared is not labelled as a past one', () => {
  for (const status of ['pending', 'creating']) {
    const target = dataSet({ is_current: false, status, generation: 2 })
    assert.equal(dataSetGenerationLabel(target), 'Incoming', status)
    assert.equal(dataSetGenerationTone(target), 'info', status)
  }
  // A target whose service was never created is not "historical" either.
  assert.equal(dataSetGenerationLabel(dataSet({ is_current: false, status: 'failed' })), 'Failed')
  // The live generation is unaffected even before its own service settles.
  assert.equal(dataSetGenerationLabel(dataSet({ is_current: true, status: 'creating' })), 'Current')
})

// A typed conflict must tell the operator what to do, not echo a status code.
test('errors explain what to do next', () => {
  assert.equal(
    replacementErrorMessage(new APIError('conflict', 409, 'replacement_target_in_use')),
    'That provider already stores a replica of this bucket. Choose a different one.'
  )
  assert.equal(
    replacementErrorMessage(new APIError('unavailable', 503)),
    'Filecoin storage is unavailable right now. Try again once it recovers.'
  )
  assert.equal(replacementErrorMessage(new APIError('boom', 400)), 'boom')
  assert.equal(replacementErrorMessage(new Error('network down')), 'network down')
  assert.equal(replacementErrorMessage('nope'), 'Could not complete the request.')
})

// #314 says choosing a different target after a failure needs a new
// confirmation. Treating a stopped replacement as owning the replica left the
// operator with nothing but Retry on a provider that will not work.
test('a stopped replacement does not block choosing a different provider', () => {
  const set = dataSet()
  const owned = { ...replacement().source, id: set.id }
  for (const status of ['preparing_target', 'migrating', 'waiting', 'retiring'] as const) {
    assert.equal(dataSetReplaceable(set, [replacement({ status, source: owned })]), false, status)
  }
  for (const status of ['failed', 'cleanup_attention'] as const) {
    assert.equal(dataSetReplaceable(set, [replacement({ status, source: owned })]), true, status)
  }
})

// The two attention states have different consequences: one means the data has
// not moved, the other means it has and the old provider is still being paid
// for. Calling both "needs your attention" hid that difference.
test('failure and cleanup attention say different things', () => {
  const failed = replacementStatusLabel('failed')
  const attention = replacementStatusLabel('cleanup_attention')
  assert.notEqual(failed, attention)
  assert.match(replacementNextStep(replacement({ status: 'failed' })) ?? '', /finished/)
  assert.match(replacementNextStep(replacement({ status: 'cleanup_attention' })) ?? '', /still being paid for/)
})

// The card shows what happened and what to do; the recorded error is developer
// diagnostics and stays behind a detail view.
test('the next step never echoes the recorded error', () => {
  const row = replacement({
    status: 'failed',
    last_error: 'copy replacement item: pull: provider timed out (max retries reached)',
  })
  const step = replacementNextStep(row) ?? ''
  assert.ok(step.length > 0)
  assert.ok(!step.includes('max retries reached'))
  assert.equal(replacementNextStep(replacement({ status: 'migrating' })), null)
})

// The API refuses to replace a data set another replacement is still migrating
// into, because the original source still owes it the data it has not copied.
// Offering the action there produced a guaranteed 409.
test('the target of a stopped replacement is not offered for replacement', () => {
  const target = dataSet({ id: 2, generation: 2 })
  const row = replacement({
    status: 'failed',
    source: { ...replacement().source, id: 1, is_current: false },
    target: { ...replacement().target, id: 2 },
  })
  assert.equal(dataSetReplaceable(target, [row]), false)
  // The replica being replaced keeps the re-target path #314 requires.
  const source = dataSet({ id: 1 })
  assert.equal(dataSetReplaceable(source, [row]), true)
})

// Changing provider is only open while the old one still holds the replica.
// Once the new one has taken it over, the copy has to be finished, not restarted
// somewhere else.
test('the next step offers a different provider only when one can be chosen', () => {
  const beforeHandover = replacement({ status: 'failed', source: { ...replacement().source, is_current: true } })
  assert.match(replacementNextStep(beforeHandover) ?? '', /different provider/)
  const afterHandover = replacement({ status: 'failed', source: { ...replacement().source, is_current: false } })
  const step = replacementNextStep(afterHandover) ?? ''
  assert.match(step, /Retry/)
  assert.ok(!step.includes('different provider'))
})

// A 5xx body is internal diagnostics. Anything the operator can act on arrives
// as a typed 4xx.
test('server faults do not surface internal text', () => {
  const message = replacementErrorMessage(new APIError('sql: transaction has already been committed', 500))
  assert.ok(!message.includes('sql:'))
  assert.match(message, /server logs/)
})

function candidate(overrides: Partial<ReplacementProviderCandidate> = {}): ReplacementProviderCandidate {
  return { provider_id: '303', eligible: true, previously_used: false, ...overrides }
}

// The API refuses these choices, so the chooser has to say so rather than
// letting the operator find out from a 409.
test('a provider that cannot take the replica says why', () => {
  assert.equal(
    providerCandidateDisabledReason(candidate({ eligible: false, ineligible_reason: 'current_source' })),
    'This is the provider being replaced'
  )
  assert.equal(
    providerCandidateDisabledReason(candidate({ eligible: false, ineligible_reason: 'already_serves_bucket' })),
    'Already stores a replica of this bucket'
  )
  // An unrecognised reason still has to read as a reason, not as a code.
  assert.equal(
    providerCandidateDisabledReason(candidate({ eligible: false, ineligible_reason: 'something_new' })),
    'Cannot take this replica'
  )
  assert.equal(providerCandidateDisabledReason(candidate()), null)
})

test('a provider reads as its name and falls back to its registry ID', () => {
  assert.equal(
    providerCandidateLabel(candidate({ provider_identity: { registry_provider_id: '303', name: 'Acme' } })),
    'Acme'
  )
  assert.equal(providerCandidateLabel(candidate()), 'Registry 303')
  // A blank name is not a name.
  assert.equal(
    providerCandidateLabel(candidate({ provider_identity: { registry_provider_id: '303', name: '  ' } })),
    'Registry 303'
  )
})

// Automatic selection avoids a provider the bucket moved away from; choosing it
// manually stays allowed, so the note explains rather than blocks.
test('a previously used provider is choosable but flagged', () => {
  assert.equal(providerCandidateNote(candidate({ previously_used: true })), 'Used by this bucket before')
  assert.equal(providerCandidateNote(candidate()), null)
  assert.equal(providerCandidateNote(candidate({ eligible: false, previously_used: true })), null)
})

// Operators search by whichever they have to hand.
test('providers are searchable by name and by registry ID', () => {
  const acme = candidate({
    provider_id: '303',
    provider_identity: { registry_provider_id: '303', name: 'Acme Storage', location: 'Berlin' },
  })
  assert.equal(providerCandidateMatches(acme, 'acme'), true)
  assert.equal(providerCandidateMatches(acme, '303'), true)
  assert.equal(providerCandidateMatches(acme, 'berlin'), true)
  assert.equal(providerCandidateMatches(acme, ''), true)
  assert.equal(providerCandidateMatches(acme, 'zzz'), false)
  // A provider with no identity is still findable by ID.
  assert.equal(providerCandidateMatches(candidate({ provider_id: '404' }), '404'), true)
  assert.equal(providerCandidateMatches(candidate({ provider_id: '404' }), 'acme'), false)
})

// A bare number under the name reads as an index or a rank. The dashboard
// writes a provider ID as "Registry N" everywhere else.
test('the id line under a provider name says what the number is', () => {
  assert.equal(
    providerCandidateRegistryLine(
      candidate({ provider_id: '5', provider_identity: { registry_provider_id: '5', name: 'Mongo2Stor' } })
    ),
    'Registry 5'
  )
  // With no name the heading is already "Registry 5"; repeating it says nothing.
  assert.equal(providerCandidateRegistryLine(candidate({ provider_id: '5' })), null)
})
