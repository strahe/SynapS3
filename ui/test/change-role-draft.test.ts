import assert from 'node:assert/strict'
import test from 'node:test'

import { syncClosedRoleDraft } from '../src/components/settings/change-role-draft.ts'

test('syncClosedRoleDraft uses latest server role while dialog is closed', () => {
  assert.equal(syncClosedRoleDraft(false, 'user', 'admin'), 'admin')
})

test('syncClosedRoleDraft preserves current draft while dialog is open', () => {
  assert.equal(syncClosedRoleDraft(true, 'userplus', 'admin'), 'userplus')
})
