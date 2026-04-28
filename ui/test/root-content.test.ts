import assert from 'node:assert/strict'
import test from 'node:test'

import { rootContentKind } from '../src/routes/-root-content.ts'

test('root content renders outlet while settings are loading', () => {
  assert.equal(rootContentKind(undefined, '/wallet'), 'outlet')
})

test('root content blocks wallet in setup mode', () => {
  assert.equal(rootContentKind({ mode: 'setup' }, '/wallet'), 'setup-required')
})

test('root content allows settings in setup mode', () => {
  assert.equal(rootContentKind({ mode: 'setup' }, '/settings'), 'outlet')
})
