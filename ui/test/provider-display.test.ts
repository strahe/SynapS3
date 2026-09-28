import assert from 'node:assert/strict'
import test from 'node:test'

import { providerDisplayName, providerLocationLabel } from '../src/lib/provider-display.ts'

test('a provider without a name reads as its registry ID', () => {
  assert.equal(providerDisplayName('4', 'infrafolio-calib'), 'infrafolio-calib')
  assert.equal(providerDisplayName('4', '  '), 'Registry 4')
  assert.equal(providerDisplayName('4'), 'Registry 4')
})

test('structured registry locations read as places and free text stays as declared', () => {
  assert.equal(providerLocationLabel('C=US;ST=Texas;L=Austin'), 'Austin, Texas, US')
  assert.equal(providerLocationLabel('C=US;ST=Texas;L=Austin', { compact: true }), 'Austin, US')
  assert.equal(providerLocationLabel('c=DE, st=Bavaria', { compact: true }), 'Bavaria, DE')
  assert.equal(providerLocationLabel('C=HK;ST=HongKong;L=Hong Kong'), 'Hong Kong, HK')
  assert.equal(providerLocationLabel('C=RU'), 'RU')
  assert.equal(providerLocationLabel('CN=node-1'), 'CN=node-1')
  assert.equal(providerLocationLabel('laptop, macOS arm64'), 'laptop, macOS arm64')
  assert.equal(providerLocationLabel('  earth '), 'earth')
  assert.equal(providerLocationLabel(''), undefined)
  assert.equal(providerLocationLabel(undefined), undefined)
})
