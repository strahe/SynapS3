import assert from 'node:assert/strict'
import test from 'node:test'

import { buildCopyableValueModel, middleTruncate } from '../src/lib/copyable-value.ts'

test('middleTruncate keeps short values intact and shortens long values in the middle', () => {
  assert.equal(middleTruncate('short-value'), 'short-value')
  assert.equal(middleTruncate('0xfed1b6ba439b372edc10dce78ae900'), '0xfed1b6ba...8ae900')
  assert.equal(middleTruncate('0xfed1b6ba439b372edc10dce78ae900', 16), '0xfed1b...8ae900')
  assert.equal(middleTruncate('0xfed1b6ba439b372edc10dce78ae900', 28), '0xfed1b6ba439...10dce78ae900')
  assert.equal(middleTruncate('abcdefghijklmnopqrstuvwxyz', 3), '...')
  assert.equal(middleTruncate('abcdefghijklmnopqrstuvwxyz', 4), 'a...')
})

test('copyable value model separates visible text from copied value', () => {
  const model = buildCopyableValueModel({
    value: 'full recorded provider error text',
    displayValue: 'Recorded error available',
  })

  assert.equal(model.displayText, 'Recorded error available')
  assert.equal(model.tooltipValue, 'full recorded provider error text')
  assert.equal(model.copyValue, 'full recorded provider error text')
})
