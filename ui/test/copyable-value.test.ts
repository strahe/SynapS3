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

test('copyable value model preserves optional link metadata without changing copied text', () => {
  const model = buildCopyableValueModel({
    value: 'https://provider.example/status/abc',
    displayValue: 'Provider status URL',
    linkHref: 'https://provider.example/status/abc',
    external: true,
  })

  assert.equal(model.displayText, 'Provider status URL')
  assert.equal(model.tooltipValue, 'https://provider.example/status/abc')
  assert.equal(model.copyValue, 'https://provider.example/status/abc')
  assert.equal(model.linkHref, 'https://provider.example/status/abc')
  assert.equal(model.external, true)
})

test('copyable value model accepts absolute http links', () => {
  const model = buildCopyableValueModel({
    value: 'http://provider.example/status/abc',
    linkHref: 'http://provider.example/status/abc',
  })

  assert.equal(model.linkHref, 'http://provider.example/status/abc')
  assert.equal(model.copyValue, 'http://provider.example/status/abc')
})

test('copyable value model drops unsafe link targets without changing copied text', () => {
  const model = buildCopyableValueModel({
    value: 'javascript:alert(1)',
    displayValue: 'Provider status URL',
    linkHref: 'javascript:alert(1)',
    external: true,
  })

  assert.equal(model.displayText, 'Provider status URL')
  assert.equal(model.tooltipValue, 'javascript:alert(1)')
  assert.equal(model.copyValue, 'javascript:alert(1)')
  assert.equal(model.linkHref, undefined)
  assert.equal(model.external, false)
})

test('copyable value model keeps empty values stable', () => {
  const model = buildCopyableValueModel({ value: '', maxLength: 8 })

  assert.equal(model.displayText, '')
  assert.equal(model.tooltipValue, '')
  assert.equal(model.copyValue, '')
  assert.equal(model.external, false)
})
