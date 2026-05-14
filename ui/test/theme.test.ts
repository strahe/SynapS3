import assert from 'node:assert/strict'
import test from 'node:test'

import {
  getSystemThemeMediaQuery,
  normalizeThemePreference,
  readSystemPrefersDark,
  readThemePreference,
  resolveThemeDark,
  themeStorageKey,
  writeThemePreference,
} from '../src/lib/theme.ts'

test('theme preference falls back to system for missing and invalid values', () => {
  assert.equal(normalizeThemePreference(null), 'system')
  assert.equal(normalizeThemePreference(undefined), 'system')
  assert.equal(normalizeThemePreference('auto'), 'system')
  assert.equal(normalizeThemePreference('dark'), 'dark')
  assert.equal(normalizeThemePreference('light'), 'light')
  assert.equal(normalizeThemePreference('system'), 'system')
})

test('theme resolution follows system preference only in system mode', () => {
  assert.equal(resolveThemeDark('system', true), true)
  assert.equal(resolveThemeDark('system', false), false)
  assert.equal(resolveThemeDark('dark', false), true)
  assert.equal(resolveThemeDark('light', true), false)
})

test('theme preference reads and writes browser storage defensively', () => {
  const values = new Map<string, string>()
  const storage = {
    getItem(key: string) {
      return values.get(key) ?? null
    },
    setItem(key: string, value: string) {
      values.set(key, value)
    },
  }

  assert.equal(readThemePreference(storage), 'system')

  writeThemePreference('dark', storage)
  assert.equal(values.get(themeStorageKey), 'dark')
  assert.equal(readThemePreference(storage), 'dark')

  values.set(themeStorageKey, 'unknown')
  assert.equal(readThemePreference(storage), 'system')
})

test('theme preference ignores unavailable browser storage', () => {
  const storage = {
    getItem() {
      throw new Error('blocked')
    },
    setItem() {
      throw new Error('blocked')
    },
  }

  assert.equal(readThemePreference(storage), 'system')
  assert.doesNotThrow(() => writeThemePreference('light', storage))
})

test('system theme detection handles missing and restricted media query APIs', () => {
  assert.equal(readSystemPrefersDark(undefined), false)
  assert.equal(readSystemPrefersDark({}), false)
  assert.equal(
    readSystemPrefersDark({
      matchMedia() {
        throw new Error('blocked')
      },
    }),
    false
  )
  assert.equal(
    readSystemPrefersDark({
      matchMedia() {
        return { matches: true }
      },
    }),
    true
  )
})

test('system theme media query returns null when the API is unavailable', () => {
  assert.equal(getSystemThemeMediaQuery(undefined), null)
  assert.equal(getSystemThemeMediaQuery({}), null)
  assert.equal(
    getSystemThemeMediaQuery({
      matchMedia() {
        throw new Error('blocked')
      },
    }),
    null
  )
  assert.equal(
    getSystemThemeMediaQuery({
      matchMedia() {
        return { matches: false }
      },
    })?.matches,
    false
  )
})
