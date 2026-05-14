export type ThemePreference = 'system' | 'dark' | 'light'

export const themeStorageKey = 'synaps3.theme'

interface ThemePreferenceStorage {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
}

type SystemThemeMediaQuery = Pick<MediaQueryList, 'addEventListener' | 'matches' | 'removeEventListener'>

interface SystemThemeSource {
  matchMedia?: (query: string) => SystemThemeMediaQuery
}

export function normalizeThemePreference(value: unknown): ThemePreference {
  if (value === 'dark' || value === 'light' || value === 'system') return value
  return 'system'
}

export function readThemePreference(storage: ThemePreferenceStorage | null | undefined = browserStorage()) {
  if (!storage) return 'system'

  try {
    return normalizeThemePreference(storage.getItem(themeStorageKey))
  } catch {
    return 'system'
  }
}

export function writeThemePreference(
  preference: ThemePreference,
  storage: ThemePreferenceStorage | null | undefined = browserStorage()
) {
  if (!storage) return

  try {
    storage.setItem(themeStorageKey, preference)
  } catch {
    return
  }
}

export function resolveThemeDark(preference: ThemePreference, systemPrefersDark: boolean) {
  if (preference === 'dark') return true
  if (preference === 'light') return false
  return systemPrefersDark
}

export function readSystemPrefersDark(source: SystemThemeSource | null | undefined = browserWindow()) {
  return getSystemThemeMediaQuery(source)?.matches ?? false
}

export function getSystemThemeMediaQuery(source: SystemThemeSource | null | undefined = browserWindow()) {
  if (!source || typeof source.matchMedia !== 'function') return null

  try {
    return source.matchMedia('(prefers-color-scheme: dark)')
  } catch {
    return null
  }
}

function browserStorage() {
  if (typeof window === 'undefined') return null

  try {
    return window.localStorage
  } catch {
    return null
  }
}

function browserWindow() {
  if (typeof window === 'undefined') return null
  return window
}
