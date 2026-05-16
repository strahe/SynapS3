export type RootSettingsState = {
  mode: 'ready' | 'setup'
  runtime_available?: boolean
}

export type RootContentKind = 'outlet' | 'setup-required'

export function rootContentKind(settings: RootSettingsState | undefined, pathname: string): RootContentKind {
  if (settings?.mode === 'setup' && pathname !== '/settings') {
    return 'setup-required'
  }
  return 'outlet'
}

export function filecoinRuntimeReadinessEnabled(settings: RootSettingsState | undefined, settingsLoading: boolean) {
  return !settingsLoading && settings?.mode === 'ready' && settings.runtime_available === true
}
