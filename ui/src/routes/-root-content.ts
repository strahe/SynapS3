export type RootSettingsState = {
  mode: 'ready' | 'setup'
  runtime_available?: boolean
}

export type RootContentKind = 'outlet' | 'setup-required'

export function rootUsesSetupShell(settings: RootSettingsState | undefined) {
  return settings?.mode === 'setup' || settings?.runtime_available === false
}

export function rootContentKind(settings: RootSettingsState | undefined, pathname: string): RootContentKind {
  if (rootUsesSetupShell(settings) && pathname !== '/settings') {
    return 'setup-required'
  }
  return 'outlet'
}

export function fullRuntimeAvailable(settings: RootSettingsState | undefined, settingsLoading: boolean) {
  return !settingsLoading && settings?.mode === 'ready' && settings.runtime_available !== false
}

export function filecoinRuntimeReadinessEnabled(settings: RootSettingsState | undefined, settingsLoading: boolean) {
  return fullRuntimeAvailable(settings, settingsLoading)
}
