export type RootSettingsState = {
  mode: 'ready' | 'setup'
}

export type RootContentKind = 'outlet' | 'setup-required'

export function rootContentKind(settings: RootSettingsState | undefined, pathname: string): RootContentKind {
  if (settings?.mode === 'setup' && pathname !== '/settings') {
    return 'setup-required'
  }
  return 'outlet'
}
