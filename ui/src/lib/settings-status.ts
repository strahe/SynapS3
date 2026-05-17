import type { SettingsData } from '@/api/client'

export function settingsSetupBannerVisible(settings: Pick<SettingsData, 'mode'>) {
  return settings.mode === 'setup'
}

export function settingsRuntimeRestartBannerVisible(settings: Pick<SettingsData, 'mode' | 'runtime_available'>) {
  return settings.mode === 'ready' && settings.runtime_available === false
}

export function settingsSavedBannerVisible(settings: Pick<SettingsData, 'restart_required' | 'runtime_available'>) {
  return settings.restart_required && settings.runtime_available !== false
}
