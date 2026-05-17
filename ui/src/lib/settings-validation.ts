import type { SettingsFieldError, SettingsUpdatePayload } from '@/api/client'

export type SettingsValidationDraft = {
  payloadKey: string
  validation_errors: SettingsFieldError[]
}

export function settingsValidationPayloadKey(payload: SettingsUpdatePayload) {
  return JSON.stringify(payload)
}

type SettingsDraftValidationInput = {
  writable: boolean | undefined
  formDirty: boolean
  payload: SettingsUpdatePayload | null | undefined
  payloadKey: string | null | undefined
}

type EnabledSettingsDraftValidationInput = SettingsDraftValidationInput & {
  writable: true
  formDirty: true
  payload: SettingsUpdatePayload
  payloadKey: string
}

export function settingsDraftValidationEnabled(
  input: SettingsDraftValidationInput
): input is EnabledSettingsDraftValidationInput {
  const { writable, formDirty, payload, payloadKey } = input
  return Boolean(writable && formDirty && payload && payloadKey)
}

export function activeSettingsValidationErrors(
  baseErrors: SettingsFieldError[],
  draft: SettingsValidationDraft | null | undefined,
  currentPayloadKey: string | null | undefined
) {
  if (draft && currentPayloadKey && draft.payloadKey === currentPayloadKey) {
    return draft.validation_errors
  }
  return baseErrors
}
