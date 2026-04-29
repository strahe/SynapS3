import { internalRootOwnerAccessKey } from '@/api/client'

export function ownerLabel(ownerAccessKey: string | null) {
  if (!ownerAccessKey) return 'Unassigned'
  if (ownerAccessKey === internalRootOwnerAccessKey) return 'Internal root'
  return ownerAccessKey
}
