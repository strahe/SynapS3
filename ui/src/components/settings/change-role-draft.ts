import type { S3UserRole } from '@/api/client'

export function syncClosedRoleDraft(open: boolean, currentRole: S3UserRole, userRole: S3UserRole): S3UserRole {
  if (open) {
    return currentRole
  }
  return userRole
}
