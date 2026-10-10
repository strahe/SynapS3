import { internalRootOwnerAccessKey, type S3User } from '../api/client.ts'

type NamedS3User = Pick<S3User, 'access_key' | 'name'>

export function s3UserLabel(user: NamedS3User) {
  return user.name ? `${user.name} (…${user.access_key.slice(-6)})` : user.access_key
}

export function ownerLabel(ownerAccessKey: string | null, users: readonly NamedS3User[] = []) {
  if (!ownerAccessKey) return 'Unassigned'
  if (ownerAccessKey === internalRootOwnerAccessKey) return 'Internal root'
  const user = users.find((candidate) => candidate.access_key === ownerAccessKey)
  return user ? s3UserLabel(user) : ownerAccessKey
}

const s3UserRoleLabels: Record<string, string> = {
  admin: 'Admin',
  user: 'User',
  userplus: 'User+',
}

export function s3UserRoleLabel(role: string) {
  return s3UserRoleLabels[role] ?? (role || 'Unknown')
}

export function s3UserRoleDescription(role: string) {
  switch (role) {
    case 'admin':
      return 'Can administer S3 API operations and access all buckets.'
    case 'user':
      return 'Can access buckets it owns, but cannot create new buckets.'
    case 'userplus':
      return 'Can create buckets and access buckets it owns.'
    default:
      return 'Unknown S3 role.'
  }
}
