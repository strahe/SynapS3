import { internalRootOwnerAccessKey } from '@/api/client'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { s3UserLabel, s3UserRoleLabel } from '@/lib/s3-owner'

export function BucketOwnerSelect({
  id,
  value,
  disabled,
  invalid,
  users,
  onChange,
}: {
  id: string
  value: string | undefined
  disabled?: boolean
  invalid?: boolean
  users: Array<{ access_key: string; name: string; role: string }>
  onChange: (value: string) => void
}) {
  return (
    <Select value={value || undefined} onValueChange={onChange} disabled={disabled}>
      <SelectTrigger id={id} className="w-full" aria-invalid={invalid}>
        <SelectValue placeholder="Select owner" />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {users.map((user) => (
            <SelectItem key={user.access_key} value={user.access_key}>
              {s3UserLabel(user)} ({s3UserRoleLabel(user.role)})
            </SelectItem>
          ))}
          {value && value !== internalRootOwnerAccessKey && !users.some((user) => user.access_key === value) && (
            <SelectItem value={value}>{value}</SelectItem>
          )}
          <SelectItem value={internalRootOwnerAccessKey}>Internal root</SelectItem>
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}
