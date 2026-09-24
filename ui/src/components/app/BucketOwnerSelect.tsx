import { internalRootOwnerAccessKey } from '@/api/client'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { s3UserLabel } from '@/lib/s3-owner'

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
          <SelectItem value={internalRootOwnerAccessKey}>Internal root</SelectItem>
          {value && value !== internalRootOwnerAccessKey && !users.some((user) => user.access_key === value) && (
            <SelectItem value={value}>{value}</SelectItem>
          )}
          {users.map((user) => (
            <SelectItem key={user.access_key} value={user.access_key}>
              {s3UserLabel(user)} ({user.role})
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}
