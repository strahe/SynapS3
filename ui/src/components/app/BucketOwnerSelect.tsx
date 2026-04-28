import { internalRootOwnerAccessKey } from '@/api/client'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

export function BucketOwnerSelect({
  id,
  value,
  disabled,
  users,
  onChange,
}: {
  id: string
  value: string | undefined
  disabled?: boolean
  users: Array<{ access_key: string; role: string }>
  onChange: (value: string) => void
}) {
  return (
    <Select value={value || undefined} onValueChange={onChange} disabled={disabled}>
      <SelectTrigger id={id} className="w-full">
        <SelectValue placeholder="Select owner" />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          <SelectItem value={internalRootOwnerAccessKey}>Internal root</SelectItem>
          {users.map((user) => (
            <SelectItem key={user.access_key} value={user.access_key}>
              {user.access_key} ({user.role})
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}
