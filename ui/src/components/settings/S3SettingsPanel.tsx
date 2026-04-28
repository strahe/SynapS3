import { AlertTriangle, Info, Loader2, Plus } from 'lucide-react'
import type * as React from 'react'
import { useEffect, useState } from 'react'
import type {
  S3User,
  S3UserCredentials,
  S3UserRole,
  SettingsData,
  SettingsEditableConfig,
  SettingsS3Credentials,
} from '@/api/client'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useCreateS3User, useDeleteS3User, useRotateS3UserSecret, useS3Users, useUpdateS3User } from '@/hooks/queries'
import { cn } from '@/lib/utils'
import { syncClosedRoleDraft } from './change-role-draft'

const s3UserRoles: S3UserRole[] = ['userplus', 'user', 'admin']

export function S3SettingsPanel({
  data,
  value,
  errors,
  onChange,
  onCredentials,
}: {
  data: SettingsData
  value: SettingsEditableConfig['s3']
  errors: Record<string, string>
  onChange: (value: SettingsEditableConfig['s3']) => void
  onCredentials: (credentials: SettingsS3Credentials) => void
}) {
  return (
    <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(20rem,24rem)]">
      <S3UsersSection data={data} onCredentials={onCredentials} />

      <div className="flex flex-col gap-4">
        <S3Section title="S3 Runtime">
          <div className="flex flex-col gap-4">
            <TextField
              label="Region"
              field="s3.region"
              value={value.region}
              data={data}
              errors={errors}
              onChange={(region) => onChange({ ...value, region })}
            />
          </div>
        </S3Section>
      </div>
    </div>
  )
}

function S3UsersSection({
  data,
  onCredentials,
}: {
  data: SettingsData
  onCredentials: (credentials: SettingsS3Credentials) => void
}) {
  const s3UsersAvailable = data.s3_users.available
  const unavailableReason = data.s3_users.reason || 'S3 user management is currently unavailable.'
  const { data: users = [], isLoading, error } = useS3Users(s3UsersAvailable)
  const createUser = useCreateS3User()
  const rotateUserSecret = useRotateS3UserSecret()
  const deleteUser = useDeleteS3User()
  const [createOpen, setCreateOpen] = useState(false)
  const [createRole, setCreateRole] = useState<S3UserRole>('userplus')
  const [deleteTarget, setDeleteTarget] = useState<S3User | null>(null)

  const mutationError = createUser.error ?? rotateUserSecret.error ?? deleteUser.error ?? null
  const errorMessage = error instanceof Error ? error.message : mutationError?.message

  function handleCreateUser() {
    if (!s3UsersAvailable || createUser.isPending) return
    createUser.mutate(
      { role: createRole },
      {
        onSuccess: (credentials: S3UserCredentials) => {
          onCredentials(credentials)
          setCreateRole('userplus')
          setCreateOpen(false)
        },
      }
    )
  }

  function handleRotateUser(user: S3User) {
    if (!s3UsersAvailable) return
    rotateUserSecret.mutate(user.access_key, {
      onSuccess: (credentials) => onCredentials(credentials),
    })
  }

  function handleDeleteUser() {
    if (!deleteTarget || deleteTarget.bucket_count > 0 || !s3UsersAvailable) return
    deleteUser.mutate(deleteTarget.access_key, {
      onSuccess: () => setDeleteTarget(null),
    })
  }

  return (
    <>
      <S3Section title="S3 Users">
        <div className="flex flex-col gap-4">
          {!s3UsersAvailable && (
            <Banner tone="warning" icon={AlertTriangle}>
              {unavailableReason}
            </Banner>
          )}
          {errorMessage && s3UsersAvailable && (
            <Banner tone="danger" icon={AlertTriangle}>
              {errorMessage}
            </Banner>
          )}

          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="text-sm text-muted-foreground">
              Manage S3 access keys used by clients. Secrets are only shown when created or rotated.
            </div>
            <Dialog open={createOpen} onOpenChange={setCreateOpen}>
              <DialogTrigger asChild>
                <Button type="button" disabled={!s3UsersAvailable || createUser.isPending}>
                  {createUser.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
                  Create S3 user
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Create S3 user</DialogTitle>
                  <DialogDescription>Select the role for this access key. The secret is shown once.</DialogDescription>
                </DialogHeader>
                <div className="flex flex-col gap-2">
                  <Label htmlFor="create-s3-user-role">Role</Label>
                  <RoleSelect
                    id="create-s3-user-role"
                    value={createRole}
                    disabled={!s3UsersAvailable || createUser.isPending}
                    onChange={setCreateRole}
                  />
                  <p className="text-xs text-muted-foreground">{roleDescription(createRole)}</p>
                </div>
                <DialogFooter>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => setCreateOpen(false)}
                    disabled={createUser.isPending}
                  >
                    Cancel
                  </Button>
                  <Button type="button" disabled={!s3UsersAvailable || createUser.isPending} onClick={handleCreateUser}>
                    {createUser.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
                    Create user
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>

          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full min-w-[48rem] text-left text-sm">
              <thead className="border-b border-border bg-muted/40 text-xs text-muted-foreground">
                <tr>
                  <th className="px-3 py-2 font-medium">Access Key</th>
                  <th className="w-36 px-3 py-2 font-medium">Role</th>
                  <th className="w-24 px-3 py-2 text-right font-medium">Buckets</th>
                  <th className="w-72 px-3 py-2 text-right font-medium">Actions</th>
                </tr>
              </thead>
              <tbody>
                {!s3UsersAvailable ? (
                  <tr>
                    <td className="px-3 py-6 text-center text-muted-foreground" colSpan={4}>
                      User list unavailable.
                    </td>
                  </tr>
                ) : isLoading ? (
                  <tr>
                    <td className="px-3 py-6 text-center text-muted-foreground" colSpan={4}>
                      <Loader2 className="mx-auto h-5 w-5 animate-spin" />
                    </td>
                  </tr>
                ) : users.length === 0 ? (
                  <tr>
                    <td className="px-3 py-6 text-center text-muted-foreground" colSpan={4}>
                      No additional S3 users.
                    </td>
                  </tr>
                ) : (
                  users.map((user) => {
                    const rotating = rotateUserSecret.isPending && rotateUserSecret.variables === user.access_key
                    const deleting = deleteUser.isPending && deleteUser.variables === user.access_key
                    return (
                      <tr key={user.access_key} className="border-b border-border last:border-b-0">
                        <td className="max-w-0 px-3 py-2">
                          <code className="block truncate text-xs">{user.access_key}</code>
                        </td>
                        <td className="px-3 py-2">
                          <RolePill role={user.role} />
                        </td>
                        <td className="px-3 py-2 text-right tabular-nums">{user.bucket_count}</td>
                        <td className="px-3 py-2">
                          <div className="flex justify-end gap-2">
                            <ChangeRoleDialog user={user} disabled={!s3UsersAvailable} />
                            <Button
                              type="button"
                              variant="outline"
                              size="xs"
                              disabled={!s3UsersAvailable || rotating}
                              onClick={() => handleRotateUser(user)}
                            >
                              {rotating && <Loader2 className="h-3 w-3 animate-spin" />}
                              Rotate secret
                            </Button>
                            <Button
                              type="button"
                              variant="destructive"
                              size="xs"
                              disabled={!s3UsersAvailable || deleting || user.bucket_count > 0}
                              title={
                                user.bucket_count > 0
                                  ? "Transfer this user's buckets before deleting the user."
                                  : undefined
                              }
                              onClick={() => setDeleteTarget(user)}
                            >
                              {deleting && <Loader2 className="h-3 w-3 animate-spin" />}
                              Delete
                            </Button>
                          </div>
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>
      </S3Section>

      <AlertDialog open={Boolean(deleteTarget)} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete S3 user?</AlertDialogTitle>
            <AlertDialogDescription>
              {deleteTarget
                ? `Delete ${deleteTarget.access_key}. Existing requests signed with this key will fail.`
                : ''}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel type="button">Cancel</AlertDialogCancel>
            <AlertDialogAction type="button" variant="destructive" onClick={handleDeleteUser}>
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}

function ChangeRoleDialog({ user, disabled }: { user: S3User; disabled?: boolean }) {
  const updateUser = useUpdateS3User()
  const [open, setOpen] = useState(false)
  const [role, setRole] = useState<S3UserRole>(user.role)
  const updating = updateUser.isPending && updateUser.variables?.accessKey === user.access_key

  useEffect(() => {
    setRole((currentRole) => syncClosedRoleDraft(open, currentRole, user.role))
  }, [open, user.role])

  const handleOpenChange = (next: boolean) => {
    if (!next) {
      updateUser.reset()
    }
    setOpen(next)
  }

  const handleUpdate = () => {
    if (role === user.role) return
    updateUser.mutate(
      { accessKey: user.access_key, role },
      {
        onSuccess: () => setOpen(false),
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button type="button" variant="outline" size="xs" disabled={disabled}>
          Change role
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Change S3 user role</DialogTitle>
          <DialogDescription>
            Existing bucket ownership is unchanged. The role controls whether this key can create new buckets.
          </DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-2">
          <Label htmlFor={`role-${user.access_key}`}>Role</Label>
          <RoleSelect id={`role-${user.access_key}`} value={role} disabled={updating} onChange={setRole} />
          <p className="text-xs text-muted-foreground">{roleDescription(role)}</p>
        </div>
        {updateUser.error && <p className="text-sm text-destructive">{updateUser.error.message}</p>}
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => handleOpenChange(false)} disabled={updating}>
            Cancel
          </Button>
          <Button type="button" disabled={role === user.role || updating} onClick={handleUpdate}>
            {updating && <Loader2 className="h-4 w-4 animate-spin" />}
            Save role
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function S3Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="rounded-lg border border-border bg-card p-4">
      <h2 className="mb-4 text-sm font-medium text-muted-foreground">{title}</h2>
      {children}
    </section>
  )
}

function TextField({
  label,
  field,
  value,
  data,
  errors,
  onChange,
}: {
  label: string
  field: string
  value: string
  data: SettingsData
  errors: Record<string, string>
  onChange: (value: string) => void
}) {
  const disabled = fieldDisabled(data, field)
  return (
    <FieldShell label={label} field={field} data={data} errors={errors}>
      <Input
        value={value}
        disabled={disabled}
        aria-invalid={Boolean(errors[field])}
        onChange={(event) => onChange(event.target.value)}
      />
    </FieldShell>
  )
}

function FieldShell({
  label,
  field,
  data,
  errors,
  children,
}: {
  label: string
  field: string
  data: SettingsData
  errors: Record<string, string>
  children: React.ReactNode
}) {
  const meta = data.metadata[field]
  const envOverride = data.env_managed[field]
  const displayLabel = meta?.label ?? label

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <Label className="truncate text-xs text-muted-foreground">{displayLabel}</Label>
          {meta && <InfoTooltip metadata={meta} />}
        </div>
        {envOverride && <EnvOverrideBadge env={envOverride} />}
      </div>
      {children}
      {envOverride && (
        <p className="flex items-center gap-1 text-xs text-yellow-600">
          <AlertTriangle className="h-3.5 w-3.5" />
          Overridden by {envOverride}
        </p>
      )}
      {errors[field] && <p className="text-xs text-destructive">{errors[field]}</p>}
    </div>
  )
}

function RoleSelect({
  id,
  value,
  disabled,
  onChange,
}: {
  id?: string
  value: S3UserRole
  disabled?: boolean
  onChange: (value: S3UserRole) => void
}) {
  return (
    <select
      id={id}
      value={value}
      disabled={disabled}
      onChange={(event) => onChange(event.target.value as S3UserRole)}
      className="h-8 w-full rounded-lg border border-input bg-background px-2.5 py-1 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:pointer-events-none disabled:bg-input/50 disabled:opacity-50"
    >
      {s3UserRoles.map((role) => (
        <option key={role} value={role}>
          {roleLabel(role)}
        </option>
      ))}
    </select>
  )
}

function RolePill({ role }: { role: string }) {
  return (
    <span className="inline-flex rounded-md border border-border bg-muted/40 px-2 py-0.5 text-xs font-medium">
      {roleLabel(role)}
    </span>
  )
}

function roleLabel(role: string) {
  switch (role) {
    case 'admin':
      return 'Admin'
    case 'user':
      return 'User'
    case 'userplus':
      return 'User+'
    default:
      return role || 'Unknown'
  }
}

function roleDescription(role: string) {
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

function Banner({
  tone,
  icon: Icon,
  children,
}: {
  tone: 'warning' | 'danger'
  icon: typeof AlertTriangle
  children: React.ReactNode
}) {
  const classes = {
    warning: 'border-yellow-500/30 bg-yellow-500/10 text-yellow-600',
    danger: 'border-destructive/30 bg-destructive/10 text-destructive',
  }

  return (
    <div className={cn('flex items-start gap-3 rounded-lg border p-3 text-sm', classes[tone])}>
      <Icon className="mt-0.5 h-4 w-4 shrink-0" />
      <div>{children}</div>
    </div>
  )
}

function InfoTooltip({ metadata }: { metadata: SettingsData['metadata'][string] }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <button type="button" className="rounded text-muted-foreground hover:text-foreground" aria-label="Field info">
          <Info className="h-3.5 w-3.5" />
        </button>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs">
        <div className="flex flex-col gap-1">
          <span>{metadata.description}</span>
          {metadata.env && <span className="font-mono opacity-80">{metadata.env}</span>}
        </div>
      </TooltipContent>
    </Tooltip>
  )
}

function EnvOverrideBadge({ env }: { env: string }) {
  return (
    <span className="inline-flex max-w-48 items-center gap-1 truncate rounded-md border border-yellow-500/30 bg-yellow-500/10 px-1.5 py-0.5 text-xs text-yellow-600">
      <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
      <span className="truncate">{env}</span>
    </span>
  )
}

function fieldDisabled(data: SettingsData, field: string) {
  return !data.writable || Boolean(data.env_managed[field])
}
