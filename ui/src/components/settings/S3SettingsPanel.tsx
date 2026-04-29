import { AlertTriangle, Loader2, Plus } from 'lucide-react'
import { useEffect, useState } from 'react'
import type {
  S3User,
  S3UserCredentials,
  S3UserRole,
  SettingsData,
  SettingsEditableConfig,
  SettingsS3Credentials,
} from '@/api/client'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { StatusBadge } from '@/components/app/StatusBadge'
import {
  SettingsBanner as Banner,
  SettingsFieldShell as FieldShell,
  SettingsSection as S3Section,
  SettingsSelect,
} from '@/components/settings/settings-form'
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
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useCreateS3User, useDeleteS3User, useRotateS3UserSecret, useS3Users, useUpdateS3User } from '@/hooks/queries'
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
  const [createReviewing, setCreateReviewing] = useState(false)
  const [rotateTarget, setRotateTarget] = useState<S3User | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<S3User | null>(null)

  const mutationError = createUser.error ?? rotateUserSecret.error ?? deleteUser.error ?? null
  const errorMessage = error instanceof Error ? error.message : mutationError?.message
  const rotatePending = rotateUserSecret.isPending
  const deletePending = deleteUser.isPending

  function handleCreateUser() {
    if (!s3UsersAvailable || createUser.isPending) return
    if (createRole === 'admin' && !createReviewing) {
      setCreateReviewing(true)
      return
    }
    createUser.mutate(
      { role: createRole },
      {
        onSuccess: (credentials: S3UserCredentials) => {
          onCredentials(credentials)
          setCreateRole('userplus')
          setCreateReviewing(false)
          setCreateOpen(false)
        },
      }
    )
  }

  function handleRotateUser() {
    if (!rotateTarget || rotatePending || !s3UsersAvailable) return
    rotateUserSecret.mutate(rotateTarget.access_key, {
      onSuccess: (credentials) => {
        onCredentials(credentials)
        setRotateTarget(null)
      },
    })
  }

  function openRotateUser(user: S3User) {
    if (rotatePending) return
    rotateUserSecret.reset()
    setRotateTarget(user)
  }

  function openDeleteUser(user: S3User) {
    if (deletePending) return
    deleteUser.reset()
    setDeleteTarget(user)
  }

  function handleDeleteUser() {
    if (!deleteTarget || deletePending || deleteTarget.bucket_count > 0 || !s3UsersAvailable) return
    deleteUser.mutate(deleteTarget.access_key, {
      onSuccess: () => setDeleteTarget(null),
    })
  }

  function handleCreateOpenChange(next: boolean) {
    if (!next) {
      setCreateRole('userplus')
      setCreateReviewing(false)
      createUser.reset()
    }
    setCreateOpen(next)
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
            <Dialog open={createOpen} onOpenChange={handleCreateOpenChange}>
              <DialogTrigger asChild>
                <Button type="button" disabled={!s3UsersAvailable || createUser.isPending}>
                  {createUser.isPending ? (
                    <Loader2 data-icon="inline-start" className="animate-spin" />
                  ) : (
                    <Plus data-icon="inline-start" />
                  )}
                  Create S3 user
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>{createReviewing ? 'Review admin S3 user' : 'Create S3 user'}</DialogTitle>
                  <DialogDescription>
                    {createReviewing
                      ? 'Admin users can administer S3 API operations and access all buckets.'
                      : 'Select the role for this access key. The secret is shown once.'}
                  </DialogDescription>
                </DialogHeader>
                {createReviewing ? (
                  <ReviewDetails
                    rows={[
                      { id: 'role', label: 'Role', value: roleLabel(createRole) },
                      { id: 'access', label: 'Access', value: 'All buckets and S3 administration' },
                    ]}
                  />
                ) : (
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
                )}
                <DialogFooter>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => (createReviewing ? setCreateReviewing(false) : handleCreateOpenChange(false))}
                    disabled={createUser.isPending}
                  >
                    {createReviewing ? 'Back' : 'Cancel'}
                  </Button>
                  <Button type="button" disabled={!s3UsersAvailable || createUser.isPending} onClick={handleCreateUser}>
                    {createUser.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
                    {createReviewing ? 'Create admin user' : createRole === 'admin' ? 'Review' : 'Create user'}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>

          <div className="overflow-hidden rounded-md border border-border">
            <Table className="min-w-[48rem]">
              <TableHeader>
                <TableRow className="bg-muted/40">
                  <TableHead className="px-3">Access Key</TableHead>
                  <TableHead className="w-36 px-3">Role</TableHead>
                  <TableHead className="w-24 px-3 text-right">Buckets</TableHead>
                  <TableHead className="w-72 px-3 text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {!s3UsersAvailable ? (
                  <TableRow>
                    <TableCell className="h-24 text-center text-muted-foreground" colSpan={4}>
                      User list unavailable.
                    </TableCell>
                  </TableRow>
                ) : isLoading ? (
                  <TableRow>
                    <TableCell className="h-24 text-center text-muted-foreground" colSpan={4}>
                      <Loader2 className="mx-auto h-5 w-5 animate-spin" />
                    </TableCell>
                  </TableRow>
                ) : users.length === 0 ? (
                  <TableRow>
                    <TableCell className="h-24 text-center text-muted-foreground" colSpan={4}>
                      No additional S3 users.
                    </TableCell>
                  </TableRow>
                ) : (
                  users.map((user) => {
                    const rotating = rotatePending && rotateUserSecret.variables === user.access_key
                    const deleting = deletePending && deleteUser.variables === user.access_key
                    return (
                      <TableRow key={user.access_key}>
                        <TableCell className="max-w-0 px-3">
                          <code className="block truncate text-xs">{user.access_key}</code>
                        </TableCell>
                        <TableCell className="px-3">
                          <RolePill role={user.role} />
                        </TableCell>
                        <TableCell className="px-3 text-right tabular-nums">{user.bucket_count}</TableCell>
                        <TableCell className="px-3">
                          <div className="flex justify-end gap-2">
                            <ChangeRoleDialog user={user} disabled={!s3UsersAvailable} />
                            <Button
                              type="button"
                              variant="outline"
                              size="xs"
                              disabled={!s3UsersAvailable || rotatePending}
                              onClick={() => openRotateUser(user)}
                            >
                              {rotating && <Loader2 data-icon="inline-start" className="animate-spin" />}
                              Rotate secret
                            </Button>
                            <Button
                              type="button"
                              variant="destructive"
                              size="xs"
                              disabled={!s3UsersAvailable || deletePending || user.bucket_count > 0}
                              title={
                                user.bucket_count > 0
                                  ? "Transfer this user's buckets before deleting the user."
                                  : undefined
                              }
                              onClick={() => openDeleteUser(user)}
                            >
                              {deleting && <Loader2 data-icon="inline-start" className="animate-spin" />}
                              Delete
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    )
                  })
                )}
              </TableBody>
            </Table>
          </div>
        </div>
      </S3Section>

      <DangerActionAlertDialog
        open={Boolean(rotateTarget)}
        onOpenChange={(open) => !open && setRotateTarget(null)}
        title="Rotate S3 secret?"
        description={
          rotateTarget
            ? `Rotate the secret for ${rotateTarget.access_key}. Existing clients using the old secret will fail immediately. The new secret is shown once.`
            : ''
        }
        confirmLabel="Rotate secret"
        pending={Boolean(
          rotateTarget && rotateUserSecret.isPending && rotateUserSecret.variables === rotateTarget.access_key
        )}
        error={rotateUserSecret.error?.message}
        onConfirm={handleRotateUser}
      />

      <DangerActionAlertDialog
        open={Boolean(deleteTarget)}
        onOpenChange={(open) => !open && setDeleteTarget(null)}
        title="Delete S3 user?"
        description={
          deleteTarget ? `Delete ${deleteTarget.access_key}. Existing requests signed with this key will fail.` : ''
        }
        confirmLabel="Delete user"
        pending={Boolean(deleteTarget && deleteUser.isPending && deleteUser.variables === deleteTarget.access_key)}
        error={deleteUser.error?.message}
        onConfirm={handleDeleteUser}
      />
    </>
  )
}

function ChangeRoleDialog({ user, disabled }: { user: S3User; disabled?: boolean }) {
  const updateUser = useUpdateS3User()
  const [open, setOpen] = useState(false)
  const [role, setRole] = useState<S3UserRole>(user.role)
  const [reviewing, setReviewing] = useState(false)
  const updating = updateUser.isPending && updateUser.variables?.accessKey === user.access_key

  useEffect(() => {
    setRole((currentRole) => syncClosedRoleDraft(open, currentRole, user.role))
  }, [open, user.role])

  const handleOpenChange = (next: boolean) => {
    if (!next) {
      setReviewing(false)
      updateUser.reset()
    }
    setOpen(next)
  }

  const handleUpdate = () => {
    if (role === user.role) return
    if (!reviewing) {
      setReviewing(true)
      return
    }
    updateUser.mutate(
      { accessKey: user.access_key, role },
      {
        onSuccess: () => {
          setReviewing(false)
          setOpen(false)
        },
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
          <DialogTitle>{reviewing ? 'Review S3 user role' : 'Change S3 user role'}</DialogTitle>
          <DialogDescription>
            {reviewing && role === 'admin'
              ? 'Admin users can administer S3 API operations and access all buckets.'
              : 'Existing bucket ownership is unchanged. The role controls whether this key can create new buckets.'}
          </DialogDescription>
        </DialogHeader>
        {reviewing ? (
          <ReviewDetails
            rows={[
              { id: 'access-key', label: 'Access key', value: user.access_key },
              { id: 'current-role', label: 'Current role', value: roleLabel(user.role) },
              { id: 'new-role', label: 'New role', value: roleLabel(role) },
            ]}
          />
        ) : (
          <div className="flex flex-col gap-2">
            <Label htmlFor={`role-${user.access_key}`}>Role</Label>
            <RoleSelect id={`role-${user.access_key}`} value={role} disabled={updating} onChange={setRole} />
            <p className="text-xs text-muted-foreground">{roleDescription(role)}</p>
          </div>
        )}
        {updateUser.error && <p className="text-sm text-destructive">{updateUser.error.message}</p>}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => (reviewing ? setReviewing(false) : handleOpenChange(false))}
            disabled={updating}
          >
            {reviewing ? 'Back' : 'Cancel'}
          </Button>
          <Button type="button" disabled={role === user.role || updating} onClick={handleUpdate}>
            {updating && <Loader2 data-icon="inline-start" className="animate-spin" />}
            {reviewing ? 'Confirm role' : 'Review'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
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
    <SettingsSelect
      id={id}
      value={value}
      disabled={disabled}
      options={s3UserRoles.map((role) => ({ value: role, label: roleLabel(role) }))}
      onChange={(next) => onChange(next as S3UserRole)}
    />
  )
}

function RolePill({ role }: { role: string }) {
  return <StatusBadge tone="neutral">{roleLabel(role)}</StatusBadge>
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

function fieldDisabled(data: SettingsData, field: string) {
  return !data.writable || Boolean(data.env_managed[field])
}
