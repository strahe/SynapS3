import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute } from '@tanstack/react-router'
import { AlertTriangle, KeyRound, Loader2, Pencil, Plus, RotateCw, Trash2 } from 'lucide-react'
import { useEffect, useState } from 'react'
import { toast } from 'sonner'
import type { S3User, S3UserCredentials, S3UserRole, SettingsS3Credentials } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import { DataTableFrame, TableSkeleton, tableHeaderRowClassName } from '@/components/app/DataTable'
import { PageHeader, RefreshButton } from '@/components/app/PageHeader'
import { EmptyState, PageError } from '@/components/app/PageState'
import { ReviewDetails } from '@/components/app/ReviewDetails'
import { RowActionItem, RowActionsMenu } from '@/components/app/RowActionsMenu'
import { StatusBadge } from '@/components/app/StatusBadge'
import { SettingsBanner, SettingsSelect, SettingsValueField } from '@/components/settings/settings-form'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { DropdownMenuSeparator } from '@/components/ui/dropdown-menu'
import { Field, FieldDescription, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import {
  useCreateS3User,
  useDeleteS3User,
  useRotateS3UserSecret,
  useS3Users,
  useSettings,
  useUpdateS3User,
} from '@/hooks/queries'
import { s3UserLabel, s3UserRoleDescription, s3UserRoleLabel } from '@/lib/s3-owner'

export const Route = createFileRoute('/s3-users')({
  component: S3UsersPage,
})

const s3UserRoles: S3UserRole[] = ['userplus', 'user', 'admin']

function S3UsersPage() {
  const queryClient = useQueryClient()
  const { data: settings } = useSettings()
  const available = settings?.s3_users.available ?? true
  const unavailableReason = settings?.s3_users.reason || 'S3 user management is currently unavailable.'
  const users = useS3Users(available)
  const rotateUserSecret = useRotateS3UserSecret()
  const deleteUser = useDeleteS3User()
  const [createOpen, setCreateOpen] = useState(false)
  const [editTarget, setEditTarget] = useState<S3User | null>(null)
  const [rotateTarget, setRotateTarget] = useState<S3User | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<S3User | null>(null)
  const [credentials, setCredentials] = useState<SettingsS3Credentials | null>(null)

  const handleRotateUser = () => {
    if (!rotateTarget || rotateUserSecret.isPending) return
    rotateUserSecret.mutate(rotateTarget.access_key, {
      onSuccess: (next) => {
        setCredentials(next)
        setRotateTarget(null)
      },
    })
  }

  const handleDeleteUser = () => {
    if (!deleteTarget || deleteUser.isPending || deleteTarget.bucket_count > 0) return
    const label = s3UserLabel(deleteTarget)
    deleteUser.mutate(deleteTarget.access_key, {
      onSuccess: () => {
        toast.success(`Deleted S3 user ${label}`)
        setDeleteTarget(null)
      },
    })
  }

  const list = users.data ?? []

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader
        title="S3 users"
        description="Access keys used by S3 clients. Secrets are shown only when a user is created or its secret is rotated."
        actions={
          <>
            <RefreshButton
              onClick={() => queryClient.invalidateQueries({ queryKey: ['s3Users'] })}
              refreshing={users.isFetching}
            />
            <Button size="sm" disabled={!available} onClick={() => setCreateOpen(true)}>
              <Plus data-icon="inline-start" />
              Create S3 user
            </Button>
          </>
        }
      />

      {!available ? (
        <SettingsBanner tone="warning" icon={AlertTriangle}>
          {unavailableReason}
        </SettingsBanner>
      ) : users.isLoading ? (
        <TableSkeleton rows={3} />
      ) : users.error ? (
        <PageError
          title="Failed to load S3 users"
          description={users.error.message}
          onRetry={() => users.refetch()}
          retrying={users.isFetching}
        />
      ) : list.length === 0 ? (
        <EmptyState
          icon={<KeyRound />}
          title="No S3 users yet"
          description="Create a user to give an S3 client its own access key."
          action={
            <Button size="sm" onClick={() => setCreateOpen(true)}>
              <Plus data-icon="inline-start" />
              Create S3 user
            </Button>
          }
        />
      ) : (
        <DataTableFrame>
          <Table className="min-w-[40rem]">
            <TableHeader>
              <TableRow className={tableHeaderRowClassName}>
                <TableHead className="px-4">User</TableHead>
                <TableHead className="w-36 px-4">Role</TableHead>
                <TableHead className="w-28 px-4 text-right">Buckets</TableHead>
                <TableHead className="w-16 px-4 text-right">
                  <span className="sr-only">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {list.map((user) => (
                <TableRow key={user.access_key}>
                  <TableCell className="max-w-0 px-4">
                    <CopyableValue
                      label="Access key"
                      value={user.access_key}
                      displayValue={s3UserLabel(user)}
                      maxLength={s3UserLabel(user).length}
                    />
                  </TableCell>
                  <TableCell className="px-4">
                    <StatusBadge tone="neutral">{s3UserRoleLabel(user.role)}</StatusBadge>
                  </TableCell>
                  <TableCell className="px-4 text-right tabular-nums">{user.bucket_count}</TableCell>
                  <TableCell className="px-4 text-right">
                    <RowActionsMenu label={s3UserLabel(user)}>
                      <RowActionItem onSelect={() => setEditTarget(user)}>
                        <Pencil data-icon="inline-start" />
                        Edit
                      </RowActionItem>
                      <RowActionItem
                        onSelect={() => {
                          rotateUserSecret.reset()
                          setRotateTarget(user)
                        }}
                      >
                        <RotateCw data-icon="inline-start" />
                        Rotate secret
                      </RowActionItem>
                      <DropdownMenuSeparator />
                      <RowActionItem
                        variant="destructive"
                        disabledReason={
                          user.bucket_count > 0 ? "Transfer this user's buckets before deleting it." : undefined
                        }
                        onSelect={() => {
                          deleteUser.reset()
                          setDeleteTarget(user)
                        }}
                      >
                        <Trash2 data-icon="inline-start" />
                        Delete
                      </RowActionItem>
                    </RowActionsMenu>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </DataTableFrame>
      )}

      <CreateS3UserDialog open={createOpen} onOpenChange={setCreateOpen} onCreated={setCredentials} />
      <EditS3UserDialog user={editTarget} onClose={() => setEditTarget(null)} />
      <DangerActionAlertDialog
        open={Boolean(rotateTarget)}
        onOpenChange={(open) => {
          if (!open) setRotateTarget(null)
        }}
        title="Rotate S3 secret?"
        description={
          rotateTarget
            ? `Rotate the secret for ${s3UserLabel(rotateTarget)}. Existing clients using the old secret will fail immediately. The new secret is shown once.`
            : ''
        }
        confirmLabel="Rotate secret"
        pending={rotateUserSecret.isPending}
        error={rotateUserSecret.error?.message}
        onConfirm={handleRotateUser}
      />
      <DangerActionAlertDialog
        open={Boolean(deleteTarget)}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null)
        }}
        title="Delete S3 user?"
        description={
          deleteTarget ? `Delete ${s3UserLabel(deleteTarget)}. Existing requests signed with this key will fail.` : ''
        }
        confirmLabel="Delete user"
        pending={deleteUser.isPending}
        error={deleteUser.error?.message}
        onConfirm={handleDeleteUser}
      />
      <GeneratedCredentialsDialog
        credentials={credentials}
        onOpenChange={(open) => {
          if (!open) setCredentials(null)
        }}
      />
    </div>
  )
}

function CreateS3UserDialog({
  open,
  onOpenChange,
  onCreated,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onCreated: (credentials: S3UserCredentials) => void
}) {
  const createUser = useCreateS3User()
  const [name, setName] = useState('')
  const [role, setRole] = useState<S3UserRole>('userplus')
  const [reviewing, setReviewing] = useState(false)

  const handleOpenChange = (next: boolean) => {
    if (!next) {
      setName('')
      setRole('userplus')
      setReviewing(false)
      createUser.reset()
    }
    onOpenChange(next)
  }

  const handleCreate = () => {
    if (createUser.isPending) return
    if (role === 'admin' && !reviewing) {
      setReviewing(true)
      return
    }
    createUser.mutate(
      { name: name.trim(), role },
      {
        onSuccess: (credentials) => {
          onCreated(credentials)
          handleOpenChange(false)
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{reviewing ? 'Review admin S3 user' : 'Create S3 user'}</DialogTitle>
          <DialogDescription>
            {reviewing
              ? 'Admin users can administer S3 API operations and access all buckets.'
              : 'Add an optional name and select a role. The secret is shown once.'}
          </DialogDescription>
        </DialogHeader>
        {reviewing ? (
          <ReviewDetails
            rows={[
              { id: 'name', label: 'Name', value: name.trim() || 'None' },
              { id: 'role', label: 'Role', value: s3UserRoleLabel(role) },
              { id: 'access', label: 'Access', value: 'All buckets and S3 administration' },
            ]}
          />
        ) : (
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="create-s3-user-name">Name</FieldLabel>
              <Input
                id="create-s3-user-name"
                value={name}
                onChange={(event) => setName(event.target.value)}
                disabled={createUser.isPending}
                placeholder="Optional name"
              />
              <FieldDescription>Names must be unique. You can add one later.</FieldDescription>
            </Field>
            <Field>
              <FieldLabel htmlFor="create-s3-user-role">Role</FieldLabel>
              <RoleSelect id="create-s3-user-role" value={role} disabled={createUser.isPending} onChange={setRole} />
              <FieldDescription>{s3UserRoleDescription(role)}</FieldDescription>
            </Field>
          </FieldGroup>
        )}
        {createUser.error && (
          <Alert variant="destructive">
            <AlertDescription>{createUser.error.message}</AlertDescription>
          </Alert>
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => (reviewing ? setReviewing(false) : handleOpenChange(false))}
            disabled={createUser.isPending}
          >
            {reviewing ? 'Back' : 'Cancel'}
          </Button>
          <Button type="button" disabled={createUser.isPending} onClick={handleCreate}>
            {createUser.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            {reviewing ? 'Create admin user' : role === 'admin' ? 'Review' : 'Create user'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function EditS3UserDialog({ user, onClose }: { user: S3User | null; onClose: () => void }) {
  const updateUser = useUpdateS3User()
  const open = user !== null
  const [name, setName] = useState(user?.name ?? '')
  const [role, setRole] = useState<S3UserRole>(user?.role ?? 'userplus')
  const [reviewing, setReviewing] = useState(false)
  const trimmedName = name.trim()
  const nameChanged = user !== null && trimmedName !== user.name
  const roleChanged = user !== null && role !== user.role

  useEffect(() => {
    if (!user) return
    setName(user.name)
    setRole(user.role)
    setReviewing(false)
  }, [user])

  const handleOpenChange = (next: boolean) => {
    if (next) return
    setReviewing(false)
    updateUser.reset()
    onClose()
  }

  const handleUpdate = () => {
    if (!user || (!nameChanged && !roleChanged)) return
    if (roleChanged && !reviewing) {
      setReviewing(true)
      return
    }
    updateUser.mutate(
      { accessKey: user.access_key, ...(nameChanged ? { name: trimmedName } : {}), ...(roleChanged ? { role } : {}) },
      {
        onSuccess: () => {
          toast.success(`Updated S3 user ${s3UserLabel({ access_key: user.access_key, name: trimmedName })}`)
          handleOpenChange(false)
        },
      }
    )
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{reviewing ? 'Review S3 user changes' : 'Edit S3 user'}</DialogTitle>
          <DialogDescription>
            {reviewing && role === 'admin'
              ? 'Admin users can administer S3 API operations and access all buckets.'
              : 'Changing the name does not affect bucket ownership. The role controls S3 access.'}
          </DialogDescription>
        </DialogHeader>
        {user &&
          (reviewing ? (
            <ReviewDetails
              rows={[
                { id: 'access-key', label: 'Access key', value: user.access_key, copyable: true },
                ...(nameChanged ? [{ id: 'name', label: 'Name', value: trimmedName || 'None' }] : []),
                { id: 'current-role', label: 'Current role', value: s3UserRoleLabel(user.role) },
                { id: 'new-role', label: 'New role', value: s3UserRoleLabel(role) },
              ]}
            />
          ) : (
            <FieldGroup>
              <Field>
                <FieldLabel htmlFor="edit-s3-user-name">Name</FieldLabel>
                <Input
                  id="edit-s3-user-name"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                  disabled={updateUser.isPending}
                  placeholder="Optional name"
                />
                <FieldDescription>Leave blank to show the access key.</FieldDescription>
              </Field>
              <Field>
                <FieldLabel htmlFor="edit-s3-user-role">Role</FieldLabel>
                <RoleSelect id="edit-s3-user-role" value={role} disabled={updateUser.isPending} onChange={setRole} />
                <FieldDescription>{s3UserRoleDescription(role)}</FieldDescription>
              </Field>
            </FieldGroup>
          ))}
        {updateUser.error && (
          <Alert variant="destructive">
            <AlertDescription>{updateUser.error.message}</AlertDescription>
          </Alert>
        )}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            onClick={() => (reviewing ? setReviewing(false) : handleOpenChange(false))}
            disabled={updateUser.isPending}
          >
            {reviewing ? 'Back' : 'Cancel'}
          </Button>
          <Button
            type="button"
            disabled={(!nameChanged && !roleChanged) || updateUser.isPending}
            onClick={handleUpdate}
          >
            {updateUser.isPending && <Loader2 data-icon="inline-start" className="animate-spin" />}
            {reviewing ? 'Confirm changes' : roleChanged ? 'Review' : 'Save'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
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
      options={s3UserRoles.map((role) => ({ value: role, label: s3UserRoleLabel(role) }))}
      onChange={(next) => onChange(next as S3UserRole)}
    />
  )
}

function GeneratedCredentialsDialog({
  credentials,
  onOpenChange,
}: {
  credentials: SettingsS3Credentials | null
  onOpenChange: (open: boolean) => void
}) {
  return (
    <Dialog open={Boolean(credentials)} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>S3 credentials generated</DialogTitle>
          <DialogDescription>These credentials are shown once.</DialogDescription>
        </DialogHeader>
        {credentials && (
          <div className="flex flex-col gap-3">
            {credentials.role && <SettingsValueField label="Role" value={s3UserRoleLabel(credentials.role)} />}
            <SettingsValueField label="Access key" value={credentials.access_key} copy mono />
            <SettingsValueField label="Secret key" value={credentials.secret_key} copy mono />
          </div>
        )}
        <DialogFooter>
          <DialogClose asChild>
            <Button type="button">Close</Button>
          </DialogClose>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
