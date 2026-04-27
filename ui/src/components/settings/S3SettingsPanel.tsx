import { AlertTriangle, ChevronDown, Info, KeyRound, Loader2, Plus, RotateCcw, Save, Trash2 } from 'lucide-react'
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
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useCreateS3User, useDeleteS3User, useRotateS3UserSecret, useS3Users, useUpdateS3User } from '@/hooks/queries'
import { cn } from '@/lib/utils'

const s3UserRoles: S3UserRole[] = ['userplus', 'user', 'admin']

export function S3SettingsPanel({
  data,
  value,
  errors,
  generateRootOpen,
  generateRootDisabled,
  generateRootPending,
  onChange,
  onCredentials,
  onGenerateRoot,
  onGenerateRootOpenChange,
}: {
  data: SettingsData
  value: SettingsEditableConfig['s3']
  errors: Record<string, string>
  generateRootOpen: boolean
  generateRootDisabled: boolean
  generateRootPending: boolean
  onChange: (value: SettingsEditableConfig['s3']) => void
  onCredentials: (credentials: SettingsS3Credentials) => void
  onGenerateRoot: () => void
  onGenerateRootOpenChange: (open: boolean) => void
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
            <TextField
              label="IAM Directory"
              field="s3.iam_dir"
              value={value.iam_dir}
              data={data}
              errors={errors}
              onChange={(iam_dir) => onChange({ ...value, iam_dir })}
            />
          </div>
        </S3Section>

        <details className="group rounded-lg border border-border bg-card">
          <summary className="flex cursor-pointer list-none items-center justify-between gap-3 p-4 text-sm font-medium">
            Advanced Root Credentials
            <ChevronDown className="h-4 w-4 text-muted-foreground transition-transform group-open:rotate-180" />
          </summary>
          <div className="flex flex-col gap-4 border-t border-border p-4">
            <RootCredentialStatus data={data} label="Root Access Key" field={data.manual.s3_access_key} />
            <RootCredentialStatus data={data} label="Root Secret Key" field={data.manual.s3_secret_key} />
            <AlertDialog open={generateRootOpen} onOpenChange={onGenerateRootOpenChange}>
              <Button
                type="button"
                variant="outline"
                disabled={generateRootDisabled}
                onClick={() => onGenerateRootOpenChange(true)}
              >
                {generateRootPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <KeyRound className="h-4 w-4" />}
                Rotate root key
              </Button>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Rotate root S3 credentials?</AlertDialogTitle>
                  <AlertDialogDescription>
                    Root credentials are config-backed recovery credentials. Existing S3 users are unchanged. Restart
                    SynapS3 after rotating root credentials.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel type="button">Cancel</AlertDialogCancel>
                  <AlertDialogAction type="button" onClick={onGenerateRoot}>
                    Rotate
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        </details>
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
  const updateUser = useUpdateS3User()
  const rotateUserSecret = useRotateS3UserSecret()
  const deleteUser = useDeleteS3User()
  const [createRole, setCreateRole] = useState<S3UserRole>('userplus')
  const [roleDrafts, setRoleDrafts] = useState<Record<string, S3UserRole>>({})
  const [deleteTarget, setDeleteTarget] = useState<S3User | null>(null)

  useEffect(() => {
    setRoleDrafts((current) => {
      const next: Record<string, S3UserRole> = {}
      for (const user of users) next[user.access_key] = current[user.access_key] ?? user.role
      return next
    })
  }, [users])

  const mutationError = createUser.error ?? updateUser.error ?? rotateUserSecret.error ?? deleteUser.error ?? null
  const errorMessage = error instanceof Error ? error.message : mutationError?.message

  function handleCreateUser() {
    if (!s3UsersAvailable || createUser.isPending) return
    createUser.mutate(
      { role: createRole },
      {
        onSuccess: (credentials: S3UserCredentials) => {
          onCredentials(credentials)
          setCreateRole('userplus')
        },
      }
    )
  }

  function handleUpdateUser(user: S3User) {
    const role = roleDrafts[user.access_key] ?? user.role
    if (!s3UsersAvailable || role === user.role) return
    updateUser.mutate({ accessKey: user.access_key, role })
  }

  function handleRotateUser(user: S3User) {
    if (!s3UsersAvailable) return
    rotateUserSecret.mutate(user.access_key, {
      onSuccess: (credentials) => onCredentials(credentials),
    })
  }

  function handleDeleteUser() {
    if (!deleteTarget || !s3UsersAvailable) return
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

          <div className="grid gap-3 md:grid-cols-[minmax(12rem,18rem)_auto] md:items-end">
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs text-muted-foreground">Role</Label>
              <RoleSelect
                value={createRole}
                disabled={!s3UsersAvailable || createUser.isPending}
                onChange={setCreateRole}
              />
            </div>
            <Button type="button" disabled={!s3UsersAvailable || createUser.isPending} onClick={handleCreateUser}>
              {createUser.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
              Create user
            </Button>
          </div>

          <div className="overflow-x-auto rounded-md border border-border">
            <table className="w-full min-w-[42rem] text-left text-sm">
              <thead className="border-b border-border bg-muted/40 text-xs text-muted-foreground">
                <tr>
                  <th className="px-3 py-2 font-medium">Access Key</th>
                  <th className="w-44 px-3 py-2 font-medium">Role</th>
                  <th className="w-32 px-3 py-2 text-right font-medium">Actions</th>
                </tr>
              </thead>
              <tbody>
                {!s3UsersAvailable ? (
                  <tr>
                    <td className="px-3 py-6 text-center text-muted-foreground" colSpan={3}>
                      User list unavailable.
                    </td>
                  </tr>
                ) : isLoading ? (
                  <tr>
                    <td className="px-3 py-6 text-center text-muted-foreground" colSpan={3}>
                      <Loader2 className="mx-auto h-5 w-5 animate-spin" />
                    </td>
                  </tr>
                ) : users.length === 0 ? (
                  <tr>
                    <td className="px-3 py-6 text-center text-muted-foreground" colSpan={3}>
                      No additional S3 users.
                    </td>
                  </tr>
                ) : (
                  users.map((user) => {
                    const draftRole = roleDrafts[user.access_key] ?? user.role
                    const updating = updateUser.isPending && updateUser.variables?.accessKey === user.access_key
                    const rotating = rotateUserSecret.isPending && rotateUserSecret.variables === user.access_key
                    const deleting = deleteUser.isPending && deleteUser.variables === user.access_key
                    return (
                      <tr key={user.access_key} className="border-b border-border last:border-b-0">
                        <td className="max-w-0 px-3 py-2">
                          <code className="block truncate text-xs">{user.access_key}</code>
                        </td>
                        <td className="px-3 py-2">
                          <RoleSelect
                            value={draftRole}
                            disabled={!s3UsersAvailable || updating}
                            onChange={(role) => setRoleDrafts((current) => ({ ...current, [user.access_key]: role }))}
                          />
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex justify-end gap-1">
                            <IconActionButton
                              label="Update role"
                              disabled={!s3UsersAvailable || draftRole === user.role || updating}
                              onClick={() => handleUpdateUser(user)}
                              icon={updating ? Loader2 : Save}
                              spinning={updating}
                            />
                            <IconActionButton
                              label="Rotate secret"
                              disabled={!s3UsersAvailable || rotating}
                              onClick={() => handleRotateUser(user)}
                              icon={rotating ? Loader2 : RotateCcw}
                              spinning={rotating}
                            />
                            <IconActionButton
                              label="Delete user"
                              variant="destructive"
                              disabled={!s3UsersAvailable || deleting}
                              onClick={() => setDeleteTarget(user)}
                              icon={deleting ? Loader2 : Trash2}
                              spinning={deleting}
                            />
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

function RootCredentialStatus({
  label,
  field,
  data,
}: {
  label: string
  field: { configured: boolean; field: string; env?: string }
  data: SettingsData
}) {
  const meta = data.metadata[field.field]
  const envOverride = data.env_managed[field.field] || field.env

  return (
    <div className="flex flex-col gap-1.5 rounded-md border border-border p-3">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="truncate text-sm font-medium">{meta?.label ?? label}</span>
          {meta && <InfoTooltip metadata={meta} />}
        </div>
        <span className={cn('text-xs font-medium', field.configured ? 'text-green-500' : 'text-yellow-500')}>
          {field.configured ? 'Configured' : 'Missing'}
        </span>
      </div>
      {envOverride ? (
        <p className="flex items-center gap-1 break-all text-xs text-yellow-600">
          <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
          Overridden by {envOverride}
        </p>
      ) : (
        <p className="break-all font-mono text-xs text-muted-foreground">{data.config_path}</p>
      )}
    </div>
  )
}

function RoleSelect({
  value,
  disabled,
  onChange,
}: {
  value: S3UserRole
  disabled?: boolean
  onChange: (value: S3UserRole) => void
}) {
  return (
    <select
      value={value}
      disabled={disabled}
      onChange={(event) => onChange(event.target.value as S3UserRole)}
      className="h-8 w-full rounded-lg border border-input bg-background px-2.5 py-1 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:pointer-events-none disabled:bg-input/50 disabled:opacity-50"
    >
      {s3UserRoles.map((role) => (
        <option key={role} value={role}>
          {role}
        </option>
      ))}
    </select>
  )
}

function IconActionButton({
  label,
  icon: Icon,
  spinning = false,
  variant = 'ghost',
  disabled,
  onClick,
}: {
  label: string
  icon: typeof Save
  spinning?: boolean
  variant?: React.ComponentProps<typeof Button>['variant']
  disabled?: boolean
  onClick: () => void
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button type="button" variant={variant} size="icon-sm" disabled={disabled} onClick={onClick} aria-label={label}>
          <Icon className={cn('h-4 w-4', spinning && 'animate-spin')} />
        </Button>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  )
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
