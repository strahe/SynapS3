import { AlertTriangle, Info } from 'lucide-react'
import { type ReactNode, useId } from 'react'
import type { SettingsData } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { Field, FieldDescription, FieldError, FieldLabel, FieldTitle } from '@/components/ui/field'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

export function SettingsSection({
  title,
  action,
  children,
}: {
  title: string
  action?: ReactNode
  children: ReactNode
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        {action && <CardAction>{action}</CardAction>}
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  )
}

const bannerToneClasses: Record<'warning' | 'danger' | 'success', { alert?: string; description?: string }> = {
  warning: {
    alert:
      'border-[color:var(--status-warning-border)] bg-[var(--status-warning-bg)] text-[color:var(--status-warning)]',
    description: 'text-[color:var(--status-warning)]',
  },
  danger: {},
  success: {
    alert:
      'border-[color:var(--status-success-border)] bg-[var(--status-success-bg)] text-[color:var(--status-success)]',
    description: 'text-[color:var(--status-success)]',
  },
}

export function SettingsBanner({
  tone,
  icon: Icon,
  title,
  children,
}: {
  tone: 'warning' | 'danger' | 'success'
  icon: typeof AlertTriangle
  title?: ReactNode
  children: ReactNode
}) {
  const toneClass = bannerToneClasses[tone]

  return (
    <Alert variant={tone === 'danger' ? 'destructive' : 'default'} className={toneClass.alert}>
      <Icon />
      {title && <AlertTitle>{title}</AlertTitle>}
      <AlertDescription className={cn(toneClass.description)}>{children}</AlertDescription>
    </Alert>
  )
}

export function SettingsFieldShell({
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
  children: ReactNode
}) {
  const meta = data.metadata[field]
  const envOverride = data.env_managed[field]
  const displayLabel = meta?.label ?? label
  const error = errors[field]

  return (
    <Field data-invalid={Boolean(error)} data-disabled={!data.writable || Boolean(envOverride)}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <FieldLabel className="truncate text-xs text-muted-foreground">{displayLabel}</FieldLabel>
          {meta && <InfoTooltip metadata={meta} />}
        </div>
        {envOverride && <EnvOverrideBadge env={envOverride} />}
      </div>
      {children}
      {envOverride && (
        <FieldDescription className="flex items-center gap-1">
          <AlertTriangle className="size-3.5" />
          Overridden by {envOverride}
        </FieldDescription>
      )}
      {error && <FieldError>{error}</FieldError>}
    </Field>
  )
}

export function SettingsSelect({
  id,
  value,
  options,
  disabled,
  invalid,
  onChange,
}: {
  id?: string
  value: string
  options: Array<string | { value: string; label: string }>
  disabled?: boolean
  invalid?: boolean
  onChange: (value: string) => void
}) {
  return (
    <Select value={value} onValueChange={onChange} disabled={disabled}>
      <SelectTrigger id={id} className="w-full" aria-invalid={invalid}>
        <SelectValue />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {options.map((option) => {
            const value = typeof option === 'string' ? option : option.value
            const label = typeof option === 'string' ? option : option.label
            return (
              <SelectItem key={value} value={value}>
                {label}
              </SelectItem>
            )
          })}
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}

export function SettingsCheckbox({
  checked,
  disabled,
  invalid,
  label,
  onChange,
}: {
  checked: boolean
  disabled?: boolean
  invalid?: boolean
  label: string
  onChange: (checked: boolean) => void
}) {
  const generatedId = useId()

  return (
    <div className="flex min-h-8 items-center gap-2 text-sm">
      <Checkbox
        id={generatedId}
        checked={checked}
        disabled={disabled}
        aria-invalid={invalid}
        onCheckedChange={(next) => onChange(next === true)}
      />
      <FieldLabel htmlFor={generatedId}>{label}</FieldLabel>
    </div>
  )
}

export function SettingsReadOnlyField({ data, field, value }: { data: SettingsData; field: string; value: string }) {
  const meta = data.metadata[field]
  const envOverride = data.env_managed[field]

  return (
    <Field data-disabled={Boolean(envOverride)}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <FieldLabel className="truncate text-xs text-muted-foreground">{meta?.label ?? field}</FieldLabel>
          {meta && <InfoTooltip metadata={meta} />}
        </div>
        {envOverride && <EnvOverrideBadge env={envOverride} />}
      </div>
      <Input readOnly value={value} className="font-mono text-sm" />
      {envOverride && (
        <FieldDescription className="flex items-center gap-1">
          <AlertTriangle className="size-3.5" />
          Overridden by {envOverride}
        </FieldDescription>
      )}
    </Field>
  )
}

export function SettingsValueField({
  label,
  value,
  copy = false,
  mono = false,
}: {
  label: string
  value: string
  copy?: boolean
  mono?: boolean
}) {
  return (
    <Field>
      <FieldLabel className="text-xs text-muted-foreground">{label}</FieldLabel>
      {copy ? (
        <div className="flex min-h-8 min-w-0 items-center rounded-md border border-input bg-background px-2 py-1">
          <CopyableValue label={label} value={value} monospace={mono} maxLength={36} />
        </div>
      ) : (
        <Input readOnly value={value} className={mono ? 'font-mono text-xs' : undefined} />
      )}
    </Field>
  )
}

export function SettingsStatusField({
  label,
  metadata,
  status,
  children,
}: {
  label: ReactNode
  metadata?: SettingsData['metadata'][string]
  status: ReactNode
  children?: ReactNode
}) {
  return (
    <Field className="rounded-lg border border-border p-3">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <FieldTitle className="truncate text-sm font-medium">{label}</FieldTitle>
          {metadata && <InfoTooltip metadata={metadata} />}
        </div>
        {status}
      </div>
      {children && <FieldDescription className="break-all text-xs">{children}</FieldDescription>}
    </Field>
  )
}

export function InfoTooltip({ metadata }: { metadata: SettingsData['metadata'][string] }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button type="button" variant="ghost" size="icon-xs" aria-label="Field info">
          <Info />
        </Button>
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

export function EnvOverrideBadge({ env }: { env: string }) {
  return (
    <StatusBadge tone="warning" className="max-w-48">
      <AlertTriangle data-icon="inline-start" />
      <span className="truncate">{env}</span>
    </StatusBadge>
  )
}
