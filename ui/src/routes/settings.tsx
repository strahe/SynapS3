import { createFileRoute } from '@tanstack/react-router'
import { AlertTriangle, CheckCircle2, Loader2, Save } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import type { SettingsData, SettingsEditableConfig, SettingsFieldError, SettingsUpdatePayload } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useSettings, useUpdateSettings } from '@/hooks/queries'
import { cn } from '@/lib/utils'

export const Route = createFileRoute('/settings')({
  component: SettingsPage,
})

function SettingsPage() {
  const { data, isLoading, error } = useSettings()
  const updateSettings = useUpdateSettings()
  const [form, setForm] = useState<SettingsEditableConfig | null>(null)
  const formDirty = Boolean(form && data && JSON.stringify(form) !== JSON.stringify(data.config))

  useEffect(() => {
    if (data && (!form || !formDirty)) setForm(data.config)
  }, [data, form, formDirty])

  const fieldErrors = useMemo(() => toFieldErrorMap(data?.validation_errors ?? []), [data?.validation_errors])

  if (error) {
    return <div className="flex h-full items-center justify-center text-destructive">Failed to load settings</div>
  }

  if (isLoading || !data || !form) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  const submitDisabled = !data.writable || updateSettings.isPending

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!data || !form || submitDisabled) return
    updateSettings.mutate(buildSettingsPayload(form, data.env_managed), {
      onSuccess: (saved) => setForm(saved.config),
    })
  }

  return (
    <form className="space-y-6 p-6" onSubmit={handleSubmit}>
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold">Settings</h1>
          <p className="mt-1 break-all font-mono text-xs text-muted-foreground">{data.config_path}</p>
        </div>
        <Button type="submit" disabled={submitDisabled}>
          {updateSettings.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
          Save
        </Button>
      </div>

      <StatusBanners data={data} mutationError={updateSettings.error} />

      <Section title="Manual Credentials">
        <div className="grid gap-3 md:grid-cols-3">
          <ManualStatus label="S3 Access Key" field={data.manual.s3_access_key} configPath={data.config_path} />
          <ManualStatus label="S3 Secret Key" field={data.manual.s3_secret_key} configPath={data.config_path} />
          <ManualStatus
            label="Filecoin Private Key"
            field={data.manual.filecoin_private_key}
            configPath={data.config_path}
          />
        </div>
      </Section>

      <div className="grid gap-4 xl:grid-cols-2">
        <Section title="Server">
          <div className="grid gap-4 md:grid-cols-2">
            <TextField
              label="S3 Port"
              field="server.port"
              value={form.server.port}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, server: { ...form.server, port: value } })}
            />
            <NumberField
              label="Max Connections"
              field="server.max_connections"
              value={form.server.max_connections}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, server: { ...form.server, max_connections: value } })}
            />
            <NumberField
              label="Max Requests"
              field="server.max_requests"
              value={form.server.max_requests}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, server: { ...form.server, max_requests: value } })}
            />
            <CheckboxField
              label="TLS Enabled"
              field="server.tls.enabled"
              checked={form.server.tls.enabled}
              data={data}
              onChange={(checked) =>
                setForm({ ...form, server: { ...form.server, tls: { ...form.server.tls, enabled: checked } } })
              }
            />
            <TextField
              label="TLS Cert File"
              field="server.tls.cert_file"
              value={form.server.tls.cert_file}
              data={data}
              errors={fieldErrors}
              onChange={(value) =>
                setForm({ ...form, server: { ...form.server, tls: { ...form.server.tls, cert_file: value } } })
              }
            />
            <TextField
              label="TLS Key File"
              field="server.tls.key_file"
              value={form.server.tls.key_file}
              data={data}
              errors={fieldErrors}
              onChange={(value) =>
                setForm({ ...form, server: { ...form.server, tls: { ...form.server.tls, key_file: value } } })
              }
            />
          </div>
        </Section>

        <Section title="Filecoin">
          <div className="grid gap-4 md:grid-cols-2">
            <SelectField
              label="Network"
              field="filecoin.network"
              value={form.filecoin.network}
              options={['calibration', 'mainnet']}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, filecoin: { ...form.filecoin, network: value } })}
            />
            <TextField
              label="RPC URL"
              field="filecoin.rpc_url"
              value={form.filecoin.rpc_url}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, filecoin: { ...form.filecoin, rpc_url: value } })}
            />
            <TextField
              label="Source"
              field="filecoin.source"
              value={form.filecoin.source}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, filecoin: { ...form.filecoin, source: value } })}
            />
            <div className="grid gap-3">
              <CheckboxField
                label="Use CDN"
                field="filecoin.with_cdn"
                checked={form.filecoin.with_cdn}
                data={data}
                onChange={(checked) => setForm({ ...form, filecoin: { ...form.filecoin, with_cdn: checked } })}
              />
              <CheckboxField
                label="Allow Private Networks"
                field="filecoin.allow_private_networks"
                checked={form.filecoin.allow_private_networks}
                data={data}
                onChange={(checked) =>
                  setForm({ ...form, filecoin: { ...form.filecoin, allow_private_networks: checked } })
                }
              />
            </div>
          </div>
        </Section>

        <Section title="S3">
          <TextField
            label="Region"
            field="s3.region"
            value={form.s3.region}
            data={data}
            errors={fieldErrors}
            onChange={(value) => setForm({ ...form, s3: { ...form.s3, region: value } })}
          />
        </Section>

        <Section title="Cache">
          <div className="grid gap-4 md:grid-cols-2">
            <TextField
              label="Directory"
              field="cache.dir"
              value={form.cache.dir}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, cache: { ...form.cache, dir: value } })}
            />
            <NumberField
              label="Max Size GB"
              field="cache.max_size_gb"
              value={form.cache.max_size_gb}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, cache: { ...form.cache, max_size_gb: value } })}
            />
            <SelectField
              label="Eviction Policy"
              field="cache.eviction_policy"
              value={form.cache.eviction_policy}
              options={['lru', 'manual', 'none']}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, cache: { ...form.cache, eviction_policy: value } })}
            />
          </div>
        </Section>

        <WorkerSection
          title="Upload Worker"
          prefix="worker.upload"
          value={form.worker.upload}
          data={data}
          errors={fieldErrors}
          onChange={(value) => setForm({ ...form, worker: { ...form.worker, upload: value } })}
        />

        <WorkerSection
          title="Evictor Worker"
          prefix="worker.evictor"
          value={form.worker.evictor}
          data={data}
          errors={fieldErrors}
          onChange={(value) => setForm({ ...form, worker: { ...form.worker, evictor: value } })}
        />

        <Section title="Logging">
          <div className="grid gap-4 md:grid-cols-2">
            <SelectField
              label="Level"
              field="logging.level"
              value={form.logging.level}
              options={['debug', 'info', 'warn', 'error']}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, logging: { ...form.logging, level: value } })}
            />
            <SelectField
              label="Format"
              field="logging.format"
              value={form.logging.format}
              options={['json', 'text']}
              data={data}
              errors={fieldErrors}
              onChange={(value) => setForm({ ...form, logging: { ...form.logging, format: value } })}
            />
          </div>
        </Section>

        <Section title="Manual Runtime">
          <dl className="space-y-3 text-sm">
            <ReadOnlyRow label="Database Driver" value={data.manual.database.driver} />
            <ReadOnlyRow label="Database DSN" value={data.manual.database.dsn_configured ? 'Configured' : 'Missing'} />
            <ReadOnlyRow
              label="Database Pool"
              value={`${data.manual.database.max_idle_conns}/${data.manual.database.max_open_conns}`}
            />
            <ReadOnlyRow label="Admin Address" value={data.manual.admin.addr_configured ? 'Configured' : 'Missing'} />
          </dl>
        </Section>
      </div>
    </form>
  )
}

function StatusBanners({ data, mutationError }: { data: SettingsData; mutationError: Error | null }) {
  return (
    <div className="space-y-3">
      {data.mode === 'setup' && (
        <Banner tone="warning" icon={AlertTriangle}>
          Setup mode is active. Save non-secret settings here, configure credentials outside the browser, then restart.
        </Banner>
      )}
      {!data.writable && (
        <Banner tone="danger" icon={AlertTriangle}>
          Settings writes are disabled because the admin server is not bound to a loopback address.
        </Banner>
      )}
      {data.restart_required && (
        <Banner tone="success" icon={CheckCircle2}>
          Settings were saved. Restart SynapS3 to apply runtime changes.
        </Banner>
      )}
      {mutationError && (
        <Banner tone="danger" icon={AlertTriangle}>
          {mutationError.message}
        </Banner>
      )}
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
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
        onChange={(e) => onChange(e.target.value)}
      />
    </FieldShell>
  )
}

function NumberField({
  label,
  field,
  value,
  data,
  errors,
  onChange,
}: {
  label: string
  field: string
  value: number
  data: SettingsData
  errors: Record<string, string>
  onChange: (value: number) => void
}) {
  const disabled = fieldDisabled(data, field)
  return (
    <FieldShell label={label} field={field} data={data} errors={errors}>
      <Input
        type="number"
        value={value}
        disabled={disabled}
        aria-invalid={Boolean(errors[field])}
        onChange={(e) => onChange(Number(e.target.value))}
      />
    </FieldShell>
  )
}

function SelectField({
  label,
  field,
  value,
  options,
  data,
  errors,
  onChange,
}: {
  label: string
  field: string
  value: string
  options: string[]
  data: SettingsData
  errors: Record<string, string>
  onChange: (value: string) => void
}) {
  const disabled = fieldDisabled(data, field)
  return (
    <FieldShell label={label} field={field} data={data} errors={errors}>
      <select
        value={value}
        disabled={disabled}
        aria-invalid={Boolean(errors[field])}
        onChange={(e) => onChange(e.target.value)}
        className="h-8 w-full rounded-lg border border-input bg-background px-2.5 py-1 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:pointer-events-none disabled:bg-input/50 disabled:opacity-50"
      >
        {options.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    </FieldShell>
  )
}

function CheckboxField({
  label,
  field,
  checked,
  data,
  onChange,
}: {
  label: string
  field: string
  checked: boolean
  data: SettingsData
  onChange: (checked: boolean) => void
}) {
  const disabled = fieldDisabled(data, field)
  return (
    <label className="flex min-h-8 items-center gap-2 rounded-md border border-border px-2.5 py-1.5 text-sm">
      <input type="checkbox" checked={checked} disabled={disabled} onChange={(e) => onChange(e.target.checked)} />
      <span>{label}</span>
      {data.env_managed[field] && (
        <span className="ml-auto truncate text-xs text-muted-foreground">{data.env_managed[field]}</span>
      )}
    </label>
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
  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between gap-2">
        <Label className="text-xs text-muted-foreground">{label}</Label>
        {data.env_managed[field] && (
          <span className="truncate text-xs text-muted-foreground">{data.env_managed[field]}</span>
        )}
      </div>
      {children}
      {errors[field] && <p className="text-xs text-destructive">{errors[field]}</p>}
    </div>
  )
}

function WorkerSection({
  title,
  prefix,
  value,
  data,
  errors,
  onChange,
}: {
  title: string
  prefix: 'worker.upload' | 'worker.evictor'
  value: SettingsEditableConfig['worker']['upload']
  data: SettingsData
  errors: Record<string, string>
  onChange: (value: SettingsEditableConfig['worker']['upload']) => void
}) {
  return (
    <Section title={title}>
      <div className="grid gap-4 md:grid-cols-3">
        <NumberField
          label="Concurrency"
          field={`${prefix}.concurrency`}
          value={value.concurrency}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, concurrency: next })}
        />
        <TextField
          label="Poll Interval"
          field={`${prefix}.poll_interval`}
          value={value.poll_interval}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, poll_interval: next })}
        />
        <NumberField
          label="Max Retries"
          field={`${prefix}.max_retries`}
          value={value.max_retries}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, max_retries: next })}
        />
      </div>
    </Section>
  )
}

function ManualStatus({
  label,
  field,
  configPath,
}: {
  label: string
  field: { configured: boolean; env?: string }
  configPath: string
}) {
  return (
    <div className="rounded-md border border-border p-3">
      <div className="flex items-center justify-between gap-2">
        <span className="text-sm font-medium">{label}</span>
        <span className={cn('text-xs font-medium', field.configured ? 'text-green-500' : 'text-yellow-500')}>
          {field.configured ? 'Configured' : 'Missing'}
        </span>
      </div>
      <p className="mt-2 break-all font-mono text-xs text-muted-foreground">{field.env ?? configPath}</p>
    </div>
  )
}

function ReadOnlyRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="grid gap-2 sm:grid-cols-[150px_1fr]">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="break-all font-mono">{value}</dd>
    </div>
  )
}

function Banner({
  tone,
  icon: Icon,
  children,
}: {
  tone: 'warning' | 'danger' | 'success'
  icon: typeof AlertTriangle
  children: React.ReactNode
}) {
  const classes = {
    warning: 'border-yellow-500/30 bg-yellow-500/10 text-yellow-600 dark:text-yellow-500',
    danger: 'border-destructive/30 bg-destructive/10 text-destructive',
    success: 'border-green-500/30 bg-green-500/10 text-green-600 dark:text-green-500',
  }

  return (
    <div className={cn('flex items-start gap-3 rounded-lg border p-3 text-sm', classes[tone])}>
      <Icon className="mt-0.5 h-4 w-4 shrink-0" />
      <div>{children}</div>
    </div>
  )
}

function toFieldErrorMap(errors: SettingsFieldError[]) {
  const out: Record<string, string> = {}
  for (const error of errors) out[error.field] = error.message
  return out
}

function fieldDisabled(data: SettingsData, field: string) {
  return !data.writable || Boolean(data.env_managed[field])
}

function buildSettingsPayload(form: SettingsEditableConfig, envManaged: Record<string, string>): SettingsUpdatePayload {
  const include = (field: string) => !envManaged[field]
  const payload: SettingsUpdatePayload = {}

  payload.server = {}
  if (include('server.port')) payload.server.port = form.server.port
  if (include('server.max_connections')) payload.server.max_connections = form.server.max_connections
  if (include('server.max_requests')) payload.server.max_requests = form.server.max_requests
  payload.server.tls = {}
  if (include('server.tls.enabled')) payload.server.tls.enabled = form.server.tls.enabled
  if (include('server.tls.cert_file')) payload.server.tls.cert_file = form.server.tls.cert_file
  if (include('server.tls.key_file')) payload.server.tls.key_file = form.server.tls.key_file

  payload.s3 = {}
  if (include('s3.region')) payload.s3.region = form.s3.region

  payload.filecoin = {}
  if (include('filecoin.network')) payload.filecoin.network = form.filecoin.network
  if (include('filecoin.rpc_url')) payload.filecoin.rpc_url = form.filecoin.rpc_url
  if (include('filecoin.source')) payload.filecoin.source = form.filecoin.source
  if (include('filecoin.with_cdn')) payload.filecoin.with_cdn = form.filecoin.with_cdn
  if (include('filecoin.allow_private_networks'))
    payload.filecoin.allow_private_networks = form.filecoin.allow_private_networks

  payload.cache = {}
  if (include('cache.dir')) payload.cache.dir = form.cache.dir
  if (include('cache.max_size_gb')) payload.cache.max_size_gb = form.cache.max_size_gb
  if (include('cache.eviction_policy')) payload.cache.eviction_policy = form.cache.eviction_policy

  const upload: NonNullable<NonNullable<SettingsUpdatePayload['worker']>['upload']> = {}
  const evictor: NonNullable<NonNullable<SettingsUpdatePayload['worker']>['evictor']> = {}
  if (include('worker.upload.concurrency')) upload.concurrency = form.worker.upload.concurrency
  if (include('worker.upload.poll_interval')) upload.poll_interval = form.worker.upload.poll_interval
  if (include('worker.upload.max_retries')) upload.max_retries = form.worker.upload.max_retries
  if (include('worker.evictor.concurrency')) evictor.concurrency = form.worker.evictor.concurrency
  if (include('worker.evictor.poll_interval')) evictor.poll_interval = form.worker.evictor.poll_interval
  if (include('worker.evictor.max_retries')) evictor.max_retries = form.worker.evictor.max_retries
  payload.worker = { upload, evictor }

  payload.logging = {}
  if (include('logging.level')) payload.logging.level = form.logging.level
  if (include('logging.format')) payload.logging.format = form.logging.format

  return payload
}
