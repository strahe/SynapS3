import { createFileRoute } from '@tanstack/react-router'
import { AlertTriangle, Check, CheckCircle2, Copy, Info, Loader2, Save } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import type {
  SettingsData,
  SettingsEditableConfig,
  SettingsFieldError,
  SettingsS3Credentials,
  SettingsUpdatePayload,
} from '@/api/client'
import { S3SettingsPanel } from '@/components/settings/S3SettingsPanel'
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
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useGenerateS3Credentials, useSettings, useUpdateSettings } from '@/hooks/queries'
import { cn } from '@/lib/utils'

export const Route = createFileRoute('/settings')({
  component: SettingsPage,
})

const tabFields = {
  s3: ['s3.access_key', 's3.secret_key', 's3.region', 's3.iam_dir'],
  server: [
    'server.port',
    'server.max_connections',
    'server.max_requests',
    'server.tls.enabled',
    'server.tls.cert_file',
    'server.tls.key_file',
  ],
  filecoin: [
    'filecoin.network',
    'filecoin.rpc_url',
    'filecoin.private_key',
    'filecoin.source',
    'filecoin.with_cdn',
    'filecoin.allow_private_networks',
  ],
  cache: ['cache.dir', 'cache.max_size_gb', 'cache.eviction_policy'],
  workers: [
    'worker.upload.concurrency',
    'worker.upload.poll_interval',
    'worker.upload.max_retries',
    'worker.evictor.concurrency',
    'worker.evictor.poll_interval',
    'worker.evictor.max_retries',
  ],
  logging: ['logging.level', 'logging.format'],
  runtime: ['database.driver', 'database.dsn', 'database.max_open_conns', 'database.max_idle_conns', 'admin.addr'],
} as const

function SettingsPage() {
  const { data, isLoading, error } = useSettings()
  const updateSettings = useUpdateSettings()
  const generateS3Credentials = useGenerateS3Credentials()
  const [form, setForm] = useState<SettingsEditableConfig | null>(null)
  const [generatedCredentials, setGeneratedCredentials] = useState<SettingsS3Credentials | null>(null)
  const [confirmGenerateOpen, setConfirmGenerateOpen] = useState(false)
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
  const generateDisabled =
    !data.writable ||
    generateS3Credentials.isPending ||
    Boolean(data.env_managed['s3.access_key'] || data.env_managed['s3.secret_key'])

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!data || !form || submitDisabled) return
    updateSettings.mutate(buildSettingsPayload(form, data.config, data.env_managed), {
      onSuccess: (saved) => setForm(saved.config),
    })
  }

  function handleGenerateS3Credentials() {
    if (generateDisabled) return
    generateS3Credentials.mutate(undefined, {
      onSuccess: (result) => {
        setForm((current) => {
          if (!current || !data) return result.settings.config
          return JSON.stringify(current) === JSON.stringify(data.config) ? result.settings.config : current
        })
        setGeneratedCredentials(result.credentials)
        setConfirmGenerateOpen(false)
      },
    })
  }

  function handleFilecoinNetworkChange(network: string) {
    if (!data) return
    const defaults = data.defaults.filecoin_rpc_urls
    const rpcURLLocked = Boolean(data.env_managed['filecoin.rpc_url'])

    setForm((current) => {
      if (!current) return current

      const currentNetwork = normalizeNetworkName(current.filecoin.network)
      const nextNetwork = normalizeNetworkName(network)
      const currentRPCURL = current.filecoin.rpc_url.trim()
      const previousDefaultRPCURL = defaults[currentNetwork]
      const nextDefaultRPCURL = defaults[nextNetwork]
      const currentRPCURLIsDefault = currentRPCURL === '' || currentRPCURL === previousDefaultRPCURL

      return {
        ...current,
        filecoin: {
          ...current.filecoin,
          network,
          rpc_url:
            !rpcURLLocked && nextDefaultRPCURL && currentRPCURLIsDefault ? nextDefaultRPCURL : current.filecoin.rpc_url,
        },
      }
    })
  }

  return (
    <form className="flex flex-col gap-6 p-6" onSubmit={handleSubmit}>
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

      <StatusBanners data={data} mutationError={updateSettings.error ?? generateS3Credentials.error ?? null} />

      <Tabs defaultValue="s3" className="gap-4">
        <TabsList className="w-full justify-start overflow-x-auto">
          <SettingsTabTrigger value="s3" label="S3" data={data} errors={fieldErrors} missing={s3Missing(data)} />
          <SettingsTabTrigger value="server" label="Server" data={data} errors={fieldErrors} />
          <SettingsTabTrigger
            value="filecoin"
            label="Filecoin"
            data={data}
            errors={fieldErrors}
            missing={!data.secrets.filecoin_private_key_configured}
          />
          <SettingsTabTrigger value="cache" label="Cache" data={data} errors={fieldErrors} />
          <SettingsTabTrigger value="workers" label="Workers" data={data} errors={fieldErrors} />
          <SettingsTabTrigger value="logging" label="Logging" data={data} errors={fieldErrors} />
          <SettingsTabTrigger value="runtime" label="Runtime" data={data} errors={fieldErrors} />
        </TabsList>

        <TabsContent value="s3">
          <S3SettingsPanel
            data={data}
            value={form.s3}
            errors={fieldErrors}
            generateRootOpen={confirmGenerateOpen}
            generateRootDisabled={generateDisabled}
            generateRootPending={generateS3Credentials.isPending}
            onChange={(s3) => setForm({ ...form, s3 })}
            onCredentials={setGeneratedCredentials}
            onGenerateRoot={handleGenerateS3Credentials}
            onGenerateRootOpenChange={setConfirmGenerateOpen}
          />
        </TabsContent>

        <TabsContent value="server">
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
                errors={fieldErrors}
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
        </TabsContent>

        <TabsContent value="filecoin">
          <Section title="Filecoin">
            <div className="grid gap-4 md:grid-cols-2">
              <CredentialStatusCard data={data} label="Filecoin Private Key" field={data.manual.filecoin_private_key} />
              <SelectField
                label="Network"
                field="filecoin.network"
                value={form.filecoin.network}
                options={['calibration', 'mainnet']}
                data={data}
                errors={fieldErrors}
                onChange={handleFilecoinNetworkChange}
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
              <CheckboxField
                label="Use CDN"
                field="filecoin.with_cdn"
                checked={form.filecoin.with_cdn}
                data={data}
                errors={fieldErrors}
                onChange={(checked) => setForm({ ...form, filecoin: { ...form.filecoin, with_cdn: checked } })}
              />
              <CheckboxField
                label="Allow Private Networks"
                field="filecoin.allow_private_networks"
                checked={form.filecoin.allow_private_networks}
                data={data}
                errors={fieldErrors}
                onChange={(checked) =>
                  setForm({ ...form, filecoin: { ...form.filecoin, allow_private_networks: checked } })
                }
              />
            </div>
          </Section>
        </TabsContent>

        <TabsContent value="cache">
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
        </TabsContent>

        <TabsContent value="workers">
          <div className="grid gap-4 xl:grid-cols-2">
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
          </div>
        </TabsContent>

        <TabsContent value="logging">
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
        </TabsContent>

        <TabsContent value="runtime">
          <Section title="Runtime">
            <div className="grid gap-4 md:grid-cols-2">
              <ReadOnlyRow data={data} field="database.driver" value={data.manual.database.driver} />
              <ReadOnlyRow
                data={data}
                field="database.dsn"
                value={data.manual.database.dsn_configured ? 'Configured' : 'Missing'}
              />
              <ReadOnlyRow
                data={data}
                field="database.max_idle_conns"
                value={`${data.manual.database.max_idle_conns}/${data.manual.database.max_open_conns}`}
              />
              <ReadOnlyRow
                data={data}
                field="admin.addr"
                value={data.manual.admin.addr_configured ? 'Configured' : 'Missing'}
              />
            </div>
          </Section>
        </TabsContent>
      </Tabs>

      <GeneratedCredentialsDialog
        credentials={generatedCredentials}
        onOpenChange={(open) => !open && setGeneratedCredentials(null)}
      />
    </form>
  )
}

function StatusBanners({ data, mutationError }: { data: SettingsData; mutationError: Error | null }) {
  return (
    <div className="flex flex-col gap-3">
      {data.mode === 'setup' && (
        <Banner tone="warning" icon={AlertTriangle}>
          Setup mode is active. Save settings here, then restart the service.
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

function SettingsTabTrigger({
  value,
  label,
  data,
  errors,
  missing = false,
}: {
  value: keyof typeof tabFields
  label: string
  data: SettingsData
  errors: Record<string, string>
  missing?: boolean
}) {
  const hasWarning = missing || tabFields[value].some((field) => Boolean(errors[field] || data.env_managed[field]))

  return (
    <TabsTrigger value={value}>
      {label}
      {hasWarning && <AlertTriangle className="h-3.5 w-3.5 text-yellow-500" aria-label={`${label} needs attention`} />}
    </TabsTrigger>
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
  errors,
  onChange,
}: {
  label: string
  field: string
  checked: boolean
  data: SettingsData
  errors: Record<string, string>
  onChange: (checked: boolean) => void
}) {
  const disabled = fieldDisabled(data, field)
  return (
    <FieldShell label={label} field={field} data={data} errors={errors}>
      <label className="flex min-h-8 items-center gap-2 rounded-md border border-border px-2.5 py-1.5 text-sm">
        <input type="checkbox" checked={checked} disabled={disabled} onChange={(e) => onChange(e.target.checked)} />
        <span>{data.metadata[field]?.label ?? label}</span>
      </label>
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
      <div className="grid gap-4 md:grid-cols-3 xl:grid-cols-1">
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

function CredentialStatusCard({
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
  const setupHint = credentialSetupHint(data, field, meta)
  return (
    <div className="rounded-md border border-border p-3">
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
        <p className="mt-2 flex items-center gap-1 break-all text-xs text-yellow-600">
          <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
          Overridden by {envOverride}
        </p>
      ) : setupHint ? (
        <p className="mt-2 break-all text-xs text-muted-foreground">{setupHint}</p>
      ) : (
        <p className="mt-2 break-all font-mono text-xs text-muted-foreground">{data.config_path}</p>
      )}
    </div>
  )
}

function credentialSetupHint(
  data: SettingsData,
  field: { configured: boolean; field: string; env?: string },
  metadata?: SettingsData['metadata'][string]
) {
  if (field.configured || metadata?.editable !== false) return ''
  const env = metadata?.env || field.env
  const envHint = env ? ` or set ${env}` : ''
  return `Set ${field.field} in ${data.config_path}${envHint}, then restart SynapS3.`
}

function ReadOnlyRow({ data, field, value }: { data: SettingsData; field: string; value: string }) {
  const meta = data.metadata[field]
  const envOverride = data.env_managed[field]
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-1.5">
          <Label className="truncate text-xs text-muted-foreground">{meta?.label ?? field}</Label>
          {meta && <InfoTooltip metadata={meta} />}
        </div>
        {envOverride && <EnvOverrideBadge env={envOverride} />}
      </div>
      <div className="min-h-8 rounded-lg border border-input bg-muted/40 px-2.5 py-1 font-mono text-sm break-all">
        {value}
      </div>
      {envOverride && (
        <p className="flex items-center gap-1 text-xs text-yellow-600">
          <AlertTriangle className="h-3.5 w-3.5" />
          Overridden by {envOverride}
        </p>
      )}
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
            <CopyableSecret label="Access Key" value={credentials.access_key} />
            <CopyableSecret label="Secret Key" value={credentials.secret_key} />
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

function CopyableSecret({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [])

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(value)
      setCopied(true)
      if (timerRef.current) clearTimeout(timerRef.current)
      timerRef.current = setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard API can be unavailable in some browser contexts.
    }
  }

  return (
    <div className="flex flex-col gap-1.5">
      <Label className="text-xs text-muted-foreground">{label}</Label>
      <div className="flex items-center gap-2 rounded-md border border-border bg-muted/40 p-2">
        <code className="min-w-0 flex-1 break-all text-xs">{value}</code>
        <Button type="button" variant="ghost" size="icon-sm" onClick={handleCopy} aria-label={`Copy ${label}`}>
          {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
        </Button>
      </div>
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
    warning: 'border-yellow-500/30 bg-yellow-500/10 text-yellow-600',
    danger: 'border-destructive/30 bg-destructive/10 text-destructive',
    success: 'border-green-500/30 bg-green-500/10 text-green-600',
  }

  return (
    <div className={cn('flex items-start gap-3 rounded-lg border p-3 text-sm', classes[tone])}>
      <Icon className="mt-0.5 h-4 w-4 shrink-0" />
      <div>{children}</div>
    </div>
  )
}

function s3Missing(data: SettingsData) {
  return !data.secrets.s3_access_key_configured || !data.secrets.s3_secret_key_configured
}

function toFieldErrorMap(errors: SettingsFieldError[]) {
  const out: Record<string, string> = {}
  for (const error of errors) out[error.field] = error.message
  return out
}

function fieldDisabled(data: SettingsData, field: string) {
  return !data.writable || Boolean(data.env_managed[field])
}

function normalizeNetworkName(network: string) {
  return network.trim().toLowerCase()
}

function buildSettingsPayload(
  form: SettingsEditableConfig,
  initial: SettingsEditableConfig,
  envManaged: Record<string, string>
): SettingsUpdatePayload {
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
  if (include('s3.iam_dir') && form.s3.iam_dir !== initial.s3.iam_dir) payload.s3.iam_dir = form.s3.iam_dir

  payload.filecoin = {}
  if (include('filecoin.network')) payload.filecoin.network = form.filecoin.network
  if (include('filecoin.rpc_url')) payload.filecoin.rpc_url = form.filecoin.rpc_url
  if (include('filecoin.source')) payload.filecoin.source = form.filecoin.source
  if (include('filecoin.with_cdn')) payload.filecoin.with_cdn = form.filecoin.with_cdn
  if (include('filecoin.allow_private_networks'))
    payload.filecoin.allow_private_networks = form.filecoin.allow_private_networks

  payload.cache = {}
  if (include('cache.dir') && form.cache.dir !== initial.cache.dir) payload.cache.dir = form.cache.dir
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
