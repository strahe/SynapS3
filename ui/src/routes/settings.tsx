import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { AlertTriangle, CheckCircle2, Loader2, RefreshCw, Save, Undo2 } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import { toast } from 'sonner'
import type {
  FilecoinReadinessData,
  SettingsData,
  SettingsEditableConfig,
  SettingsFieldError,
  SettingsFieldMetadata,
  SettingsUpdatePayload,
} from '@/api/client'
import { DangerActionAlertDialog } from '@/components/app/DangerActionAlertDialog'
import { FilecoinReadinessDialog } from '@/components/app/FilecoinReadinessDialog'
import { PageHeader } from '@/components/app/PageHeader'
import { PageError, PageLoading } from '@/components/app/PageState'
import { StatusBadge } from '@/components/app/StatusBadge'
import {
  SettingsBanner as Banner,
  SettingsFieldShell as FieldShell,
  SettingsSection as Section,
  SettingsCheckbox,
  SettingsReadOnlyField,
  SettingsSelect,
  SettingsStatusField,
} from '@/components/settings/settings-form'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useFilecoinPreflight, useSettings, useUpdateSettings, useValidateSettings } from '@/hooks/queries'
import { settingsOptions } from '@/lib/display-labels'
import {
  buildFilecoinPreflightPayload,
  filecoinPreflightPayloadKey,
  filecoinReadinessStatusLabel,
  filecoinReadinessStatusTone,
  filecoinReadinessSummary,
} from '@/lib/filecoin-readiness'
import {
  classifySettingsRisk,
  collectSettingsRiskChanges,
  type SettingsRiskChange,
  settingsRiskNeedsStrongConfirmation,
} from '@/lib/risk-confirmation'
import { buildSettingsPayload } from '@/lib/settings-payload'
import {
  settingsRuntimeRestartBannerVisible,
  settingsSavedBannerVisible,
  settingsSetupBannerVisible,
} from '@/lib/settings-status'
import {
  activeSettingsValidationErrors,
  type SettingsValidationDraft,
  settingsDraftValidationEnabled,
  settingsValidationPayloadKey,
} from '@/lib/settings-validation'

type SettingsTab = keyof typeof tabFields

export const Route = createFileRoute('/settings')({
  validateSearch: (search: Record<string, unknown>): { tab?: SettingsTab } => ({
    tab: typeof search.tab === 'string' && search.tab in tabFields ? (search.tab as SettingsTab) : undefined,
  }),
  component: SettingsPage,
})

const tabFields = {
  's3-api': [
    'server.port',
    'server.max_connections',
    'server.max_requests',
    'server.tls.enabled',
    'server.tls.cert_file',
    'server.tls.key_file',
    's3.region',
  ],
  filecoin: [
    'filecoin.network',
    'filecoin.rpc_url',
    'filecoin.private_key',
    'filecoin.with_cdn',
    'filecoin.allow_private_networks',
    'filecoin.default_copies',
    'filecoin.anchor_provider_tier',
    'filecoin.observability.interval',
    'filecoin.observability.timeout',
    'filecoin.observability.concurrency',
  ],
  cache: [
    'cache.dir',
    'cache.max_size_gb',
    'cache.eviction_policy',
    'cache.lru_high_watermark_percent',
    'cache.lru_low_watermark_percent',
  ],
  workers: [
    'worker.tasks.concurrency',
    'worker.tasks.poll_interval',
    'worker.tasks.lease_duration',
    'worker.tasks.upload_concurrency',
    'worker.tasks.commit_max_pieces',
    'worker.tasks.commit_max_wait',
    'worker.tasks.commit_seal_on_cache_pressure',
    'worker.tasks.commit_max_backlog',
  ],
  logging: ['logging.level', 'logging.format', 'logging.s3_access.enabled', 'logging.s3_access.level'],
  runtime: ['database.driver', 'database.dsn', 'database.max_open_conns', 'database.max_idle_conns', 'admin.addr'],
} as const

function SettingsPage() {
  const search = Route.useSearch()
  const navigate = useNavigate()
  const tab = search.tab ?? 's3-api'
  const { data, isLoading, error, refetch, isFetching } = useSettings()
  const updateSettings = useUpdateSettings()
  const validateSettings = useValidateSettings()
  const filecoinPreflight = useFilecoinPreflight()
  const [form, setForm] = useState<SettingsEditableConfig | null>(null)
  const [draftValidation, setDraftValidation] = useState<SettingsValidationDraft | null>(null)
  const [checkedPreflightKey, setCheckedPreflightKey] = useState<string | null>(null)
  const [preflightDetailData, setPreflightDetailData] = useState<FilecoinReadinessData | null>(null)
  const [pendingSettingsPayload, setPendingSettingsPayload] = useState<SettingsUpdatePayload | null>(null)
  const [pendingRiskChanges, setPendingRiskChanges] = useState<SettingsRiskChange[]>([])
  const currentSettingsPayload = useMemo(
    () => (form && data ? buildSettingsPayload(form, data.config, data.env_managed) : null),
    [form, data]
  )
  const currentSettingsPayloadKey = useMemo(
    () => (currentSettingsPayload ? settingsValidationPayloadKey(currentSettingsPayload) : null),
    [currentSettingsPayload]
  )
  const currentSettingsPayloadKeyRef = useRef<string | null>(currentSettingsPayloadKey)
  const validateSettingsMutateRef = useRef(validateSettings.mutate)
  const formDirty = Boolean(form && data && JSON.stringify(form) !== JSON.stringify(data.config))
  const strongRiskConfirmation = settingsRiskNeedsStrongConfirmation(pendingRiskChanges)

  useEffect(() => {
    if (data && (!form || !formDirty)) setForm(data.config)
  }, [data, form, formDirty])

  useEffect(() => {
    currentSettingsPayloadKeyRef.current = currentSettingsPayloadKey
  }, [currentSettingsPayloadKey])

  useEffect(() => {
    validateSettingsMutateRef.current = validateSettings.mutate
  }, [validateSettings.mutate])

  useEffect(() => {
    const draftValidationInput = {
      writable: data?.writable,
      formDirty,
      payload: currentSettingsPayload,
      payloadKey: currentSettingsPayloadKey,
    }
    if (!settingsDraftValidationEnabled(draftValidationInput)) {
      setDraftValidation(null)
      return
    }

    const { payload, payloadKey } = draftValidationInput
    const timer = window.setTimeout(() => {
      validateSettingsMutateRef.current(payload, {
        onSuccess: (result) => {
          if (currentSettingsPayloadKeyRef.current !== payloadKey) return
          setDraftValidation({ payloadKey, validation_errors: result.validation_errors ?? [] })
        },
      })
    }, 300)

    return () => window.clearTimeout(timer)
  }, [currentSettingsPayload, currentSettingsPayloadKey, data?.writable, formDirty])

  const activeValidationErrors = useMemo(
    () => activeSettingsValidationErrors(data?.validation_errors ?? [], draftValidation, currentSettingsPayloadKey),
    [data?.validation_errors, draftValidation, currentSettingsPayloadKey]
  )
  const fieldErrors = useMemo(() => toFieldErrorMap(activeValidationErrors), [activeValidationErrors])
  const currentFilecoinPreflightPayload =
    form && data ? buildFilecoinPreflightPayload(form.filecoin, data.env_managed) : null
  const currentFilecoinPreflightKey = currentFilecoinPreflightPayload
    ? filecoinPreflightPayloadKey(currentFilecoinPreflightPayload)
    : null
  const preflightMatchesCurrentDraft = Boolean(
    checkedPreflightKey && currentFilecoinPreflightKey && checkedPreflightKey === currentFilecoinPreflightKey
  )

  useEffect(() => {
    if (preflightDetailData && !preflightMatchesCurrentDraft) setPreflightDetailData(null)
  }, [preflightDetailData, preflightMatchesCurrentDraft])

  if (error || isLoading || !data || !form) {
    return (
      <div className="flex flex-col gap-6 p-6">
        <PageHeader title="Settings" />
        {error ? (
          <PageError
            title="Failed to load settings"
            description={error.message}
            onRetry={() => refetch()}
            retrying={isFetching}
          />
        ) : (
          <PageLoading />
        )}
      </div>
    )
  }

  const submitDisabled = !data.writable || !formDirty || updateSettings.isPending

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!data || !form || !currentSettingsPayload || submitDisabled) return
    updateSettings.reset()
    const payload = currentSettingsPayload
    const riskChanges = collectSettingsRiskChanges(data.config, form, data.env_managed, data.metadata)
    if (riskChanges.length > 0) {
      setPendingSettingsPayload(payload)
      setPendingRiskChanges(riskChanges)
      return
    }
    saveSettings(payload)
  }

  function saveSettings(payload: SettingsUpdatePayload) {
    updateSettings.mutate(payload, {
      onSuccess: (saved) => {
        setForm(saved.config)
        toast.success('Settings saved')
      },
    })
  }

  function handleConfirmRiskSettings() {
    if (!pendingSettingsPayload) return
    updateSettings.mutate(pendingSettingsPayload, {
      onSuccess: (saved) => {
        setForm(saved.config)
        setPendingSettingsPayload(null)
        setPendingRiskChanges([])
        toast.success('Settings saved')
      },
    })
  }

  function discardChanges() {
    if (!data) return
    updateSettings.reset()
    setForm(data.config)
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

  function runFilecoinPreflight() {
    if (
      !currentFilecoinPreflightPayload ||
      !currentFilecoinPreflightKey ||
      !data?.writable ||
      filecoinPreflight.isPending
    ) {
      return
    }
    setCheckedPreflightKey(currentFilecoinPreflightKey)
    filecoinPreflight.mutate(currentFilecoinPreflightPayload)
  }

  return (
    <form className="flex flex-col gap-6 p-6" onSubmit={handleSubmit}>
      <PageHeader
        title="Settings"
        description={<span className="break-all font-mono text-xs">{data.config_path}</span>}
        actions={
          <>
            {formDirty && (
              <>
                <span className="text-sm text-muted-foreground">Unsaved changes</span>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={discardChanges}
                  disabled={updateSettings.isPending}
                >
                  <Undo2 data-icon="inline-start" />
                  Discard
                </Button>
              </>
            )}
            <Button type="submit" size="sm" disabled={submitDisabled}>
              {updateSettings.isPending ? (
                <Loader2 data-icon="inline-start" className="animate-spin" />
              ) : (
                <Save data-icon="inline-start" />
              )}
              Save
            </Button>
          </>
        }
      />

      <StatusBanners data={data} mutationError={updateSettings.error ?? null} />

      <Tabs
        value={tab}
        onValueChange={(next) => navigate({ to: '/settings', search: { tab: next as SettingsTab }, replace: true })}
        className="gap-4"
      >
        <TabsList className="max-w-full justify-start overflow-x-auto">
          <SettingsTabTrigger value="s3-api" label="S3 API" data={data} errors={fieldErrors} />
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

        <TabsContent value="s3-api">
          <Section title="S3 API">
            <div className="grid gap-4 md:grid-cols-2">
              <TextField
                label="Region"
                field="s3.region"
                value={form.s3.region}
                data={data}
                errors={fieldErrors}
                onChange={(region) => setForm({ ...form, s3: { ...form.s3, region } })}
              />
              <TextField
                label="S3 port"
                field="server.port"
                value={form.server.port}
                data={data}
                errors={fieldErrors}
                onChange={(value) => setForm({ ...form, server: { ...form.server, port: value } })}
              />
              <NumberField
                label="Max connections"
                field="server.max_connections"
                value={form.server.max_connections}
                data={data}
                errors={fieldErrors}
                onChange={(value) => setForm({ ...form, server: { ...form.server, max_connections: value } })}
              />
              <NumberField
                label="Max requests"
                field="server.max_requests"
                value={form.server.max_requests}
                data={data}
                errors={fieldErrors}
                onChange={(value) => setForm({ ...form, server: { ...form.server, max_requests: value } })}
              />
              <CheckboxField
                label="TLS enabled"
                field="server.tls.enabled"
                checked={form.server.tls.enabled}
                data={data}
                errors={fieldErrors}
                onChange={(checked) =>
                  setForm({ ...form, server: { ...form.server, tls: { ...form.server.tls, enabled: checked } } })
                }
              />
              <TextField
                label="TLS certificate file"
                field="server.tls.cert_file"
                value={form.server.tls.cert_file}
                data={data}
                errors={fieldErrors}
                onChange={(value) =>
                  setForm({ ...form, server: { ...form.server, tls: { ...form.server.tls, cert_file: value } } })
                }
              />
              <TextField
                label="TLS key file"
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
          <Section
            title="Filecoin"
            action={
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={!data.writable || filecoinPreflight.isPending}
                onClick={runFilecoinPreflight}
              >
                <RefreshCw
                  data-icon="inline-start"
                  className={filecoinPreflight.isPending ? 'animate-spin' : undefined}
                />
                Run preflight
              </Button>
            }
          >
            <FilecoinPreflightSummary
              data={preflightMatchesCurrentDraft ? filecoinPreflight.data : undefined}
              pending={preflightMatchesCurrentDraft && filecoinPreflight.isPending}
              error={preflightMatchesCurrentDraft ? filecoinPreflight.error : null}
              onDetails={setPreflightDetailData}
            />
            <div className="grid gap-4 md:grid-cols-2">
              <CredentialStatusCard data={data} label="Filecoin private key" field={data.manual.filecoin_private_key} />
              <SelectField
                label="Network"
                field="filecoin.network"
                value={form.filecoin.network}
                options={settingsOptions('filecoin.network', ['calibration', 'mainnet'])}
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
              <SelectField
                label="Provider requirement"
                field="filecoin.anchor_provider_tier"
                value={form.filecoin.anchor_provider_tier}
                options={settingsOptions('filecoin.anchor_provider_tier', ['approved', 'endorsed', 'none'])}
                data={data}
                errors={fieldErrors}
                onChange={(value) => {
                  if (value === 'approved' || value === 'endorsed' || value === 'none') {
                    setForm({ ...form, filecoin: { ...form.filecoin, anchor_provider_tier: value } })
                  }
                }}
              />
              <NumberField
                label="Default replicas"
                field="filecoin.default_copies"
                value={form.filecoin.default_copies}
                data={data}
                errors={fieldErrors}
                onChange={(value) => setForm({ ...form, filecoin: { ...form.filecoin, default_copies: value } })}
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
                label="Allow private networks"
                field="filecoin.allow_private_networks"
                checked={form.filecoin.allow_private_networks}
                data={data}
                errors={fieldErrors}
                onChange={(checked) =>
                  setForm({ ...form, filecoin: { ...form.filecoin, allow_private_networks: checked } })
                }
              />
              <TextField
                label="Observability interval"
                field="filecoin.observability.interval"
                value={form.filecoin.observability.interval}
                data={data}
                errors={fieldErrors}
                onChange={(value) =>
                  setForm({
                    ...form,
                    filecoin: {
                      ...form.filecoin,
                      observability: { ...form.filecoin.observability, interval: value },
                    },
                  })
                }
              />
              <TextField
                label="Observability timeout"
                field="filecoin.observability.timeout"
                value={form.filecoin.observability.timeout}
                data={data}
                errors={fieldErrors}
                onChange={(value) =>
                  setForm({
                    ...form,
                    filecoin: {
                      ...form.filecoin,
                      observability: { ...form.filecoin.observability, timeout: value },
                    },
                  })
                }
              />
              <NumberField
                label="Observability concurrency"
                field="filecoin.observability.concurrency"
                value={form.filecoin.observability.concurrency}
                data={data}
                errors={fieldErrors}
                onChange={(value) =>
                  setForm({
                    ...form,
                    filecoin: {
                      ...form.filecoin,
                      observability: { ...form.filecoin.observability, concurrency: value },
                    },
                  })
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
                label="Max cache size (GiB)"
                field="cache.max_size_gb"
                value={form.cache.max_size_gb}
                data={data}
                errors={fieldErrors}
                onChange={(value) => setForm({ ...form, cache: { ...form.cache, max_size_gb: value } })}
              />
              <SelectField
                label="Eviction policy"
                field="cache.eviction_policy"
                value={form.cache.eviction_policy}
                options={[
                  { value: 'lru', label: 'LRU (capacity based)' },
                  { value: 'after_upload', label: 'After remote upload' },
                  { value: 'none', label: 'No automatic eviction' },
                ]}
                data={data}
                errors={fieldErrors}
                onChange={(value) => setForm({ ...form, cache: { ...form.cache, eviction_policy: value } })}
              />
              {form.cache.eviction_policy === 'lru' && (
                <>
                  <NumberField
                    label="LRU high watermark (%)"
                    field="cache.lru_high_watermark_percent"
                    value={form.cache.lru_high_watermark_percent}
                    data={data}
                    errors={fieldErrors}
                    onChange={(value) =>
                      setForm({ ...form, cache: { ...form.cache, lru_high_watermark_percent: value } })
                    }
                  />
                  <NumberField
                    label="LRU low watermark (%)"
                    field="cache.lru_low_watermark_percent"
                    value={form.cache.lru_low_watermark_percent}
                    data={data}
                    errors={fieldErrors}
                    onChange={(value) =>
                      setForm({ ...form, cache: { ...form.cache, lru_low_watermark_percent: value } })
                    }
                  />
                </>
              )}
            </div>
          </Section>
        </TabsContent>

        <TabsContent value="workers">
          <TaskWorkerSection
            value={form.worker.tasks}
            data={data}
            errors={fieldErrors}
            onChange={(value) => setForm({ ...form, worker: { tasks: value } })}
          />
        </TabsContent>

        <TabsContent value="logging">
          <Section title="Logging">
            <div className="grid gap-4 md:grid-cols-2">
              <SelectField
                label="Level"
                field="logging.level"
                value={form.logging.level}
                options={settingsOptions('logging.level', ['debug', 'info', 'warn', 'error'])}
                data={data}
                errors={fieldErrors}
                onChange={(value) => setForm({ ...form, logging: { ...form.logging, level: value } })}
              />
              <SelectField
                label="Format"
                field="logging.format"
                value={form.logging.format}
                options={settingsOptions('logging.format', ['json', 'text'])}
                data={data}
                errors={fieldErrors}
                onChange={(value) => setForm({ ...form, logging: { ...form.logging, format: value } })}
              />
              <CheckboxField
                data={data}
                field="logging.s3_access.enabled"
                label="S3 access log"
                checked={form.logging.s3_access.enabled}
                errors={fieldErrors}
                onChange={(checked) =>
                  setForm({
                    ...form,
                    logging: {
                      ...form.logging,
                      s3_access: { ...form.logging.s3_access, enabled: checked },
                    },
                  })
                }
              />
              <SelectField
                label="S3 access log level"
                field="logging.s3_access.level"
                value={form.logging.s3_access.level}
                options={settingsOptions('logging.s3_access.level', ['debug', 'info', 'warn', 'error'])}
                data={data}
                errors={fieldErrors}
                onChange={(value) =>
                  setForm({
                    ...form,
                    logging: {
                      ...form.logging,
                      s3_access: { ...form.logging.s3_access, level: value },
                    },
                  })
                }
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

      <FilecoinReadinessDialog
        title="Filecoin preflight"
        data={preflightDetailData}
        open={Boolean(preflightDetailData)}
        onOpenChange={(open) => !open && setPreflightDetailData(null)}
      />

      <DangerActionAlertDialog
        open={Boolean(pendingSettingsPayload)}
        onOpenChange={(open) => {
          if (!open) {
            setPendingSettingsPayload(null)
            setPendingRiskChanges([])
          }
        }}
        title={strongRiskConfirmation ? 'Save high-risk settings?' : 'Save reviewed settings?'}
        description={
          strongRiskConfirmation
            ? 'Review these high-risk settings before saving. Type SAVE to confirm. Changes are written to the config file and may require a restart.'
            : 'Review these settings before saving. Changes are written to the config file and may require a restart.'
        }
        confirmLabel="Save settings"
        typedTarget={strongRiskConfirmation ? 'SAVE' : undefined}
        typedTargetLabel="Type to confirm"
        pending={updateSettings.isPending}
        error={updateSettings.error?.message}
        contentClassName="w-[calc(100vw-2rem)] max-w-[calc(100vw-2rem)] sm:max-w-2xl"
        onConfirm={handleConfirmRiskSettings}
      >
        <SettingsRiskChangeList changes={pendingRiskChanges} metadata={data.metadata} />
      </DangerActionAlertDialog>
    </form>
  )
}

function FilecoinPreflightSummary({
  data,
  pending,
  error,
  onDetails,
}: {
  data?: FilecoinReadinessData
  pending: boolean
  error: Error | null
  onDetails: (data: FilecoinReadinessData) => void
}) {
  if (!pending && !data && !error) return null

  const status = data?.status ?? 'unknown'
  const label = pending ? 'Checking' : filecoinReadinessStatusLabel(status)
  const summary = pending ? 'Checking draft Filecoin settings...' : error?.message || filecoinReadinessSummary(data)

  return (
    <div className="mb-4 flex flex-wrap items-center justify-between gap-2 rounded-md border border-border px-3 py-2">
      <div className="flex min-w-0 flex-1 items-center gap-2">
        <StatusBadge tone={filecoinReadinessStatusTone(status)}>{label}</StatusBadge>
        <span className="min-w-0 break-words text-sm text-muted-foreground">{summary}</span>
      </div>
      <Button type="button" variant="outline" size="sm" disabled={!data} onClick={() => data && onDetails(data)}>
        Details
      </Button>
    </div>
  )
}

function StatusBanners({ data, mutationError }: { data: SettingsData; mutationError: Error | null }) {
  return (
    <div className="flex flex-col gap-3">
      {settingsSetupBannerVisible(data) && (
        <Banner tone="warning" icon={AlertTriangle}>
          Setup mode is active. Save settings here, then restart the service.
        </Banner>
      )}
      {settingsRuntimeRestartBannerVisible(data) && (
        <Banner tone="warning" icon={AlertTriangle}>
          Settings are valid. Restart SynapS3 to enable the full dashboard.
        </Banner>
      )}
      {!data.writable && (
        <Banner tone="danger" icon={AlertTriangle}>
          Settings changes cannot be saved because the configuration is read-only.
        </Banner>
      )}
      {settingsSavedBannerVisible(data) && (
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
      {hasWarning && <AlertTriangle data-icon="inline-end" aria-label={`${label} needs attention`} />}
    </TabsTrigger>
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
  options: Array<string | { value: string; label: string }>
  data: SettingsData
  errors: Record<string, string>
  onChange: (value: string) => void
}) {
  const disabled = fieldDisabled(data, field)
  return (
    <FieldShell label={label} field={field} data={data} errors={errors}>
      <SettingsSelect
        value={value}
        options={options}
        disabled={disabled}
        invalid={Boolean(errors[field])}
        onChange={onChange}
      />
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
      <SettingsCheckbox
        checked={checked}
        disabled={disabled}
        invalid={Boolean(errors[field])}
        label="Enabled"
        ariaLabel={data.metadata[field]?.label ?? label}
        onChange={onChange}
      />
    </FieldShell>
  )
}

function TaskWorkerSection({
  value,
  data,
  errors,
  onChange,
}: {
  value: SettingsEditableConfig['worker']['tasks']
  data: SettingsData
  errors: Record<string, string>
  onChange: (value: SettingsEditableConfig['worker']['tasks']) => void
}) {
  return (
    <Section title="Task engine">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <NumberField
          label="Task concurrency"
          field="worker.tasks.concurrency"
          value={value.concurrency}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, concurrency: next })}
        />
        <TextField
          label="Task poll interval"
          field="worker.tasks.poll_interval"
          value={value.poll_interval}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, poll_interval: next })}
        />
        <TextField
          label="Task lease duration"
          field="worker.tasks.lease_duration"
          value={value.lease_duration}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, lease_duration: next })}
        />
        <NumberField
          label="Upload concurrency"
          field="worker.tasks.upload_concurrency"
          value={value.upload_concurrency}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, upload_concurrency: next })}
        />
        <NumberField
          label="Batch size"
          field="worker.tasks.commit_max_pieces"
          value={value.commit_max_pieces}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, commit_max_pieces: next })}
        />
        <TextField
          label="Batch wait"
          field="worker.tasks.commit_max_wait"
          value={value.commit_max_wait}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, commit_max_wait: next })}
        />
        <NumberField
          label="Batch backlog"
          field="worker.tasks.commit_max_backlog"
          value={value.commit_max_backlog}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, commit_max_backlog: next })}
        />
        <CheckboxField
          label="Submit batches early to free cache space"
          field="worker.tasks.commit_seal_on_cache_pressure"
          checked={value.commit_seal_on_cache_pressure}
          data={data}
          errors={errors}
          onChange={(next) => onChange({ ...value, commit_seal_on_cache_pressure: next })}
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
    <SettingsStatusField
      label={meta?.label ?? label}
      metadata={meta}
      status={
        <StatusBadge tone={field.configured ? 'success' : 'warning'}>
          {field.configured ? 'Configured' : 'Missing'}
        </StatusBadge>
      }
    >
      {envOverride ? (
        <span className="flex items-center gap-1">
          <AlertTriangle className="size-3.5" />
          Overridden by {envOverride}
        </span>
      ) : setupHint ? (
        setupHint
      ) : (
        <span className="font-mono">{data.config_path}</span>
      )}
    </SettingsStatusField>
  )
}

function SettingsRiskChangeList({
  changes,
  metadata,
}: {
  changes: SettingsRiskChange[]
  metadata: Record<string, SettingsFieldMetadata>
}) {
  if (changes.length === 0) return null

  return (
    <div className="max-h-[60vh] overflow-y-auto rounded-md border border-border">
      <div className="divide-y divide-border">
        {changes.map((change) => {
          const confirmation = classifySettingsRisk(change)
          const description = metadata[change.field]?.description

          return (
            <div key={change.field} className="grid gap-3 p-3">
              <div className="flex min-w-0 flex-col gap-1">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-medium">{change.label}</span>
                  <StatusBadge tone={confirmation === 'strong' ? 'danger' : 'warning'}>
                    {confirmation === 'strong' ? 'Strong confirmation' : 'Review'}
                  </StatusBadge>
                </div>
                <code className="block break-all text-xs text-muted-foreground">{change.field}</code>
                {description && <p className="break-words text-xs text-muted-foreground">{description}</p>}
              </div>
              <p className="break-words text-xs text-muted-foreground">{change.reason}</p>
              <div className="grid min-w-0 gap-3 sm:grid-cols-2">
                <RiskValue label="From" value={change.from} />
                <RiskValue label="To" value={change.to} />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function RiskValue({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <span className="text-xs font-medium text-muted-foreground">{label}</span>
      <code className="block min-h-8 break-all rounded-md bg-muted px-2 py-1.5 text-xs">{value || '(empty)'}</code>
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
  return <SettingsReadOnlyField data={data} field={field} value={value} />
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
