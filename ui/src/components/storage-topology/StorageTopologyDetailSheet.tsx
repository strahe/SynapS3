import { Link } from '@tanstack/react-router'
import { ChevronRight, Database, Gauge, Info, Loader2, RefreshCw, TriangleAlert } from 'lucide-react'
import { type ReactNode, type RefObject, useEffect, useMemo, useRef } from 'react'
import type {
  ObservabilityDataSetObservation,
  ObservabilityFreshness,
  ObservabilityProviderFacts,
  ObservabilityProviderObservation,
  ObservabilitySignal,
  ProviderProfile,
  ProviderTierRefreshResult,
} from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { ProviderProfileDetails } from '@/components/app/ProviderProfileDetails'
import { StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useRefreshProvider, useRefreshProviderTiers, useTestProviderUploadSpeed } from '@/hooks/queries'
import { activePiecesValue } from '@/lib/data-set-storage-health'
import { providerDisplayName, providerLocationLabel, providerRegistryLabel } from '@/lib/provider-display'
import {
  canTestProviderUploadSpeed,
  providerUploadSampleSize,
  providerUploadSpeedLabel,
  providerUploadSpeedTestErrorMessage,
} from '@/lib/provider-upload-speed'
import { replicaLabel } from '@/lib/storage-status-labels'
import {
  bucketIssueTone,
  countLabel,
  dataSetDisplayLabel,
  freshnessLabel,
  localStatusLabel,
  localStatusTone,
  observabilitySignalDetails,
  observabilityStatusTone,
  type ResolvedStorageTopologySelection,
  type StorageTopologyDetailTarget,
  type StorageTopologyGraph,
  type StorageTopologySelection,
  storageTopologyDataSetSelection,
  storageTopologyProviderSelection,
  topologyDetailTarget,
} from '@/lib/storage-topology'
import { cn, timeAgo } from '@/lib/utils'
import { TopologySignalBadge } from './TopologyStatus'

type ProviderTarget = Extract<StorageTopologyDetailTarget, { kind: 'provider' }>
type DataSetTarget = Extract<StorageTopologyDetailTarget, { kind: 'data-set' }>
type BucketTarget = Extract<StorageTopologyDetailTarget, { kind: 'bucket' }>
type TitleRef = RefObject<HTMLHeadingElement | null>

interface DetailNavigation {
  toProvider: (providerID: string) => void
  toDataSet: (localDataSetID: number) => void
}

export function TopologyDetailSheet({
  selection,
  graph,
  providers,
  dataSets,
  onOpenChange,
  onNavigate,
}: {
  selection: ResolvedStorageTopologySelection | null
  graph: StorageTopologyGraph
  providers: ObservabilityProviderObservation[]
  dataSets: ObservabilityDataSetObservation[]
  onOpenChange: (open: boolean) => void
  onNavigate: (selection: StorageTopologySelection) => void
}) {
  const titleRef = useRef<HTMLHeadingElement>(null)
  const target = useMemo(
    () => (selection ? topologyDetailTarget(selection, graph, providers, dataSets) : null),
    [dataSets, graph, providers, selection]
  )
  // Keep describing the last item while the panel animates closed.
  const lastTargetRef = useRef(target)
  if (target) lastTargetRef.current = target
  const shownTarget = target ?? lastTargetRef.current
  const targetKey = target ? detailTargetKey(target) : null
  const previousTargetKey = useRef(targetKey)

  useEffect(() => {
    const previous = previousTargetKey.current
    previousTargetKey.current = targetKey
    // Opening a related item replaces the row that had focus.
    if (previous && targetKey && previous !== targetKey) titleRef.current?.focus({ preventScroll: true })
  }, [targetKey])

  const navigation: DetailNavigation = {
    toProvider: (providerID) => onNavigate(storageTopologyProviderSelection(graph, providerID)),
    toDataSet: (localDataSetID) => onNavigate(storageTopologyDataSetSelection(graph, localDataSetID)),
  }

  return (
    <Sheet open={Boolean(target)} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="gap-0 data-[side=right]:w-[min(720px,calc(100vw-2rem))] data-[side=right]:sm:max-w-[720px]"
        onOpenAutoFocus={(event) => {
          event.preventDefault()
          titleRef.current?.focus({ preventScroll: true })
        }}
      >
        {shownTarget?.kind === 'provider' ? (
          <ProviderDetails
            key={detailTargetKey(shownTarget)}
            target={shownTarget}
            titleRef={titleRef}
            navigation={navigation}
          />
        ) : shownTarget?.kind === 'data-set' ? (
          <DataSetDetails
            key={detailTargetKey(shownTarget)}
            target={shownTarget}
            titleRef={titleRef}
            navigation={navigation}
          />
        ) : shownTarget?.kind === 'bucket' ? (
          <BucketDetails
            key={detailTargetKey(shownTarget)}
            target={shownTarget}
            providers={providers}
            titleRef={titleRef}
            navigation={navigation}
          />
        ) : (
          <SheetHeader className="sr-only">
            <SheetTitle>Storage details</SheetTitle>
            <SheetDescription>Nothing is selected.</SheetDescription>
          </SheetHeader>
        )}
      </SheetContent>
    </Sheet>
  )
}

function ProviderDetails({
  target,
  titleRef,
  navigation,
}: {
  target: ProviderTarget
  titleRef: TitleRef
  navigation: DetailNavigation
}) {
  const { providerID, provider, dataSets } = target
  const profile = provider?.provider_profile
  const name = providerDisplayName(providerID, profile?.name)
  const registry = providerRegistryLabel(providerID)
  const location = providerLocationLabel(profile?.registry_snapshot?.pdp_offering?.location)
  const test = provider?.upload_speed_test

  return (
    <>
      <DetailHeader
        titleRef={titleRef}
        kind="Provider"
        title={name}
        badge={<TopologySignalBadge status={provider?.signal.status ?? 'unknown'} signal={provider?.signal} />}
        subtitle={[name === registry ? undefined : registry, location].filter(Boolean).join(' · ') || registry}
        actions={<ProviderActions providerID={providerID} provider={provider} name={name} />}
      />
      <DetailBody>
        {provider ? (
          <HealthCallout signal={provider.signal} />
        ) : (
          <Alert>
            <Info />
            <AlertTitle>No health check recorded yet</AlertTitle>
            <AlertDescription>Refresh this provider to check it now.</AlertDescription>
          </Alert>
        )}
        {provider && (
          <DetailSection title="Overview">
            <DetailGrid className="@xl:grid-cols-2">
              <DetailField label="Upload speed">
                {providerUploadSpeedLabel(test)}
                {test?.tested_at && (
                  <DetailNote>
                    Tested {timeAgo(test.tested_at)} with {providerUploadSampleSize(test)}
                  </DetailNote>
                )}
              </DetailField>
              <DetailField label="Declared location">{location ?? '—'}</DetailField>
              <DetailField label="Registry status">{registryStateLabel(provider.facts)}</DetailField>
              <DetailField label="Health check">
                {healthCheckLabel(provider.facts.health_status)}
                <DetailNote>{checkedLabel(provider.signal.freshness)}</DetailNote>
              </DetailField>
              <ProviderTierField profile={profile} />
              <DetailField label="Service URL" wide>
                {provider.facts.service_url ? (
                  <CopyableValue
                    label="Service URL"
                    value={provider.facts.service_url}
                    linkHref={provider.facts.service_url}
                    external
                    monospace
                    maxLength={72}
                  />
                ) : (
                  '—'
                )}
              </DetailField>
            </DetailGrid>
          </DetailSection>
        )}
        <DetailSection title={`Data sets on this provider (${dataSets.length})`}>
          {dataSets.length > 0 ? (
            <div className="flex flex-col gap-2">
              {dataSets.map((dataSet) => (
                <RelatedRow
                  key={dataSet.facts.local_data_set_id}
                  title={`${dataSet.facts.bucket_name} · ${replicaLabel(dataSet.facts.copy_index)}`}
                  subtitle={dataSetDisplayLabel(dataSet)}
                  signal={dataSet.signal}
                  onClick={() => navigation.toDataSet(dataSet.facts.local_data_set_id)}
                />
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">No data sets use this provider.</p>
          )}
        </DetailSection>
        <DisclosureSection title="Registry details">
          <ProviderProfileDetails profile={profile} />
        </DisclosureSection>
      </DetailBody>
    </>
  )
}

function ProviderActions({
  providerID,
  provider,
  name,
}: {
  providerID: string
  provider?: ObservabilityProviderObservation
  name: string
}) {
  const refreshProvider = useRefreshProvider()
  const testUploadSpeed = useTestProviderUploadSpeed()
  const testing = testUploadSpeed.isPending || provider?.upload_speed_test?.state === 'testing'
  const refreshResult = refreshProvider.data

  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-wrap gap-2">
        {(testing || canTestProviderUploadSpeed(provider)) && (
          <Button size="sm" variant="outline" disabled={testing} onClick={() => testUploadSpeed.mutate(providerID)}>
            {testing ? (
              <Loader2 data-icon="inline-start" className="animate-spin" />
            ) : (
              <Gauge data-icon="inline-start" />
            )}
            {testing ? 'Testing upload speed…' : 'Test upload speed'}
          </Button>
        )}
        <Button
          size="sm"
          variant="outline"
          disabled={refreshProvider.isPending}
          onClick={() => refreshProvider.mutate(providerID)}
        >
          <RefreshCw data-icon="inline-start" className={refreshProvider.isPending ? 'animate-spin' : undefined} />
          Refresh provider
        </Button>
      </div>
      {testUploadSpeed.isError && (
        <p className="text-xs text-destructive">{providerUploadSpeedTestErrorMessage(testUploadSpeed.error, name)}</p>
      )}
      {refreshResult && !refreshResult.profile_result.success && (
        <p className="text-xs text-destructive">
          Registry refresh failed {timeAgo(refreshResult.profile_result.attempted_at)}. The previous details remain
          available.
        </p>
      )}
      {refreshResult?.profile_result.success && !refreshResult.health_result.success && (
        <p className="text-xs text-destructive">
          Health check failed {timeAgo(refreshResult.health_result.attempted_at)}. The previous result remains visible.
        </p>
      )}
      {refreshProvider.isError && <p className="text-xs text-destructive">Could not refresh this provider.</p>}
    </div>
  )
}

function ProviderTierField({ profile }: { profile?: ProviderProfile }) {
  const refreshTiers = useRefreshProviderTiers()
  const tiers = [
    { label: 'Approved', listed: profile?.approved, checkedAt: profile?.approved_checked_at },
    { label: 'Endorsed', listed: profile?.endorsed, checkedAt: profile?.endorsed_checked_at },
  ]
  const refreshMessage = refreshTiers.isError
    ? { text: 'Could not refresh FWSS lists', failed: true }
    : tierRefreshMessage(refreshTiers.data)

  return (
    <DetailField
      label="FWSS"
      wide
      action={
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon-xs"
              aria-label="Refresh FWSS lists for all providers"
              disabled={refreshTiers.isPending}
              onClick={() => refreshTiers.mutate()}
            >
              <RefreshCw className={refreshTiers.isPending ? 'animate-spin' : undefined} />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Refresh FWSS lists for all providers</TooltipContent>
        </Tooltip>
      }
    >
      <div className="grid grid-cols-1 gap-x-6 gap-y-3 @sm:grid-cols-2">
        {tiers.map((tier) => (
          <div key={tier.label}>
            {tier.label}: {tier.checkedAt ? (tier.listed ? 'Yes' : 'No') : 'Unknown'}
            {tier.checkedAt && <DetailNote>Checked {timeAgo(tier.checkedAt)}</DetailNote>}
          </div>
        ))}
      </div>
      {refreshMessage && (
        <DetailNote className={refreshMessage.failed ? 'text-destructive' : undefined}>
          {refreshMessage.text}
        </DetailNote>
      )}
    </DetailField>
  )
}

function DataSetDetails({
  target,
  titleRef,
  navigation,
}: {
  target: DataSetTarget
  titleRef: TitleRef
  navigation: DetailNavigation
}) {
  const { dataSet, provider } = target
  const facts = dataSet.facts
  const providerName = providerDisplayName(facts.provider_id, provider?.provider_profile?.name)
  const registry = providerRegistryLabel(facts.provider_id)
  const providerLocation = providerLocationLabel(
    provider?.provider_profile?.registry_snapshot?.pdp_offering?.location,
    {
      compact: true,
    }
  )

  return (
    <>
      <DetailHeader
        titleRef={titleRef}
        kind="Data set"
        title={dataSetDisplayLabel(dataSet)}
        badge={<TopologySignalBadge status={dataSet.signal.status} signal={dataSet.signal} />}
        subtitle={`${facts.bucket_name} · ${replicaLabel(facts.copy_index)}`}
        actions={<OpenBucketButton bucketName={facts.bucket_name} />}
      />
      <DetailBody>
        <HealthCallout signal={dataSet.signal} />
        <DetailSection title="Overview">
          <DetailGrid>
            <DetailField label="Bucket">
              <Link to="/buckets/$name" params={{ name: facts.bucket_name }} className="font-medium hover:underline">
                {facts.bucket_name}
              </Link>
            </DetailField>
            <DetailField label="Replica">{replicaLabel(facts.copy_index)}</DetailField>
            <DetailField label="Local status">
              <StatusBadge tone={localStatusTone(facts.local_status)}>
                {localStatusLabel(facts.local_status)}
              </StatusBadge>
            </DetailField>
            <DetailField label="Pieces">{activePiecesValue(facts)}</DetailField>
            <DetailField label="Last checked">{freshnessLabel(dataSet.signal.freshness)}</DetailField>
          </DetailGrid>
        </DetailSection>
        <DetailSection title="Provider">
          <RelatedRow
            title={providerName}
            subtitle={[providerName === registry ? undefined : registry, providerLocation].filter(Boolean).join(' · ')}
            signal={provider?.signal}
            onClick={() => navigation.toProvider(facts.provider_id)}
          />
        </DetailSection>
        <DisclosureSection title="Identifiers">
          <IdentifierGrid
            items={[
              ['Chain data set ID', facts.chain_data_set_id],
              ['Client data set ID', facts.client_data_set_id],
              ['Local ID', String(facts.local_data_set_id)],
            ]}
          />
        </DisclosureSection>
      </DetailBody>
    </>
  )
}

function BucketDetails({
  target,
  providers,
  titleRef,
  navigation,
}: {
  target: BucketTarget
  providers: ObservabilityProviderObservation[]
  titleRef: TitleRef
  navigation: DetailNavigation
}) {
  const { node, dataSets } = target
  const bucketName = node.data.bucketName ?? node.label
  const issueCount = node.data.issueCount ?? 0
  const providerNames = new Map(
    providers.map((provider) => [
      provider.facts.provider_id,
      providerDisplayName(provider.facts.provider_id, provider.provider_profile?.name),
    ])
  )

  return (
    <>
      <DetailHeader
        titleRef={titleRef}
        kind="Bucket"
        title={bucketName}
        badge={
          <StatusBadge tone={bucketIssueTone(issueCount, node.tone)}>
            {issueCount > 0 ? countLabel(issueCount, 'issue') : 'Healthy'}
          </StatusBadge>
        }
        subtitle={`${countLabel(node.data.replicaCount ?? 0, 'replica')} · ${countLabel(node.data.providerIDs?.length ?? 0, 'provider')}`}
        actions={<OpenBucketButton bucketName={bucketName} />}
      />
      <DetailBody>
        <DetailSection title="Replicas">
          <div className="flex flex-col gap-2">
            {dataSets.map((dataSet) => (
              <RelatedRow
                key={dataSet.facts.local_data_set_id}
                title={replicaLabel(dataSet.facts.copy_index)}
                subtitle={`${dataSetDisplayLabel(dataSet)} · ${
                  providerNames.get(dataSet.facts.provider_id) ?? providerRegistryLabel(dataSet.facts.provider_id)
                }`}
                signal={dataSet.signal}
                onClick={() => navigation.toDataSet(dataSet.facts.local_data_set_id)}
              />
            ))}
          </div>
        </DetailSection>
        {node.data.bucketID !== undefined && (
          <DisclosureSection title="Identifiers">
            <IdentifierGrid items={[['Bucket ID', String(node.data.bucketID)]]} />
          </DisclosureSection>
        )}
      </DetailBody>
    </>
  )
}

function DetailHeader({
  titleRef,
  kind,
  title,
  badge,
  subtitle,
  actions,
}: {
  titleRef: TitleRef
  kind: string
  title: string
  badge: ReactNode
  subtitle: string
  actions?: ReactNode
}) {
  return (
    <SheetHeader className="gap-1 border-b pr-12">
      <p className="text-xs font-medium text-muted-foreground">{kind}</p>
      <div className="flex min-w-0 items-center gap-2">
        <SheetTitle ref={titleRef} tabIndex={-1} className="min-w-0 truncate outline-none">
          {title}
        </SheetTitle>
        {badge}
      </div>
      <SheetDescription className="break-words">{subtitle}</SheetDescription>
      {actions && <div className="mt-3">{actions}</div>}
    </SheetHeader>
  )
}

function DetailBody({ children }: { children: ReactNode }) {
  return (
    <ScrollArea className="min-h-0 flex-1">
      <div className="@container flex flex-col gap-6 p-4">{children}</div>
    </ScrollArea>
  )
}

function DetailSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="flex min-w-0 flex-col gap-3">
      <h3 className="text-sm font-medium">{title}</h3>
      {children}
    </section>
  )
}

function DisclosureSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Collapsible className="flex min-w-0 flex-col gap-3">
      <CollapsibleTrigger asChild>
        <Button variant="ghost" size="sm" className="-ml-2 w-fit [&[data-state=open]>svg]:rotate-90">
          <ChevronRight data-icon="inline-start" className="transition-transform" />
          {title}
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent>{children}</CollapsibleContent>
    </Collapsible>
  )
}

function DetailGrid({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <dl className={cn('grid grid-cols-1 gap-x-6 gap-y-4 text-sm @sm:grid-cols-2 @xl:grid-cols-3', className)}>
      {children}
    </dl>
  )
}

function DetailField({
  label,
  action,
  wide,
  children,
}: {
  label: string
  action?: ReactNode
  wide?: boolean
  children: ReactNode
}) {
  return (
    <div className={cn('min-w-0', wide && 'col-span-full')}>
      <dt className="flex h-5 items-center gap-1 text-xs text-muted-foreground">
        {label}
        {action}
      </dt>
      <dd className="mt-1 min-w-0 break-words">{children}</dd>
    </div>
  )
}

function DetailNote({ className, children }: { className?: string; children: ReactNode }) {
  return <span className={cn('mt-0.5 block text-xs text-muted-foreground', className)}>{children}</span>
}

function IdentifierGrid({ items }: { items: Array<[label: string, value: string | undefined]> }) {
  return (
    <dl className="grid grid-cols-1 gap-x-6 gap-y-3 text-sm @sm:grid-cols-2">
      {items.map(([label, value]) => (
        <DetailField key={label} label={label}>
          {value ? <CopyableValue label={label} value={value} monospace maxLength={28} /> : '—'}
        </DetailField>
      ))}
    </dl>
  )
}

function HealthCallout({ signal }: { signal: ObservabilitySignal }) {
  const details = observabilitySignalDetails(signal)
  if (signal.status === 'available' && !signal.freshness.stale && details.length === 0) return null
  const [headline, ...more] = details
  const tone = signal.status === 'available' ? 'warning' : observabilityStatusTone(signal.status)
  return (
    <Alert className={calloutToneClass(tone)}>
      <TriangleAlert />
      <AlertTitle>{headline ?? healthFallbackTitle(signal)}</AlertTitle>
      <AlertDescription>
        {more.map((detail) => (
          <div key={detail} className="break-words">
            {detail}
          </div>
        ))}
        <div>{checkedLabel(signal.freshness)}</div>
      </AlertDescription>
    </Alert>
  )
}

function RelatedRow({
  title,
  subtitle,
  signal,
  onClick,
}: {
  title: string
  subtitle?: string
  signal?: ObservabilitySignal
  onClick: () => void
}) {
  const problem = signal && signal.status !== 'available' ? observabilitySignalDetails(signal)[0] : undefined
  return (
    <button
      type="button"
      onClick={onClick}
      className="flex w-full min-w-0 items-center gap-3 rounded-md border px-3 py-2 text-left transition-colors hover:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
    >
      <span className="flex min-w-0 flex-1 flex-col">
        <span className="truncate text-sm font-medium">{title}</span>
        {subtitle && <span className="truncate text-xs text-muted-foreground">{subtitle}</span>}
        {problem && signal && (
          <span className={cn('truncate text-xs', toneTextClass(observabilityStatusTone(signal.status)))}>
            {problem}
          </span>
        )}
      </span>
      <TopologySignalBadge status={signal?.status ?? 'unknown'} signal={signal} />
      <ChevronRight className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
    </button>
  )
}

function OpenBucketButton({ bucketName }: { bucketName: string }) {
  return (
    <Button asChild size="sm" variant="outline">
      <Link to="/buckets/$name" params={{ name: bucketName }}>
        <Database data-icon="inline-start" />
        Open bucket
      </Link>
    </Button>
  )
}

function detailTargetKey(target: StorageTopologyDetailTarget) {
  switch (target.kind) {
    case 'provider':
      return `provider:${target.providerID}`
    case 'data-set':
      return `data-set:${target.dataSet.facts.local_data_set_id}`
    case 'bucket':
      return target.node.id
  }
}

function checkedLabel(freshness: ObservabilityFreshness) {
  if (!freshness.last_checked_at || freshness.warnings.includes('no_state_recorded')) {
    return freshness.stale ? 'Health information is out of date' : 'Not checked yet'
  }
  const checked = `Checked ${timeAgo(freshness.last_checked_at)}`
  return freshness.stale ? `${checked} · out of date` : checked
}

function healthFallbackTitle(signal: ObservabilitySignal) {
  switch (signal.status) {
    case 'available':
      return 'Health information is out of date'
    case 'degraded':
      return 'Degraded'
    case 'unavailable':
      return 'Unavailable'
    case 'unknown':
      return 'Health is unknown'
  }
}

function healthCheckLabel(healthStatus?: string) {
  switch (healthStatus) {
    case 'reachable':
      return 'Reachable'
    case 'unreachable':
      return 'Unreachable'
    case 'n/a':
      return 'No service URL'
    default:
      return '—'
  }
}

function registryStateLabel(facts: ObservabilityProviderFacts) {
  const parts = [
    facts.active === undefined ? undefined : facts.active ? 'Active' : 'Inactive',
    facts.has_pdp === undefined ? undefined : facts.has_pdp ? 'Offers PDP' : 'No PDP offering',
  ].filter(Boolean)
  return parts.length > 0 ? parts.join(' · ') : '—'
}

function tierRefreshMessage(result?: ProviderTierRefreshResult) {
  if (!result) return undefined
  const failed = [
    result.approved_result.success ? undefined : 'approved',
    result.endorsed_result.success ? undefined : 'endorsed',
  ].filter(Boolean)
  if (failed.length === 0) return { text: 'FWSS lists updated', failed: false }
  return { text: `Could not refresh the FWSS ${failed.join(' and ')} list`, failed: true }
}

function calloutToneClass(tone: StatusTone) {
  switch (tone) {
    case 'warning':
      return 'border-[color:var(--status-warning-border)] bg-[var(--status-warning-bg)] *:[svg]:text-[color:var(--status-warning)]'
    case 'danger':
      return 'border-[color:var(--status-danger-border)] bg-[var(--status-danger-bg)] *:[svg]:text-[color:var(--status-danger)]'
    default:
      return undefined
  }
}

function toneTextClass(tone: StatusTone) {
  switch (tone) {
    case 'warning':
      return 'text-[color:var(--status-warning)]'
    case 'danger':
      return 'text-[color:var(--status-danger)]'
    default:
      return 'text-muted-foreground'
  }
}
