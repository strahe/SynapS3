import type { ReactNode } from 'react'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { providerRegistryLabel } from '@/lib/provider-display'
import {
  type ObservabilityStatusFilter,
  observabilityStatusOptions,
  type StorageTopologyFilters,
  type StorageTopologyProviderOption,
  storageTopologyAllFilterValue,
} from '@/lib/storage-topology'
import { enumLabel } from '@/lib/utils'

export type StorageTopologyTab = 'topology' | 'providers' | 'data-sets'

export function StorageTopologyToolbar({
  tab,
  filters,
  providerOptions,
  bucketOptions,
  onTabChange,
  onChange,
}: {
  tab: StorageTopologyTab
  filters: StorageTopologyFilters
  providerOptions: StorageTopologyProviderOption[]
  bucketOptions: string[]
  onTabChange: (value: string) => void
  onChange: (next: Partial<StorageTopologyFilters>) => void
}) {
  const visibleProviderOptions =
    filters.provider !== storageTopologyAllFilterValue &&
    !providerOptions.some((option) => option.value === filters.provider)
      ? [
          ...providerOptions,
          { value: filters.provider, label: `${providerRegistryLabel(filters.provider)} (not in snapshot)` },
        ]
      : providerOptions
  const visibleBucketOptions =
    filters.bucket !== storageTopologyAllFilterValue && !bucketOptions.includes(filters.bucket)
      ? [...bucketOptions, filters.bucket]
      : bucketOptions

  return (
    <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
      <Tabs value={tab} onValueChange={onTabChange} className="min-w-0">
        <TabsList className="max-w-full justify-start overflow-x-auto">
          <TabsTrigger value="topology">Topology</TabsTrigger>
          <TabsTrigger value="providers">Providers</TabsTrigger>
          <TabsTrigger value="data-sets">Data sets</TabsTrigger>
        </TabsList>
      </Tabs>

      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        <ToolbarFilter id="topology-status-filter" label="Status">
          <Select
            value={filters.status}
            onValueChange={(value) => onChange({ status: value as ObservabilityStatusFilter })}
          >
            <SelectTrigger id="topology-status-filter" className="w-40">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                {observabilityStatusOptions.map((status) => (
                  <SelectItem key={status} value={status}>
                    {status === 'all' ? 'All statuses' : enumLabel(status)}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </ToolbarFilter>

        <ToolbarFilter id="topology-provider-filter" label="Provider">
          <Select value={filters.provider} onValueChange={(value) => onChange({ provider: value })}>
            <SelectTrigger id="topology-provider-filter" className="w-52">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value={storageTopologyAllFilterValue}>All providers</SelectItem>
                {visibleProviderOptions.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </ToolbarFilter>

        <ToolbarFilter id="topology-bucket-filter" label="Bucket">
          <Select value={filters.bucket} onValueChange={(value) => onChange({ bucket: value })}>
            <SelectTrigger id="topology-bucket-filter" className="w-44">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectGroup>
                <SelectItem value={storageTopologyAllFilterValue}>All buckets</SelectItem>
                {visibleBucketOptions.map((bucket) => (
                  <SelectItem key={bucket} value={bucket}>
                    {bucketOptions.includes(bucket) ? bucket : `${bucket} (not in snapshot)`}
                  </SelectItem>
                ))}
              </SelectGroup>
            </SelectContent>
          </Select>
        </ToolbarFilter>
      </div>
    </div>
  )
}

// A label and its control wrap together, so a narrow toolbar never strands a label.
function ToolbarFilter({ id, label, children }: { id: string; label: string; children: ReactNode }) {
  return (
    <div className="flex items-center gap-2">
      <Label htmlFor={id} className="text-sm text-muted-foreground">
        {label}
      </Label>
      {children}
    </div>
  )
}
