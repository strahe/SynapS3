import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import {
  type ObservabilityStatusFilter,
  observabilityStatusOptions,
  type StorageTopologyFilters,
  storageTopologyAllFilterValue,
} from '@/lib/storage-topology'
import { titleCaseEnum } from '@/lib/utils'

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
  providerOptions: string[]
  bucketOptions: string[]
  onTabChange: (value: string) => void
  onChange: (next: Partial<StorageTopologyFilters>) => void
}) {
  const visibleProviderOptions =
    filters.provider !== storageTopologyAllFilterValue && !providerOptions.includes(filters.provider)
      ? [...providerOptions, filters.provider]
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
          <TabsTrigger value="data-sets">Data Sets</TabsTrigger>
        </TabsList>
      </Tabs>

      <div className="flex flex-wrap items-center gap-2">
        <Label htmlFor="topology-status-filter" className="text-sm text-muted-foreground">
          Status:
        </Label>
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
                  {status === 'all' ? 'All statuses' : titleCaseEnum(status)}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>

        <Label htmlFor="topology-provider-filter" className="text-sm text-muted-foreground">
          Provider:
        </Label>
        <Select value={filters.provider} onValueChange={(value) => onChange({ provider: value })}>
          <SelectTrigger id="topology-provider-filter" className="w-44">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectGroup>
              <SelectItem value={storageTopologyAllFilterValue}>All providers</SelectItem>
              {visibleProviderOptions.map((providerID) => (
                <SelectItem key={providerID} value={providerID}>
                  {providerOptions.includes(providerID)
                    ? `Provider #${providerID}`
                    : `Provider #${providerID} (not in snapshot)`}
                </SelectItem>
              ))}
            </SelectGroup>
          </SelectContent>
        </Select>

        <Label htmlFor="topology-bucket-filter" className="text-sm text-muted-foreground">
          Bucket:
        </Label>
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
      </div>
    </div>
  )
}
