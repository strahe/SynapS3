import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { FilecoinReadinessPreflightPayload, ObservabilityListParams, S3UserRole } from '@/api/client'
import { api } from '@/api/client'

export function useOverview() {
  return useQuery({
    queryKey: ['overview'],
    queryFn: api.getOverview,
    refetchInterval: 10_000,
  })
}

export function useBuckets() {
  return useQuery({
    queryKey: ['buckets'],
    queryFn: api.getBuckets,
    refetchInterval: 15_000,
  })
}

export function useBucket(name: string) {
  return useQuery({
    queryKey: ['bucket', name],
    queryFn: () => api.getBucket(name),
    enabled: Boolean(name),
    refetchInterval: 15_000,
  })
}

export function useBucketObjects(
  name: string,
  prefix: string,
  after: string,
  limit = 50,
  delimiter = '/',
  enabled = true
) {
  return useQuery({
    queryKey: ['objects', name, prefix, delimiter, after, limit],
    queryFn: () => api.getBucketObjects(name, { prefix, delimiter, after, limit }),
    enabled: Boolean(name && enabled),
    refetchInterval: 15_000,
  })
}

export function useDeletedBucketObjects(name: string, prefix: string, after: string, limit = 50, enabled = true) {
  return useQuery({
    queryKey: ['deletedObjects', name, prefix, after, limit],
    queryFn: () => api.getBucketDeletedObjects(name, { prefix, after, limit }),
    enabled: Boolean(name && enabled),
    refetchInterval: 15_000,
  })
}

export function useBucketObjectVersions(name: string, key: string, versionMarker: string, limit = 50, enabled = true) {
  return useQuery({
    queryKey: ['objectVersions', name, key, versionMarker, limit],
    queryFn: () => api.getBucketObjectVersions(name, { key, version_marker: versionMarker, limit }),
    enabled: Boolean(name && key && enabled),
    refetchInterval: 15_000,
  })
}

export function useBucketStorageRiskVersions(
  name: string,
  params: {
    prefix?: string
    key?: string
    local_data_set_id?: number
    key_marker?: string
    version_marker?: string
    created_at_marker?: string
    stale_before?: string
    limit?: number
  },
  enabled = true
) {
  return useQuery({
    queryKey: ['bucketStorageRiskVersions', name, params],
    queryFn: () => api.getBucketStorageRiskVersions(name, params),
    enabled: Boolean(name && enabled),
    refetchInterval: 15_000,
  })
}

export function useObjectStatusDetail(name: string, versionId: string, enabled = true) {
  return useQuery({
    queryKey: ['objectStatusDetail', name, versionId],
    queryFn: () => api.getObjectStatusDetail(name, versionId),
    enabled: Boolean(name && versionId && enabled),
    staleTime: Number.POSITIVE_INFINITY,
  })
}

export function useObjectProvenance(name: string, versionId: string, enabled = true) {
  return useQuery({
    queryKey: ['objectProvenance', name, versionId],
    queryFn: () => api.getObjectProvenance(name, versionId),
    enabled: Boolean(name && versionId && enabled),
    staleTime: 0,
  })
}

export function useCreateBucket() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: (payload: { name: string; ownerAccessKey: string; defaultCopies: number | null }) =>
      api.createBucket({
        name: payload.name,
        owner_access_key: payload.ownerAccessKey,
        default_copies: payload.defaultCopies,
      }),
    onSuccess: (bucket) => {
      qc.invalidateQueries({ queryKey: ['buckets'] })
      qc.invalidateQueries({ queryKey: ['bucket', bucket.name] })
      qc.invalidateQueries({ queryKey: ['s3Users'] })
    },
  })
}

export function useUpdateBucketOwner() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ name, ownerAccessKey }: { name: string; ownerAccessKey: string }) =>
      api.updateBucketOwner(name, ownerAccessKey),
    onSuccess: (bucket) => {
      qc.invalidateQueries({ queryKey: ['buckets'] })
      qc.invalidateQueries({ queryKey: ['bucket', bucket.name] })
      qc.invalidateQueries({ queryKey: ['s3Users'] })
    },
  })
}

export function useUpdateBucketCopyPolicy() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ name, defaultCopies }: { name: string; defaultCopies: number | null }) =>
      api.updateBucketCopyPolicy(name, defaultCopies),
    onSuccess: (bucket) => {
      qc.invalidateQueries({ queryKey: ['buckets'] })
      qc.invalidateQueries({ queryKey: ['bucket', bucket.name] })
    },
  })
}

export function useDeleteBucket() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ name, recursive }: { name: string; recursive: boolean }) => api.deleteBucket(name, { recursive }),
    onSuccess: (_, variables) => {
      qc.invalidateQueries({ queryKey: ['buckets'] })
      qc.invalidateQueries({ queryKey: ['bucket', variables.name] })
      qc.invalidateQueries({ queryKey: ['objects', variables.name] })
    },
  })
}

export function useDeleteBucketObject() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ name, key }: { name: string; key: string }) => api.deleteBucketObject(name, key),
    onSuccess: (_, variables) => {
      qc.invalidateQueries({ queryKey: ['bucket', variables.name] })
      qc.invalidateQueries({ queryKey: ['objects', variables.name] })
      qc.invalidateQueries({ queryKey: ['deletedObjects', variables.name] })
      qc.invalidateQueries({ queryKey: ['objectVersions', variables.name, variables.key] })
    },
  })
}

export function usePermanentDeleteBucketObjectVersion() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ name, key, versionID }: { name: string; key: string; versionID: string }) =>
      api.permanentlyDeleteBucketObjectVersion(name, { key, version_id: versionID }),
    onSuccess: (_, variables) => {
      qc.invalidateQueries({ queryKey: ['bucket', variables.name] })
      qc.invalidateQueries({ queryKey: ['objects', variables.name] })
      qc.invalidateQueries({ queryKey: ['deletedObjects', variables.name] })
      qc.invalidateQueries({ queryKey: ['objectVersions', variables.name, variables.key] })
      qc.invalidateQueries({ queryKey: ['tasks'] })
      qc.invalidateQueries({ queryKey: ['taskStats'] })
    },
  })
}

export function usePermanentDeleteDeletedBucketObject() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ name, key, deleteMarkerVersionID }: { name: string; key: string; deleteMarkerVersionID: string }) =>
      api.permanentlyDeleteDeletedBucketObject(name, {
        key,
        delete_marker_version_id: deleteMarkerVersionID,
      }),
    onSuccess: (_, variables) => {
      qc.invalidateQueries({ queryKey: ['bucket', variables.name] })
      qc.invalidateQueries({ queryKey: ['objects', variables.name] })
      qc.invalidateQueries({ queryKey: ['deletedObjects', variables.name] })
      qc.invalidateQueries({ queryKey: ['objectVersions', variables.name, variables.key] })
      qc.invalidateQueries({ queryKey: ['tasks'] })
      qc.invalidateQueries({ queryKey: ['taskStats'] })
    },
  })
}

export function useRestoreBucketObject() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ name, key, deleteMarkerVersionID }: { name: string; key: string; deleteMarkerVersionID: string }) =>
      api.restoreBucketObject(name, { key, delete_marker_version_id: deleteMarkerVersionID }),
    onSuccess: (_, variables) => {
      qc.invalidateQueries({ queryKey: ['bucket', variables.name] })
      qc.invalidateQueries({ queryKey: ['objects', variables.name] })
      qc.invalidateQueries({ queryKey: ['deletedObjects', variables.name] })
      qc.invalidateQueries({ queryKey: ['objectVersions', variables.name, variables.key] })
    },
  })
}

export function useTasks(taskType: string, stage: string, status: string, limit: number, offset: number) {
  return useQuery({
    queryKey: ['tasks', taskType, stage, status, limit, offset],
    queryFn: () => api.getTasks({ type: taskType, stage, status, limit, offset }),
    refetchInterval: 10_000,
  })
}

export function useTaskRefDetail(taskId: number, enabled = true) {
  return useQuery({
    queryKey: ['taskRefDetail', taskId],
    queryFn: () => api.getTaskRefDetail(taskId),
    enabled: Boolean(taskId && enabled),
    staleTime: 60_000,
  })
}

export function useTaskStats() {
  return useQuery({
    queryKey: ['taskStats'],
    queryFn: api.getTaskStats,
    refetchInterval: 10_000,
  })
}

export function useWallet() {
  return useQuery({
    queryKey: ['wallet'],
    queryFn: api.getWallet,
    staleTime: 10_000,
    refetchInterval: 30_000,
  })
}

export function useFilecoinReadiness(enabled = true) {
  return useQuery({
    queryKey: ['filecoinReadiness'],
    queryFn: api.getFilecoinReadiness,
    enabled,
    staleTime: 60_000,
  })
}

export function useFilecoinPreflight() {
  return useMutation({
    mutationFn: (payload: FilecoinReadinessPreflightPayload) => api.preflightFilecoin(payload),
  })
}

export function useObservabilityProviders(
  params: Pick<ObservabilityListParams, 'status' | 'provider_id' | 'limit' | 'offset'>,
  enabled = true
) {
  return useQuery({
    queryKey: ['observabilityProviders', params],
    queryFn: () => api.getObservabilityProviders(params),
    enabled,
    refetchInterval: 30_000,
  })
}

export function useObservabilityDataSets(params: ObservabilityListParams, enabled = true) {
  return useQuery({
    queryKey: ['observabilityDataSets', params],
    queryFn: () => api.getObservabilityDataSets(params),
    enabled,
    refetchInterval: 30_000,
  })
}

export function useRefreshDataSetStorageHealth() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ bucket }: { bucket?: string } = {}) => api.refreshDataSetStorageHealth({ bucket }),
    onSuccess: (_, variables) => {
      if (variables?.bucket) qc.invalidateQueries({ queryKey: ['bucket', variables.bucket] })
      qc.invalidateQueries({ queryKey: ['filecoinReadiness'] })
    },
  })
}

export function useWalletOperations(limit = 20) {
  return useQuery({
    queryKey: ['walletOperations', limit],
    queryFn: () => api.getWalletOperations(limit),
    refetchInterval: 15_000,
  })
}

export function useWalletFund() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: api.fundWallet,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['wallet'] })
      qc.invalidateQueries({ queryKey: ['walletOperations'] })
    },
  })
}

export function useWalletWithdraw() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: api.withdrawWallet,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['wallet'] })
      qc.invalidateQueries({ queryKey: ['walletOperations'] })
    },
  })
}

export function useSettings() {
  return useQuery({
    queryKey: ['settings'],
    queryFn: api.getSettings,
    refetchInterval: 30_000,
  })
}

export function useUpdateSettings() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: api.updateSettings,
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['settings'] })
    },
  })
}

export function useValidateSettings() {
  return useMutation({
    mutationFn: api.validateSettings,
  })
}

export function useS3Users(enabled = true) {
  return useQuery({
    queryKey: ['s3Users'],
    queryFn: api.getS3Users,
    enabled,
    refetchInterval: 30_000,
  })
}

export function useCreateS3User() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: (payload: { role?: S3UserRole }) => api.createS3User(payload),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['s3Users'] })
    },
  })
}

export function useUpdateS3User() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: ({ accessKey, role }: { accessKey: string; role: S3UserRole }) => api.updateS3User(accessKey, { role }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['s3Users'] })
    },
  })
}

export function useRotateS3UserSecret() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: (accessKey: string) => api.rotateS3UserSecret(accessKey),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['s3Users'] })
    },
  })
}

export function useDeleteS3User() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: (accessKey: string) => api.deleteS3User(accessKey),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['s3Users'] })
      qc.invalidateQueries({ queryKey: ['buckets'] })
    },
  })
}
