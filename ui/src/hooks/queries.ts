import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { S3UserRole } from '@/api/client'
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

export function useBucketObjects(name: string, prefix: string, after: string, limit = 50) {
  return useQuery({
    queryKey: ['objects', name, prefix, after, limit],
    queryFn: () => api.getBucketObjects(name, { prefix, after, limit }),
    refetchInterval: 15_000,
  })
}

export function useCreateBucket() {
  const qc = useQueryClient()

  return useMutation({
    mutationFn: (payload: { name: string; ownerAccessKey: string }) =>
      api.createBucket({ name: payload.name, owner_access_key: payload.ownerAccessKey }),
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

export function useTasks(taskType: string, status: string, limit: number, offset: number) {
  return useQuery({
    queryKey: ['tasks', taskType, status, limit, offset],
    queryFn: () => api.getTasks({ type: taskType, status, limit, offset }),
    refetchInterval: 10_000,
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
