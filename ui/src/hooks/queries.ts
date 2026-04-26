import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
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
    mutationFn: (name: string) => api.createBucket(name),
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
    refetchInterval: 30_000,
  })
}
