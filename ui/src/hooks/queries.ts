import { useQuery } from '@tanstack/react-query'
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

export function useBucketObjects(name: string, prefix: string, after: string, limit = 50) {
  return useQuery({
    queryKey: ['objects', name, prefix, after, limit],
    queryFn: () => api.getBucketObjects(name, { prefix, after, limit }),
    refetchInterval: 15_000,
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
