import type { QueryClient } from '@tanstack/react-query'

export const authSessionQueryOptions = {
  staleTime: 5 * 60 * 1000,
  refetchOnWindowFocus: false,
} as const

export function clearLocalAuthSession(queryClient: QueryClient) {
  void queryClient.cancelQueries()
  queryClient.removeQueries({
    predicate: (query) => query.queryKey[0] !== 'authSession',
  })
  queryClient.setQueryData(['authSession'], null)
}
