import type { QueryClient } from '@tanstack/react-query'
import { APIError, type AuthSession, api, setAdminCSRFToken } from '../api/client.ts'

export const authSessionQueryOptions = {
  staleTime: 5 * 60 * 1000,
  refetchOnWindowFocus: false,
} as const

export function clearLocalAuthSession(queryClient: QueryClient) {
  setAdminCSRFToken('')
  void queryClient.cancelQueries()
  queryClient.removeQueries({
    predicate: (query) => query.queryKey[0] !== 'authSession',
  })
  queryClient.setQueryData(['authSession'], null)
}

export function storeLocalAuthSession(queryClient: QueryClient, session: AuthSession) {
  void queryClient.cancelQueries({ queryKey: ['authSession'] })
  queryClient.setQueryData(['authSession'], session)
}

export function authSessionContentKind(data: AuthSession | null | undefined, error: unknown) {
  if (data === null || (error instanceof APIError && error.status === 401)) return 'login'
  if (data) return 'authenticated'
  if (error) return 'unavailable'
  return 'loading'
}

export async function logoutLocalAuthSession(
  queryClient: QueryClient,
  renewal: { stop: () => Promise<void>; resume: () => void }
) {
  await renewal.stop()
  await queryClient.cancelQueries({ queryKey: ['authSession'] })
  try {
    await api.logout()
    clearLocalAuthSession(queryClient)
  } catch (error) {
    renewal.resume()
    throw error
  }
}
