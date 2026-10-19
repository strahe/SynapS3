import { useQueryClient } from '@tanstack/react-query'
import { useEffect, useMemo, useRef } from 'react'
import { type AuthSession, api } from '@/api/client'
import { createAuthSessionRenewal } from '@/lib/auth-session-renewal'

const authActivityEvents = ['pointerdown', 'click', 'keydown', 'wheel'] as const

export function useAuthSessionRenewal(session: AuthSession) {
  const queryClient = useQueryClient()
  const sessionRef = useRef(session)
  sessionRef.current = session
  const renewal = useMemo(
    () =>
      createAuthSessionRenewal({
        getSession: () => sessionRef.current,
        refresh: (signal) => api.refreshAuthSession({ signal }),
        onRefreshed: (refreshedSession) => {
          sessionRef.current = refreshedSession
          queryClient.setQueryData(['authSession'], refreshedSession)
        },
      }),
    [queryClient]
  )

  useEffect(() => {
    renewal.activate()
    const handleActivity = (event: Event) => {
      if (event.isTrusted) renewal.attemptRefresh()
    }
    for (const eventName of authActivityEvents) {
      document.addEventListener(eventName, handleActivity, { passive: true })
    }
    return () => {
      renewal.deactivate()
      for (const eventName of authActivityEvents) {
        document.removeEventListener(eventName, handleActivity)
      }
    }
  }, [renewal])

  return renewal
}
