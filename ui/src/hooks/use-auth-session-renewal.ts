import { useQueryClient } from '@tanstack/react-query'
import { useCallback, useEffect, useRef } from 'react'
import { type AuthSession, api } from '@/api/client'

const refreshRetryDelayMs = 30_000
const authActivityEvents = ['pointerdown', 'click', 'keydown', 'wheel'] as const

export function useAuthSessionRenewal(session: AuthSession) {
  const queryClient = useQueryClient()
  const sessionRef = useRef(session)
  const inFlightRef = useRef<Promise<void> | null>(null)
  const abortControllerRef = useRef<AbortController | null>(null)
  const retryAfterRef = useRef(0)
  const stoppedRef = useRef(false)
  sessionRef.current = session

  const attemptRefresh = useCallback(() => {
    const now = Date.now()
    const refreshAfter = Date.parse(sessionRef.current.refresh_after)
    if (
      stoppedRef.current ||
      inFlightRef.current ||
      !Number.isFinite(refreshAfter) ||
      now < refreshAfter ||
      now < retryAfterRef.current
    ) {
      return
    }

    retryAfterRef.current = now + refreshRetryDelayMs
    const abortController = new AbortController()
    abortControllerRef.current = abortController
    const request = api
      .refreshAuthSession({ signal: abortController.signal })
      .then((refreshedSession) => {
        if (Date.parse(refreshedSession.refresh_after) > Date.now()) {
          retryAfterRef.current = 0
        }
        if (stoppedRef.current) return
        sessionRef.current = refreshedSession
        queryClient.setQueryData(['authSession'], refreshedSession)
      })
      .catch(() => undefined)
      .finally(() => {
        if (inFlightRef.current === request) {
          inFlightRef.current = null
        }
        if (abortControllerRef.current === abortController) {
          abortControllerRef.current = null
        }
      })
    inFlightRef.current = request
  }, [queryClient])

  useEffect(() => {
    stoppedRef.current = false
    const handleActivity = (event: Event) => {
      if (event.isTrusted) {
        attemptRefresh()
      }
    }
    for (const eventName of authActivityEvents) {
      document.addEventListener(eventName, handleActivity, { passive: true })
    }
    return () => {
      stoppedRef.current = true
      abortControllerRef.current?.abort()
      for (const eventName of authActivityEvents) {
        document.removeEventListener(eventName, handleActivity)
      }
    }
  }, [attemptRefresh])

  return useCallback(async () => {
    stoppedRef.current = true
    abortControllerRef.current?.abort()
    await inFlightRef.current
  }, [])
}
