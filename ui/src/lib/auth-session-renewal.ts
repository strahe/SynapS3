import { type AuthSession, getAdminAuthEpoch } from '../api/client.ts'

const refreshRetryDelayMs = 30_000

export function createAuthSessionRenewal(options: {
  getSession: () => AuthSession
  refresh: (signal: AbortSignal) => Promise<AuthSession>
  onRefreshed: (session: AuthSession) => void
  now?: () => number
}) {
  const now = options.now ?? Date.now
  let active = false
  let stopped = false
  let retryAfter = 0
  let inFlight: Promise<void> | null = null
  let controller: AbortController | null = null

  return {
    activate() {
      active = true
      stopped = false
    },
    deactivate() {
      active = false
      stopped = true
      controller?.abort()
    },
    attemptRefresh() {
      const currentTime = now()
      const refreshAfter = Date.parse(options.getSession().refresh_after)
      if (
        !active ||
        stopped ||
        inFlight ||
        !Number.isFinite(refreshAfter) ||
        currentTime < refreshAfter ||
        currentTime < retryAfter
      ) {
        return
      }
      retryAfter = currentTime + refreshRetryDelayMs
      const requestController = new AbortController()
      const epoch = getAdminAuthEpoch()
      controller = requestController
      const request = options
        .refresh(requestController.signal)
        .then((session) => {
          if (!active || stopped || requestController.signal.aborted || epoch !== getAdminAuthEpoch()) return
          if (Date.parse(session.refresh_after) > now()) retryAfter = 0
          options.onRefreshed(session)
        })
        .catch(() => undefined)
        .finally(() => {
          if (inFlight === request) inFlight = null
          if (controller === requestController) controller = null
        })
      inFlight = request
      return request
    },
    async stop() {
      stopped = true
      controller?.abort()
      await inFlight
    },
    resume() {
      if (!active) return
      stopped = false
      retryAfter = 0
    },
  }
}
