import assert from 'node:assert/strict'
import test from 'node:test'
import { QueryClient } from '@tanstack/react-query'
import { APIError, type AuthSession, api, setAdminCSRFToken } from '../src/api/client.ts'
import { createAuthSessionRenewal } from '../src/lib/auth-session-renewal.ts'

import {
  authSessionContentKind,
  authSessionQueryOptions,
  clearLocalAuthSession,
  logoutLocalAuthSession,
  storeLocalAuthSession,
} from '../src/routes/-auth-session.ts'

test('auth session query avoids focus refetch churn', () => {
  assert.equal(authSessionQueryOptions.staleTime, 5 * 60 * 1000)
  assert.equal(authSessionQueryOptions.refetchOnWindowFocus, false)
})

test('local auth clear removes other query data and marks auth session missing', () => {
  const calls: string[] = []
  const queryClient = {
    cancelQueries() {
      calls.push('cancelQueries')
      return Promise.resolve()
    },
    removeQueries(options: { predicate: (query: { queryKey: unknown[] }) => boolean }) {
      calls.push('removeQueries')
      assert.equal(options.predicate({ queryKey: ['authSession'] }), false)
      assert.equal(options.predicate({ queryKey: ['overview'] }), true)
    },
    setQueryData(key: unknown[], value: unknown) {
      calls.push(`setQueryData:${key.join('.')}:${value === null ? 'null' : 'value'}`)
    },
  }

  clearLocalAuthSession(queryClient as unknown as QueryClient)

  assert.deepEqual(calls, ['cancelQueries', 'removeQueries', 'setQueryData:authSession:null'])
})

const session: AuthSession = {
  username: 'admin',
  csrf_token: 'session-a',
  expires_at: '2030-01-01T00:00:00Z',
  refresh_after: '2020-01-01T00:00:00Z',
}

test('session verification failures preserve an authenticated shell and initial failures can retry', async () => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const unavailable = new APIError('Unavailable', 503)
  try {
    assert.equal(authSessionContentKind(undefined, unavailable), 'unavailable')
    assert.equal(authSessionContentKind(session, unavailable), 'authenticated')
    assert.equal(authSessionContentKind(session, new APIError('Unauthorized', 401)), 'login')
    assert.equal(authSessionContentKind(null, undefined), 'login')
    const options = {
      queryKey: ['authSession'],
      queryFn: async () => {
        throw unavailable
      },
    }
    await assert.rejects(queryClient.fetchQuery(options))
    assert.equal(
      authSessionContentKind(
        queryClient.getQueryData(['authSession']),
        queryClient.getQueryState(['authSession'])?.error
      ),
      'unavailable'
    )
    await queryClient.fetchQuery({ ...options, queryFn: async () => session })
    assert.equal(
      authSessionContentKind(
        queryClient.getQueryData(['authSession']),
        queryClient.getQueryState(['authSession'])?.error
      ),
      'authenticated'
    )
  } finally {
    queryClient.clear()
  }
})

test('a delayed session query cannot replace a new login in the query cache', async () => {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const originalFetch = globalThis.fetch
  const delayed = Promise.withResolvers<Response>()
  let sessionRequest: Promise<AuthSession> | undefined
  const next = { ...session, csrf_token: 'session-b' }
  globalThis.fetch = (async (input) =>
    String(input).endsWith('/auth/login') ? new Response(JSON.stringify(next)) : delayed.promise) as typeof fetch
  try {
    setAdminCSRFToken(session.csrf_token)
    const pending = queryClient
      .fetchQuery({
        queryKey: ['authSession'],
        queryFn: ({ signal }) => {
          sessionRequest = api.getAuthSession({ signal })
          return sessionRequest
        },
      })
      .catch(() => undefined)
    const loggedIn = await api.login({ username: 'admin', password: 'password' })
    storeLocalAuthSession(queryClient, loggedIn)
    delayed.resolve(new Response(JSON.stringify(session)))
    assert.ok(sessionRequest)
    await assert.rejects(sessionRequest, { name: 'AbortError' })
    await pending
    assert.equal(queryClient.getQueryData(['authSession']), loggedIn)
  } finally {
    setAdminCSRFToken('')
    globalThis.fetch = originalFetch
    queryClient.clear()
  }
})

test('renewal stop aborts and awaits its request, and resume cannot revive an unmounted session', async () => {
  const delayed = Promise.withResolvers<AuthSession>()
  let signal: AbortSignal | undefined
  const published: AuthSession[] = []
  const renewal = createAuthSessionRenewal({
    getSession: () => session,
    refresh: async (requestSignal) => {
      signal = requestSignal
      return delayed.promise
    },
    onRefreshed: (next) => published.push(next),
  })
  renewal.activate()
  renewal.attemptRefresh()
  let stopped = false
  const stop = renewal.stop().then(() => {
    stopped = true
  })
  assert.equal(signal?.aborted, true)
  assert.equal(stopped, false)
  delayed.resolve(session)
  await stop
  assert.deepEqual(published, [])
  renewal.deactivate()
  renewal.resume()
  assert.equal(renewal.attemptRefresh(), undefined)
})

for (const failure of ['503', 'network']) {
  test(`failed ${failure} logout retains the session and resumes renewal`, async () => {
    const queryClient = new QueryClient()
    const originalFetch = globalThis.fetch
    const renewed = { ...session, csrf_token: 'renewed', refresh_after: '2030-01-01T00:00:00Z' }
    let refreshed = 0
    globalThis.fetch = (async (input, init) => {
      if (String(input).endsWith('/auth/logout')) {
        if (failure === 'network') throw new TypeError('Network error')
        return new Response(JSON.stringify({ error: 'Unavailable' }), { status: 503 })
      }
      assert.equal(new Headers(init?.headers).get('X-SynapS3-CSRF'), session.csrf_token)
      refreshed++
      return new Response(JSON.stringify(renewed))
    }) as typeof fetch
    const renewal = createAuthSessionRenewal({
      getSession: () => session,
      refresh: (signal) => api.refreshAuthSession({ signal }),
      onRefreshed: (next) => queryClient.setQueryData(['authSession'], next),
    })
    try {
      setAdminCSRFToken(session.csrf_token)
      queryClient.setQueryData(['authSession'], session)
      renewal.activate()
      await assert.rejects(logoutLocalAuthSession(queryClient, renewal))
      assert.equal(queryClient.getQueryData(['authSession']), session)
      await renewal.attemptRefresh()
      assert.equal(refreshed, 1)
      assert.deepEqual(queryClient.getQueryData(['authSession']), renewed)
    } finally {
      renewal.deactivate()
      queryClient.clear()
      setAdminCSRFToken('')
      globalThis.fetch = originalFetch
    }
  })
}
