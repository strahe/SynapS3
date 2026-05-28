import assert from 'node:assert/strict'
import test from 'node:test'
import type { QueryClient } from '@tanstack/react-query'

import { authSessionQueryOptions, clearLocalAuthSession } from '../src/routes/-auth-session.ts'

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
