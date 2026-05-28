import { useMutation } from '@tanstack/react-query'
import { LockKeyhole } from 'lucide-react'
import { type FormEvent, useState } from 'react'
import { type AuthSession, api } from '@/api/client'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Field, FieldGroup, FieldLabel } from '@/components/ui/field'
import { Input } from '@/components/ui/input'

export function AdminLogin({ onAuthenticated }: { onAuthenticated: (session: AuthSession) => void }) {
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('')
  const login = useMutation({
    mutationFn: api.login,
    onSuccess: onAuthenticated,
  })

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    login.mutate({ username, password })
  }

  return (
    <main className="flex min-h-screen items-center justify-center bg-background p-6">
      <form className="flex w-full max-w-sm flex-col gap-5" onSubmit={submit}>
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <div className="flex size-9 items-center justify-center rounded-md border border-border bg-muted">
              <LockKeyhole />
            </div>
            <div className="flex min-w-0 flex-col">
              <h1 className="text-lg font-semibold leading-tight">SynapS3 Admin</h1>
              <p className="text-sm text-muted-foreground">Sign in to continue.</p>
            </div>
          </div>
        </div>

        {login.error instanceof Error && (
          <Alert variant="destructive">
            <AlertDescription>{login.error.message}</AlertDescription>
          </Alert>
        )}

        <FieldGroup>
          <Field>
            <FieldLabel htmlFor="admin-username">Username</FieldLabel>
            <Input
              id="admin-username"
              autoComplete="username"
              value={username}
              onChange={(event) => setUsername(event.target.value)}
            />
          </Field>
          <Field data-invalid={login.isError}>
            <FieldLabel htmlFor="admin-password">Password</FieldLabel>
            <Input
              id="admin-password"
              type="password"
              autoComplete="current-password"
              value={password}
              aria-invalid={login.isError}
              onChange={(event) => setPassword(event.target.value)}
            />
          </Field>
        </FieldGroup>

        <Button type="submit" disabled={login.isPending || username.trim() === '' || password === ''}>
          Sign In
        </Button>
      </form>
    </main>
  )
}
