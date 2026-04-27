import type { QueryClient } from '@tanstack/react-query'
import { createRootRouteWithContext, Link, Outlet, useLocation } from '@tanstack/react-router'
import { AlertTriangle, Database, HardDrive, LayoutDashboard, ListTodo, Loader2, Settings, Wallet } from 'lucide-react'
import { useEffect, useState } from 'react'
import { useSettings } from '@/hooks/queries'
import { cn } from '@/lib/utils'

interface RouterContext {
  queryClient: QueryClient
}

export const Route = createRootRouteWithContext<RouterContext>()({
  component: RootLayout,
})

const navItems = [
  { to: '/' as const, label: 'Overview', icon: LayoutDashboard },
  { to: '/buckets' as const, label: 'Buckets', icon: Database },
  { to: '/tasks' as const, label: 'Tasks', icon: ListTodo },
  { to: '/wallet' as const, label: 'Wallet', icon: Wallet },
  { to: '/settings' as const, label: 'Settings', icon: Settings },
]

const setupNavItems = [{ to: '/settings' as const, label: 'Settings', icon: Settings }]

function RootLayout() {
  const [collapsed, setCollapsed] = useState(false)
  const [dark, setDark] = useState(false)
  const location = useLocation()
  const { data: settings, isLoading: settingsLoading } = useSettings()
  const setupMode = settings?.mode === 'setup'
  const activeNavItems = settingsLoading || setupMode ? setupNavItems : navItems

  useEffect(() => {
    const mq = window.matchMedia('(prefers-color-scheme: dark)')
    setDark(mq.matches)
    const handler = (e: MediaQueryListEvent) => setDark(e.matches)
    mq.addEventListener('change', handler)
    return () => mq.removeEventListener('change', handler)
  }, [])

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
  }, [dark])

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside
        className={cn(
          'flex flex-col border-r border-border bg-sidebar transition-all duration-200',
          collapsed ? 'w-14' : 'w-60'
        )}
      >
        <div className="flex h-14 items-center gap-2 border-b border-border px-3">
          <HardDrive className="h-6 w-6 shrink-0 text-sidebar-primary" />
          {!collapsed && <span className="font-semibold text-sidebar-foreground">SynapS3</span>}
        </div>
        <nav className="flex-1 space-y-1 p-2">
          {activeNavItems.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              className="flex items-center gap-3 rounded-md px-3 py-2 text-sm text-sidebar-foreground hover:bg-sidebar-accent [&.active]:bg-sidebar-accent [&.active]:font-medium"
            >
              <item.icon className="h-4 w-4 shrink-0" />
              {!collapsed && <span>{item.label}</span>}
            </Link>
          ))}
        </nav>
        <div className="border-t border-border p-2">
          <button
            type="button"
            onClick={() => setCollapsed(!collapsed)}
            className="flex w-full items-center justify-center rounded-md px-3 py-2 text-sm text-muted-foreground hover:bg-sidebar-accent"
          >
            {collapsed ? '→' : '← Collapse'}
          </button>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-auto">
        {settingsLoading ? (
          <ShellLoading />
        ) : setupMode && location.pathname !== '/settings' ? (
          <SetupRequired configPath={settings.config_path} />
        ) : (
          <Outlet />
        )}
      </main>
    </div>
  )
}

function ShellLoading() {
  return (
    <div className="flex h-full items-center justify-center">
      <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
    </div>
  )
}

function SetupRequired({ configPath }: { configPath: string }) {
  return (
    <div className="flex h-full items-center justify-center p-6">
      <div className="max-w-xl rounded-lg border border-yellow-500/30 bg-yellow-500/10 p-5">
        <div className="flex items-start gap-3">
          <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-yellow-500" />
          <div className="space-y-2">
            <h1 className="font-semibold text-foreground">Setup required</h1>
            <p className="text-sm text-muted-foreground">
              SynapS3 is running in setup mode. Complete configuration in Settings, then restart the service.
            </p>
            <p className="break-all font-mono text-xs text-muted-foreground">{configPath}</p>
            <Link
              to="/settings"
              className="inline-flex rounded-md bg-primary px-3 py-2 text-sm text-primary-foreground"
            >
              Open Settings
            </Link>
          </div>
        </div>
      </div>
    </div>
  )
}
