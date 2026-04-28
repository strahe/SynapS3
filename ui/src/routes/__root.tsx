import type { QueryClient } from '@tanstack/react-query'
import { createRootRouteWithContext, Link, Outlet, useLocation } from '@tanstack/react-router'
import { AlertTriangle, Database, HardDrive, LayoutDashboard, ListTodo, Loader2, Settings, Wallet } from 'lucide-react'
import { useEffect, useState } from 'react'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  Sidebar,
  SidebarContent,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarRail,
  SidebarTrigger,
  useSidebar,
} from '@/components/ui/sidebar'
import { useSettings } from '@/hooks/queries'

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
type NavItem = (typeof navItems)[number]

const setupNavItems: NavItem[] = [{ to: '/settings', label: 'Settings', icon: Settings }]
const sidebarCookieName = 'sidebar_state'

function readSidebarDefaultOpen() {
  if (typeof document === 'undefined') return true

  const cookie = document.cookie.split('; ').find((row) => row.startsWith(`${sidebarCookieName}=`))
  return cookie ? cookie.split('=')[1] === 'true' : true
}

function RootLayout() {
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

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() !== 'b' || (!event.metaKey && !event.ctrlKey)) return
      if (!(event.target instanceof HTMLElement)) return
      if (event.target.closest('input, textarea, select, [contenteditable="true"], [role="textbox"]')) {
        event.stopPropagation()
      }
    }

    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [])

  return (
    <SidebarProvider defaultOpen={readSidebarDefaultOpen()}>
      <AppSidebar activeNavItems={activeNavItems} pathname={location.pathname} />

      <SidebarInset className="overflow-auto">
        <div className="flex h-14 items-center gap-2 border-b border-border px-4 md:hidden">
          <SidebarTrigger />
          <span className="font-semibold">SynapS3</span>
        </div>
        {settingsLoading ? (
          <ShellLoading />
        ) : setupMode && location.pathname !== '/settings' ? (
          <SetupRequired configPath={settings.config_path} />
        ) : (
          <Outlet />
        )}
      </SidebarInset>
    </SidebarProvider>
  )
}

function AppSidebar({ activeNavItems, pathname }: { activeNavItems: NavItem[]; pathname: string }) {
  const { isMobile, setOpenMobile } = useSidebar()
  const closeMobileSidebar = () => {
    if (isMobile) setOpenMobile(false)
  }

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader>
        <div className="flex items-center gap-2 group-data-[collapsible=icon]:flex-col group-data-[collapsible=icon]:gap-1">
          <SidebarMenu className="min-w-0 flex-1 group-data-[collapsible=icon]:flex-none">
            <SidebarMenuItem>
              <SidebarMenuButton asChild size="lg" tooltip="SynapS3">
                <Link to="/" onClick={closeMobileSidebar}>
                  <span className="flex size-8 shrink-0 items-center justify-center rounded-md bg-sidebar-primary text-sidebar-primary-foreground">
                    <HardDrive />
                  </span>
                  <span className="truncate font-semibold group-data-[collapsible=icon]:hidden">SynapS3</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
          <SidebarTrigger className="shrink-0" />
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarMenu className="p-2">
          {activeNavItems.map((item) => (
            <SidebarMenuItem key={item.to}>
              <SidebarMenuButton
                asChild
                isActive={pathname === item.to || (item.to !== '/' && pathname.startsWith(item.to))}
                tooltip={item.label}
              >
                <Link to={item.to} onClick={closeMobileSidebar}>
                  <item.icon />
                  <span className="group-data-[collapsible=icon]:hidden">{item.label}</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarContent>
      <SidebarRail />
    </Sidebar>
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
      <Alert className="max-w-xl">
        <AlertTriangle />
        <AlertTitle>Setup required</AlertTitle>
        <AlertDescription className="flex flex-col items-start gap-3">
          <span>SynapS3 is running in setup mode. Complete configuration in Settings, then restart the service.</span>
          <span className="break-all font-mono text-xs">{configPath}</span>
          <Button asChild>
            <Link to="/settings">Open Settings</Link>
          </Button>
        </AlertDescription>
      </Alert>
    </div>
  )
}
