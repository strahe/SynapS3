import { type QueryClient, useQueryClient } from '@tanstack/react-query'
import { createRootRouteWithContext, Link, Outlet, useLocation } from '@tanstack/react-router'
import {
  AlertTriangle,
  Database,
  HardDrive,
  LayoutDashboard,
  ListTodo,
  PanelLeftOpen,
  Settings,
  Wallet,
} from 'lucide-react'
import { type MouseEvent, useEffect, useState } from 'react'
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
import { applyProviderIdentityEventData } from '@/lib/provider-identity-events'
import { applyUploadProgressEventData, applyUploadStateChangedEventData } from '@/lib/upload-progress-events'
import { applyWalletOperationEventData } from '@/lib/wallet-operation-events'
import { rootContentKind } from './-root-content'

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
  const contentKind = rootContentKind(settings, location.pathname)

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
      <AdminEventsBridge enabled={!settingsLoading && !setupMode} />
      <AppSidebar activeNavItems={activeNavItems} pathname={location.pathname} />

      <SidebarInset className="overflow-auto">
        <div className="flex h-14 items-center gap-2 border-b border-border px-4 md:hidden">
          <SidebarTrigger />
          <span className="font-semibold">SynapS3</span>
        </div>
        {contentKind === 'setup-required' ? <SetupRequired configPath={settings?.config_path ?? ''} /> : <Outlet />}
      </SidebarInset>
    </SidebarProvider>
  )
}

function AdminEventsBridge({ enabled }: { enabled: boolean }) {
  const queryClient = useQueryClient()

  useEffect(() => {
    if (!enabled) return

    const events = new EventSource('/api/v1/events')
    events.addEventListener('provider_identity_updated', (event) => {
      applyProviderIdentityEventData(queryClient, event.data)
    })
    events.addEventListener('upload_progress_updated', (event) => {
      applyUploadProgressEventData(queryClient, event.data)
    })
    events.addEventListener('upload_state_changed', (event) => {
      applyUploadStateChangedEventData(queryClient, event.data)
    })
    events.addEventListener('wallet_operation_updated', (event) => {
      applyWalletOperationEventData(queryClient, event.data)
    })
    return () => events.close()
  }, [enabled, queryClient])

  return null
}

function AppSidebar({ activeNavItems, pathname }: { activeNavItems: NavItem[]; pathname: string }) {
  const { isMobile, setOpenMobile, state, toggleSidebar } = useSidebar()
  const closeMobileSidebar = () => {
    if (isMobile) setOpenMobile(false)
  }
  const handleLogoClick = (event: MouseEvent<HTMLAnchorElement>) => {
    if (!isMobile && state === 'collapsed') {
      event.preventDefault()
      toggleSidebar()
      return
    }
    closeMobileSidebar()
  }

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="h-16 justify-center">
        <div className="flex h-10 items-center gap-2">
          <SidebarMenu className="min-w-0 flex-1">
            <SidebarMenuItem>
              <SidebarMenuButton
                asChild
                size="lg"
                tooltip={state === 'collapsed' && !isMobile ? 'Expand sidebar' : 'SynapS3'}
              >
                <Link
                  to="/"
                  onClick={handleLogoClick}
                  className="group/logo"
                  aria-label={state === 'collapsed' && !isMobile ? 'Expand sidebar' : 'SynapS3'}
                >
                  <span className="relative flex size-8 shrink-0 items-center justify-center rounded-md bg-sidebar-primary text-sidebar-primary-foreground">
                    <HardDrive className="size-4 transition-opacity group-data-[collapsible=icon]:group-hover/logo:opacity-0" />
                    <PanelLeftOpen className="absolute size-4 opacity-0 transition-opacity group-data-[collapsible=icon]:group-hover/logo:opacity-100" />
                  </span>
                  <span className="truncate font-semibold group-data-[collapsible=icon]:hidden">SynapS3</span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
          <SidebarTrigger className="shrink-0 group-data-[collapsible=icon]:hidden" />
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
