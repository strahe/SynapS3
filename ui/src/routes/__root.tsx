import { type QueryClient, useQueryClient } from '@tanstack/react-query'
import { createRootRouteWithContext, Link, Outlet, useLocation } from '@tanstack/react-router'
import {
  AlertTriangle,
  BellOff,
  Database,
  HardDrive,
  LayoutDashboard,
  ListTodo,
  Monitor,
  Moon,
  PanelLeftOpen,
  RefreshCw,
  Settings,
  Sun,
  Wallet,
} from 'lucide-react'
import { type MouseEvent, useEffect, useState } from 'react'
import { FilecoinReadinessDialog } from '@/components/app/FilecoinReadinessDialog'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
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
import { useFilecoinReadiness, useSettings } from '@/hooks/queries'
import {
  filecoinReadinessStatusLabel,
  filecoinReadinessStatusTone,
  filecoinReadinessSummary,
  importantFilecoinReadinessChecks,
  isDismissibleFilecoinReadinessCheck,
  readDismissedFilecoinReadinessChecks,
  writeDismissedFilecoinReadinessCheck,
} from '@/lib/filecoin-readiness'
import { applyProviderIdentityEventData } from '@/lib/provider-identity-events'
import {
  getSystemThemeMediaQuery,
  normalizeThemePreference,
  readSystemPrefersDark,
  readThemePreference,
  resolveThemeDark,
  type ThemePreference,
  writeThemePreference,
} from '@/lib/theme'
import { applyUploadProgressEventData, applyUploadStateChangedEventData } from '@/lib/upload-progress-events'
import { applyWalletOperationEventData } from '@/lib/wallet-operation-events'
import {
  filecoinRuntimeReadinessEnabled,
  fullRuntimeAvailable,
  rootContentKind,
  rootUsesSetupShell,
} from './-root-content'

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
const systemThemeOption = { value: 'system', label: 'System', icon: Monitor } as const
const themeOptions = [
  systemThemeOption,
  { value: 'dark', label: 'Dark', icon: Moon },
  { value: 'light', label: 'Light', icon: Sun },
] as const

function readSidebarDefaultOpen() {
  if (typeof document === 'undefined') return true

  const cookie = document.cookie.split('; ').find((row) => row.startsWith(`${sidebarCookieName}=`))
  return cookie ? cookie.split('=')[1] === 'true' : true
}

function RootLayout() {
  const [themePreference, setThemePreference] = useState<ThemePreference>(() => readThemePreference())
  const [systemPrefersDark, setSystemPrefersDark] = useState(readSystemPrefersDark)
  const location = useLocation()
  const { data: settings, isLoading: settingsLoading } = useSettings()
  const runtimeAvailable = fullRuntimeAvailable(settings, settingsLoading)
  const activeNavItems = settingsLoading || rootUsesSetupShell(settings) ? setupNavItems : navItems
  const contentKind = rootContentKind(settings, location.pathname)
  const dark = resolveThemeDark(themePreference, systemPrefersDark)

  useEffect(() => {
    const mq = getSystemThemeMediaQuery()
    if (!mq) return
    setSystemPrefersDark(mq.matches)
    const handler = (e: MediaQueryListEvent) => setSystemPrefersDark(e.matches)
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

  function handleThemePreferenceChange(preference: ThemePreference) {
    setThemePreference(preference)
    writeThemePreference(preference)
  }

  return (
    <SidebarProvider defaultOpen={readSidebarDefaultOpen()}>
      <AdminEventsBridge enabled={runtimeAvailable} />
      <AppSidebar
        activeNavItems={activeNavItems}
        pathname={location.pathname}
        themePreference={themePreference}
        onThemePreferenceChange={handleThemePreferenceChange}
      />

      <SidebarInset className="overflow-auto">
        <div className="flex h-14 items-center gap-2 border-b border-border px-4 md:hidden">
          <SidebarTrigger />
          <span className="font-semibold">SynapS3</span>
        </div>
        {contentKind === 'setup-required' ? (
          <SetupRequired configPath={settings?.config_path ?? ''} />
        ) : (
          <>
            <GlobalFilecoinReadinessAlert enabled={filecoinRuntimeReadinessEnabled(settings, settingsLoading)} />
            <Outlet />
          </>
        )}
      </SidebarInset>
    </SidebarProvider>
  )
}

function GlobalFilecoinReadinessAlert({ enabled }: { enabled: boolean }) {
  const readiness = useFilecoinReadiness(enabled)
  const [detailsOpen, setDetailsOpen] = useState(false)
  const [dismissedCheckIds, setDismissedCheckIds] = useState(() => readDismissedFilecoinReadinessChecks())
  const data = readiness.data
  const status = data?.status ?? 'unknown'
  const failed = readiness.error instanceof Error
  const visibleChecks = data ? importantFilecoinReadinessChecks(data.checks, dismissedCheckIds) : []
  const primaryCheck = visibleChecks[0]
  const showDataAlert =
    data != null && data.status !== 'ready' && (visibleChecks.length > 0 || data.checks.length === 0)
  const show = enabled && (failed || showDataAlert)

  if (!show) return null

  const title = failed ? 'Filecoin readiness could not be checked' : 'Filecoin uploads need attention'
  const summary = failed ? readiness.error?.message : filecoinReadinessSummary(data, dismissedCheckIds)
  const danger = failed || status === 'blocked'
  const attention = !danger
  const canDismissPrimaryCheck = Boolean(primaryCheck && isDismissibleFilecoinReadinessCheck(primaryCheck.id))

  function dismissCheck(id: string) {
    writeDismissedFilecoinReadinessCheck(id, true)
    setDismissedCheckIds((current) => {
      const next = new Set(current)
      next.add(id)
      return next
    })
  }

  return (
    <>
      <div className="w-full min-w-0 px-6 pt-6">
        <Alert
          variant={danger ? 'destructive' : 'default'}
          className={
            attention
              ? 'max-w-full border-[color:var(--status-warning-border)] bg-[var(--status-warning-bg)] text-[color:var(--status-warning)]'
              : 'max-w-full'
          }
        >
          <AlertTriangle />
          <AlertTitle className="flex flex-wrap items-center gap-2">
            {title}
            {!failed && (
              <StatusBadge tone={filecoinReadinessStatusTone(status)}>
                {filecoinReadinessStatusLabel(status)}
              </StatusBadge>
            )}
          </AlertTitle>
          <AlertDescription className={attention ? 'text-[color:var(--status-warning)]' : undefined}>
            <div className="flex min-w-0 flex-wrap items-center justify-between gap-3">
              <span className="min-w-0 flex-1 break-words">{summary}</span>
              <div className="flex shrink-0 flex-wrap items-center gap-2">
                {canDismissPrimaryCheck && primaryCheck && (
                  <Button type="button" variant="outline" size="sm" onClick={() => dismissCheck(primaryCheck.id)}>
                    <BellOff data-icon="inline-start" />
                    Dismiss
                  </Button>
                )}
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => readiness.refetch()}
                  disabled={readiness.isFetching}
                >
                  <RefreshCw data-icon="inline-start" className={readiness.isFetching ? 'animate-spin' : undefined} />
                  Refresh
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={!data}
                  onClick={() => data && setDetailsOpen(true)}
                >
                  Details
                </Button>
              </div>
            </div>
          </AlertDescription>
        </Alert>
      </div>
      <FilecoinReadinessDialog
        title="Filecoin Readiness"
        data={data}
        open={detailsOpen}
        onOpenChange={setDetailsOpen}
        dismissedCheckIds={dismissedCheckIds}
        onDismissCheck={dismissCheck}
      />
    </>
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

function AppSidebar({
  activeNavItems,
  pathname,
  themePreference,
  onThemePreferenceChange,
}: {
  activeNavItems: NavItem[]
  pathname: string
  themePreference: ThemePreference
  onThemePreferenceChange: (preference: ThemePreference) => void
}) {
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
      <SidebarFooter>
        <SidebarMenu>
          <SidebarMenuItem>
            <ThemeMenu
              value={themePreference}
              isMobile={isMobile}
              onChange={(preference) => {
                onThemePreferenceChange(preference)
                closeMobileSidebar()
              }}
            />
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  )
}

function ThemeMenu({
  value,
  isMobile,
  onChange,
}: {
  value: ThemePreference
  isMobile: boolean
  onChange: (preference: ThemePreference) => void
}) {
  const activeOption = themeOptions.find((option) => option.value === value) ?? systemThemeOption
  const ActiveIcon = activeOption.icon

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <SidebarMenuButton tooltip={`Theme: ${activeOption.label}`} aria-label={`Theme: ${activeOption.label}`}>
          <ActiveIcon />
          <span className="group-data-[collapsible=icon]:hidden">Theme: {activeOption.label}</span>
        </SidebarMenuButton>
      </DropdownMenuTrigger>
      <DropdownMenuContent side={isMobile ? 'top' : 'right'} align="end" className="w-40">
        <DropdownMenuRadioGroup value={value} onValueChange={(next) => onChange(normalizeThemePreference(next))}>
          {themeOptions.map((option) => {
            const Icon = option.icon
            return (
              <DropdownMenuRadioItem key={option.value} value={option.value}>
                <Icon />
                {option.label}
              </DropdownMenuRadioItem>
            )
          })}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
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
