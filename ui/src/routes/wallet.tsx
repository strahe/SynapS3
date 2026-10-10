import { useQueryClient } from '@tanstack/react-query'
import { createFileRoute, Link } from '@tanstack/react-router'
import {
  AlertTriangle,
  ArrowDownToLine,
  ArrowUpFromLine,
  Clock,
  Coins,
  Fuel,
  Loader2,
  type LucideIcon,
  ShieldCheck,
  Wallet,
} from 'lucide-react'
import { type ReactNode, useMemo, useState } from 'react'
import type { PaymentAccountData, WalletOperation, WalletOperationStatus } from '@/api/client'
import { CopyableValue } from '@/components/app/CopyableValue'
import { DataTableFrame, tableHeaderRowClassName } from '@/components/app/DataTable'
import { DetailTextDialog } from '@/components/app/DetailTextDialog'
import { PageHeader, RefreshButton } from '@/components/app/PageHeader'
import { EmptyState, PageError } from '@/components/app/PageState'
import { RelativeTime } from '@/components/app/RelativeTime'
import { StatusBadge, type StatusTone } from '@/components/app/StatusBadge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import {
  useFilecoinReadiness,
  useWallet,
  useWalletApprove,
  useWalletFund,
  useWalletOperations,
  useWalletWithdraw,
} from '@/hooks/queries'
import { walletOperationStatusLabel, walletOperationTypeLabel } from '@/lib/display-labels'
import { filecoinReadinessStatusLabel, filecoinReadinessStatusTone } from '@/lib/filecoin-readiness'
import { cn, formatAttoFIL, formatDuration, formatTokenAmount } from '@/lib/utils'
import {
  baseUnitsToDecimal,
  createWalletOperationDraft,
  decimalToBaseUnits,
  fundedUntilCaption,
  fundedUntilTone,
  topUpNeeded,
  type WalletOperationDialogCloseReason,
  type WalletOperationDraft,
  type WalletRunwayTone,
  walletFwssApprovalState,
  walletOperationDetail,
  walletOperationDialogShouldClearDraft,
  walletOperationMutationError,
  walletOperationPayload,
} from '@/lib/wallet-operations'

export const Route = createFileRoute('/wallet')({
  component: WalletPage,
})

const usdfcDecimals = 18
const topUpTargets = [
  { label: '30 days', epochs: 86_400 },
  { label: '3 months', epochs: 259_200 },
  { label: '6 months', epochs: 518_400 },
] as const
const walletOperationsDefaultLimit = 10
const walletOperationsLimitOptions = [10, 20, 50, 100] as const

interface PendingWalletOperation extends WalletOperationDraft {
  clearInput?: () => void
}

function WalletPage() {
  const queryClient = useQueryClient()
  const { data, isLoading, error, refetch, isFetching } = useWallet()
  const readiness = useFilecoinReadiness(Boolean(data?.configured))
  const [operationsLimit, setOperationsLimit] = useState(walletOperationsDefaultLimit)
  const { data: operationsData } = useWalletOperations(operationsLimit)
  const nextOperationsLimit = walletOperationsLimitOptions.find((limit) => limit > operationsLimit)
  const fundMutation = useWalletFund()
  const withdrawMutation = useWalletWithdraw()
  const approveMutation = useWalletApprove()
  const [fundAmount, setFundAmount] = useState('')
  const [withdrawAmount, setWithdrawAmount] = useState('')
  const [formError, setFormError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [pendingOperation, setPendingOperation] = useState<PendingWalletOperation | null>(null)
  const [operationDetailText, setOperationDetailText] = useState<string | null>(null)

  const paymentAccount = data?.payment_account ?? null
  const decimals = data?.contracts?.usdfc_decimals ?? usdfcDecimals
  const operations = operationsData?.operations ?? []
  const fwssApproval = walletFwssApprovalState({
    readiness: readiness.data,
    error: readiness.error,
    isLoading: readiness.isLoading || readiness.isFetching,
  })
  const mutationError = walletOperationMutationError(
    pendingOperation?.type,
    fundMutation.error,
    withdrawMutation.error,
    approveMutation.error
  )
  const isMutating = fundMutation.isPending || withdrawMutation.isPending || approveMutation.isPending

  const topUpAmounts = useMemo(() => {
    return topUpTargets.map((target) => ({
      ...target,
      amount: paymentAccount ? topUpNeeded(paymentAccount, target.epochs) : 0n,
    }))
  }, [paymentAccount])

  const refreshWallet = () => {
    queryClient.invalidateQueries({ queryKey: ['wallet'] })
    queryClient.invalidateQueries({ queryKey: ['walletOperations'] })
    queryClient.invalidateQueries({ queryKey: ['filecoinReadiness'] })
  }
  const header = (
    <PageHeader title="Wallet" actions={<RefreshButton onClick={refreshWallet} refreshing={isFetching} />} />
  )

  if (isLoading) return <WalletSkeleton header={header} />

  if (error || !data || !data.configured) {
    return (
      <div className="flex flex-col gap-6 p-6">
        {header}
        {error || !data ? (
          <PageError
            title="Failed to load wallet"
            description={error?.message}
            onRetry={() => refetch()}
            retrying={isFetching}
          />
        ) : (
          <EmptyState
            icon={<Wallet />}
            title="Wallet not configured"
            description="Set the Filecoin private key in the configuration to enable wallet features."
            action={
              <Button asChild size="sm" variant="outline">
                <Link to="/settings" search={{ tab: 'filecoin' }}>
                  Open settings
                </Link>
              </Button>
            }
          />
        )}
      </div>
    )
  }

  const submitFund = () => {
    const parsed = decimalToBaseUnits(fundAmount, decimals)
    if (!parsed.ok) {
      setFormError(parsed.error)
      return
    }
    setFormError(null)
    setNotice(null)
    queueWalletOperation('fund', parsed.value.toString(), () => setFundAmount(''))
  }

  const submitWithdraw = () => {
    const parsed = decimalToBaseUnits(withdrawAmount, decimals)
    if (!parsed.ok) {
      setFormError(parsed.error)
      return
    }
    setFormError(null)
    setNotice(null)
    queueWalletOperation('withdraw', parsed.value.toString(), () => setWithdrawAmount(''))
  }

  const withdrawMax = () => {
    if (!paymentAccount?.available_funds) return
    setWithdrawAmount(baseUnitsToDecimal(paymentAccount.available_funds, decimals))
    setFormError(null)
  }

  const topUpRunway = (amount: bigint, label: string) => {
    if (amount <= 0n) {
      setNotice(`Runway already meets ${label}`)
      setFormError(null)
      return
    }
    setNotice(null)
    setFormError(null)
    queueWalletOperation('fund', amount.toString(), undefined, `runway-${label.replace(/\s+/g, '-')}`)
  }

  const submitApprove = () => {
    setNotice(null)
    setFormError(null)
    queueWalletOperation('approve', '0', undefined, 'approve-fwss')
  }

  const queueWalletOperation = (
    type: PendingWalletOperation['type'],
    amount: string,
    clearInput?: () => void,
    requestPrefix: string = type
  ) => {
    fundMutation.reset()
    withdrawMutation.reset()
    approveMutation.reset()
    const draft = createWalletOperationDraft({ type, amountBaseUnits: amount, decimals, requestPrefix })
    setPendingOperation({ ...draft, clearInput })
    setNotice(null)
    setFormError(null)
  }

  const clearPendingOperation = (reason: WalletOperationDialogCloseReason) => {
    if (walletOperationDialogShouldClearDraft(reason, isMutating)) {
      fundMutation.reset()
      withdrawMutation.reset()
      approveMutation.reset()
      setPendingOperation(null)
    }
  }

  const handlePendingOperationOpenChange = (open: boolean) => {
    if (!open) clearPendingOperation('dismiss')
  }

  const confirmPendingOperation = () => {
    if (!pendingOperation) return
    const clearInput = pendingOperation.clearInput
    const onSuccess = () => {
      clearInput?.()
      clearPendingOperation('success')
    }
    if (pendingOperation.type === 'approve') {
      approveMutation.mutate(walletOperationPayload(pendingOperation), { onSuccess })
    } else if (pendingOperation.type === 'withdraw') {
      const payload = { client_request_id: pendingOperation.clientRequestID, amount: pendingOperation.amount }
      withdrawMutation.mutate(payload, { onSuccess })
    } else {
      const payload = { client_request_id: pendingOperation.clientRequestID, amount: pendingOperation.amount }
      fundMutation.mutate(payload, { onSuccess })
    }
  }

  return (
    <div className="flex flex-col gap-6 p-6">
      {header}

      {data.partial_errors && Object.keys(data.partial_errors).length > 0 && (
        <Alert>
          <AlertTriangle />
          <AlertTitle>Some data could not be retrieved</AlertTitle>
          <AlertDescription>
            <div className="flex flex-col gap-1">
              {Object.entries(data.partial_errors).map(([key, msg]) => (
                <p key={key} className="text-xs text-muted-foreground">
                  <span className="font-mono">{key}</span>: {msg}
                </p>
              ))}
            </div>
          </AlertDescription>
        </Alert>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Account</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-5">
          <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <IdentityField
              label="Address"
              value={data.identity?.address ?? '—'}
              copyable
              className="sm:col-span-2 lg:col-span-2"
            />
            <IdentityField label="Network" value={data.chain?.network ?? '—'} badge />
            <IdentityField label="Chain ID" value={data.chain?.chain_id?.toString() ?? '—'} />
            <IdentityField label="Nonce" value={data.identity?.nonce?.toString() ?? '—'} />
            <IdentityField label="Current epoch" value={data.chain?.current_epoch ?? '—'} />
          </dl>
          <div className="grid gap-4 sm:grid-cols-2">
            <BalanceCard
              icon={Fuel}
              title="FIL balance"
              amount={formatAttoFIL(data.wallet_balances?.fil_gas)}
              raw={data.wallet_balances?.fil_gas}
            />
            <BalanceCard
              icon={Coins}
              title="USDFC balance"
              amount={formatTokenAmount(data.wallet_balances?.usdfc, decimals, 'USDFC')}
              raw={data.wallet_balances?.usdfc}
            />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>USDFC payment account</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-5">
          {paymentAccount ? (
            <>
              <PaymentAccountStats account={paymentAccount} decimals={decimals} />
              <div className="grid gap-4 lg:grid-cols-2">
                <OperationBox
                  label="Fund"
                  value={fundAmount}
                  onValueChange={setFundAmount}
                  onSubmit={submitFund}
                  disabled={isMutating}
                  icon={<ArrowDownToLine />}
                />
                <OperationBox
                  label="Withdraw"
                  value={withdrawAmount}
                  onValueChange={setWithdrawAmount}
                  onSubmit={submitWithdraw}
                  disabled={isMutating}
                  icon={<ArrowUpFromLine />}
                  secondaryAction={
                    <Button
                      type="button"
                      variant="outline"
                      onClick={withdrawMax}
                      disabled={!paymentAccount.available_funds || isMutating}
                    >
                      Max
                    </Button>
                  }
                />
              </div>
              <div className="rounded-md border border-border p-4">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                  <div>
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="text-sm font-medium">FWSS approval</div>
                      <StatusBadge tone={filecoinReadinessStatusTone(fwssApproval.status)}>
                        {filecoinReadinessStatusLabel(fwssApproval.status)}
                      </StatusBadge>
                    </div>
                    <p className="mt-1 text-sm text-muted-foreground">{fwssApproval.message}</p>
                    {fwssApproval.action && <p className="mt-1 text-xs text-muted-foreground">{fwssApproval.action}</p>}
                  </div>
                  {fwssApproval.canApprove && (
                    <Button type="button" variant="outline" onClick={submitApprove} disabled={isMutating}>
                      <ShieldCheck data-icon="inline-start" />
                      Approve FWSS
                    </Button>
                  )}
                </div>
              </div>
              <div className="flex flex-col gap-2">
                <span className="text-sm text-muted-foreground">Top up to target runway</span>
                <div className="grid gap-2 sm:grid-cols-3">
                  {topUpAmounts.map((target) => (
                    <Button
                      key={target.label}
                      type="button"
                      variant="outline"
                      size="sm"
                      className="h-auto min-h-12 justify-start whitespace-normal px-3 py-2 text-left"
                      disabled={isMutating || paymentAccount.no_active_spend}
                      onClick={() => topUpRunway(target.amount, target.label)}
                      title={`Additional needed: ${formatTokenAmount(target.amount.toString(), decimals, 'USDFC')}`}
                    >
                      <Clock data-icon="inline-start" />
                      <span className="flex min-w-0 flex-col items-start gap-0.5">
                        <span className="leading-none">To {target.label}</span>
                        <span className="max-w-full truncate text-xs font-normal text-muted-foreground group-hover/button:text-foreground">
                          {paymentAccount.no_active_spend
                            ? 'Not needed'
                            : target.amount > 0n
                              ? formatTokenAmount(target.amount.toString(), decimals, 'USDFC')
                              : 'Already funded'}
                        </span>
                      </span>
                    </Button>
                  ))}
                </div>
              </div>
              {(formError || mutationError || notice) && (
                <p className={cn('text-sm', formError || mutationError ? 'text-destructive' : 'text-muted-foreground')}>
                  {formError ?? mutationError ?? notice}
                </p>
              )}
            </>
          ) : (
            <p className="text-sm text-muted-foreground">USDFC payment account data unavailable</p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Operations</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <OperationsTable operations={operations} decimals={decimals} onOpenDetails={setOperationDetailText} />
          {nextOperationsLimit && operations.length >= operationsLimit && (
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="self-center"
              onClick={() => setOperationsLimit(nextOperationsLimit)}
            >
              Show more
            </Button>
          )}
        </CardContent>
      </Card>

      {data.contracts && (
        <Card>
          <CardHeader>
            <CardTitle>Advanced</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid gap-4 sm:grid-cols-2">
              <IdentityField label="Payments contract" value={data.contracts.payments_address} copyable />
              <IdentityField label="USDFC token" value={data.contracts.usdfc_address} copyable />
            </dl>
          </CardContent>
        </Card>
      )}
      <AlertDialog open={pendingOperation != null} onOpenChange={handlePendingOperationOpenChange}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{pendingOperation?.confirmation.title ?? 'Confirm operation'}</AlertDialogTitle>
            <AlertDialogDescription>{pendingOperation?.confirmation.description}</AlertDialogDescription>
          </AlertDialogHeader>
          <div className="rounded-md border border-border p-3">
            <div className="text-xs text-muted-foreground">Amount</div>
            <div className="mt-1 font-mono text-sm">{pendingOperation?.confirmation.amount ?? '—'}</div>
          </div>
          {mutationError && (
            <Alert variant="destructive">
              <AlertDescription>{mutationError}</AlertDescription>
            </Alert>
          )}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isMutating}>Cancel</AlertDialogCancel>
            <Button type="button" disabled={isMutating} onClick={confirmPendingOperation}>
              {isMutating && <Loader2 data-icon="inline-start" className="animate-spin" />}
              {pendingOperation?.confirmation.actionLabel ?? 'Confirm'}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
      <DetailTextDialog
        title="Operation details"
        text={operationDetailText}
        onClose={() => setOperationDetailText(null)}
      />
    </div>
  )
}

function WalletSkeleton({ header }: { header: ReactNode }) {
  return (
    <div className="flex flex-col gap-6 p-6">
      {header}
      <Card>
        <CardHeader>
          <CardTitle>Account</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-5">
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <SkeletonField className="sm:col-span-2 lg:col-span-2" />
            <SkeletonField />
            <SkeletonField />
            <SkeletonField />
            <SkeletonField />
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <Skeleton className="h-20" />
            <Skeleton className="h-20" />
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>USDFC payment account</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <Skeleton className="h-16" />
            <Skeleton className="h-16" />
            <Skeleton className="h-16" />
            <Skeleton className="h-16" />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function SkeletonField({ className }: { className?: string }) {
  return (
    <div className={className}>
      <Skeleton className="h-3 w-20" />
      <Skeleton className="mt-2 h-5 w-full max-w-72" />
    </div>
  )
}

function IdentityField({
  label,
  value,
  copyable,
  badge,
  className,
}: {
  label: string
  value: string
  copyable?: boolean
  badge?: boolean
  className?: string
}) {
  return (
    <div className={className}>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="mt-1 flex min-w-0 items-center gap-2">
        {badge ? (
          <StatusBadge tone="info" className="capitalize">
            {value}
          </StatusBadge>
        ) : copyable && value !== '—' ? (
          <CopyableValue label={label} value={value} monospace maxLength={36} />
        ) : (
          <span className="min-w-0 truncate font-mono text-sm" title={value}>
            {value}
          </span>
        )}
      </dd>
    </div>
  )
}

function BalanceCard({
  icon: Icon,
  title,
  amount,
  raw,
}: {
  icon: LucideIcon
  title: string
  amount: string
  raw: string | null | undefined
}) {
  return (
    <div className="rounded-md border border-border p-4">
      <div className="flex items-center gap-2 text-muted-foreground">
        <Icon className="size-4" />
        <span className="text-sm">{title}</span>
      </div>
      <div className={cn('mt-2 text-2xl font-bold tabular-nums', raw == null && 'text-muted-foreground')}>{amount}</div>
    </div>
  )
}

function PaymentAccountStats({ account, decimals }: { account: PaymentAccountData; decimals: number }) {
  const riskTone = fundedUntilTone(account)
  const riskCaption = fundedUntilCaption(account)
  const fundedDisplay = account.no_active_spend
    ? 'No active spend'
    : [
        account.funded_until_epoch ?? '—',
        account.funded_until_time ? new Date(account.funded_until_time).toLocaleString() : null,
      ]
        .filter(Boolean)
        .join(' · ')
  const runwayDisplay =
    account.no_active_spend || account.runway_seconds == null
      ? '—'
      : formatDuration(Math.max(0, account.runway_seconds))

  return (
    <dl className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
      <AccountMetric label="Total deposited" value={formatTokenAmount(account.funds, decimals, 'USDFC')} />
      <AccountMetric
        label="Available"
        value={formatTokenAmount(account.available_funds, decimals, 'USDFC')}
        highlight
      />
      <AccountMetric label="Locked" value={formatTokenAmount(account.lockup_current, decimals, 'USDFC')} />
      <AccountMetric label="Runway" value={runwayDisplay} tone={riskTone} caption={riskCaption} />
      <AccountMetric label="Lock rate" value={formatTokenAmount(account.lockup_rate, decimals, 'USDFC/epoch')} />
      <AccountMetric
        label="Daily spend"
        value={formatTokenAmount(account.lockup_rate_per_day, decimals, 'USDFC/day')}
      />
      <AccountMetric
        label="Monthly spend"
        value={formatTokenAmount(account.lockup_rate_per_month, decimals, 'USDFC/month')}
      />
      <AccountMetric label="Funded until epoch" value={fundedDisplay} tone={riskTone} caption={riskCaption} />
    </dl>
  )
}

const accountMetricTextClasses: Record<WalletRunwayTone, string> = {
  danger: 'text-[color:var(--status-danger)]',
  warning: 'text-[color:var(--status-warning)]',
  neutral: '',
}

function AccountMetric({
  label,
  value,
  highlight,
  tone = 'neutral',
  caption,
}: {
  label: string
  value: string
  highlight?: boolean
  tone?: WalletRunwayTone
  caption?: string
}) {
  return (
    <div className="min-w-0 rounded-md border border-border p-3">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd
        className={cn(
          'mt-1 truncate text-sm tabular-nums',
          highlight && 'font-semibold text-primary',
          accountMetricTextClasses[tone]
        )}
        title={value}
      >
        {value}
      </dd>
      {caption && <div className={cn('mt-1 text-xs font-medium', accountMetricTextClasses[tone])}>{caption}</div>}
    </div>
  )
}

function OperationBox({
  label,
  value,
  onValueChange,
  onSubmit,
  disabled,
  icon,
  secondaryAction,
}: {
  label: string
  value: string
  onValueChange: (value: string) => void
  onSubmit: () => void
  disabled: boolean
  icon: ReactNode
  secondaryAction?: ReactNode
}) {
  return (
    <div className="rounded-md border border-border p-4">
      <Label htmlFor={`wallet-${label.toLowerCase()}`}>{label} USDFC</Label>
      <div className="mt-2 flex gap-2">
        <Input
          id={`wallet-${label.toLowerCase()}`}
          inputMode="decimal"
          value={value}
          onChange={(event) => onValueChange(event.target.value)}
          placeholder="0.0"
          disabled={disabled}
        />
        {secondaryAction}
        <Button type="button" onClick={onSubmit} disabled={disabled}>
          {icon}
          {label}
        </Button>
      </div>
    </div>
  )
}

function OperationsTable({
  operations,
  decimals,
  onOpenDetails,
}: {
  operations: WalletOperation[]
  decimals: number
  onOpenDetails: (detail: string) => void
}) {
  if (operations.length === 0) {
    return <p className="text-sm text-muted-foreground">No wallet operations yet.</p>
  }

  return (
    <DataTableFrame>
      <Table className="min-w-[760px]">
        <TableHeader>
          <TableRow className={tableHeaderRowClassName}>
            <TableHead className="px-4">Type</TableHead>
            <TableHead className="px-4">Status</TableHead>
            <TableHead className="px-4">Amount</TableHead>
            <TableHead className="px-4">Transaction</TableHead>
            <TableHead className="px-4">Details</TableHead>
            <TableHead className="px-4">Updated</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {operations.map((operation) => {
            const detail = walletOperationDetail(operation)
            return (
              <TableRow key={operation.id}>
                <TableCell className="px-4">{walletOperationTypeLabel(operation.type)}</TableCell>
                <TableCell className="px-4">
                  <StatusBadge tone={walletOperationStatusTone(operation.status)}>
                    {walletOperationStatusLabel(operation.status)}
                  </StatusBadge>
                </TableCell>
                <TableCell className="px-4 tabular-nums">
                  {operation.type === 'approve' ? (
                    <span className="text-muted-foreground">—</span>
                  ) : (
                    formatTokenAmount(operation.amount, decimals, 'USDFC')
                  )}
                </TableCell>
                <TableCell className="px-4">
                  {operation.tx_hash ? (
                    <CopyableValue label="Transaction hash" value={operation.tx_hash} monospace maxLength={24} />
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </TableCell>
                <TableCell className="px-4">
                  {detail ? (
                    <Button
                      type="button"
                      variant="link"
                      onClick={() => onOpenDetails(detail)}
                      className="h-auto max-w-80 justify-start p-0 text-left text-xs font-normal text-muted-foreground hover:text-foreground"
                    >
                      <span className="truncate">{detail}</span>
                    </Button>
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </TableCell>
                <TableCell className="px-4 text-muted-foreground">
                  <RelativeTime value={operation.updated_at} />
                </TableCell>
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    </DataTableFrame>
  )
}

function walletOperationStatusTone(status: WalletOperationStatus): StatusTone {
  switch (status) {
    case 'confirmed':
      return 'success'
    case 'pending':
    case 'running':
    case 'submitted':
      return 'warning'
    case 'failed':
      return 'danger'
    case 'unknown':
      return 'info'
    default:
      return 'neutral'
  }
}
