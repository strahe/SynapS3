import { createFileRoute } from '@tanstack/react-router'
import { AlertTriangle, ArrowDownToLine, ArrowUpFromLine, Clock, Wallet } from 'lucide-react'
import { type ReactNode, useMemo, useState } from 'react'
import type { PaymentAccountData, WalletOperation, WalletOperationStatus } from '@/api/client'
import { CopyButton } from '@/components/app/CopyButton'
import { DetailTextDialog } from '@/components/app/DetailTextDialog'
import { PageHeader } from '@/components/app/PageHeader'
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
import { Card, CardAction, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectGroup, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { useWallet, useWalletFund, useWalletOperations, useWalletWithdraw } from '@/hooks/queries'
import { cn, formatAttoFIL, formatDuration, formatTokenAmount, timeAgo } from '@/lib/utils'
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
  const { data, isLoading, error } = useWallet()
  const [operationsLimit, setOperationsLimit] = useState(walletOperationsDefaultLimit)
  const { data: operationsData } = useWalletOperations(operationsLimit)
  const fundMutation = useWalletFund()
  const withdrawMutation = useWalletWithdraw()
  const [fundAmount, setFundAmount] = useState('')
  const [withdrawAmount, setWithdrawAmount] = useState('')
  const [formError, setFormError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [pendingOperation, setPendingOperation] = useState<PendingWalletOperation | null>(null)
  const [operationDetailText, setOperationDetailText] = useState<string | null>(null)

  const paymentAccount = data?.payment_account ?? null
  const decimals = data?.contracts?.usdfc_decimals ?? usdfcDecimals
  const operations = operationsData?.operations ?? []
  const mutationError = walletOperationMutationError(pendingOperation?.type, fundMutation.error, withdrawMutation.error)
  const isMutating = fundMutation.isPending || withdrawMutation.isPending

  const topUpAmounts = useMemo(() => {
    return topUpTargets.map((target) => ({
      ...target,
      amount: paymentAccount ? topUpNeeded(paymentAccount, target.epochs) : 0n,
    }))
  }, [paymentAccount])

  if (isLoading) return <WalletSkeleton />

  if (error || !data) {
    return <div className="flex h-full items-center justify-center text-destructive">Failed to load wallet data</div>
  }

  if (!data.configured) {
    return (
      <Empty className="h-full border-0">
        <EmptyHeader>
          <EmptyMedia variant="icon">
            <Wallet />
          </EmptyMedia>
          <EmptyTitle>Wallet not configured</EmptyTitle>
          <EmptyDescription>
            Set your Filecoin private key in the configuration to enable wallet features.
          </EmptyDescription>
        </EmptyHeader>
      </Empty>
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

  const queueWalletOperation = (
    type: PendingWalletOperation['type'],
    amount: string,
    clearInput?: () => void,
    requestPrefix: string = type
  ) => {
    fundMutation.reset()
    withdrawMutation.reset()
    const draft = createWalletOperationDraft({ type, amountBaseUnits: amount, decimals, requestPrefix })
    setPendingOperation({ ...draft, clearInput })
    setNotice(null)
    setFormError(null)
  }

  const clearPendingOperation = (reason: WalletOperationDialogCloseReason) => {
    if (walletOperationDialogShouldClearDraft(reason, isMutating)) {
      fundMutation.reset()
      withdrawMutation.reset()
      setPendingOperation(null)
    }
  }

  const handlePendingOperationOpenChange = (open: boolean) => {
    if (!open) clearPendingOperation('dismiss')
  }

  const confirmPendingOperation = () => {
    if (!pendingOperation) return
    const payload = walletOperationPayload(pendingOperation)
    const clearInput = pendingOperation.clearInput
    const onSuccess = () => {
      clearInput?.()
      clearPendingOperation('success')
    }
    if (pendingOperation.type === 'withdraw') {
      withdrawMutation.mutate(payload, { onSuccess })
    } else {
      fundMutation.mutate(payload, { onSuccess })
    }
  }

  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader title="Wallet" />

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
          <CardTitle>Wallet</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-5">
          <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-6">
            <IdentityField
              label="Address"
              value={data.identity?.address ?? '—'}
              copyable
              className="sm:col-span-2 lg:col-span-3"
            />
            <IdentityField label="Network" value={data.chain?.network ?? '—'} badge />
            <IdentityField label="Chain ID" value={data.chain?.chain_id?.toString() ?? '—'} />
            <IdentityField label="Nonce" value={data.identity?.nonce?.toString() ?? '—'} />
            <IdentityField label="Current Epoch" value={data.chain?.current_epoch ?? '—'} />
          </dl>
          <div className="grid gap-4 sm:grid-cols-2">
            <BalanceCard
              title="FIL Balance"
              amount={formatAttoFIL(data.wallet_balances?.fil_gas)}
              raw={data.wallet_balances?.fil_gas}
            />
            <BalanceCard
              title="USDFC Balance"
              amount={formatTokenAmount(data.wallet_balances?.usdfc, decimals, 'USDFC')}
              raw={data.wallet_balances?.usdfc}
            />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>USDFC Payment Account</CardTitle>
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
                      size="sm"
                      onClick={withdrawMax}
                      disabled={!paymentAccount.available_funds || isMutating}
                    >
                      Max withdraw
                    </Button>
                  }
                />
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
                      disabled={isMutating}
                      onClick={() => topUpRunway(target.amount, target.label)}
                      title={`Additional needed: ${formatTokenAmount(target.amount.toString(), decimals, 'USDFC')}`}
                    >
                      <Clock data-icon="inline-start" />
                      <span className="flex min-w-0 flex-col items-start gap-0.5">
                        <span className="leading-none">To {target.label}</span>
                        <span className="max-w-full truncate text-[11px] font-normal text-muted-foreground group-hover/button:text-foreground">
                          {target.amount > 0n
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
          <CardAction className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">Latest</span>
            <Label htmlFor="wallet-operations-limit" className="sr-only">
              Operation count
            </Label>
            <Select value={operationsLimit.toString()} onValueChange={(value) => setOperationsLimit(Number(value))}>
              <SelectTrigger id="wallet-operations-limit" className="h-7 w-20 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {walletOperationsLimitOptions.map((limit) => (
                    <SelectItem key={limit} value={limit.toString()}>
                      {limit}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </CardAction>
        </CardHeader>
        <CardContent>
          <OperationsTable operations={operations} decimals={decimals} onOpenDetails={setOperationDetailText} />
        </CardContent>
      </Card>

      {data.business && (
        <Card>
          <CardHeader>
            <CardTitle>Business</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 sm:grid-cols-3">
              <StatItem label="Data Sets" value={data.business.data_set_count} />
              <StatItem label="On-chain Tasks Pending" value={data.business.onchain_tasks_pending} />
              <StatItem label="On-chain Tasks Completed" value={data.business.onchain_tasks_completed} />
            </div>
          </CardContent>
        </Card>
      )}

      {data.contracts && (
        <Card>
          <CardHeader>
            <CardTitle>Advanced</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid gap-4 sm:grid-cols-2">
              <IdentityField label="Payments Contract" value={data.contracts.payments_address} copyable />
              <IdentityField label="USDFC Token" value={data.contracts.usdfc_address} copyable />
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
          {mutationError && <p className="text-sm text-destructive">{mutationError}</p>}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isMutating}>Cancel</AlertDialogCancel>
            <Button type="button" disabled={isMutating} onClick={confirmPendingOperation}>
              {pendingOperation?.confirmation.actionLabel ?? 'Confirm'}
            </Button>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
      <DetailTextDialog
        title="Operation Details"
        text={operationDetailText}
        onClose={() => setOperationDetailText(null)}
      />
    </div>
  )
}

function WalletSkeleton() {
  return (
    <div className="flex flex-col gap-6 p-6">
      <PageHeader title="Wallet" />
      <Card>
        <CardHeader>
          <CardTitle>Wallet</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-5">
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-6">
            <SkeletonField className="sm:col-span-2 lg:col-span-3" />
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
          <CardTitle>USDFC Payment Account</CardTitle>
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
        ) : (
          <span className="min-w-0 truncate font-mono text-sm" title={value}>
            {value}
          </span>
        )}
        {copyable && value !== '—' && <CopyButton value={value} label={label} size="icon-xs" />}
      </dd>
    </div>
  )
}

function BalanceCard({ title, amount, raw }: { title: string; amount: string; raw: string | null | undefined }) {
  return (
    <div className="rounded-md border border-border p-4">
      <div className="flex items-center gap-2 text-muted-foreground">
        <Wallet className="size-4" />
        <span className="text-sm">{title}</span>
      </div>
      <div className={raw == null ? 'mt-2 text-2xl font-bold text-muted-foreground' : 'mt-2 text-2xl font-bold'}>
        {amount}
      </div>
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
      <AccountMetric label="Total Deposited" value={formatTokenAmount(account.funds, decimals, 'USDFC')} />
      <AccountMetric
        label="Available"
        value={formatTokenAmount(account.available_funds, decimals, 'USDFC')}
        highlight
      />
      <AccountMetric label="Locked" value={formatTokenAmount(account.lockup_current, decimals, 'USDFC')} />
      <AccountMetric label="Runway" value={runwayDisplay} tone={riskTone} caption={riskCaption} />
      <AccountMetric label="Lock Rate" value={formatTokenAmount(account.lockup_rate, decimals, 'USDFC/epoch')} />
      <AccountMetric
        label="Daily Spend"
        value={formatTokenAmount(account.lockup_rate_per_day, decimals, 'USDFC/day')}
      />
      <AccountMetric
        label="Monthly Spend"
        value={formatTokenAmount(account.lockup_rate_per_month, decimals, 'USDFC/month')}
      />
      <AccountMetric label="Funded Until Epoch" value={fundedDisplay} tone={riskTone} caption={riskCaption} />
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
          'mt-1 truncate font-mono text-sm',
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
        <Button type="button" onClick={onSubmit} disabled={disabled}>
          {icon}
          {label}
        </Button>
      </div>
      {secondaryAction && <div className="mt-2 flex justify-end">{secondaryAction}</div>}
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
    return <p className="text-sm text-muted-foreground">No wallet operations yet</p>
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Type</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Amount</TableHead>
          <TableHead>Tx Hash</TableHead>
          <TableHead>Details</TableHead>
          <TableHead>Updated</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {operations.map((operation) => {
          const detail = walletOperationDetail(operation)
          return (
            <TableRow key={operation.id}>
              <TableCell className="capitalize">{operation.type}</TableCell>
              <TableCell>
                <StatusBadge tone={walletOperationStatusTone(operation.status)} className="capitalize">
                  {operation.status}
                </StatusBadge>
              </TableCell>
              <TableCell className="font-mono">{formatTokenAmount(operation.amount, decimals, 'USDFC')}</TableCell>
              <TableCell>
                {operation.tx_hash ? (
                  <span className="inline-flex max-w-48 items-center gap-1">
                    <span className="truncate font-mono text-xs" title={operation.tx_hash}>
                      {operation.tx_hash}
                    </span>
                    <CopyButton value={operation.tx_hash} label="Transaction hash" size="icon-xs" />
                  </span>
                ) : (
                  <span className="text-muted-foreground">—</span>
                )}
              </TableCell>
              <TableCell>
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
              <TableCell className="text-muted-foreground">{timeAgo(operation.updated_at)}</TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}

function StatItem({ label, value }: { label: string; value: number }) {
  return (
    <div className="text-center">
      <div className="text-2xl font-bold">{value}</div>
      <div className="mt-1 text-xs text-muted-foreground">{label}</div>
    </div>
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
