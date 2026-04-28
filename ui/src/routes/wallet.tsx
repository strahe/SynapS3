import { createFileRoute } from '@tanstack/react-router'
import { AlertTriangle, Loader2, Wallet } from 'lucide-react'
import { CopyButton } from '@/components/app/CopyButton'
import { PageHeader } from '@/components/app/PageHeader'
import { StatusBadge } from '@/components/app/StatusBadge'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from '@/components/ui/empty'
import { useWallet } from '@/hooks/queries'
import { formatAttoFIL, formatTokenAmount } from '@/lib/utils'

export const Route = createFileRoute('/wallet')({
  component: WalletPage,
})

function WalletPage() {
  const { data, isLoading, error } = useWallet()

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

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
          <CardTitle>Identity</CardTitle>
        </CardHeader>
        <CardContent>
          <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-6">
            <IdentityField
              label="Address"
              value={data.address ?? '—'}
              copyable
              className="sm:col-span-2 lg:col-span-3"
            />
            <IdentityField label="Network" value={data.network ?? '—'} badge />
            <IdentityField label="Chain ID" value={data.chain_id?.toString() ?? '—'} />
            <IdentityField label="Nonce" value={data.nonce?.toString() ?? '—'} />
          </dl>
        </CardContent>
      </Card>

      <div className="grid gap-4 sm:grid-cols-2">
        <BalanceCard title="FIL Balance" amount={formatAttoFIL(data.fil_balance)} raw={data.fil_balance} />
        <BalanceCard
          title="USDFC Balance"
          amount={formatTokenAmount(data.usdfc_balance, data.usdfc_decimals ?? 18, 'USDFC')}
          raw={data.usdfc_balance}
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        {data.fil_account && (
          <AccountCard title="PDP Account — FIL" account={data.fil_account} decimals={18} symbol="FIL" />
        )}
        {data.usdfc_account && (
          <AccountCard
            title="PDP Account — USDFC"
            account={data.usdfc_account}
            decimals={data.usdfc_decimals ?? 18}
            symbol="USDFC"
          />
        )}
        {!data.fil_account && !data.usdfc_account && (
          <Card className="col-span-2">
            <CardContent>
              <p className="text-sm text-muted-foreground">PDP account data unavailable</p>
            </CardContent>
          </Card>
        )}
      </div>

      {(data.payments_address || data.usdfc_address) && (
        <Card>
          <CardHeader>
            <CardTitle>Contract Addresses</CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid gap-4 sm:grid-cols-2">
              {data.payments_address && (
                <IdentityField label="Payments Contract" value={data.payments_address} copyable />
              )}
              {data.usdfc_address && <IdentityField label="USDFC Token" value={data.usdfc_address} copyable />}
            </dl>
          </CardContent>
        </Card>
      )}

      {data.business && (
        <Card>
          <CardHeader>
            <CardTitle>Business Stats</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-4 sm:grid-cols-3">
              <StatItem label="Proof Sets" value={data.business.proof_set_count} />
              <StatItem label="On-chain Tasks Pending" value={data.business.onchain_tasks_pending} />
              <StatItem label="On-chain Tasks Completed" value={data.business.onchain_tasks_completed} />
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

// --- Sub-components ---

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

function BalanceCard({ title, amount, raw }: { title: string; amount: string; raw: string | null }) {
  return (
    <Card>
      <CardContent>
        <div className="flex items-center gap-2 text-muted-foreground">
          <Wallet className="size-4" />
          <span className="text-sm">{title}</span>
        </div>
        <div className={raw == null ? 'mt-2 text-2xl font-bold text-muted-foreground' : 'mt-2 text-2xl font-bold'}>
          {amount}
        </div>
      </CardContent>
    </Card>
  )
}

// uint256.max sentinel — Solidity uses this to mean "unlimited/∞"
const UINT256_MAX = '115792089237316195423570985008687907853269984665640564039457584007913129639935'

function AccountCard({
  title,
  account,
  decimals,
  symbol,
}: {
  title: string
  account: {
    funds: string | null
    available_funds: string | null
    lockup_current: string | null
    lockup_rate: string | null
    funded_until_epoch: string | null
  }
  decimals: number
  symbol: string
}) {
  const fundedDisplay = account.funded_until_epoch === UINT256_MAX ? '∞' : (account.funded_until_epoch ?? '—')

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <dl className="flex flex-col gap-3 text-sm">
          <AccountRow label="Total Deposited" value={formatTokenAmount(account.funds, decimals, symbol)} />
          <AccountRow
            label="Available"
            value={formatTokenAmount(account.available_funds, decimals, symbol)}
            highlight
          />
          <AccountRow label="Locked" value={formatTokenAmount(account.lockup_current, decimals, symbol)} />
          <AccountRow label="Lock Rate" value={formatTokenAmount(account.lockup_rate, decimals, `${symbol}/epoch`)} />
          <AccountRow label="Funded Until Epoch" value={fundedDisplay} />
        </dl>
      </CardContent>
    </Card>
  )
}

function AccountRow({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={highlight ? 'font-mono font-semibold text-primary' : 'font-mono'}>{value}</dd>
    </div>
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
