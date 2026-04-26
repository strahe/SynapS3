import { createFileRoute } from '@tanstack/react-router'
import { AlertTriangle, Check, Copy, Loader2, Wallet } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'
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
      <div className="flex h-full flex-col items-center justify-center gap-4 text-muted-foreground">
        <Wallet className="h-12 w-12" />
        <p className="text-lg">Wallet not configured</p>
        <p className="text-sm">Set your Filecoin private key in the configuration to enable wallet features.</p>
      </div>
    )
  }

  return (
    <div className="space-y-6 p-6">
      <h1 className="text-2xl font-bold">Wallet</h1>

      {/* Partial errors banner */}
      {data.partial_errors && Object.keys(data.partial_errors).length > 0 && (
        <div className="flex items-start gap-3 rounded-lg border border-yellow-500/30 bg-yellow-500/10 p-4">
          <AlertTriangle className="h-5 w-5 shrink-0 text-yellow-500" />
          <div className="space-y-1">
            <p className="text-sm font-medium text-yellow-500">Some data could not be retrieved</p>
            {Object.entries(data.partial_errors).map(([key, msg]) => (
              <p key={key} className="text-xs text-muted-foreground">
                <span className="font-mono">{key}</span>: {msg}
              </p>
            ))}
          </div>
        </div>
      )}

      {/* Identity card */}
      <div className="rounded-lg border border-border bg-card p-5">
        <h2 className="mb-4 text-sm font-medium text-muted-foreground">Identity</h2>
        <dl className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <IdentityField label="Address" value={data.address ?? '—'} copyable />
          <IdentityField label="Network" value={data.network ?? '—'} badge />
          <IdentityField label="Chain ID" value={data.chain_id?.toString() ?? '—'} />
          <IdentityField label="Nonce" value={data.nonce?.toString() ?? '—'} />
        </dl>
      </div>

      {/* Balance cards */}
      <div className="grid gap-4 sm:grid-cols-2">
        <BalanceCard
          title="FIL Balance"
          amount={formatAttoFIL(data.fil_balance)}
          raw={data.fil_balance}
          color="text-blue-500"
        />
        <BalanceCard
          title="USDFC Balance"
          amount={formatTokenAmount(data.usdfc_balance, data.usdfc_decimals ?? 18, 'USDFC')}
          raw={data.usdfc_balance}
          color="text-green-500"
        />
      </div>

      {/* PDP Contract Accounts */}
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
          <div className="col-span-2 rounded-lg border border-border bg-card p-5">
            <p className="text-sm text-muted-foreground">PDP account data unavailable</p>
          </div>
        )}
      </div>

      {/* Contract Addresses */}
      {(data.payments_address || data.usdfc_address) && (
        <div className="rounded-lg border border-border bg-card p-5">
          <h2 className="mb-4 text-sm font-medium text-muted-foreground">Contract Addresses</h2>
          <dl className="grid gap-4 sm:grid-cols-2">
            {data.payments_address && (
              <IdentityField label="Payments Contract" value={data.payments_address} copyable />
            )}
            {data.usdfc_address && <IdentityField label="USDFC Token" value={data.usdfc_address} copyable />}
          </dl>
        </div>
      )}

      {/* Business stats */}
      {data.business && (
        <div className="rounded-lg border border-border bg-card p-5">
          <h2 className="mb-4 text-sm font-medium text-muted-foreground">Business Stats</h2>
          <div className="grid gap-4 sm:grid-cols-3">
            <StatItem label="Proof Sets" value={data.business.proof_set_count} />
            <StatItem label="On-chain Tasks Pending" value={data.business.onchain_tasks_pending} />
            <StatItem label="On-chain Tasks Completed" value={data.business.onchain_tasks_completed} />
          </div>
        </div>
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
}: {
  label: string
  value: string
  copyable?: boolean
  badge?: boolean
}) {
  const [copied, setCopied] = useState(false)
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current)
    }
  }, [])

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(value)
      setCopied(true)
      if (timerRef.current) clearTimeout(timerRef.current)
      timerRef.current = setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard API may fail on non-HTTPS contexts
    }
  }, [value])

  return (
    <div>
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="mt-1 flex items-center gap-2">
        {badge ? (
          <span className="inline-flex items-center rounded-full bg-primary/10 px-2.5 py-0.5 text-xs font-medium text-primary capitalize">
            {value}
          </span>
        ) : (
          <span className="font-mono text-sm break-all" title={value}>
            {value}
          </span>
        )}
        {copyable && value !== '—' && (
          <button
            type="button"
            onClick={handleCopy}
            className="rounded p-1 text-muted-foreground hover:bg-muted hover:text-foreground"
            title="Copy to clipboard"
            aria-label={copied ? 'Copied' : 'Copy to clipboard'}
          >
            {copied ? <Check className="h-3.5 w-3.5 text-green-500" /> : <Copy className="h-3.5 w-3.5" />}
          </button>
        )}
      </dd>
    </div>
  )
}

function BalanceCard({
  title,
  amount,
  raw,
  color,
}: {
  title: string
  amount: string
  raw: string | null
  color: string
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-5">
      <div className="flex items-center gap-2 text-muted-foreground">
        <Wallet className="h-4 w-4" />
        <span className="text-sm">{title}</span>
      </div>
      <div className={`mt-2 text-2xl font-bold ${raw == null ? 'text-muted-foreground' : color}`}>{amount}</div>
    </div>
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
    <div className="rounded-lg border border-border bg-card p-5">
      <h3 className="mb-4 text-sm font-medium text-muted-foreground">{title}</h3>
      <dl className="space-y-3 text-sm">
        <AccountRow label="Total Deposited" value={formatTokenAmount(account.funds, decimals, symbol)} />
        <AccountRow label="Available" value={formatTokenAmount(account.available_funds, decimals, symbol)} highlight />
        <AccountRow label="Locked" value={formatTokenAmount(account.lockup_current, decimals, symbol)} />
        <AccountRow label="Lock Rate" value={formatTokenAmount(account.lockup_rate, decimals, `${symbol}/epoch`)} />
        <AccountRow label="Funded Until Epoch" value={fundedDisplay} />
      </dl>
    </div>
  )
}

function AccountRow({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <div className="flex items-center justify-between">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className={`font-mono ${highlight ? 'font-semibold text-green-500' : ''}`}>{value}</dd>
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
