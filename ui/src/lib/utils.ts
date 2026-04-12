import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatBytes(bytes: number): string {
  if (bytes < 0) return '—'
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(k)), sizes.length - 1)
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toString()
}

export function formatDuration(seconds: number): string {
  const d = Math.floor(seconds / 86400)
  const h = Math.floor((seconds % 86400) / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  if (d > 0) return `${d}d ${h}h`
  if (h > 0) return `${h}h ${m}m`
  return `${m}m`
}

export function timeAgo(dateStr: string): string {
  const time = new Date(dateStr).getTime()
  if (isNaN(time)) return '—'
  const diff = (Date.now() - time) / 1000
  if (diff < 0) return 'just now'
  if (diff < 60) return 'just now'
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

/**
 * Format an attoFIL string (18 decimals) to a human-readable FIL amount.
 * Uses pure string arithmetic to avoid JS floating-point precision loss.
 */
export function formatAttoFIL(raw: string | null | undefined): string {
  return formatTokenAmount(raw, 18, 'FIL')
}

/**
 * Format a token amount string with the given decimal places.
 * Operates on the raw string to preserve full precision.
 */
export function formatTokenAmount(raw: string | null | undefined, decimals: number, suffix?: string): string {
  if (raw == null || raw === '') return '—'
  // Reject negative values — chain balances should never be negative
  if (raw.startsWith('-')) return '—'
  // Pad left to ensure we have enough digits
  const padded = raw.padStart(decimals + 1, '0')
  const intPart = padded.slice(0, padded.length - decimals) || '0'
  const fracPart = padded.slice(padded.length - decimals)
  // Trim trailing zeros but keep up to 8 significant fractional digits for display
  const trimmed = fracPart.replace(/0+$/, '')
  const displayFrac = trimmed.length > 8 ? trimmed.slice(0, 8) : trimmed
  // Detect dust: non-zero sub-unit amount below display precision
  if (intPart === '0' && displayFrac.replace(/0/g, '') === '' && trimmed.replace(/0/g, '') !== '') {
    const threshold = `< 0.${'0'.repeat(7)}1`
    return suffix ? `${threshold} ${suffix}` : threshold
  }
  const formatted = displayFrac.length > 0 ? `${intPart}.${displayFrac}` : intPart
  return suffix ? `${formatted} ${suffix}` : formatted
}

/**
 * Shorten a hex address for display: 0x1234...abcd
 */
export function shortenAddress(addr: string): string {
  if (addr.length <= 12) return addr
  return `${addr.slice(0, 6)}...${addr.slice(-4)}`
}
