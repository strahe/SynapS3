import { timeAgo } from '@/lib/utils'

/** A relative time ("5m ago") with the local date and time on hover. */
export function RelativeTime({ value, className }: { value?: string | null; className?: string }) {
  if (!value) return <span className={className}>—</span>
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return <span className={className}>—</span>
  return (
    <time dateTime={value} title={date.toLocaleString()} className={className}>
      {timeAgo(value)}
    </time>
  )
}
