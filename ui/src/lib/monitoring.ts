import type { MonitoringData, MonitoringPoint, MonitoringStatus } from '../api/client.ts'

export type MonitoringChartPoint = Omit<MonitoringPoint, 'at' | 'gap_before'> & { timestamp: number }

const byteUnits = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB']

export function monitoringChartPoints(points: MonitoringPoint[]): MonitoringChartPoint[] {
  const rows: MonitoringChartPoint[] = []
  let previousTimestamp: number | undefined
  for (const point of points) {
    const { at, gap_before, ...values } = point
    const timestamp = Date.parse(at)
    if (gap_before && previousTimestamp !== undefined && timestamp > previousTimestamp) {
      rows.push({
        timestamp: previousTimestamp + (timestamp - previousTimestamp) / 2,
        cpu_cores: null,
        rss_bytes: null,
        client_upload_bytes_per_second: null,
        client_download_bytes_per_second: null,
        storage_upload_bytes_per_second: null,
        storage_download_bytes_per_second: null,
      })
    }
    rows.push({ timestamp, ...values })
    previousTimestamp = timestamp
  }
  return rows
}

export function formatMonitoringBytes(value: string): string {
  if (!/^\d+$/.test(value)) return '—'
  const bytes = BigInt(value)
  let divisor = 1n
  let unit = 0
  while (bytes >= divisor * 1024n && unit < byteUnits.length - 1) {
    divisor *= 1024n
    unit++
  }
  if (unit === 0) return `${bytes} B`
  const tenths = (bytes * 10n + divisor / 2n) / divisor
  const fraction = tenths % 10n
  return `${tenths / 10n}${fraction === 0n ? '' : `.${fraction}`} ${byteUnits[unit]}`
}

export function formatMonitoringRate(value: number | null): string {
  if (value === null || !Number.isFinite(value) || value < 0) return '—'
  if (value === 0) return '0 B/s'
  if (value < 0.01) return '<0.01 B/s'
  const unit = Math.min(Math.max(0, Math.floor(Math.log(value) / Math.log(1024))), byteUnits.length - 1)
  const amount = value / 1024 ** unit
  return `${Number(amount.toFixed(amount < 1 ? 2 : 1))} ${byteUnits[unit]}/s`
}

export function formatMonitoringCPU(value: number | null): string {
  if (value === null || !Number.isFinite(value) || value < 0) return '—'
  if (value > 0 && value < 0.01) return '<0.01 cores'
  return `${Number(value.toFixed(2))} cores`
}

export function monitoringStatusLabel(status: MonitoringStatus): string {
  if (status === 'collecting') return 'Waiting for sample'
  return status === 'unavailable' ? 'Unavailable' : '—'
}

export function monitoringIsStale(
  data: Pick<MonitoringData, 'sampled_at' | 'snapshot_at'>,
  receivedAt: number,
  now: number
): boolean {
  if (data.sampled_at === null) return false
  const sampleAge = Math.max(0, Date.parse(data.snapshot_at) - Date.parse(data.sampled_at))
  return sampleAge + Math.max(0, now - receivedAt) > 45_000
}

export function formatMonitoringTime(value: string | number, seconds = false): string {
  return new Date(value).toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    ...(seconds ? { second: '2-digit' } : {}),
    hour12: false,
  })
}
