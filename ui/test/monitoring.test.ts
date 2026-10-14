import assert from 'node:assert/strict'
import test from 'node:test'
import type { MonitoringPoint } from '../src/api/client.ts'
import {
  formatMonitoringBytes,
  formatMonitoringCPU,
  formatMonitoringRate,
  monitoringChartPoints,
  monitoringIsStale,
} from '../src/lib/monitoring.ts'

test('monitoring charts break every series at a sampling gap without changing measured values', () => {
  const first: MonitoringPoint = {
    at: '2026-10-14T12:00:00Z',
    cpu_cores: 0,
    rss_bytes: 1024,
    client_upload_bytes_per_second: 0,
    client_download_bytes_per_second: null,
    storage_upload_bytes_per_second: 2,
    storage_download_bytes_per_second: 3,
    gap_before: false,
  }
  const second: MonitoringPoint = { ...first, at: '2026-10-14T12:01:00Z', gap_before: true, rss_bytes: 2048 }
  const rows = monitoringChartPoints([first, second])
  assert.equal(rows.length, 3)
  assert.equal(rows[0]?.cpu_cores, 0)
  assert.equal(rows[0]?.client_download_bytes_per_second, null)
  assert.deepEqual(rows[1], {
    timestamp: Date.parse('2026-10-14T12:00:30Z'),
    cpu_cores: null,
    rss_bytes: null,
    client_upload_bytes_per_second: null,
    client_download_bytes_per_second: null,
    storage_upload_bytes_per_second: null,
    storage_download_bytes_per_second: null,
  })
  assert.equal(rows[2]?.timestamp, Date.parse(second.at))
  assert.equal(rows[2]?.rss_bytes, 2048)
})

test('monitoring rate and CPU formatting distinguishes missing values, idle, and small positive activity', () => {
  for (const [value, expected] of [
    [null, '—'],
    [0, '0 B/s'],
    [0.001, '<0.01 B/s'],
    [0.25, '0.25 B/s'],
    [1024, '1 KiB/s'],
  ] as const) {
    assert.equal(formatMonitoringRate(value), expected)
  }
  assert.equal(formatMonitoringCPU(null), '—')
  assert.equal(formatMonitoringCPU(0), '0 cores')
  assert.equal(formatMonitoringCPU(0.001), '<0.01 cores')
  assert.equal(formatMonitoringCPU(2.5), '2.5 cores')
})

test('monitoring totals format decimal byte strings without rounding through unsafe JavaScript integers', () => {
  assert.equal(formatMonitoringBytes('0'), '0 B')
  assert.equal(formatMonitoringBytes('1023'), '1023 B')
  assert.equal(formatMonitoringBytes('1536'), '1.5 KiB')
  assert.equal(formatMonitoringBytes('1073741824'), '1 GiB')
  assert.equal(formatMonitoringBytes('11821949021847552'), '10.5 PiB')
  assert.equal(formatMonitoringBytes('11315294063768371'), '10 PiB')
  assert.equal(formatMonitoringBytes('18446744073709551615'), '16 EiB')
})

test('monitoring freshness uses the last sample rather than a successful history request', () => {
  const sampledAt = '2026-10-14T12:00:00Z'
  const baseline = Date.parse(sampledAt)
  const data = { sampled_at: sampledAt, snapshot_at: sampledAt }
  assert.equal(monitoringIsStale(data, baseline, baseline + 45_000), false)
  assert.equal(monitoringIsStale(data, baseline, baseline + 45_001), true)
  assert.equal(monitoringIsStale({ ...data, sampled_at: null }, baseline, baseline + 60_000), false)

  const receivedAt = baseline + 3 * 60 * 60 * 1000
  const delayed = { ...data, snapshot_at: '2026-10-14T12:00:30Z' }
  assert.equal(monitoringIsStale(delayed, receivedAt, receivedAt + 10_000), false)
  assert.equal(monitoringIsStale(delayed, receivedAt, receivedAt + 15_001), true)
  assert.equal(monitoringIsStale(delayed, receivedAt, receivedAt - 10_000), false)
})
