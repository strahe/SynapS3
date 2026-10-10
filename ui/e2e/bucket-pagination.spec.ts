import type { BucketDetail, BucketStorageRiskVersion, DeletedObjectItem } from '../src/api/client'
import { expect, test } from './fixtures'

const timestamp = '2026-10-10T00:00:00Z'
const bucket: BucketDetail = {
  id: 1,
  name: 'pagination-e2e',
  owner_access_key: 'SYSTEMTESTOWNER',
  default_copies: 2,
  minimum_durable_copies: 2,
  provider_selection_strategy: 'distribution',
  status: 'ready',
  object_count: 3,
  total_size_bytes: 3,
  created_at: timestamp,
  updated_at: timestamp,
  versioning_status: 'Enabled',
  versioning_enforced: true,
  data_sets: [],
  replacements: [],
  storage_health: {
    status: 'degraded',
    reason_codes: [],
    stale: false,
    abnormal_data_sets: 1,
    affected_versions_capped: 3,
    affected_versions_cap: 100,
    affected_versions_exceeds_cap: false,
  },
}

const views = [
  { name: 'Objects', search: '', path: 'objects', marker: 'after' },
  { name: 'Trash', search: '?tab=trash', path: 'objects/deleted', marker: 'after' },
  {
    name: 'Affected versions',
    search: '?tab=storage&risk=true',
    path: 'storage-health/affected-versions',
    marker: 'key_marker',
  },
] as const

for (const view of views) {
  test(`${view.name} pagination survives loading and browser back and forward`, async ({ page, systemServer }) => {
    const secondPage = Promise.withResolvers<void>()
    await page.route('**/api/v1/buckets/pagination-e2e', (route) => route.fulfill({ json: bucket }))
    await page.route(`**/api/v1/buckets/pagination-e2e/${view.path}?*`, async (route) => {
      const marker = new URL(route.request().url()).searchParams.get(view.marker)
      const number = marker === 'marker-2' ? 3 : marker === 'marker-1' ? 2 : 1
      if (number === 2) await secondPage.promise
      const key = `page-${number}`
      const paging = { has_more: number < 3, next_marker: number < 3 ? `marker-${number}` : undefined }
      if (view.name === 'Objects') {
        await route.fulfill({ json: { ...paging, folders: [{ name: key, prefix: `${key}/` }], objects: [] } })
      } else if (view.name === 'Trash') {
        const object: DeletedObjectItem = {
          key,
          delete_marker_version_id: `deleted-${number}`,
          deleted_at: timestamp,
          restore_version_id: `version-${number}`,
          restore_size: 1,
          restore_content_type: 'text/plain',
          restore_etag: 'etag',
        }
        await route.fulfill({ json: { ...paging, objects: [object] } })
      } else {
        const version: BucketStorageRiskVersion = {
          key,
          version_id: `version-${number}`,
          size: 1,
          state: 'stored',
          is_current: true,
          in_cache: true,
          content_type: 'text/plain',
          etag: 'etag',
          created_at: timestamp,
          updated_at: timestamp,
          readable_alternative_count: 0,
          has_readable_alternative: false,
          risk_data_sets: [],
        }
        await route.fulfill({
          json: {
            versions: [version],
            has_more: number < 3,
            next_key_marker: number < 3 ? `marker-${number}` : undefined,
            next_version_marker: `version-${number}`,
            next_created_at_marker: timestamp,
            stale_before: timestamp,
          },
        })
      }
    })
    await page.goto(systemServer.adminURL)
    await page.getByLabel('Username').fill('admin')
    await page.getByLabel('Password').fill('system-test-admin-password')
    await page.getByRole('button', { name: 'Sign in' }).click()
    await expect(page.getByRole('heading', { name: 'Overview', exact: true })).toBeVisible()
    const url = new URL(`/buckets/pagination-e2e${view.search}`, systemServer.adminURL)
    await page.goto(url.toString())
    const content = (number: number) =>
      view.name === 'Objects'
        ? page.getByRole('button', { name: `page-${number}`, exact: true })
        : page.getByRole('note', { name: `Object key: page-${number}`, exact: true })
    await expect(content(1)).toBeVisible()
    await page.getByRole('button', { name: 'Next', exact: true }).click()
    await expect(page.getByRole('status', { name: 'Loading', exact: true })).toBeVisible()
    secondPage.resolve()
    await expect(content(2)).toBeVisible()
    await page.getByRole('button', { name: 'Next', exact: true }).click()
    await expect(content(3)).toBeVisible()
    await page.getByRole('button', { name: 'Previous', exact: true }).click()
    await expect(content(2)).toBeVisible()
    await page.getByRole('button', { name: 'Next', exact: true }).click()
    await expect(content(3)).toBeVisible()
    await page.goBack()
    await expect(content(2)).toBeVisible()
    await page.goForward()
    await expect(content(3)).toBeVisible()
    await page.goBack()
    await expect(content(2)).toBeVisible()
    await page.getByRole('button', { name: 'Previous', exact: true }).click()
    await expect(content(1)).toBeVisible()
  })
}
