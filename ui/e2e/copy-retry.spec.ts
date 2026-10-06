import type { Page } from '@playwright/test'
import type { ObjectProvenance, TaskItem } from '../src/api/client'
import { expect, test } from './fixtures'

async function signIn(page: Page, adminURL: string) {
  await page.goto(adminURL)
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign In' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
}

test('failed and dismissed tasks retry the replica and refresh admission after a conflict', async ({
  page,
  systemServer,
}) => {
  let available = true
  let requests = 0
  let reads = 0
  const copyID = 128
  await page.route('**/api/v1/tasks?*', (route) => {
    reads++
    const dismissed = new URL(route.request().url()).searchParams.get('status') === 'dismissed'
    const row: TaskItem = {
      id: 201,
      type: 'storage_pull',
      operation: 'Transfer data',
      status: 'failed',
      presentation_status: dismissed ? 'dismissed' : 'failed',
      subject_type: 'storage_copy',
      subject_key: String(copyID),
      retry_count: 0,
      retryable: false,
      acknowledgeable: !dismissed,
      copy_retry: available
        ? { copy_id: copyID, available: true }
        : { copy_id: copyID, available: false, reason_code: 'no_source' },
      last_error: 'Storage provider 101 could not copy the piece from provider 100',
      available_at: '2026-10-03T00:00:00Z',
      created_at: '2026-10-03T00:00:00Z',
      updated_at: '2026-10-03T00:02:13Z',
      finished_at: '2026-10-03T00:02:13Z',
    }
    return route.fulfill({ json: { tasks: [row] } })
  })
  await page.route(`**/api/v1/storage-copies/${copyID}/retry`, (route) => {
    expect(route.request().method()).toBe('POST')
    requests++
    available = false
    return requests === 1
      ? route.fulfill({ status: 202, json: { copy_id: copyID, task_id: 202 } })
      : route.fulfill({ status: 409, json: { code: 'no_source', error: 'replica cannot be retried' } })
  })
  await signIn(page, systemServer.adminURL)
  for (const status of ['failed', 'dismissed']) {
    available = true
    await page.goto(new URL(`/tasks?status=${status}`, systemServer.adminURL).toString())
    const retry = page.getByRole('button', { name: 'Retry replica', exact: true })
    await expect(retry).toBeVisible()
    await expect(page.getByRole('button', { name: 'Retry', exact: true })).toHaveCount(0)
    const previousReads = reads
    await retry.click()
    await expect(retry).toHaveCount(0)
    await expect(page.getByText('No source available', { exact: true }).first()).toBeVisible()
    expect(reads).toBeGreaterThan(previousReads)
  }
  expect(requests).toBe(2)
  await expect(page.getByRole('alert').getByText('No source available', { exact: true })).toBeVisible()
})

test('provenance retries a failed replica and shows its updated recovery state', async ({ page, systemServer }) => {
  await signIn(page, systemServer.adminURL)
  await page.getByRole('link', { name: 'Buckets', exact: true }).click()
  await page.getByRole('button', { name: 'Create Bucket' }).click()
  const create = page.getByRole('dialog', { name: 'Create Bucket' })
  await create.getByLabel('Bucket name').fill('copy-retry-e2e')
  await create.getByLabel('Owner').click()
  await page.getByRole('option', { name: 'SYSTEMTESTOWNER (userplus)' }).click()
  await create.getByRole('button', { name: 'Create', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'copy-retry-e2e' })).toBeVisible()
  await page.getByRole('button', { name: 'Upload', exact: true }).click()
  const upload = page.getByRole('dialog', { name: 'Upload objects' })
  await upload.getByLabel('Files').setInputFiles({
    name: 'retry.bin',
    mimeType: 'application/octet-stream',
    buffer: Buffer.alloc(132_000, 'r'),
  })
  await upload.getByRole('button', { name: 'Upload', exact: true }).click()
  await expect(upload.getByText('Uploaded', { exact: true })).toBeVisible()
  await page.keyboard.press('Escape')
  await expect(upload).toBeHidden()
  let retried = false
  let copyID = 0
  await page.route('**/api/v1/buckets/copy-retry-e2e/objects/provenance?*', async (route) => {
    const response = await route.fetch()
    const provenance: ObjectProvenance = await response.json()
    const copy = provenance.copies[0]
    copyID = copy.copy_id
    copy.status = retried ? 'pending' : 'failed'
    copy.last_error = retried ? undefined : 'Storage provider 101 could not copy the piece from provider 100'
    copy.retry = retried ? undefined : { available: true }
    await route.fulfill({ json: provenance })
  })
  await page.route('**/api/v1/storage-copies/*/retry', (route) => {
    expect(new URL(route.request().url()).pathname).toBe(`/api/v1/storage-copies/${copyID}/retry`)
    expect(route.request().method()).toBe('POST')
    retried = true
    return route.fulfill({ status: 202, json: { copy_id: copyID, task_id: 202 } })
  })
  const object = page.getByRole('row').filter({ hasText: 'retry.bin' })
  await object.getByRole('button', { name: 'Actions for retry.bin' }).click()
  await page.getByRole('menuitem', { name: 'Provenance' }).click()
  const provenance = page.getByRole('dialog', { name: 'Storage provenance' })
  const retry = provenance.getByRole('button', { name: 'Retry replica', exact: true })
  await expect(retry).toBeVisible()
  await expect(provenance.getByText('Recovery', { exact: true })).toBeVisible()
  await retry.click()
  await expect(retry).toHaveCount(0)
  expect(retried).toBe(true)
  await expect(provenance.getByText('Last error', { exact: true })).toHaveCount(0)
})
