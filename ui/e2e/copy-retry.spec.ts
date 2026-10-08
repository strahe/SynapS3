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

test('failed tasks retry through their task ID before and after acknowledgement', async ({ page, systemServer }) => {
  let available = true
  let requests = 0
  let reads = 0
  let acknowledged = false
  const copyID = 128
  const failedTask = (): TaskItem => ({
    id: 201,
    type: 'storage_pull',
    operation: 'Transfer data',
    status: 'failed',
    presentation_status: 'failed',
    subject_type: 'storage_copy',
    subject_key: String(copyID),
    retry_count: 5,
    retry_limit: 5,
    retry_of_task_id: null,
    retryable: available,
    retry_task_id: available ? 201 : null,
    acknowledgeable: !acknowledged,
    acknowledged_at: acknowledged ? '2026-10-03T00:03:00Z' : undefined,
    last_error: 'Storage provider 101 could not copy the piece from provider 100',
    available_at: '2026-10-03T00:00:00Z',
    created_at: '2026-10-03T00:00:00Z',
    started_at: '2026-10-03T00:00:00Z',
    updated_at: '2026-10-03T00:02:13Z',
    finished_at: '2026-10-03T00:02:13Z',
  })
  await page.route('**/api/v1/tasks?*', (route) => {
    reads++
    return route.fulfill({ json: { tasks: [failedTask()] } })
  })
  await page.route('**/api/v1/tasks/201/retry', (route) => {
    expect(route.request().method()).toBe('POST')
    requests++
    available = false
    return requests === 1
      ? route.fulfill({ status: 202, json: { task_id: 202 } })
      : route.fulfill({ status: 409, json: { code: 'no_source', error: 'replica cannot be retried' } })
  })
  await page.route('**/api/v1/tasks/201', (route) =>
    route.fulfill({
      json: {
        task: failedTask(),
        policy: {
          max_attempts: 6,
          initial_delay: '10s',
          maximum_delay: '5m0s',
          multiplier: 2,
          jitter: 0.2,
          invocation_timeout: '0s',
          observation_window: '0s',
          legacy: false,
        },
      },
    })
  )
  await page.route('**/api/v1/tasks/201/history*', (route) => route.fulfill({ json: { tasks: [failedTask()] } }))
  await page.route('**/api/v1/tasks/201/events*', (route) => route.fulfill({ json: { events: [] } }))
  await signIn(page, systemServer.adminURL)
  for (const viewed of [false, true]) {
    available = true
    acknowledged = viewed
    await page.goto(new URL('/tasks?status=failed', systemServer.adminURL).toString())
    const retry = page.getByRole('button', { name: 'Retry', exact: true })
    await expect(retry).toBeVisible()
    if (!viewed) {
      await page.getByRole('button', { name: 'View', exact: true }).click()
      const details = page.getByRole('dialog', { name: 'Task 201', exact: true })
      await expect(details.getByRole('tab', { name: 'Overview', exact: true })).toHaveAttribute('aria-selected', 'true')
      await expect(details.getByRole('heading', { name: 'Execution policy' })).toBeVisible()
      await expect(details.getByText('6 / 6', { exact: true })).toBeVisible()
      await expect(details.getByRole('note', { name: `Task error: ${failedTask().last_error}` })).toBeVisible()
      await details.getByRole('tab', { name: 'History', exact: true }).click()
      await expect(
        details.getByRole('tabpanel', { name: 'History', exact: true }).getByText('Task 201', { exact: true })
      ).toBeVisible()
      await details.getByRole('tab', { name: 'Events', exact: true }).click()
      await expect(details.getByText('No events recorded.', { exact: true })).toBeVisible()
      await details.getByRole('button', { name: 'Close', exact: true }).click()
      await page.getByRole('button', { name: 'View', exact: true }).click()
      await expect(details.getByRole('tab', { name: 'Overview', exact: true })).toHaveAttribute('aria-selected', 'true')
      await details.getByRole('button', { name: 'Close', exact: true }).click()
    }
    const previousReads = reads
    await retry.click()
    await expect(retry).toHaveCount(0)
    expect(reads).toBeGreaterThan(previousReads)
  }
  expect(requests).toBe(2)
  await expect(page.getByRole('alert').first()).toBeVisible()
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
  await page.route('**/api/v1/buckets/copy-retry-e2e/objects/provenance?*', async (route) => {
    const response = await route.fetch()
    const provenance: ObjectProvenance = await response.json()
    const copy = provenance.copies[0]
    copy.status = retried ? 'pending' : 'failed'
    copy.last_error = retried ? undefined : 'Storage provider 101 could not copy the piece from provider 100'
    copy.retryable = !retried
    copy.retry_task_id = retried ? null : 201
    await route.fulfill({ json: provenance })
  })
  await page.route('**/api/v1/tasks/201/retry', (route) => {
    expect(new URL(route.request().url()).pathname).toBe('/api/v1/tasks/201/retry')
    expect(route.request().method()).toBe('POST')
    retried = true
    return route.fulfill({ status: 202, json: { task_id: 202 } })
  })
  const object = page.getByRole('row').filter({ hasText: 'retry.bin' })
  await object.getByRole('button', { name: 'Actions for retry.bin' }).click()
  await page.getByRole('menuitem', { name: 'Provenance' }).click()
  const provenance = page.getByRole('dialog', { name: 'Storage provenance' })
  const retry = provenance.getByRole('button', { name: 'Retry', exact: true })
  await expect(retry).toBeVisible()
  await expect(provenance.getByText('Recovery', { exact: true })).toBeVisible()
  await retry.click()
  await expect(retry).toHaveCount(0)
  expect(retried).toBe(true)
  await expect(provenance.getByText('Last error', { exact: true })).toHaveCount(0)
})
