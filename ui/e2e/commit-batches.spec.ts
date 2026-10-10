import { Buffer } from 'node:buffer'
import type { CommitBatch } from '../src/api/client'
import { expect, test } from './fixtures'

test.use({ commitMaxWait: '30m' })

test('URL status changes reset batch pagination without reusing another status cursor', async ({
  page,
  systemServer,
}) => {
  const requests: URL[] = []
  await page.route('**/api/v1/commit-batches?*', (route) => {
    const url = new URL(route.request().url())
    requests.push(url)
    const status = url.searchParams.get('status') === 'confirmed' ? 'confirmed' : 'collecting'
    const number = url.searchParams.has('cursor') ? 2 : 1
    const batch: CommitBatch = {
      request_id: `${status}-${number}`,
      status,
      bucket_name: 'pagination-e2e',
      provider_id: '101',
      provider_name: null,
      data_set_id: '1',
      data_set_row_id: 1,
      member_count: 1,
      max_pieces: 32,
      total_bytes: 1,
      oldest_ready_at: null,
      collection_deadline: null,
      seal_requested_at: null,
      sealed_at: null,
      submitted_at: null,
      confirmed_at: null,
      created_at: '2026-10-10T00:00:00Z',
      can_seal: false,
      task_id: null,
      task_status: null,
      status_message: null,
      last_error: null,
      transaction_id: null,
    }
    const cursor = Buffer.from(JSON.stringify({ created_at: batch.created_at, request_id: batch.request_id })).toString(
      'base64url'
    )
    return route.fulfill({ json: { batches: [batch], next_cursor: number === 1 ? cursor : undefined } })
  })
  await page.goto(systemServer.adminURL)
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('heading', { name: 'Overview', exact: true })).toBeVisible()
  await page.goto(new URL('/commit-batches?status=confirmed', systemServer.adminURL).toString())
  await expect(page.getByRole('note', { name: 'Batch ID: confirmed-1', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Next', exact: true }).click()
  await expect(page.getByRole('note', { name: 'Batch ID: confirmed-2', exact: true })).toBeVisible()
  await page.getByRole('link', { name: 'Batches', exact: true }).click()
  await expect(page.getByRole('note', { name: 'Batch ID: collecting-1', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Previous', exact: true })).toBeDisabled()
  expect(
    requests
      .filter((url) => url.searchParams.get('status') === 'collecting')
      .every((url) => !url.searchParams.has('cursor'))
  ).toBe(true)
  await page.getByRole('combobox', { name: 'Status', exact: true }).click()
  await page.getByRole('option', { name: 'Completed', exact: true }).click()
  await expect(page.getByRole('note', { name: 'Batch ID: confirmed-1', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Previous', exact: true })).toBeDisabled()
})

test('manual submission keeps the list open and exposes batch contents and confirmation history', async ({
  page,
  systemServer,
}) => {
  test.setTimeout(120_000)
  await page.goto(systemServer.adminURL)
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
  await page.getByRole('link', { name: 'Buckets', exact: true }).click()
  await page.getByRole('button', { name: 'Create bucket' }).first().click()
  const create = page.getByRole('dialog', { name: 'Create bucket' })
  await create.getByLabel('Bucket name').fill('manual-batches-e2e')
  await create.getByLabel('Owner').click()
  await page.getByRole('option', { name: 'SYSTEMTESTOWNER (User+)' }).click()
  await create.getByRole('button', { name: 'Create', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'manual-batches-e2e' })).toBeVisible()
  await page.getByRole('button', { name: 'Upload', exact: true }).click()
  const upload = page.getByRole('dialog', { name: 'Upload objects' })
  await upload
    .getByLabel('Files')
    .setInputFiles({ name: 'manual.bin', mimeType: 'application/octet-stream', buffer: Buffer.alloc(132_000, 'b') })
  await upload.getByRole('button', { name: 'Upload', exact: true }).click()
  await expect(upload.getByText('Uploaded', { exact: true })).toBeVisible()
  await page.keyboard.press('Escape')
  await page.getByRole('link', { name: 'Batches', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Batches', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Submit next batch', exact: true }).first()).toBeVisible({
    timeout: 30_000,
  })
  await expect(page.getByText(/Submit next batch sends one now; the remaining pieces keep collecting\./)).toBeVisible()
  await expect(page.getByRole('cell', { name: 'Collecting data', exact: true }).first()).toBeVisible()
  await expect(page.getByRole('cell', { name: '1', exact: true }).first()).toBeVisible()
  const detail = page.getByRole('dialog', { name: 'manual-batches-e2e →' })
  const detailTitle = detail.getByRole('heading', { name: /^manual-batches-e2e → / })
  await page
    .getByRole('button', { name: /^Details for batch / })
    .first()
    .click()
  await expect(detail).toBeVisible()
  await expect(detailTitle).toBeFocused()
  await expect(detail.getByRole('note', { name: 'Object key: manual.bin', exact: true })).toBeVisible()
  await expect(detail.getByRole('note', { name: /^Piece CID:/ })).toBeVisible()
  await expect(detail.getByText(/Smaller batches may increase transaction costs\./)).toBeVisible()
  await page.keyboard.press('Escape')
  // Each of the default three replicas collects independently.
  for (let replica = 0; replica < 3; replica++) {
    await page.getByRole('button', { name: 'Refresh', exact: true }).click()
    const row = page
      .getByRole('row')
      .filter({ has: page.getByRole('button', { name: 'Submit next batch', exact: true }) })
      .first()
    await expect(row).toBeVisible({
      timeout: 30_000,
    })
    const detailsLabel =
      (await row.getByRole('button', { name: /^Details for batch / }).getAttribute('aria-label')) ?? ''
    expect(detailsLabel).toMatch(/^Details for batch /)
    const batchID = detailsLabel.replace('Details for batch ', '')
    await row.getByRole('button', { name: 'Submit next batch', exact: true }).click()
    await expect(page.getByText(`Batch ${batchID}`, { exact: true })).toBeVisible()
    await expect(detail).not.toBeVisible()
    if (replica === 0) {
      await page.getByRole('combobox', { name: 'Status' }).click()
      await page.getByRole('option', { name: 'All statuses', exact: true }).click()
    }
    await page.getByRole('button', { name: detailsLabel, exact: true }).click()
    await expect(detailTitle).toBeFocused()
    await expect(detail.getByRole('note', { name: `Batch ID: ${batchID}`, exact: true })).toBeVisible()
    await expect(detail.getByRole('note', { name: 'Object key: manual.bin', exact: true })).toBeVisible()
    await expect(detail.getByText('Completed', { exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(detail.getByRole('note', { name: /^Transaction:/ })).toBeVisible()
    await expect(detail.locator('time[datetime]')).toHaveCount(2)
    await expect(detail.getByRole('button', { name: 'Submit next batch', exact: true })).toHaveCount(0)
    await page.keyboard.press('Escape')
  }
  await page.getByRole('combobox', { name: 'Status' }).click()
  await page.getByRole('option', { name: 'Completed', exact: true }).click()
  await expect(page.getByRole('cell', { name: 'Completed', exact: true })).toHaveCount(3)
  await page
    .getByRole('button', { name: /^Details for batch / })
    .first()
    .click()
  await expect(detailTitle).toBeFocused()
  await expect(detail.getByText('Completed', { exact: true })).toBeVisible()
  await expect(detail.getByRole('button', { name: 'Submit next batch', exact: true })).toHaveCount(0)
  await page.keyboard.press('Escape')
  await page.getByRole('link', { name: 'Settings', exact: true }).click()
  await page.getByRole('tab', { name: 'Workers', exact: true }).click()
  await expect(page.getByRole('checkbox', { name: 'Submit batches early to free cache space' })).not.toBeChecked()
})
