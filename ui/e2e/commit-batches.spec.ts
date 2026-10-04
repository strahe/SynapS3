import { Buffer } from 'node:buffer'
import { expect, test } from './fixtures'

test.use({ commitMaxWait: '30m' })

test('manual submission keeps the list open and exposes batch contents and confirmation history', async ({
  page,
  systemServer,
}) => {
  test.setTimeout(120_000)
  await page.goto(systemServer.adminURL)
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign In' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
  await page.getByRole('link', { name: 'Buckets', exact: true }).click()
  await page.getByRole('button', { name: 'Create Bucket' }).click()
  const create = page.getByRole('dialog', { name: 'Create Bucket' })
  await create.getByLabel('Bucket name').fill('manual-batches-e2e')
  await create.getByLabel('Owner').click()
  await page.getByRole('option', { name: 'SYSTEMTESTOWNER (userplus)' }).click()
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
  await expect(page.getByRole('button', { name: 'Submit batch', exact: true }).first()).toBeVisible({
    timeout: 30_000,
  })
  await expect(
    page.getByText('Submit a batch before it fills or its wait ends. Smaller batches may increase transaction costs.', {
      exact: true,
    })
  ).toBeVisible()
  await expect(page.getByRole('cell', { name: 'Waiting to submit', exact: true }).first()).toBeVisible()
  await expect(page.getByRole('cell', { name: '1 / 32', exact: true }).first()).toBeVisible()
  const detail = page.getByRole('dialog', { name: 'Batch details' })
  await page
    .getByRole('button', { name: /^Details for batch / })
    .first()
    .click()
  await expect(detail).toBeVisible()
  await expect(detail.getByRole('heading', { name: 'Batch details' })).toBeFocused()
  await expect(detail.getByRole('note', { name: 'Object key: manual.bin', exact: true })).toBeVisible()
  await expect(detail.getByRole('note', { name: /^Piece CID:/ })).toBeVisible()
  await expect(detail.getByText(/Smaller batches may increase transaction costs\./)).toBeVisible()
  await page.keyboard.press('Escape')
  // Each of the default three replicas collects independently.
  for (let replica = 0; replica < 3; replica++) {
    await page.getByRole('button', { name: 'Refresh', exact: true }).click()
    const row = page
      .getByRole('row')
      .filter({ has: page.getByRole('button', { name: 'Submit batch', exact: true }) })
      .first()
    await expect(row).toBeVisible({
      timeout: 30_000,
    })
    const detailsLabel =
      (await row.getByRole('button', { name: /^Details for batch / }).getAttribute('aria-label')) ?? ''
    expect(detailsLabel).toMatch(/^Details for batch /)
    const batchID = detailsLabel.replace('Details for batch ', '')
    await row.getByRole('button', { name: 'Submit batch', exact: true }).click()
    await expect(
      page.getByRole('alert').filter({ has: page.getByRole('note', { name: `Batch ID: ${batchID}`, exact: true }) })
    ).toBeVisible()
    await expect(detail).not.toBeVisible()
    if (replica === 0) {
      await page.getByRole('combobox', { name: 'Status' }).click()
      await page.getByRole('option', { name: 'All statuses', exact: true }).click()
    }
    await page.getByRole('button', { name: detailsLabel, exact: true }).click()
    await expect(detail.getByRole('heading', { name: 'Batch details' })).toBeFocused()
    await expect(detail.getByRole('note', { name: `Batch ID: ${batchID}`, exact: true })).toBeVisible()
    await expect(detail.getByRole('note', { name: 'Object key: manual.bin', exact: true })).toBeVisible()
    await expect(detail.getByText('Completed', { exact: true })).toBeVisible({ timeout: 30_000 })
    await expect(detail.getByRole('note', { name: /^Transaction:/ })).toBeVisible()
    await expect(detail.locator('time[datetime]')).toHaveCount(3)
    await expect(detail.getByRole('button', { name: 'Submit batch', exact: true })).toHaveCount(0)
    await page.keyboard.press('Escape')
  }
  await page.getByRole('combobox', { name: 'Status' }).click()
  await page.getByRole('option', { name: 'Completed', exact: true }).click()
  await expect(page.getByRole('cell', { name: 'Completed', exact: true })).toHaveCount(3)
  await page
    .getByRole('button', { name: /^Details for batch / })
    .first()
    .click()
  await expect(detail.getByRole('heading', { name: 'Batch details' })).toBeFocused()
  await expect(detail.getByText('Completed', { exact: true })).toBeVisible()
  await expect(detail.getByRole('button', { name: 'Submit batch', exact: true })).toHaveCount(0)
  await page.keyboard.press('Escape')
  await page.getByRole('link', { name: 'Settings', exact: true }).click()
  await page.getByRole('tab', { name: 'Workers', exact: true }).click()
  await expect(page.getByRole('checkbox', { name: 'Submit batches early to free cache space' })).not.toBeChecked()
})
