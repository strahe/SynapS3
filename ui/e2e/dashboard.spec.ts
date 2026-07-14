import { Buffer } from 'node:buffer'
import { expect, test } from './fixtures'

test.describe.configure({ mode: 'serial' })

test('admin dashboard manages and observes a stored object', async ({ page, systemServer }) => {
  await page.goto(systemServer.adminURL)
  await expect(page.getByRole('heading', { name: 'SynapS3 Admin' })).toBeVisible()
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign In' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
  await expect(page.getByText('Setup required')).toHaveCount(0)
  for (const navigation of ['Overview', 'Buckets', 'Topology', 'Tasks', 'Wallet', 'Settings']) {
    await expect(page.getByRole('link', { name: navigation })).toBeVisible()
  }

  await page.getByRole('link', { name: 'Buckets' }).click()
  await expect(page.getByRole('heading', { name: 'Buckets' })).toBeVisible()
  await page.getByRole('button', { name: 'Create Bucket' }).click()
  const createDialog = page.getByRole('dialog', { name: 'Create Bucket' })
  await createDialog.getByLabel('Bucket name').fill('dashboard-e2e')
  await createDialog.getByLabel('Owner').click()
  await page.getByRole('option', { name: 'SYSTEMTESTOWNER (userplus)' }).click()
  await createDialog.getByRole('button', { name: 'Create', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'dashboard-e2e' })).toBeVisible()

  await page.getByRole('button', { name: 'Upload', exact: true }).click()
  const uploadDialog = page.getByRole('dialog', { name: 'Upload objects' })
  await uploadDialog.getByLabel('Files').setInputFiles({
    name: 'dashboard.bin',
    mimeType: 'application/octet-stream',
    buffer: Buffer.alloc(132_000, 's'),
  })
  await uploadDialog.getByRole('button', { name: 'Upload', exact: true }).click()
  await expect(uploadDialog.getByText('Uploaded', { exact: true })).toBeVisible()
  await page.keyboard.press('Escape')
  await expect(uploadDialog).toBeHidden()

  const objectRow = page.getByRole('row').filter({ hasText: 'dashboard.bin' })
  await expect(objectRow).toBeVisible()
  await expect(objectRow.getByText('Filecoin')).toBeVisible()
  await objectRow.getByRole('button', { name: 'Actions for dashboard.bin' }).click()
  await page.getByRole('menuitem', { name: 'Provenance' }).click()
  const provenance = page.getByRole('dialog', { name: 'Storage provenance' })
  await expect(provenance.getByText('3 / 3', { exact: true })).toBeVisible()
  await expect(provenance.getByText('Stored', { exact: true })).toHaveCount(3)
  await provenance.getByRole('button', { name: 'Close' }).click()
  await expect(provenance).toBeHidden()

  await page.getByRole('button', { name: 'Upload', exact: true }).click()
  await uploadDialog.getByLabel('Files').setInputFiles({
    name: 'dashboard.bin',
    mimeType: 'application/octet-stream',
    buffer: Buffer.alloc(132_000, 't'),
  })
  await uploadDialog.getByRole('button', { name: 'Upload', exact: true }).click()
  await expect(uploadDialog.getByText('Uploaded', { exact: true })).toBeVisible()
  await page.keyboard.press('Escape')
  await expect(uploadDialog).toBeHidden()

  await objectRow.getByRole('button', { name: 'Actions for dashboard.bin' }).click()
  await page.getByRole('menuitem', { name: 'Versions' }).click()
  const versionsDialog = page.getByRole('dialog', { name: 'Object versions' })
  await expect(versionsDialog.locator('tbody tr')).toHaveCount(2)
  const currentVersionAction = versionsDialog
    .getByRole('row')
    .filter({ hasText: 'Current' })
    .getByRole('button', { name: /Actions for/ })
  await currentVersionAction.click()
  await expect(page.getByRole('menuitem', { name: 'Restore as new version' })).toHaveCount(0)
  await page.keyboard.press('Escape')

  const sourceVersionAction = versionsDialog
    .locator('tbody tr')
    .nth(1)
    .getByRole('button', { name: /Actions for/ })
  const sourceVersionActionName = await sourceVersionAction.getAttribute('aria-label')
  expect(sourceVersionActionName).toBeTruthy()
  await sourceVersionAction.click()
  await page.getByRole('menuitem', { name: 'Restore as new version' }).click()
  const restoreDialog = page.getByRole('dialog', { name: 'Restore as new version' })
  await expect(restoreDialog.getByText('Current version', { exact: true })).toBeVisible()
  await restoreDialog.getByRole('button', { name: 'Restore as new version' }).click()
  await expect(restoreDialog).toBeHidden()
  await expect(versionsDialog.locator('tbody tr')).toHaveCount(3)
  await expect(versionsDialog.getByRole('row').filter({ hasText: 'Current' })).toHaveCount(1)
  const retainedSourceAction = versionsDialog.getByRole('button', { name: sourceVersionActionName ?? '' })
  await expect(retainedSourceAction).toBeVisible()

  await retainedSourceAction.click()
  await page.getByRole('menuitem', { name: 'Restore as new version' }).click()
  await restoreDialog.getByRole('button', { name: 'Restore as new version' }).click()
  await expect(
    restoreDialog.getByText('This version already matches the current object. No new version was created.')
  ).toBeVisible()
  await expect(restoreDialog.getByRole('button', { name: 'Restore as new version' })).toBeDisabled()
  await restoreDialog.getByRole('button', { name: 'Cancel' }).click()
  await expect(versionsDialog.locator('tbody tr')).toHaveCount(3)
  await page.keyboard.press('Escape')
  await expect(versionsDialog).toBeHidden()

  await page.getByRole('link', { name: 'Topology' }).click()
  await expect(page.getByRole('heading', { name: 'Storage Topology' })).toBeVisible()
  await page.getByRole('tab', { name: 'Providers' }).click()
  for (const providerID of ['101', '102', '103']) {
    await expect(page.getByRole('row').filter({ hasText: providerID })).toBeVisible()
  }

  const pages = [
    { link: 'Overview', heading: 'Overview' },
    { link: 'Buckets', heading: 'Buckets' },
    { link: 'Tasks', heading: 'Tasks' },
    { link: 'Wallet', heading: 'Wallet' },
    { link: 'Settings', heading: 'Settings' },
  ]
  for (const target of pages) {
    await page.getByRole('link', { name: target.link }).click()
    await expect(page.getByRole('heading', { name: target.heading })).toBeVisible()
  }

  await page.getByRole('link', { name: 'Wallet' }).click()
  await expect(page.getByText('FWSS approval is sufficient.')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Approve FWSS' })).toHaveCount(0)
})
