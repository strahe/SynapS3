import { expect, startServer, stopProcess, test } from './fixtures'

test('worker concurrency settings are saved and read back', async ({ page }) => {
  const settingsServer = await startServer('1s')
  try {
    await page.goto(settingsServer.adminURL)
    await page.getByLabel('Username').fill('admin')
    await page.getByLabel('Password').fill('system-test-admin-password')
    await page.getByRole('button', { name: 'Sign In' }).click()
    await page.getByRole('link', { name: 'Settings' }).click()
    await page.getByRole('tab', { name: 'Workers', exact: true }).click()
    const uploadConcurrency = page
      .getByRole('group')
      .filter({ has: page.getByText('Upload Concurrency', { exact: true }) })
      .getByRole('spinbutton')
    const taskConcurrency = page
      .getByRole('group')
      .filter({ has: page.getByText('Task Concurrency', { exact: true }) })
      .getByRole('spinbutton')
    await expect(uploadConcurrency).toHaveValue('4')
    await expect(taskConcurrency).toHaveValue('4')
    await expect(page.getByText('Storage Mutation Concurrency', { exact: true })).toHaveCount(0)
    await expect(page.getByText('Removal Concurrency', { exact: true })).toHaveCount(0)
    await uploadConcurrency.fill('6')
    await page.getByRole('button', { name: 'Save', exact: true }).click()
    const settingsReview = page.getByRole('alertdialog', { name: 'Save reviewed settings?' })
    await expect(settingsReview.getByText('Upload Concurrency', { exact: true })).toBeVisible()
    await expect(settingsReview.getByText('Increases concurrent uploads.', { exact: true })).toBeVisible()
    const savedSettings = page.waitForResponse(
      (response) => response.url().endsWith('/api/v1/settings') && response.request().method() === 'PUT'
    )
    await settingsReview.getByRole('button', { name: 'Save settings', exact: true }).click()
    expect((await savedSettings).status()).toBe(200)
    await expect(settingsReview).toBeHidden()
    await expect(page.getByText('Settings were saved. Restart SynapS3 to apply runtime changes.')).toBeVisible()
    await page.reload()
    await page.getByRole('tab', { name: 'Workers', exact: true }).click()
    await expect(uploadConcurrency).toHaveValue('6')
    await expect(taskConcurrency).toHaveValue('4')
  } finally {
    expect(await stopProcess(settingsServer.process, 10_000)).toEqual({ exitCode: 0, forced: false })
  }
})
