import { Buffer } from 'node:buffer'
import type { Page, Request } from '@playwright/test'
import { expect, test } from './fixtures'

test.describe.configure({ mode: 'serial' })

type AuthRefreshTestState = {
  requests: number
  completed: number
  aborted: number
  offsetMs: number
}

function readAuthRefreshTestState(page: Page) {
  return page.evaluate(
    () => (window as typeof window & { authRefreshTestState: AuthRefreshTestState }).authRefreshTestState
  )
}

test('admin dashboard manages and observes a stored object', async ({ page, systemServer }) => {
  await page.goto(systemServer.adminURL)
  await expect(page.getByRole('heading', { name: 'SynapS3 Admin' })).toBeVisible()
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  const rememberLogin = page.getByRole('checkbox', { name: 'Keep me signed in' })
  await expect(rememberLogin).not.toBeChecked()
  await expect(page.getByText('Use only on a trusted device.')).toBeVisible()
  await rememberLogin.click()
  await expect(rememberLogin).toBeChecked()
  await page.getByRole('button', { name: 'Sign In' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
  const adminSessionCookie = (await page.context().cookies(systemServer.adminURL)).find(
    (cookie) => cookie.name === 'synaps3_admin_session'
  )
  expect(adminSessionCookie).toBeDefined()
  expect(adminSessionCookie?.expires ?? 0).toBeGreaterThan(Math.floor(Date.now() / 1000) + 29 * 24 * 60 * 60)
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
  const requestCounts = { bucket: 0, provenance: 0 }
  const countStorageRequests = (request: Request) => {
    const path = new URL(request.url()).pathname
    if (path === '/api/v1/buckets/dashboard-e2e') requestCounts.bucket++
    if (path === '/api/v1/buckets/dashboard-e2e/objects/provenance') requestCounts.provenance++
  }
  page.on('request', countStorageRequests)
  await objectRow.getByRole('button', { name: 'Actions for dashboard.bin' }).click()
  await page.getByRole('menuitem', { name: 'Provenance' }).click()
  const provenance = page.getByRole('dialog', { name: 'Storage provenance' })
  await expect(provenance.getByText('3 / 3', { exact: true })).toBeVisible({ timeout: 30_000 })
  await expect(provenance.getByText('Stored', { exact: true })).toHaveCount(3)
  page.off('request', countStorageRequests)
  expect(requestCounts.bucket).toBeLessThan(100)
  expect(requestCounts.provenance).toBeLessThan(100)
  await provenance.getByRole('button', { name: 'Close' }).click()
  await expect(provenance).toBeHidden()

  await page.getByRole('button', { name: 'Details', exact: true }).click()
  const bucketDetails = page.getByRole('dialog', { name: 'Bucket details' })
  await bucketDetails.getByRole('button', { name: 'Replace provider for Replica 1' }).click()
  const replacement = page.getByRole('alertdialog', { name: 'Replace provider' })
  await replacement.getByRole('combobox', { name: 'New provider' }).click()
  await page.getByRole('option', { name: 'Choose a provider' }).click()
  const providerPicker = replacement.locator('#replacement-provider')
  await providerPicker.click()
  await page.getByRole('button', { name: /System provider 104/ }).click()
  await replacement.getByRole('button', { name: 'Select this provider' }).click()
  await expect(replacement.getByText(/Storage rate:/)).toBeVisible()
  const confirmation = replacement.getByRole('textbox', { name: /Type to confirm/ })
  await confirmation.fill('replace')
  await expect(replacement.getByRole('button', { name: 'Replace provider' })).toBeEnabled()
  const refreshURL = '**/api/v1/observability/providers/104/refresh'
  await page.route(refreshURL, (route) => route.fulfill({ status: 503, body: '{"error":"unavailable"}' }))
  await replacement.getByRole('button', { name: 'Refresh provider' }).click()
  await expect(replacement.getByText('Could not refresh this provider. Try again.')).toBeVisible()
  await page.unroute(refreshURL)
  await providerPicker.click()
  await page.getByRole('button', { name: /System provider 102/ }).click()
  await expect(providerPicker).toHaveText('Review a provider')
  await expect(confirmation).toHaveValue('')
  await expect(replacement.getByText('Could not refresh this provider. Try again.')).toHaveCount(0)
  await expect(replacement.getByRole('button', { name: 'Replace provider' })).toBeDisabled()
  await replacement.getByRole('button', { name: 'Cancel' }).click()
  await page.keyboard.press('Escape')
  await expect(bucketDetails).toBeHidden()

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

test('S3 user name appears in user and owner flows while copying the full access key', async ({
  page,
  systemServer,
}) => {
  await page.goto(systemServer.adminURL)
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign In' }).click()
  await page.getByRole('link', { name: 'Settings' }).click()
  await page.getByRole('button', { name: 'Create S3 user' }).click()
  const createDialog = page.getByRole('dialog', { name: 'Create S3 user' })
  await createDialog.getByLabel('Name').fill('E2E backup client')
  const createdResponse = page.waitForResponse(
    (response) => response.url().endsWith('/api/v1/s3-users') && response.request().method() === 'POST'
  )
  await createDialog.getByRole('button', { name: 'Create user' }).click()
  const created = (await (await createdResponse).json()) as { access_key: string }
  await page
    .getByRole('dialog', { name: 'S3 credentials generated' })
    .getByRole('button', { name: 'Close' })
    .first()
    .click()
  const initialLabel = `E2E backup client (…${created.access_key.slice(-6)})`
  const userRow = page.getByRole('row').filter({ hasText: 'E2E backup client' })
  await expect(userRow).toContainText(initialLabel)
  await userRow.getByRole('button', { name: 'Edit user' }).click()
  const editDialog = page.getByRole('dialog', { name: 'Edit S3 user' })
  await editDialog.getByLabel('Name').fill('E2E archive client')
  await editDialog.getByRole('button', { name: 'Save' }).click()
  await expect(page.getByRole('row').filter({ hasText: 'E2E archive client' })).toBeVisible()

  await page.getByRole('link', { name: 'Buckets' }).click()
  await page.getByRole('button', { name: 'Create Bucket' }).click()
  await page.getByRole('dialog', { name: 'Create Bucket' }).getByLabel('Owner').click()
  await expect(
    page.getByRole('option', { name: `E2E archive client (…${created.access_key.slice(-6)}) (userplus)` })
  ).toBeVisible()
  await page.getByRole('option', { name: `E2E archive client (…${created.access_key.slice(-6)}) (userplus)` }).click()
  await page.getByRole('dialog', { name: 'Create Bucket' }).getByLabel('Bucket name').fill('named-owner-e2e')
  await page.getByRole('dialog', { name: 'Create Bucket' }).getByRole('button', { name: 'Create', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'named-owner-e2e' })).toBeVisible()
  await page.getByRole('button', { name: 'Details' }).click()
  const ownerNote = page.getByRole('note', { name: `Owner: ${created.access_key}` }).first()
  await expect(ownerNote).toContainText(`E2E archive client (…${created.access_key.slice(-6)})`)
  await page.context().grantPermissions(['clipboard-read', 'clipboard-write'])
  await ownerNote.locator('..').getByRole('button', { name: 'Copy Owner' }).click()
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(created.access_key)

  await page.keyboard.press('Escape')
  await page.getByRole('link', { name: 'Buckets' }).first().click()
  const bucketRow = page.getByRole('row').filter({ hasText: 'named-owner-e2e' })
  await expect(bucketRow).toContainText(`E2E archive client (…${created.access_key.slice(-6)})`)
  await bucketRow.getByRole('button', { name: 'Change owner' }).click()
  await page.getByRole('dialog', { name: 'Change bucket owner' }).getByLabel('Owner').click()
  await page.getByRole('option', { name: 'Internal root' }).click()
  await page.getByRole('dialog', { name: 'Change bucket owner' }).getByRole('button', { name: 'Review' }).click()
  await expect(page.getByRole('dialog', { name: 'Review bucket owner' })).toContainText(
    `E2E archive client (…${created.access_key.slice(-6)})`
  )
  await page.getByRole('dialog', { name: 'Review bucket owner' }).getByRole('button', { name: 'Back' }).click()
  await page.getByRole('dialog', { name: 'Change bucket owner' }).getByRole('button', { name: 'Cancel' }).click()

  await page.route('**/api/v1/s3-users', (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([
        { access_key: created.access_key, name: 'L'.repeat(128), role: 'userplus', bucket_count: 1 },
      ]),
    })
  )
  await page.reload()
  const longNameOwnerCell = page.getByRole('row').filter({ hasText: 'named-owner-e2e' }).getByRole('cell').nth(1)
  await expect(longNameOwnerCell).toContainText(created.access_key.slice(-6))
  await expect
    .poll(() => longNameOwnerCell.evaluate((cell) => cell.getBoundingClientRect().width))
    .toBeLessThanOrEqual(256)
  await page.unroute('**/api/v1/s3-users')

  await page.route('**/api/v1/s3-users', (route) =>
    route.fulfill({ status: 500, contentType: 'application/json', body: '{"error":"unavailable"}' })
  )
  await page.reload()
  const fallbackRow = page.getByRole('row').filter({ hasText: 'named-owner-e2e' })
  await expect(fallbackRow).toContainText(created.access_key)
  await fallbackRow.getByRole('button', { name: 'Change owner' }).click()
  await expect(page.getByRole('dialog', { name: 'Change bucket owner' }).getByLabel('Owner')).toContainText(
    created.access_key
  )
})

test('admin session renewal follows trusted activity and sign-out cancels an in-flight request', async ({
  page,
  systemServer,
}) => {
  await page.goto(systemServer.adminURL)
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign In' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()

  await page.evaluate(() => {
    const state: AuthRefreshTestState = { requests: 0, completed: 0, aborted: 0, offsetMs: 6 * 60 * 1000 }
    const testWindow = window as typeof window & { authRefreshTestState: typeof state }
    testWindow.authRefreshTestState = state
    const originalFetch = window.fetch.bind(window)
    const currentTime = Date.now.bind(Date)
    Date.now = () => currentTime() + state.offsetMs
    window.fetch = async (input, init) => {
      const requestURL = input instanceof Request ? input.url : input.toString()
      if (!requestURL.endsWith('/api/v1/auth/refresh')) {
        return originalFetch(input, init)
      }

      state.requests += 1
      if (state.requests === 1) {
        const response = await originalFetch(input, init)
        const session = (await response.json()) as { refresh_after: string }
        session.refresh_after = new Date(Date.now() + 60_000).toISOString()
        state.completed += 1
        return new Response(JSON.stringify(session), {
          status: response.status,
          headers: { 'Content-Type': 'application/json' },
        })
      }

      return new Promise<Response>((_resolve, reject) => {
        const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined)
        const abort = () => {
          state.aborted += 1
          reject(new DOMException('The operation was aborted.', 'AbortError'))
        }
        if (signal?.aborted) {
          abort()
          return
        }
        signal?.addEventListener('abort', abort, { once: true })
      })
    }
  })

  await page.evaluate(() => {
    document.dispatchEvent(new Event('visibilitychange'))
    document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Tab' }))
  })
  expect((await readAuthRefreshTestState(page)).requests).toBe(0)

  await page.keyboard.press('Tab')
  await expect.poll(async () => (await readAuthRefreshTestState(page)).completed).toBe(1)
  await page.keyboard.press('Tab')
  expect((await readAuthRefreshTestState(page)).requests).toBe(1)

  await page.evaluate(() => {
    const state = (window as typeof window & { authRefreshTestState: AuthRefreshTestState }).authRefreshTestState
    state.offsetMs += 61_000
  })
  await page.keyboard.press('Tab')
  await page.keyboard.press('Tab')
  expect((await readAuthRefreshTestState(page)).requests).toBe(2)

  await page.getByRole('button', { name: 'Sign Out' }).click()
  await expect(page.getByRole('heading', { name: 'SynapS3 Admin' })).toBeVisible()
  expect(await readAuthRefreshTestState(page)).toMatchObject({ requests: 2, completed: 1, aborted: 1 })
})
