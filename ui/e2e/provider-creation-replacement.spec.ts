import type { BucketDetail } from '../src/api/client'
import { expect, test } from './fixtures'

test('provider replacement renders mocked setup eligibility and confirmations', async ({ page, systemServer }) => {
  await page.goto(systemServer.adminURL)
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
  await page.getByRole('link', { name: 'Buckets', exact: true }).click()
  await page.getByRole('button', { name: 'Create bucket' }).first().click()
  const create = page.getByRole('dialog', { name: 'Create bucket' })
  const name = 'creation-replacement-e2e'
  await create.getByLabel('Bucket name').fill(name)
  await create.getByLabel('Owner').click()
  await page.getByRole('option', { name: 'SYSTEMTESTOWNER (User+)' }).click()
  await create.getByRole('button', { name: 'Create', exact: true }).click()
  await expect(page.getByRole('heading', { name })).toBeVisible()
  const endpoint = new URL(`/api/v1/buckets/${name}`, page.url()).toString()
  let bucket: BucketDetail | undefined
  await expect
    .poll(async () => {
      bucket = (await (await page.request.get(endpoint)).json()) as BucketDetail
      return bucket.data_sets.some((row) => row.is_current && row.copy_index === 0 && row.status === 'ready')
    })
    .toBe(true)
  if (!bucket) throw new Error('Bucket response is missing')
  const baseline = bucket
  const source = baseline.data_sets.find((row) => row.is_current && row.copy_index === 0)
  if (!source) throw new Error('Replica is missing')

  for (const state of [
    'unsent',
    'rejected',
    'unknown',
    'ready',
    'ready-with-refused-target',
    'unsent-with-refused-target',
  ] as const) {
    const ready = state === 'ready' || state === 'ready-with-refused-target'
    const abandonedTarget = state.endsWith('with-refused-target')
    const setupError = state === 'rejected' ? 'Storage provider rejected setup. Choose another provider.' : undefined
    const response: BucketDetail = {
      ...baseline,
      data_sets: baseline.data_sets.map((row) =>
        row.id !== source.id
          ? row
          : {
              ...row,
              status: ready ? 'ready' : 'pending',
              data_set_id: ready ? row.data_set_id : undefined,
              client_data_set_id: state.startsWith('unsent') ? undefined : (row.client_data_set_id ?? '909'),
              replacement_has_late_service_risk: state === 'rejected' || abandonedTarget,
              replaceable: state !== 'unknown',
              replacement_blocked_reason: state === 'unknown' ? 'replacement_source_outcome_unknown' : undefined,
              setup_error: setupError,
            }
      ),
    }
    await page.route(endpoint, (route) => route.fulfill({ json: response }))
    await page.reload()
    await page.getByRole('tab', { name: 'Storage', exact: true }).click()
    const details = page.getByRole('tabpanel', { name: 'Storage', exact: true })
    const actions = details.getByRole('button', { name: 'Actions for Replica 1', exact: true })
    if (state === 'unknown') {
      await expect(actions).toHaveCount(0)
      await expect(
        details.getByText(
          'Could not confirm whether storage setup succeeded. Check its task before replacing the provider.'
        )
      ).toBeVisible()
    } else {
      if (setupError) await expect(details.getByText(setupError, { exact: true })).toBeVisible()
      await actions.click()
      await page.getByRole('menuitem', { name: 'Replace provider', exact: true }).click()
      const confirmation = page.getByRole('alertdialog', { name: 'Replace provider' })
      if (ready) {
        await expect(confirmation.getByText('Data to copy', { exact: true })).toBeVisible()
        await expect(confirmation.getByText(/The old provider is ended/)).toBeVisible()
      } else {
        await expect(confirmation.getByText(/This creates a replica on the new provider/)).toBeVisible()
        await expect(confirmation.getByText('Data to copy', { exact: true })).toHaveCount(0)
        await expect(confirmation.getByText(/The old provider is ended/)).toHaveCount(0)
        await expect(confirmation.getByText(/New uploads that need this replica wait/)).toBeVisible()
      }
      await expect(confirmation.getByText(/An earlier request may still create a paid data set/)).toHaveCount(
        state === 'rejected' || abandonedTarget ? 1 : 0
      )
      await confirmation.getByRole('button', { name: 'Cancel' }).click()
      await expect(confirmation).toBeHidden()
    }
    await page.unroute(endpoint)
  }
})
