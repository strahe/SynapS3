import type { Page } from '@playwright/test'
import type { TaskItem, TaskSubjectInfo } from '../src/api/client'
import { expect, test } from './fixtures'

const fileKey = 'projects/archive/2026/october/reports/a-complete-and-long-object-key.txt'
const subject: TaskSubjectInfo = {
  subject_type: 'storage_content',
  subject_key: '128',
  bucket: 'files',
  size: 1024,
  file: { key: fileKey, source: 'current', other_versions: 2 },
}

function task(id: number, key = '128'): TaskItem {
  return {
    id,
    type: 'storage_store',
    operation: 'Upload data',
    status: 'completed',
    presentation_status: 'completed',
    subject_type: 'storage_content',
    subject_key: key,
    retry_count: 0,
    max_attempts: 6,
    retry_of_task_id: null,
    retry_task_id: null,
    retryable: false,
    acknowledgeable: false,
    available_at: '2026-10-03T00:00:00Z',
    created_at: '2026-10-03T00:00:00Z',
    updated_at: '2026-10-03T00:02:13Z',
    started_at: '2026-10-03T00:00:00Z',
    finished_at: '2026-10-03T00:02:13Z',
  }
}

async function openTasks(page: Page, adminURL: string, rows = [task(201), task(200), task(199, '129')]) {
  await page.route('**/api/v1/tasks?*', (route) =>
    route.fulfill({
      json: route.request().url().includes('cursor=100')
        ? { tasks: [task(100)] }
        : {
            tasks: rows.filter(
              (row) => !row.acknowledged_at || new URL(route.request().url()).searchParams.get('scope') === 'history'
            ),
            next_cursor: 100,
          },
    })
  )
  await page.goto(adminURL)
  await page.getByLabel('Username').fill('admin')
  await page.getByLabel('Password').fill('system-test-admin-password')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
  await page.getByRole('link', { name: 'Tasks', exact: true }).click()
  await expect(page.getByRole('button', { name: 'Content #128', exact: true }).first()).toBeVisible()
}

async function pauseClock(page: Page) {
  await page.clock.pauseAt(await page.evaluate(() => Date.now() + 1000))
}

test('Open and Closed reset filters and pagination while preserving Retry and round navigation', async ({
  page,
  systemServer,
}) => {
  const work = task(201)
  work.status = 'failed'
  work.presentation_status = 'failed'
  const completed = task(200)
  const acknowledged = task(199)
  acknowledged.status = 'failed'
  acknowledged.presentation_status = 'failed'
  acknowledged.acknowledged_at = '2026-10-03T00:03:00Z'
  acknowledged.retryable = true
  acknowledged.retry_task_id = 199
  let retried = false
  const requests: URL[] = []
  await page.route('**/api/v1/tasks/acknowledge/preview*', (route) =>
    route.fulfill({ json: { count: 1, as_of: '2026-10-03T00:03:00Z' } })
  )
  await openTasks(page, systemServer.adminURL, [work])
  await page.route('**/api/v1/tasks?*', (route) => {
    const url = new URL(route.request().url())
    requests.push(url)
    const scope = url.searchParams.get('scope')
    const rows = scope === 'history' ? [completed, acknowledged] : [work]
    const status = url.searchParams.get('status')
    return route.fulfill({
      json: {
        tasks: rows.filter((row) => !status || row.status === status),
        next_cursor: scope === 'work' && !url.searchParams.has('cursor') ? 100 : undefined,
      },
    })
  })
  await page.getByRole('button', { name: 'Next', exact: true }).click()
  await expect.poll(() => requests.at(-1)?.searchParams.get('cursor')).toBe('100')
  await page.getByRole('combobox', { name: 'Status', exact: true }).click()
  await page.getByRole('option', { name: 'Failed', exact: true }).click()
  await expect(page.getByRole('button', { name: 'Acknowledge all', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Acknowledge all', exact: true }).click()
  const acknowledge = page.getByRole('alertdialog', { name: 'Acknowledge failed tasks' })
  await expect(acknowledge).toContainText('1 failed task will move to Closed.')
  await expect(acknowledge).toContainText('Their results and Retry availability remain unchanged.')
  await acknowledge.getByRole('button', { name: 'Cancel', exact: true }).click()
  await page.getByRole('tab', { name: 'Closed', exact: true }).click()
  await expect.poll(() => requests.at(-1)?.searchParams.get('scope')).toBe('history')
  expect(requests.at(-1)?.searchParams.has('status')).toBe(false)
  expect(requests.at(-1)?.searchParams.has('cursor')).toBe(false)
  await expect(page).toHaveURL(/scope=history/)
  await expect(page.getByRole('button', { name: 'Acknowledge all', exact: true })).toHaveCount(0)
  await expect(page.getByText('Completed', { exact: true })).toBeVisible()
  await expect(page.getByText('Acknowledged', { exact: true })).toBeVisible()
  await page.getByRole('combobox', { name: 'Status', exact: true }).click()
  await expect(page.getByRole('option', { name: 'Running', exact: true })).toHaveCount(0)
  await page.getByRole('option', { name: 'Failed', exact: true }).click()
  await expect(page.getByText('Completed', { exact: true })).toHaveCount(0)
  await page.route('**/api/v1/tasks/199/retry', (route) => {
    retried = true
    acknowledged.retryable = false
    acknowledged.retry_task_id = null
    acknowledged.superseded_at = '2026-10-03T00:04:00Z'
    return route.fulfill({ status: 202, json: { task_id: 202 } })
  })
  await page.getByRole('button', { name: 'Retry', exact: true }).click()
  await expect(page.getByRole('button', { name: 'Retry', exact: true })).toHaveCount(0)
  expect(retried).toBe(true)
  await page.route('**/api/v1/tasks/199', (route) => route.fulfill({ json: { task: acknowledged, policy: null } }))
  await page.route('**/api/v1/tasks/200', (route) => route.fulfill({ json: { task: completed, policy: null } }))
  for (const id of [199, 200]) {
    await page.route(`**/api/v1/tasks/${id}/history*`, (route) =>
      route.fulfill({ json: { tasks: [completed, acknowledged] } })
    )
    await page.route(`**/api/v1/tasks/${id}/events*`, (route) => route.fulfill({ json: { events: [] } }))
  }
  await page.getByRole('button', { name: 'Details for task 199', exact: true }).click()
  const details = page.getByRole('dialog')
  await details.getByRole('tab', { name: 'History', exact: true }).click()
  await details.getByRole('button', { name: 'Task 200', exact: true }).click()
  await expect(details.getByRole('heading', { name: 'Task 200', exact: true })).toBeVisible()
  await expect(details.getByText('Completed', { exact: true }).first()).toBeVisible()
})

test('task details keep historical errors separate and show the actual upload start', async ({
  page,
  systemServer,
}) => {
  const row = task(201)
  row.created_at = '2026-10-02T23:30:00Z'
  row.status_message = 'Checking storage transfer'
  row.last_error = 'Previous request failed'
  await openTasks(page, systemServer.adminURL, [row])
  const tableRow = page
    .getByRole('row')
    .filter({ has: page.getByRole('button', { name: 'Content #128', exact: true }) })
  await expect(tableRow.getByText('Checking storage transfer', { exact: true })).toBeVisible()
  await expect(tableRow.getByText('Previous request failed', { exact: true })).not.toBeVisible()
  await tableRow.getByText('Last error', { exact: true }).click()
  await expect(tableRow.getByText('Previous request failed', { exact: true })).toBeVisible()
  await tableRow.getByRole('cell').nth(4).locator('span').hover()
  await expect(page.getByRole('tooltip').getByText('Created', { exact: true })).toBeVisible()
  await expect(page.getByRole('tooltip').getByText('Upload started', { exact: true })).toBeVisible()
  await expect(tableRow.getByText('2m 13s', { exact: true })).toBeVisible()
  await tableRow.getByText('Last error', { exact: true }).click()
  row.status = 'running'
  row.presentation_status = 'running'
  row.started_at = undefined
  row.finished_at = undefined
  await page.getByRole('button', { name: 'Refresh', exact: true }).click()
  await expect(tableRow.getByText('Checking storage transfer', { exact: true })).toBeVisible()
  await expect(tableRow.getByText('Previous request failed', { exact: true })).not.toBeVisible()
  await expect(tableRow.getByText('2m 13s', { exact: true })).toHaveCount(0)
  row.status = 'failed'
  row.presentation_status = 'failed'
  row.started_at = '2026-10-03T00:00:00Z'
  row.finished_at = '2026-10-03T00:02:13Z'
  await page.getByRole('button', { name: 'Refresh', exact: true }).click()
  await expect(tableRow.getByText('Previous request failed', { exact: true })).toBeVisible()
  await expect(tableRow.getByText('Last error', { exact: true })).toHaveCount(0)
})

test('tasks without an actual start hide the start time and duration', async ({ page, systemServer }) => {
  const running = task(201)
  running.status = 'running'
  running.presentation_status = 'running'
  running.started_at = undefined
  running.finished_at = undefined
  const completed = task(200, '129')
  completed.started_at = undefined
  await openTasks(page, systemServer.adminURL, [running, completed])
  const rows = page.getByRole('row')
  for (const key of ['128', '129']) {
    const row = rows.filter({ has: page.getByRole('button', { name: `Content #${key}`, exact: true }) })
    await expect(row.getByRole('cell').nth(5)).toHaveText('—')
  }
  await rows
    .filter({ has: page.getByRole('button', { name: 'Content #128', exact: true }) })
    .getByRole('cell')
    .nth(4)
    .locator('span')
    .hover()
  await expect(page.getByRole('tooltip').getByText('Created', { exact: true })).toBeVisible()
  await expect(page.getByRole('tooltip').getByText('Upload started', { exact: true })).toHaveCount(0)
})

test('subject popovers close on the next mouse or keyboard click after dragging away', async ({
  page,
  systemServer,
}) => {
  await page.route('**/api/v1/task-subjects/**', (route) => route.fulfill({ json: subject }))
  await openTasks(page, systemServer.adminURL, [task(201)])
  const trigger = page.getByRole('button', { name: 'Content #128', exact: true })
  const heading = page.getByRole('heading', { name: 'Tasks', exact: true })
  for (const closeWith of ['mouse', 'keyboard'] as const) {
    await heading.hover()
    await trigger.hover()
    await expect(page.getByRole('tooltip').getByText(fileKey)).toBeVisible()
    // Raw mouse presses reuse the previous coordinates.
    await trigger.hover()
    await page.mouse.down()
    await expect(page.getByRole('dialog').getByText(fileKey)).toBeVisible()
    await heading.hover()
    await page.mouse.up()
    await expect(page.getByRole('dialog')).toBeVisible()
    if (closeWith === 'mouse') {
      await trigger.click()
    } else {
      await trigger.focus()
      await page.keyboard.press('Enter')
    }
    await expect(page.getByRole('dialog')).toHaveCount(0)
    await expect(page.getByRole('tooltip')).toHaveCount(0)
  }
})

test('task subjects load on demand and reuse data across overlays, refreshes, and pages', async ({
  page,
  systemServer,
}) => {
  await page.clock.install()
  const requests: string[] = []
  let failed = false
  await page.route('**/api/v1/task-subjects/**', async (route) => {
    const key = route.request().url().split('/').at(-1) ?? ''
    requests.push(key)
    if (key === '129' && !failed) {
      failed = true
      await route.fulfill({ status: 500, json: { error: 'internal' } })
    } else {
      await route.fulfill({
        json: {
          ...subject,
          subject_key: key,
          file: {
            ...subject.file,
            other_versions: key === '128' && requests.filter((value) => value === key).length > 1 ? 3 : 2,
          },
        },
      })
    }
  })
  await openTasks(page, systemServer.adminURL)
  expect(requests).toEqual([])
  await expect(page.getByText('2m 13s', { exact: true })).toHaveCount(3)
  await pauseClock(page)
  const first = page.getByRole('button', { name: 'Content #128', exact: true }).first()
  const second = page.getByRole('button', { name: 'Content #128', exact: true }).nth(1)
  await first.hover()
  await page.clock.runFor(199)
  await page.mouse.move(0, 0)
  expect(requests).toEqual([])
  await second.hover()
  await page.clock.runFor(199)
  await page.getByRole('button', { name: 'Content #129', exact: true }).hover()
  await page.clock.runFor(199)
  await page.mouse.move(0, 0)
  expect(requests).toEqual([])
  await first.hover()
  await page.clock.runFor(201)
  await page.clock.resume()
  await expect(page.getByRole('tooltip').getByText(fileKey)).toBeVisible()
  expect(requests).toEqual(['128'])
  await first.click()
  await expect(page.getByRole('dialog').getByText(fileKey)).toBeVisible()
  await expect(page.getByRole('tooltip')).toHaveCount(0)
  expect(requests).toEqual(['128'])
  await page.keyboard.press('Escape')
  await expect(page.getByRole('dialog')).toHaveCount(0)
  await page.clock.runFor(201)
  await expect(page.getByRole('tooltip')).toHaveCount(0)
  await second.click()
  await expect(page.getByRole('dialog').getByText('2 other linked versions', { exact: true })).toBeVisible()
  expect(requests).toEqual(['128'])
  await page.keyboard.press('Escape')
  await pauseClock(page)
  await page.getByRole('button', { name: 'Content #129', exact: true }).hover()
  await page.clock.runFor(199)
  await page.mouse.move(0, 0)
  expect(requests).toEqual(['128'])
  await page.clock.resume()
  const third = page.getByRole('button', { name: 'Content #129', exact: true })
  await third.focus()
  await expect(
    page.getByText('Unable to load information. Close and reopen to try again.', { exact: true })
  ).toBeVisible()
  expect(requests).toEqual(['128', '129'])
  await page.getByRole('button', { name: 'Refresh', exact: true }).focus()
  await third.focus()
  await expect(page.getByRole('tooltip').getByText(fileKey)).toBeVisible()
  expect(requests).toEqual(['128', '129', '129'])
  await page.getByRole('button', { name: 'Refresh', exact: true }).focus()
  await page.clock.runFor(60_001)
  expect(requests).toEqual(['128', '129', '129'])
  await first.click()
  await expect.poll(() => requests.length).toBe(4)
  await expect(page.getByRole('dialog').getByText('3 other linked versions', { exact: true })).toBeVisible()
  await page.keyboard.press('Escape')
  await page.getByRole('button', { name: 'Next', exact: true }).click()
  await expect(page.getByRole('button', { name: 'Content #128', exact: true })).toHaveCount(1)
  await page.getByRole('button', { name: 'Content #128', exact: true }).click()
  await expect(page.getByRole('dialog').getByText(fileKey)).toBeVisible()
  expect(requests.length).toBe(4)
  await page.keyboard.press('Escape')
  await page.getByRole('button', { name: 'Refresh', exact: true }).click()
  await page.clock.runFor(10_001)
  expect(requests.length).toBe(4)
  const row = await page.getByRole('row').nth(1).boundingBox()
  expect(row?.height).toBeLessThan(80)
})

test('an unfinished subject read survives overlay switching and cancels after its final viewer closes', async ({
  page,
  systemServer,
}) => {
  await page.clock.install()
  await page.addInitScript(() => {
    const state = { requests: 0, aborted: 0 }
    Object.assign(window, { taskSubjectFetchState: state })
    const original = window.fetch.bind(window)
    window.fetch = (input, init) => {
      if (!String(input).includes('/task-subjects/')) return original(input, init)
      state.requests++
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener(
          'abort',
          () => {
            state.aborted++
            reject(new DOMException('Aborted', 'AbortError'))
          },
          { once: true }
        )
      })
    }
  })
  const state = () =>
    page.evaluate(
      () =>
        (window as typeof window & { taskSubjectFetchState: { requests: number; aborted: number } })
          .taskSubjectFetchState
    )
  await openTasks(page, systemServer.adminURL, [
    task(205),
    task(204, '129'),
    task(203, '129'),
    task(202, '129'),
    task(201, '129'),
    task(200),
  ])
  await pauseClock(page)
  const first = page.getByRole('button', { name: 'Content #128', exact: true }).first()
  const second = page.getByRole('button', { name: 'Content #128', exact: true }).nth(1)
  await first.hover()
  await page.clock.runFor(201)
  await page.clock.resume()
  await expect(page.getByRole('tooltip').getByText('Loading…', { exact: true })).toBeVisible()
  await first.click()
  await expect(page.getByRole('dialog').getByText('Loading…', { exact: true })).toBeVisible()
  expect(await state()).toEqual({ requests: 1, aborted: 0 })
  await second.hover()
  await page.clock.runFor(201)
  await expect(page.getByRole('tooltip')).toHaveCount(1)
  await expect(page.getByRole('tooltip').getByText('Loading…', { exact: true })).toBeVisible()
  expect(await state()).toEqual({ requests: 1, aborted: 0 })
  await page.mouse.move(1000, 100, { steps: 5 })
  await expect(page.getByRole('tooltip')).toHaveCount(0)
  expect(await state()).toEqual({ requests: 1, aborted: 0 })
  await page.keyboard.press('Escape')
  await expect(page.getByRole('dialog')).toHaveCount(0)
  await expect.poll(state).toEqual({ requests: 1, aborted: 1 })
  await page.clock.runFor(201)
  expect(await state()).toEqual({ requests: 1, aborted: 1 })
  await first.click()
  await expect.poll(state).toEqual({ requests: 2, aborted: 1 })
  await page.getByRole('button', { name: 'Next', exact: true }).click()
  await expect.poll(state).toEqual({ requests: 2, aborted: 2 })
  const pagedSubject = page.getByRole('button', { name: 'Content #128', exact: true })
  await expect(pagedSubject).toHaveCount(1)
  await pagedSubject.click()
  await expect.poll(state).toEqual({ requests: 3, aborted: 2 })
  await page.getByRole('link', { name: 'Wallet', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Wallet', exact: true })).toBeVisible()
  await expect.poll(state).toEqual({ requests: 3, aborted: 3 })
})

test('touch opens the same subject information and acknowledgement preserves failure and duration', async ({
  browser,
  systemServer,
}) => {
  const context = await browser.newContext({ hasTouch: true })
  const page = await context.newPage()
  try {
    let requests = 0
    await page.route('**/api/v1/task-subjects/**', async (route) => {
      requests++
      await route.fulfill({ json: subject })
    })
    const row = task(201)
    await openTasks(page, systemServer.adminURL, [row])
    await page.getByRole('button', { name: 'Content #128', exact: true }).tap()
    await expect(page.getByRole('dialog').getByText(fileKey)).toBeVisible()
    expect(requests).toBe(1)
    await page.keyboard.press('Escape')
    row.status = 'pending'
    row.presentation_status = 'waiting'
    row.finished_at = undefined
    await page.getByRole('button', { name: 'Refresh', exact: true }).tap()
    await expect(page.getByText('2m 13s', { exact: true })).toHaveCount(0)
    row.status = 'failed'
    row.presentation_status = 'failed'
    row.finished_at = '2026-10-03T01:04:00Z'
    row.acknowledgeable = true
    await page.getByRole('button', { name: 'Refresh', exact: true }).tap()
    await expect(page.getByText('Failed', { exact: true })).toBeVisible()
    await expect(page.getByText('1h 4m', { exact: true })).toBeVisible()
    let acknowledgements = 0
    await page.route('**/api/v1/tasks/201/acknowledge', (route) => {
      expect(route.request().method()).toBe('POST')
      acknowledgements++
      row.acknowledged_at = '2026-10-04T00:00:00Z'
      row.acknowledgeable = false
      return route.fulfill({ json: { status: 'ok' } })
    })
    await page.getByRole('button', { name: 'Acknowledge', exact: true }).tap()
    await expect(page.getByText('1h 4m', { exact: true })).toHaveCount(0)
    await page.getByRole('tab', { name: 'Closed', exact: true }).click()
    await expect(page.getByText('Acknowledged', { exact: true })).toBeVisible()
    await expect(page.getByText('Failed', { exact: true })).toBeVisible()
    await expect(page.getByText('1h 4m', { exact: true })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Acknowledge', exact: true })).toHaveCount(0)
    expect(acknowledgements).toBe(1)
    expect(requests).toBe(1)
  } finally {
    await context.close()
  }
})
