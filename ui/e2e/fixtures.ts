import { type ChildProcessWithoutNullStreams, spawn } from 'node:child_process'
import path from 'node:path'
import { type Browser, test as base, expect } from '@playwright/test'

type SystemServer = {
  adminURL: string
  process: ChildProcessWithoutNullStreams
}

const serverBinary = path.resolve(process.cwd(), '../bin/synaps3-systemtest')
const processTimeout = 10_000

async function waitForExit(child: ChildProcessWithoutNullStreams, timeout: number) {
  if (child.exitCode !== null) return child.exitCode
  return await new Promise<number | null>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`systemtest server did not exit within ${timeout}ms`)), timeout)
    child.once('exit', (code) => {
      clearTimeout(timer)
      resolve(code)
    })
  })
}

async function stopProcess(child: ChildProcessWithoutNullStreams, timeout: number) {
  if (child.exitCode !== null) return { exitCode: child.exitCode, forced: false }
  child.kill('SIGTERM')
  try {
    return { exitCode: await waitForExit(child, timeout), forced: false }
  } catch (termError) {
    if (child.exitCode === null) child.kill('SIGKILL')
    try {
      return { exitCode: await waitForExit(child, timeout), forced: true }
    } catch (killError) {
      throw new Error(`systemtest server did not exit after SIGKILL: ${String(killError)}`, { cause: termError })
    }
  }
}

async function startServer() {
  const child = spawn(serverBinary, [], { stdio: ['ignore', 'pipe', 'pipe'] })
  child.stderr.on('data', (chunk) => {
    process.stderr.write(chunk)
  })
  try {
    const line = await new Promise<string>((resolve, reject) => {
      let stdout = ''
      const timer = setTimeout(
        () => reject(new Error('systemtest server did not publish its endpoint')),
        processTimeout
      )
      const fail = (error: Error) => {
        clearTimeout(timer)
        reject(error)
      }
      child.once('error', fail)
      child.once('exit', (code) => fail(new Error(`systemtest server exited with code ${code}`)))
      child.stdout.on('data', (chunk) => {
        stdout += chunk.toString()
        const newline = stdout.indexOf('\n')
        if (newline < 0) return
        clearTimeout(timer)
        resolve(stdout.slice(0, newline))
      })
    })
    const endpoint = JSON.parse(line) as { admin_url?: unknown }
    if (typeof endpoint.admin_url !== 'string' || !endpoint.admin_url.startsWith('http://127.0.0.1:')) {
      throw new Error(`systemtest server returned an invalid endpoint: ${line}`)
    }
    return { adminURL: endpoint.admin_url, process: child } satisfies SystemServer
  } catch (error) {
    await stopProcess(child, processTimeout).catch(() => undefined)
    throw error
  }
}

export const test = base.extend<object, { systemServer: SystemServer }>({
  browser: [
    async ({ playwright, browserName }, use) => {
      const attempts = process.env.CI ? 2 : 1
      let browser: Browser | undefined
      let lastError: unknown
      for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
          browser = await playwright[browserName].launch()
          break
        } catch (error) {
          lastError = error
        }
      }
      if (!browser) throw lastError
      try {
        await use(browser)
      } finally {
        await browser.close()
      }
    },
    { scope: 'worker' },
  ],
  systemServer: [
    async ({ playwright }, use) => {
      void playwright
      const server = await startServer()
      let cleanupError: Error | undefined
      try {
        await use(server)
      } finally {
        try {
          const { exitCode, forced } = await stopProcess(server.process, processTimeout)
          if (forced) cleanupError = new Error('systemtest server required SIGKILL during teardown')
          else if (exitCode !== 0) cleanupError = new Error(`systemtest server exited with code ${exitCode}`)
        } catch (error) {
          cleanupError = error instanceof Error ? error : new Error(String(error))
        }
      }
      if (cleanupError) throw cleanupError
    },
    { scope: 'worker' },
  ],
  page: async ({ page }, use, testInfo) => {
    const diagnostics: string[] = []
    page.on('console', (message) => diagnostics.push(`console ${message.type()}: ${message.text()}`))
    page.on('pageerror', (error) => diagnostics.push(`pageerror: ${error.message}`))
    page.on('requestfailed', (request) => {
      diagnostics.push(
        `requestfailed ${request.method()} ${new URL(request.url()).pathname}: ${request.failure()?.errorText}`
      )
    })
    page.on('response', (response) => {
      if (response.status() >= 400) {
        diagnostics.push(
          `response ${response.status()} ${response.request().method()} ${new URL(response.url()).pathname}`
        )
      }
    })
    await use(page)
    if (testInfo.status !== testInfo.expectedStatus && diagnostics.length > 0) {
      await testInfo.attach('browser-diagnostics', {
        body: Buffer.from(`${diagnostics.join('\n')}\n`),
        contentType: 'text/plain',
      })
    }
  },
})

export { expect }
