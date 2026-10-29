import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import App from './App.vue'
import { fetchStatus, StatusUnreachableError } from './api/status'
import { clusterHealth, gc, node, status } from './test/factories'

vi.mock('./api/status', async (importOriginal) => ({
  ...(await importOriginal<typeof import('./api/status')>()),
  fetchStatus: vi.fn(),
}))

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)
const fetchMock = vi.mocked(fetchStatus)
const DOWN = new StatusUnreachableError()

let wrapper: VueWrapper | undefined

async function mountApp(): Promise<VueWrapper> {
  wrapper = mount(App)
  await flushPromises()
  return wrapper
}

async function advance(ms: number): Promise<void> {
  await vi.advanceTimersByTimeAsync(ms)
  await flushPromises()
}

// Panel headings carry a trailing count ("Issues3"), so match on the title only.
function headings(w: VueWrapper): string[] {
  return w.findAll('h2').map((h) => (h.text().match(/^[A-Za-z ]+/)?.[0] ?? '').trim())
}

function retryButtons(w: VueWrapper) {
  return w.findAll('button').filter((b) => b.text() === 'Retry')
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date'] })
  vi.setSystemTime(NOW)
  fetchMock.mockReset()
})

afterEach(() => {
  wrapper?.unmount()
  wrapper = undefined
  window.history.replaceState({}, '', '/')
  vi.useRealTimers()
  vi.unstubAllEnvs()
})

describe('App', () => {
  it('marks the page busy while the first answer is pending', async () => {
    fetchMock.mockReturnValue(new Promise(() => undefined))
    const w = await mountApp()
    expect(w.get('main').attributes('aria-busy')).toBe('true')
    expect(w.get('h1').text()).toContain('silo')
    expect(w.find('[role=alert]').exists()).toBe(false)
  })

  it('renders the bar and every section once data arrives', async () => {
    fetchMock.mockResolvedValue(status({ nodes: [node({ id: 'node-a' }), node({ id: 'node-b' })] }))
    const w = await mountApp()
    expect(w.get('main').attributes('aria-busy')).toBe('false')
    expect(w.get('h1').text()).toContain('silo')
    expect(w.get('h1').text()).toContain('node-a')
    expect(w.get('header').text()).toContain('Healthy')
    expect(w.get('header').text()).toContain('Updated')
    expect(w.find('section[aria-label=Summary]').exists()).toBe(true)
    expect(headings(w)).not.toContain('Issues')
    expect(w.find('[role=alert]').exists()).toBe(false)
    expect(w.find('[role=status]').exists()).toBe(false)
  })

  it('shows the health and garbage collection panels once data arrives', async () => {
    fetchMock.mockResolvedValue(
      status({ nodes: [node({ id: 'node-a' })], health: clusterHealth(), gc: gc({ lastRunAt: new Date(NOW - 3600_000) }) }),
    )
    const w = await mountApp()
    expect(headings(w)).toEqual(expect.arrayContaining(['Health', 'Garbage collection']))
    expect(w.get('[aria-labelledby=health-title]').text()).toContain('3 / 3 nodes alive')
    expect(w.get('[aria-labelledby=gc-title]').text()).toContain('1 h ago')
  })

  it('says not reported in the new panels for an older silod', async () => {
    fetchMock.mockResolvedValue(status())
    const w = await mountApp()
    expect(w.get('[aria-labelledby=health-title]').text()).toContain('not reported')
    expect(w.get('[aria-labelledby=gc-title]').text()).toContain('not reported')
  })

  it('shows skeletons in the new panels while loading', async () => {
    fetchMock.mockReturnValue(new Promise(() => undefined))
    const w = await mountApp()
    expect(headings(w)).toEqual(expect.arrayContaining(['Health', 'Garbage collection']))
    expect(w.find('[aria-labelledby=health-title] [aria-hidden="true"]').exists()).toBe(true)
    expect(w.find('[aria-labelledby=gc-title] [aria-hidden="true"]').exists()).toBe(true)
  })

  it('lists the issues when the cluster has findings', async () => {
    fetchMock.mockResolvedValue(status({ nodes: [node({ id: 'node-a' }), node({ id: 'node-b', state: 'dead' })] }))
    const w = await mountApp()
    expect(headings(w)).toContain('Issues')
    expect(w.text()).toContain('node-b down')
    expect(w.get('header').text()).toContain('Degraded')
  })

  it('refreshes on a schedule', async () => {
    fetchMock.mockResolvedValue(status())
    await mountApp()
    expect(fetchMock).toHaveBeenCalledTimes(1)
    await advance(5000)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('shows the error view with a countdown when the first load fails', async () => {
    fetchMock.mockRejectedValue(DOWN)
    const w = await mountApp()
    const alert = w.get('[role=alert]')
    expect(alert.get('h2').text()).toBe('No status from silod')
    expect(alert.text()).toContain('Could not reach silod at this address.')
    expect(alert.text()).toMatch(/next try in \d+ s/)
    expect(w.get('main').attributes('aria-busy')).toBe('false')
    expect(w.find('[role=status]').exists()).toBe(false)
  })

  it('retries immediately from the error view', async () => {
    fetchMock.mockRejectedValue(DOWN)
    const w = await mountApp()
    expect(fetchMock).toHaveBeenCalledTimes(1)
    await retryButtons(w)[0]?.trigger('click')
    await flushPromises()
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('says it is retrying while a request is in flight', async () => {
    fetchMock.mockRejectedValueOnce(DOWN).mockReturnValue(new Promise(() => undefined))
    const w = await mountApp()
    await retryButtons(w)[0]?.trigger('click')
    expect(w.get('[role=alert]').text()).toContain('retrying')
  })

  it('shows no countdown when paused after a failed first load', async () => {
    fetchMock.mockRejectedValue(DOWN)
    const w = await mountApp()
    await w.get('header button').trigger('click')
    const text = w.get('[role=alert]').text()
    expect(text).not.toContain('next try')
    expect(text).not.toContain('retrying')
  })

  it('recovers from the error view when silod comes back', async () => {
    fetchMock.mockRejectedValueOnce(DOWN).mockResolvedValue(status())
    const w = await mountApp()
    expect(w.find('[role=alert]').exists()).toBe(true)
    await advance(2000)
    expect(w.find('[role=alert]').exists()).toBe(false)
    expect(w.get('header').text()).toContain('Healthy')
  })

  describe('when requests start failing after data was shown', () => {
    async function withDataThenFailures(): Promise<VueWrapper> {
      fetchMock.mockResolvedValueOnce(status()).mockRejectedValue(DOWN)
      return mountApp()
    }

    it('keeps the numbers without a banner for the first 30 seconds', async () => {
      const w = await withDataThenFailures()
      await advance(20000)
      expect(fetchMock.mock.calls.length).toBeGreaterThan(1)
      expect(w.find('[role=status]').exists()).toBe(false)
      expect(w.find('[role=alert]').exists()).toBe(false)
      expect(w.get('header').text()).toContain('Healthy')
    })

    it('warns that the numbers are old after 30 seconds', async () => {
      const w = await withDataThenFailures()
      await advance(31000)
      const banner = w.get('[role=status]')
      expect(banner.text()).toMatch(/Stale, 3\d s ago/)
      expect(banner.text()).toContain('Could not reach silod at this address.')
      expect(banner.text()).toMatch(/retry in \d+ s/)
      expect(w.find('[role=alert]').exists()).toBe(false)
      expect(w.get('header').text()).toContain('Healthy')
    })

    it('retries from the banner', async () => {
      const w = await withDataThenFailures()
      await advance(31000)
      const before = fetchMock.mock.calls.length
      await w.get('[role=status] button').trigger('click')
      await flushPromises()
      expect(fetchMock).toHaveBeenCalledTimes(before + 1)
    })

    it('drops the banner once a request succeeds again', async () => {
      let up = true
      fetchMock.mockImplementation(() => (up ? Promise.resolve(status()) : Promise.reject(DOWN)))
      const w = await mountApp()
      up = false
      await advance(31000)
      expect(w.find('[role=status]').exists()).toBe(true)
      up = true
      await w.get('[role=status] button').trigger('click')
      await flushPromises()
      expect(w.find('[role=status]').exists()).toBe(false)
      expect(w.get('header').text()).toContain('Updated')
    })

    it('pauses fetching and hides the banner when paused', async () => {
      const w = await withDataThenFailures()
      await advance(31000)
      expect(w.find('[role=status]').exists()).toBe(true)

      const toggle = w.get('header button')
      expect(toggle.text()).toContain('Pause')
      expect(toggle.attributes('aria-pressed')).toBe('false')
      await toggle.trigger('click')
      expect(toggle.text()).toContain('Resume')
      expect(toggle.attributes('aria-pressed')).toBe('true')
      expect(w.find('[role=status]').exists()).toBe(false)
      expect(w.get('header').text()).toContain('Paused')

      const frozen = fetchMock.mock.calls.length
      await advance(120000)
      expect(fetchMock).toHaveBeenCalledTimes(frozen)

      await toggle.trigger('click')
      await flushPromises()
      expect(fetchMock.mock.calls.length).toBeGreaterThan(frozen)
    })
  })

  describe('mock mode', () => {
    it('serves the chosen scenario without calling the real endpoint', async () => {
      await import('./api/mock')
      window.history.replaceState({}, '', '/?mock=attention')
      const w = await mountApp()
      expect(w.get('main').attributes('aria-busy')).toBe('true')
      await advance(400)
      expect(fetchMock).not.toHaveBeenCalled()
      expect(w.get('main').attributes('aria-busy')).toBe('false')
      expect(w.get('header').text()).toContain('Warning')
      expect(headings(w)).toContain('Issues')
    })

    it('ignores the scenario in a production build', async () => {
      vi.stubEnv('DEV', false)
      vi.stubEnv('VITE_MOCK', '')
      window.history.replaceState({}, '', '/?mock=attention')
      fetchMock.mockResolvedValue(status())
      const w = await mountApp()
      expect(fetchMock).toHaveBeenCalledTimes(1)
      expect(w.get('header').text()).toContain('Healthy')
    })

    it('serves mock data in a build that opted in with VITE_MOCK', async () => {
      await import('./api/mock')
      vi.stubEnv('DEV', false)
      vi.stubEnv('VITE_MOCK', '1')
      const w = await mountApp()
      await advance(400)
      expect(fetchMock).not.toHaveBeenCalled()
      expect(w.get('header').text()).toContain('Warning')
    })
  })
})
