import { afterEach, describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import type { HistorySpan, UsedByNode } from '../composables/useHistory'
import { GIB } from '../test/factories'
import UsedCapacityChart from './UsedCapacityChart.vue'

const NOW = Date.UTC(2026, 9, 28, 12, 0, 0)

const used: UsedByNode = {
  times: [NOW - 180_000, NOW - 120_000, NOW - 10_000],
  series: [
    { name: 'node-a', values: [10 * GIB, 10 * GIB, 10 * GIB] },
    { name: 'node-b', values: [10 * GIB, 10 * GIB, 10 * GIB] },
  ],
}

function chart(over: { usedByNode?: UsedByNode; span?: HistorySpan; capacityBytes?: number } = {}) {
  return mount(UsedCapacityChart, {
    props: { usedByNode: used, span: '1h', capacityBytes: 0, now: NOW, ...over },
  })
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('UsedCapacityChart range buttons', () => {
  it('offers three real buttons in a labelled group', () => {
    const w = chart()
    const group = w.get('[role="group"][aria-label="Chart range"]')
    const buttons = group.findAll('button')
    expect(buttons.map((b) => b.text())).toEqual(['1 h', '6 h', '24 h'])
    for (const b of buttons) expect(b.attributes('type')).toBe('button')
  })

  it.each<[HistorySpan, string]>([
    ['1h', '1 h'],
    ['6h', '6 h'],
    ['24h', '24 h'],
  ])('marks only the %s button pressed', (span, label) => {
    const w = chart({ span })
    for (const b of w.findAll('button')) {
      expect(b.attributes('aria-pressed')).toBe(String(b.text() === label))
    }
  })

  it('emits update:span with the clicked value', async () => {
    const w = chart()
    const buttons = w.findAll('button')
    await buttons[1]?.trigger('click')
    await buttons[2]?.trigger('click')
    await buttons[0]?.trigger('click')
    expect(w.emitted('update:span')).toEqual([['6h'], ['24h'], ['1h']])
  })
})

describe('UsedCapacityChart plot', () => {
  it.each<[HistorySpan, string]>([
    ['1h', 'Used capacity per node over the last hour'],
    ['6h', 'Used capacity per node over the last 6 hours'],
    ['24h', 'Used capacity per node over the last 24 hours'],
  ])('names the chart for the %s span', (span, label) => {
    expect(chart({ span }).get('svg').attributes('aria-label')).toBe(label)
  })

  it('draws one area per node and one legend entry per series name', () => {
    const w = chart()
    expect(w.findAll('[data-part="area"]')).toHaveLength(2)
    expect(w.findAll('span.font-mono').map((s) => s.text())).toEqual(['node-a', 'node-b'])
  })

  it('pads a series that has fewer values than times', () => {
    const w = chart({ usedByNode: { times: used.times, series: [{ name: 'node-a', values: [GIB] }] } })
    expect(w.findAll('[data-part="area"]')).toHaveLength(1)
  })

  it('says it is collecting samples without data', () => {
    const w = chart({ usedByNode: { times: [], series: [] } })
    expect(w.get('svg').attributes('aria-label')).toContain('collecting samples')
  })

  it('labels the last bucket "now" and earlier ones with a clock time', () => {
    const labels = chart().findAll('[data-part="xlabel"]').map((l) => l.text())
    expect(labels).toHaveLength(3)
    expect(labels[0]).toMatch(/\d{2}:\d{2}/)
    expect(labels.at(-1)).toBe('now')
  })

  it('explains the sampling on the page', () => {
    expect(chart().text()).toContain('Sampled by this page every 5 s')
  })
})

describe('UsedCapacityChart limit line', () => {
  it('draws no line and no note when the capacity is unknown', () => {
    const w = chart({ capacityBytes: 0 })
    expect(w.find('[data-part="limit"]').exists()).toBe(false)
    expect(w.find('[data-part="limit-note"]').exists()).toBe(false)
  })

  it('draws the 80 % line when usage is within reach of it', () => {
    const w = chart({ capacityBytes: 25 * GIB })
    expect(w.find('[data-part="limit"]').exists()).toBe(true)
    expect(w.find('[data-part="limit-note"]').exists()).toBe(false)
    expect(w.get('svg').text()).toContain('80 % of 25 GiB')
  })

  it('counts a series with missing trailing values as zero when judging the scale', () => {
    // node-b has no value at the last time, so the peak is node-a alone there: 10 + 10 at most.
    const short: UsedByNode = {
      times: [NOW - 60_000, NOW],
      series: [
        { name: 'node-a', values: [10 * GIB, 10 * GIB] },
        { name: 'node-b', values: [10 * GIB] },
      ],
    }
    expect(chart({ usedByNode: short, capacityBytes: 25 * GIB }).find('[data-part="limit"]').exists()).toBe(true)
    expect(chart({ usedByNode: short, capacityBytes: 60 * GIB }).find('[data-part="limit"]').exists()).toBe(false)
  })

  it('moves the line into a note when usage is far below it', () => {
    const w = chart({ capacityBytes: 1000 * GIB })
    expect(w.find('[data-part="limit"]').exists()).toBe(false)
    expect(w.get('[data-part="limit-note"]').text()).toBe('80 % of 1000 GiB, far above the curves')
  })

  it('uses the note while there is nothing to scale against yet', () => {
    const w = chart({ usedByNode: { times: [], series: [] }, capacityBytes: 25 * GIB })
    expect(w.find('[data-part="limit"]').exists()).toBe(false)
    expect(w.find('[data-part="limit-note"]').exists()).toBe(true)
  })
})

describe('UsedCapacityChart sizing', () => {
  function stubObserver() {
    const state: { fire: (entries: { contentRect: { width: number } }[]) => void; disconnect: ReturnType<typeof vi.fn> } = {
      fire: () => undefined,
      disconnect: vi.fn(),
    }
    class FakeObserver {
      constructor(cb: ResizeObserverCallback) {
        state.fire = (entries) => {
          cb(entries as unknown as ResizeObserverEntry[], this as unknown as ResizeObserver)
        }
      }
      observe = vi.fn()
      unobserve = vi.fn()
      disconnect = state.disconnect
    }
    vi.stubGlobal('ResizeObserver', FakeObserver)
    return state
  }

  const viewBox = (w: ReturnType<typeof chart>) => w.get('svg').attributes('viewBox')

  it('keeps the default width where ResizeObserver does not exist', () => {
    const w = chart()
    expect(viewBox(w)).toBe('0 0 860 220')
    expect(() => {
      w.unmount()
    }).not.toThrow()
  })

  it('follows the measured width and uses a shorter plot on narrow screens', async () => {
    const obs = stubObserver()
    const w = chart()
    obs.fire([{ contentRect: { width: 400 } }])
    await w.vm.$nextTick()
    expect(viewBox(w)).toBe('0 0 400 160')
    obs.fire([{ contentRect: { width: 700 } }])
    await w.vm.$nextTick()
    expect(viewBox(w)).toBe('0 0 700 220')
  })

  it('never goes narrower than 240', async () => {
    const obs = stubObserver()
    const w = chart()
    obs.fire([{ contentRect: { width: 100 } }])
    await w.vm.$nextTick()
    expect(viewBox(w)).toBe('0 0 240 160')
  })

  it('ignores a zero width and an empty entry list', async () => {
    const obs = stubObserver()
    const w = chart()
    obs.fire([{ contentRect: { width: 0 } }])
    obs.fire([])
    await w.vm.$nextTick()
    expect(viewBox(w)).toBe('0 0 860 220')
  })

  it('disconnects on unmount', () => {
    const obs = stubObserver()
    const w = chart()
    w.unmount()
    expect(obs.disconnect).toHaveBeenCalledTimes(1)
  })
})
